
# SlimCommonIo

<a href="https://codeberg.org/greergan/SlimTS">
  <img src="https://raw.githubusercontent.com/greergan/SlimTS/master/assets/slimts_logo.png" width="75" alt="SlimTS Logo">
</a>

A low-level, Linux-native async I/O runtime built directly on `io_uring`.  
Provides coroutine-based I/O operations, a cooperative task scheduler with cross-thread posting, per-operation kernel-enforced timeouts, and a thin ring-management layer with no external userspace dependencies.  
Part of the [SlimCommon](https://codeberg.org/greergan/SlimCommon) library.  
Built using [SlimLibraryPackager](https://codeberg.org/greergan/SlimLibraryPackager).  
CI/CD supplied by unified workflows provided by [SlimLibraryPackager](https://codeberg.org/greergan/SlimLibraryPackager).

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Default Values](#default-values)
- [Core API](#core-api)
  - [IO struct](#io-struct)
  - [Constructors and object lifetime](#constructors-and-object-lifetime)
  - [Task](#task)
  - [Awaitable](#awaitable)
  - [Operations](#operations)
  - [Scheduler](#scheduler)
  - [Runtime](#runtime)
- [Error Model](#error-model)
- [Building](#building)
- [Dependencies](#dependencies)
- [Examples](#examples)

## Overview

This library provides a coroutine-native async I/O layer backed directly by `io_uring` via raw `SYS_io_uring_setup` / `SYS_io_uring_enter` syscalls:

- Manual SQ and CQ ring setup with `mmap`, including `IORING_FEAT_SINGLE_MMAP` optimization
- A C++20 coroutine `Task<T>` type unified across value and `void` returns via a `ValueStore<T>` base
- An `Awaitable` base that hooks directly into the coroutine awaiter protocol, stages SQEs for batched submission, and supports an opt-in kernel-enforced deadline per operation via `IOSQE_IO_LINK`
- Nine concrete async operations: `Accept`, `Close`, `Connect`, `Open`, `Poll`, `Read`, `Recv`, `Send`, `Stat`, `Write`
- A cooperative `Scheduler` that owns the `IO` ring reference, drives the event loop, submits staged SQEs in batch, blocks on an `epoll` instance watching both the `io_uring` fd and an `eventfd` wakeup channel, reaps completed tasks, and supports cross-thread task posting via an `eventfd`-based inbox
- A `Runtime` that owns one dispatcher scheduler plus N worker schedulers, each on its own thread and ring, with round-robin job dispatch
- An `ErrorStatus` enum and `IOException` for constructor-time and spawn-time failures; all I/O operations return raw `int` results following Linux errno convention

[↑ Top](#table-of-contents)

## Features

| Feature | Description |
|---------|-------------|
| Raw `io_uring` | Ring setup, SQE submission, and CQE reaping done manually — no liburing dependency |
| Single-mmap optimization | CQ ring reuses the SQ `mmap` allocation when `IORING_FEAT_SINGLE_MMAP` is available |
| Coroutine tasks | `Task<T>` implements the C++20 coroutine promise protocol via a `ValueStore<T>` base; always suspends at start; handles both value and `void` returns in a single template |
| Composable awaitables | `Awaitable` base implements `await_ready` / `await_suspend` / `await_resume`; subclasses only override `prepare()`. Constructed from a `Scheduler&` — callers never need to hold or pass an `IO&` themselves |
| Kernel-enforced per-op timeout | Any `Awaitable`-derived operation can call `with_timeout(std::chrono::milliseconds)` before being `co_await`ed. This links the operation's SQE to a kernel-side `IORING_OP_LINK_TIMEOUT` SQE via `IOSQE_IO_LINK` — if the deadline passes first, the kernel cancels the operation for us automatically. No manual `IORING_OP_ASYNC_CANCEL`, no extra coroutine-visible state: on timeout the operation's own CQE still arrives normally with `result == -ECANCELED` (or `-ETIME`), the same `int` failure contract as every other error path |
| Two-phase SQE reservation | Acquiring ring slots is split into a capacity check (`has_capacity(count)`) and a commit step (`commit_one()`), reserving all slots an operation needs — one, or two when linked to a timeout — atomically. This prevents an orphaned `IOSQE_IO_LINK` SQE from being committed alone on a ring with no room left for its timeout companion |
| SQ backpressure | `Scheduler::spawn()` checks SQ capacity before calling `resume()`. If already full, it throws `IOException(ErrorStatus::SQFull)` immediately, before any user code in the task runs. A second check via a thread-local side-channel catches the case where the SQ fills mid-coroutine inside `await_suspend` — including when a linked timeout operation needs two slots and only one is free |
| Batched submission | SQEs are staged in `await_suspend` and flushed to the kernel in a single `io_uring_enter` call at the start of each `drain()` tick |
| Epoll-based blocking drain | When the CQ is empty after submission, `drain()` blocks in `epoll_wait` on an epoll instance that watches both the `io_uring` fd (CQEs) and the `eventfd` (new `post()`ed work). This prevents the scheduler thread from hanging if a `post()` arrives while the ring is completely idle |
| Cross-thread posting | `Scheduler::post(std::function<void()>)` is thread-safe; any thread (including a V8 isolate thread) can queue work onto the scheduler. An `eventfd` is written to wake the blocked event loop. If `spawn()` throws `SQFull` inside the inbox drain, the callable is re-queued and the `eventfd` is poked again so it retries on the next drain cycle |
| Cooperative scheduler | `Scheduler::run()` loops drain → reap until a `std::stop_token` fires; a `std::stop_callback` writes to the `eventfd` to break out of any blocked `epoll_wait`. `shutdown()` flushes all pending tasks |
| Encapsulated IO ring | `Scheduler` is the sole owner-facing handle to the `IO` ring. Operations and jobs take a `Scheduler&` and reach the ring internally via `Scheduler::io()`; nothing outside `Scheduler`/`Awaitable` construction needs to see `IO` directly |
| Runtime | `Runtime` owns a dispatcher IO/Scheduler/thread plus N worker IO/Scheduler/thread units. `Runtime::post()` enqueues a job onto the dispatcher, which round-robins it to a worker's `post()`/`spawn()`. Worker jobs receive only the worker's `Scheduler&`, never its `IO&` |
| Linux-only | Requires Linux 5.1+ (`io_uring`) and `linux/io_uring.h` kernel headers; no platform abstraction layer |

[↑ Top](#table-of-contents)

## Default Values

| Field / Parameter | Default | Notes |
|-------------------|---------|-------|
| `IO::IO(entries)` | `256` | SQ ring depth; CQ is always twice this size to prevent overflow |
| `IO::fd` | `-1` | Set to the live `io_uring` fd on successful construction |
| `Read::offset` | `0` | Byte offset into the file; pass explicitly for positioned reads. Length is derived from the `std::span<uint32_t>` buffer — no separate size parameter |
| `Write::offset` | `0` | Byte offset into the file; pass explicitly for positioned writes. Length is derived from the `std::span<const uint32_t>` buffer — no separate size parameter |
| `Open::mode` | `0644` | File creation mode; ignored when flags do not include `O_CREAT` |
| `Open::dfd` | `AT_FDCWD` | Directory fd for relative path resolution |
| `Stat::dfd` | `AT_FDCWD` | Directory fd for relative path resolution |
| `Stat::flags` | `0` | `statx` flags; `0` follows symlinks |
| `Stat::mask` | `STATX_BASIC_STATS` | Fields requested from the kernel |
| `Recv::flags` | `0` | `MSG_*` flags passed to the underlying `recv` |
| `Send::flags` | `0` | `MSG_*` flags passed to the underlying `send` |
| `Accept` socket flags | `SOCK_NONBLOCK \| SOCK_CLOEXEC` | Always applied; not overridable via the constructor |
| `Poll::poll_mask` | — (required) | Same bitmask as `poll(2)` — `POLLIN`, `POLLOUT`, etc. Result on success is the ready-mask (`revents`-equivalent) |
| `Connect::addr` | — (required) | Copied into internal storage at construction; caller's original `sockaddr` may safely go out of scope before the coroutine resumes |
| `Awaitable::result` | `0` | Overwritten by the CQE result on completion |
| `Awaitable` timeout | none | Opt-in only — call `with_timeout(ms)` before `co_await` to enable |
| `Runtime(worker_count, entries)` | entries=`256` | Each worker and the dispatcher use the same ring depth |

[↑ Top](#table-of-contents)

## Core API

### IO struct

```cpp
slim::common::IO io;
slim::common::IO io(512); // custom ring depth
```

`IO` is the root resource. It owns the `io_uring` file descriptor and all `mmap` regions for the SQ ring, SQ entries array, and CQ ring. A `Scheduler` holds the reference to it; operations and jobs never need to hold or pass an `IO&` of their own — they go through a `Scheduler&` instead.

`IO` is non-copyable. Move is not defined — construct one instance per thread or io context and pass it by reference (typically only to a `Scheduler` constructor).

| Member | Type | Description |
|--------|------|-------------|
| `fd` | `int` | The `io_uring` file descriptor |
| `sq` | `IO::Sq` | SQ ring: head, tail, flags, mask, entries, dropped, array, and SQE array pointers |
| `cq` | `IO::Cq` | CQ ring: head, tail, mask, entries, and CQE array pointers |

[↑ Top](#table-of-contents)

### Constructors and object lifetime

| Form | Description |
|------|-------------|
| `IO(uint32_t entries = 256)` | Sets up the `io_uring` instance. CQ is provisioned at `entries * 2`. Throws `IOException` on zero entries, `io_uring_setup` failure, or any `mmap` failure; see [Error Model](#error-model) |
| `~IO()` | Unmaps all rings (skips the CQ unmap when `IORING_FEAT_SINGLE_MMAP` was used), unmaps the SQEs array, and closes `fd` |
| Copy | Deleted |

[↑ Top](#table-of-contents)

### Task

```cpp
slim::common::io::Task<int>  t;   // value-returning task
slim::common::io::Task<void> t;   // fire-and-forget task
```

`Task<T>` is the coroutine return type used throughout the library. It is a single unified template: value storage and the `return_value` / `return_void` promise methods are provided by a `ValueStore<T>` base, which is explicitly specialised for `void` to avoid the ill-formed `void v` parameter that arises from naive `requires`-guarded approaches. The coroutine always suspends at the initial suspend point — the body does not begin running until `resume()` is called or the task is handed to `Scheduler::spawn()`.

`Task<T>` is non-copyable. It is move-only and destroys its coroutine handle on destruction.

| Method | Returns | Description |
|--------|---------|-------------|
| `resume()` | `bool` | Resumes the coroutine one step. Returns `false` if already done |
| `done()` | `bool` | Returns `true` if the coroutine has completed |
| `result()` | `T` | Returns the coroutine's return value, or rethrows a stored exception |
| `handle()` | `std::coroutine_handle<Promise>` | Raw handle; used internally by `Scheduler::spawn()` |

[↑ Top](#table-of-contents)

### Awaitable

```cpp
// Not constructed directly — subclassed by each operation
struct MyOp : slim::common::io::Awaitable {
    MyOp(Scheduler& scheduler) : Awaitable(scheduler) {}
protected:
    void prepare(io_uring_sqe* sqe) noexcept override { /* fill sqe */ }
};
```

`Awaitable` implements the C++20 awaiter protocol and handles all SQE acquisition boilerplate. It's constructed from a `Scheduler&` and fetches the `IO&` it needs from `Scheduler::io()` once, at construction — subclasses and callers never handle `IO` directly. Subclasses only override `prepare()` to fill in the opcode and operation-specific fields. SQEs are staged in the ring on suspension and flushed to the kernel in batch by `Scheduler::drain()`.

Any operation can opt into a kernel-enforced deadline by calling `with_timeout()` before `co_await`ing it:

```cpp
Recv recv_op{sched, fd, buf, sizeof(buf)};
recv_op.with_timeout(std::chrono::milliseconds(500));
int n = co_await recv_op; // -ECANCELED (or -ETIME) if 500ms passes with nothing received
```

Internally this reserves two ring slots instead of one — the operation's own SQE (flagged `IOSQE_IO_LINK`) plus a companion `IORING_OP_LINK_TIMEOUT` SQE with `user_data` left at zero. The kernel guarantees that if the timeout fires first, the linked operation is cancelled and its CQE still arrives normally, just with a cancellation result. `Scheduler::drain()` already ignores any CQE whose `user_data` is null, so the timeout companion's own completion requires no special handling.

| Method | Description |
|--------|-------------|
| `await_ready()` | Always returns `false` — every operation suspends the coroutine |
| `await_suspend(handle)` | Reserves one ring slot (or two, if `with_timeout()` was called) via a capacity check followed by commit. Calls `prepare()` on the primary slot, sets its `user_data` to `this`, and — if timed — flags it `IOSQE_IO_LINK` and fills the companion `IORING_OP_LINK_TIMEOUT` slot. If the required slots aren't all available, sets a thread-local error flag and returns `false` to resume the coroutine immediately without suspending; `Scheduler::spawn()` detects this flag after `resume()` returns and throws `IOException(ErrorStatus::SQFull)` |
| `await_resume()` | Returns `result` as `int` |
| `with_timeout(std::chrono::milliseconds)` | Opt-in. Must be called before `co_await`ing the object. Enables the linked-timeout behavior described above |
| `prepare(sqe)` | Pure virtual. Called with a zeroed SQE slot; subclass fills opcode and fields |

[↑ Top](#table-of-contents)

### Operations

All operations live in `slim::common::io` and derive from `Awaitable`. Each constructor takes a `Scheduler&` — not an `IO&` — so operations can be constructed anywhere a `Scheduler&` is in scope, including inside jobs dispatched to worker threads that never see the underlying `IO` ring. `Read` and `Write` take a `std::span` over `uint32_t` storage rather than a separate `(void*, size_t)` pair — the buffer supplies its own length, so a `std::vector<uint32_t>` (or any contiguous `uint32_t` container) can be passed directly with no explicit size argument. All operations return `int` when `co_await`ed: a non-negative value on success (semantics match the underlying syscall — `Read`/`Write` yield the raw byte count from the kernel, not an element count), or a negative errno on failure. Every operation can additionally opt into a kernel-enforced deadline via `with_timeout()` (see [Awaitable](#awaitable)).

| Operation | Constructor | `io_uring` opcode | Notes |
|-----------|-------------|-------------------|-------|
| `Accept` | `Accept(Scheduler&, int server_fd)` | `IORING_OP_ACCEPT` | Yields the accepted client fd. Socket created with `SOCK_NONBLOCK \| SOCK_CLOEXEC`. Peer address available via `addr` / `addr_len` members |
| `Close` | `Close(Scheduler&, int fd)` | `IORING_OP_CLOSE` | Async fd close |
| `Connect` | `Connect(Scheduler&, int fd, const sockaddr* addr, socklen_t addr_len)` | `IORING_OP_CONNECT` | Yields `0` on success, negative errno on failure. `addr` is copied into internal storage at construction — the caller's original `sockaddr` may go out of scope before the coroutine resumes |
| `Open` | `Open(Scheduler&, const char* path, int flags, mode_t mode = 0644, int dfd = AT_FDCWD)` | `IORING_OP_OPENAT` | Yields the opened fd on success |
| `Poll` | `Poll(Scheduler&, int fd, uint32_t poll_mask)` | `IORING_OP_POLL_ADD` | Yields the ready-mask (`revents`-equivalent, e.g. `POLLIN`) on success, negative errno on failure. Useful for driving readiness-based protocols (e.g. a TLS handshake retry loop on `SSL_ERROR_WANT_READ`/`WANT_WRITE`) without a dedicated read/write op |
| `Read` | `Read(Scheduler&, int fd, std::span<uint32_t> buf, uint64_t offset = 0)` | `IORING_OP_READ` | Yields bytes read (raw kernel byte count, not divided by `sizeof(uint32_t)`) |
| `Recv` | `Recv(Scheduler&, int fd, void* buf, size_t len, int flags = 0)` | `IORING_OP_RECV` | Yields bytes received |
| `Send` | `Send(Scheduler&, int fd, const void* buf, size_t len, int flags = 0)` | `IORING_OP_SEND` | Yields bytes sent |
| `Stat` | `Stat(Scheduler&, const char* path, int flags = 0, uint32_t mask = STATX_BASIC_STATS, int dfd = AT_FDCWD)` | `IORING_OP_STATX` | Result in `buf` member (`struct statx`) on success |
| `Write` | `Write(Scheduler&, int fd, std::span<const uint32_t> buf, uint64_t offset = 0)` | `IORING_OP_WRITE` | Yields bytes written (raw kernel byte count, not divided by `sizeof(uint32_t)`) |

[↑ Top](#table-of-contents)

### Scheduler

```cpp
slim::common::IO io;
slim::common::io::Scheduler sched(io);
```

`Scheduler` drives the coroutine event loop and is the sole owner-facing handle to the `IO` ring. It holds a `std::vector<Task<void>>` of live tasks, an `IO&` reference, an `eventfd` for cross-thread wakeup, and an `epoll` instance that watches both the `eventfd` and the `io_uring` fd. It is non-copyable.

| Method | Description |
|--------|-------------|
| `spawn(Task<T>&&)` | Checks SQ capacity before calling `resume()`. If the SQ is already full, throws `IOException(ErrorStatus::SQFull)` immediately, before any user code in the task runs. Otherwise, resumes the task to its first suspension point and takes ownership. Also throws `IOException(ErrorStatus::SQFull)` if `await_suspend` found the SQ full during that first resume (including a linked-timeout op needing two slots with only one free), or `IOException(ErrorStatus::BadAllocation)` if the internal task vector cannot grow. **Do not pass an inline temporary lambda directly** — bind it to a named variable first to avoid GCC coroutine UB (see note below) |
| `post(std::function<void()>)` | Thread-safe. Enqueues a callable onto the scheduler's inbox and writes to the `eventfd` to wake the event loop. Safe to call from any thread including a V8 isolate thread. If the callable throws `SQFull` inside the inbox drain, it is re-queued and the `eventfd` is poked again for a later retry |
| `run(std::stop_token)` | Loops `drain()` → `reap()` until the stop token is signalled. A `std::stop_callback` writes to the `eventfd` to break out of any blocked `epoll_wait` immediately |
| `shutdown()` | Flushes all remaining tasks by looping `drain()` → `reap()` until the task list is empty. Called automatically by the destructor if tasks remain |
| `eventfd()` | Returns the scheduler's `eventfd` file descriptor. Exposed for advanced use cases |
| `io()` | Returns the `IO&` the scheduler owns. Used internally by `Awaitable`'s constructor; call sites building any operation should pass the `Scheduler&` itself rather than reaching for this |

**`drain()`** — first attempts a non-blocking read of the `eventfd`; if signalled, drains the inbox. Then submits all staged SQEs via a single `io_uring_enter` call. Then processes all available CQEs, resuming the associated coroutine for each and advancing the CQ head — CQEs with a null `user_data` (the silent companion half of a linked timeout pair) are skipped rather than dereferenced. If the CQ is still empty after submission and the scheduler is not shutting down, it blocks in `epoll_wait(-1)` on an epoll instance watching both the `io_uring` fd and the `eventfd`, so that either a new CQE or a new `post()` will wake it. Returns after the wait without processing — the next `drain()` call from `run()`'s loop handles the new event.

**`reap()`** — sweeps `tasks_` and erases completed entries. Currently O(n); an intrusive list is the recommended optimization for high-IOPS paths.

**`spawn()` lambda lifetime note:** GCC may elide the closure copy into the coroutine frame when a lambda is passed as a prvalue, leaving the frame with a dangling reference after the temporary is destroyed. Always name the lambda before calling it:

```cpp
auto coro = [&]() -> Task<void> { /* ... */ };
sched.spawn(coro());  // correct — named variable
// sched.spawn([&]() -> Task<void> { ... }()); // UB — temporary lambda
```

[↑ Top](#table-of-contents)

### Runtime

```cpp
slim::common::io::Runtime rt(4);   // 4 worker threads, 256-entry rings
rt.start();
rt.post([](Scheduler& sched, size_t idx) {
    // runs on worker idx's thread — sched is all that's needed here
});
rt.stop();
```

`Runtime` owns one dispatcher IO/Scheduler/thread and N worker IO/Scheduler/thread units. Worker threads start immediately on construction. The dispatcher thread starts on `start()`. `Runtime` is non-copyable and non-movable.

| Method | Description |
|--------|-------------|
| `Runtime(size_t worker_count, uint32_t entries = 256)` | Constructs the dispatcher ring/scheduler and all worker `WorkerNode`s. Worker threads start immediately. Throws `IOException` on any ring or scheduler construction failure |
| `start()` | Starts the dispatcher thread. Throws `IOException(ErrorStatus::RuntimeNotIdle)` if called more than once or after `stop()` |
| `stop()` | Signals the dispatcher and all workers to stop, joins their threads, and calls `shutdown()` on every scheduler. Safe to call more than once |
| `post(job)` | Thread-safe. Posts a job to the dispatcher, which round-robins it to a worker's `post()`/`spawn()`. `job` receives `(Scheduler& worker_scheduler, size_t worker_idx)` — the worker's `IO` ring is not exposed; construct operations directly from `worker_scheduler` |
| `dispatcher_scheduler()` | Returns a reference to the dispatcher's `Scheduler`. **Note:** `spawn()` is not thread-safe against that scheduler's own driving thread — code running on any *other* thread must reach the dispatcher via `dispatcher_scheduler().post(...)`, never `dispatcher_scheduler().spawn(...)` directly |
| `worker(size_t idx)` | Returns a reference to the `WorkerNode` at `idx` |
| `worker_count()` | Returns the number of worker nodes |

**`WorkerNode`** — each node owns its `IO` ring, its `Scheduler`, a `std::stop_source`, and a `std::jthread` that drives the scheduler's event loop. The `IO` ring is an implementation detail of the node; jobs dispatched via `Runtime::post()` only ever see the node's `Scheduler&`. `stop_and_join()` requests stop, joins the thread, and calls `scheduler.shutdown()`.

[↑ Top](#table-of-contents)

## Error Model

Constructor failures on `IO` and `Scheduler`, spawn-time failures on `Scheduler`, and lifecycle failures on `Runtime` are reported via `IOException`, a `std::runtime_error` subclass that carries an `ErrorStatus`.

| `ErrorStatus` | String | Meaning |
|---------------|--------|---------|
| `OK` | `"OK"` | No error |
| `BadAllocation` | `"Bad allocation"` | `Scheduler::spawn()` could not grow the internal task vector |
| `EpollCreateFailed` | `"epoll create failed"` | `Scheduler` could not create its epoll instance |
| `EpollCtlFailed` | `"epoll_ctl failed"` | `Scheduler` could not register a file descriptor with epoll |
| `EventFdCreateFailed` | `"eventfd create failed"` | `Scheduler` could not create its wakeup `eventfd` |
| `EventFdReadFailed` | `"eventfd read failed"` | Unexpected error reading the wakeup `eventfd` |
| `EventFdWriteFailed` | `"eventfd write failed"` | `Scheduler::post()` could not write to the wakeup `eventfd` |
| `IOInvalidEntries` | `"IO invalid entries"` | `IO` constructed with zero entries |
| `IOMmapCqFailed` | `"IO mmap CQ ring failed"` | CQ ring `mmap` failed (legacy kernels without `IORING_FEAT_SINGLE_MMAP`) |
| `IOMmapSqesFailed` | `"IO mmap SQEs failed"` | SQEs array `mmap` failed |
| `IOMmapSqFailed` | `"IO mmap SQ ring failed"` | SQ ring `mmap` failed |
| `IOSetupFailed` | `"IO setup failed"` | `SYS_io_uring_setup` returned a negative fd |
| `RuntimeNotIdle` | `"Runtime not idle"` | `Runtime::start()` called when the runtime is not in the idle state |
| `SQFull` | `"Submission queue full"` | `Scheduler::spawn()` was called while every SQ slot was occupied, or an operation using `with_timeout()` needed two slots with only one available |

`IOException::status()` returns the `ErrorStatus` value. `what()` returns the corresponding string from `error_status_str[]`.

I/O operations (all `Awaitable` subclasses) do **not** throw — they return a negative errno as an `int` result from `co_await`, following the Linux syscall convention. This includes cancellation via a linked `with_timeout()` deadline: a timed-out operation resolves with `result == -ECANCELED` (or `-ETIME`), not an exception.

[↑ Top](#table-of-contents)

## Building

This library is built using [SlimLibraryPackager](https://codeberg.org/greergan/SlimLibraryPackager). See that repository for build instructions.

Requires Linux 5.1+ and kernel headers providing `linux/io_uring.h`.

[↑ Top](#table-of-contents)

## Dependencies

### required_packages

External package dependencies for this library are declared in the [`required_packages`](required_packages) file at the repository root. This file is read by [SlimLibraryPackager](https://codeberg.org/greergan/SlimLibraryPackager) during the build process to resolve dependencies and install them if not present.
```
none
```
[↑ Top](#table-of-contents)

## Examples

```cpp
// Construct a Runtime — 4 worker threads, 256-entry rings.
// Runtime owns every IO ring internally (one per worker, one for the
// dispatcher); nothing outside Runtime/Scheduler ever touches IO directly.
slim::common::io::Runtime rt(4);
rt.start();
```

```cpp
// Open/write/read/close a file inside a job dispatched to a worker.
// Read/Write take their buffer as a std::span<uint32_t>
// (std::span<const uint32_t> for Write), so a std::vector<uint32_t> can be
// passed with no separate size argument — the buffer's own size() supplies
// the length.
rt.post([](slim::common::io::Scheduler& sched, size_t idx) {
    auto job = [&sched]() -> slim::common::io::Task<void> {
        slim::common::io::Open open(sched, "/tmp/example.dat", O_CREAT | O_WRONLY | O_TRUNC);
        int fd = co_await open;
        if (fd < 0) co_return;

        std::vector<uint32_t> write_buf{0x11111111, 0x22222222, 0x33333333};
        slim::common::io::Write write(sched, fd, write_buf); // 12 bytes, no explicit length
        int written = co_await write;                        // raw byte count from the kernel

        slim::common::io::Close close(sched, fd);
        co_await close;

        int reopen_fd = co_await slim::common::io::Open(sched, "/tmp/example.dat", O_RDONLY);
        if (reopen_fd < 0) co_return;

        std::vector<uint32_t> read_buf(3);
        slim::common::io::Read read(sched, reopen_fd, read_buf); // fills up to read_buf.size() * 4 bytes
        int n = co_await read;                                   // also a raw byte count

        slim::common::io::Close close2(sched, reopen_fd);
        co_await close2;
    };
    auto coro = job();
    sched.spawn(std::move(coro));
});
```

```cpp
// Outbound connect with a kernel-enforced deadline. If the peer never
// responds within 2 seconds, the kernel cancels the connect for us and
// the coroutine sees a negative result -- no manual timer bookkeeping.
rt.post([addr, addr_len](slim::common::io::Scheduler& sched, size_t idx) {
    auto job = [&sched, addr, addr_len]() -> slim::common::io::Task<void> {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        int flags = ::fcntl(fd, F_GETFL);
        ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);

        slim::common::io::Connect connect_op(sched, fd,
            reinterpret_cast<const sockaddr*>(&addr), addr_len);
        connect_op.with_timeout(std::chrono::seconds(2));

        int result = co_await connect_op;
        if (result < 0) {
            // -ECANCELED (or -ETIME): deadline passed before connect finished
            ::close(fd);
            co_return;
        }
        // connected -- proceed with Send/Recv on fd
    };
    auto coro = job();
    sched.spawn(std::move(coro));
});
```

```cpp
// Poll for readiness -- e.g. driving a non-blocking TLS handshake retry
// loop on SSL_ERROR_WANT_READ / WANT_WRITE, without a dedicated recv/send.
auto wait_readable = [&sched, fd]() -> slim::common::io::Task<int> {
    slim::common::io::Poll poll_op(sched, fd, POLLIN);
    poll_op.with_timeout(std::chrono::milliseconds(500));
    int revents = co_await poll_op; // >= 0 ready-mask, or negative on timeout/error
    co_return revents;
};
```

```cpp
// Accept loop — serve connections on a worker until Runtime stops it.
// A worker's Scheduler::shutdown() (invoked internally by Runtime::stop())
// flushes pending tasks, so the loop coroutine doesn't need its own
// stop_token — it simply runs until the worker scheduler is torn down.
rt.post([server_fd](slim::common::io::Scheduler& sched, size_t idx) {
    auto accept_loop = [&sched, server_fd]() -> slim::common::io::Task<void> {
        while (true) {
            slim::common::io::Accept accept(sched, server_fd);
            int client_fd = co_await accept;
            if (client_fd < 0) break;

            slim::common::io::Send send(sched, client_fd, "hello\n", 6);
            co_await send;

            slim::common::io::Close close(sched, client_fd);
            co_await close;
        }
    };
    auto coro = accept_loop();
    sched.spawn(std::move(coro));
});
```

```cpp
// rt.post() is itself the thread-safe hand-off point — call it from any
// thread (including a V8 isolate thread) to enqueue work on a worker.
// The same rule applies to a single Scheduler in isolation: post() is
// thread-safe, spawn() is only safe from the thread driving that
// scheduler's own run()/drain() loop.
std::thread producer([&rt, client_fd]() {
    rt.post([client_fd](slim::common::io::Scheduler& sched, size_t idx) {
        auto send = [&sched, client_fd]() -> slim::common::io::Task<void> {
            slim::common::io::Send s(sched, client_fd, "pushed\n", 7);
            co_await s;
        };
        auto coro = send();
        sched.spawn(std::move(coro));
    });
});
producer.join();
```

```cpp
// Stop the runtime: signals dispatcher + all workers, joins their
// threads, and shuts down every scheduler.
rt.stop();
```

```cpp
// Handle IOException from Runtime construction (ring/scheduler setup
// failures on the dispatcher or any worker surface here)
try {
    slim::common::io::Runtime rt(4);
    rt.start();
    // use rt ...
    rt.stop();
}
catch (const slim::common::io::IOException& e) {
    std::cerr << "setup failed: " << e.what() << '\n';
    // e.status() is one of ErrorStatus::IOSetupFailed,
    // ErrorStatus::EpollCreateFailed, ErrorStatus::EpollCtlFailed,
    // ErrorStatus::EventFdCreateFailed, ErrorStatus::RuntimeNotIdle, etc.
}
```

[↑ Top](#table-of-contents)
