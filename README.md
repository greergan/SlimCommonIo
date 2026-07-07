# SlimCommonIo

<a href="https://codeberg.org/greergan/SlimTS">
  <img src="https://raw.githubusercontent.com/greergan/SlimTS/master/assets/slimts_logo.png" width="75" alt="SlimTS Logo">
</a>

A low-level, Linux-native async I/O runtime built directly on `io_uring`.  
Provides coroutine-based I/O operations, a cooperative task scheduler with cross-thread posting, and a thin ring-management layer with no external userspace dependencies.  
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
- [Error Model](#error-model)
- [Building](#building)
- [Dependencies](#dependencies)
- [Examples](#examples)

## Overview

This library provides a coroutine-native async I/O layer backed directly by `io_uring` via raw `SYS_io_uring_setup` / `SYS_io_uring_enter` syscalls:

- Manual SQ and CQ ring setup with `mmap`, including `IORING_FEAT_SINGLE_MMAP` optimization
- A C++20 coroutine `Task<T>` type unified across value and `void` returns via a `ValueStore<T>` base
- An `Awaitable` base that hooks directly into the coroutine awaiter protocol and stages SQEs for batched submission
- Eight concrete async operations: `Accept`, `Close`, `Open`, `Read`, `Recv`, `Send`, `Stat`, `Write`
- A cooperative `Scheduler` that drives the event loop, submits staged SQEs and blocks on the kernel when the CQ is empty, reaps completed tasks, and supports cross-thread task posting via an `eventfd`-based inbox
- An `ErrorStatus` enum and `IOException` for constructor-time and spawn-time failures; all I/O operations return raw `int` results following Linux errno convention

[↑ Top](#table-of-contents)

## Features

| Feature | Description |
|---------|-------------|
| Raw `io_uring` | Ring setup, SQE submission, and CQE reaping done manually — no liburing dependency |
| Single-mmap optimization | CQ ring reuses the SQ `mmap` allocation when `IORING_FEAT_SINGLE_MMAP` is available |
| Coroutine tasks | `Task<T>` implements the C++20 coroutine promise protocol via a `ValueStore<T>` base; always suspends at start; handles both value and `void` returns in a single template |
| Composable awaitables | `Awaitable` base implements `await_ready` / `await_suspend` / `await_resume`; subclasses only override `prepare()` |
| SQ backpressure | If the SQ is full when `Scheduler::spawn()` is called, `spawn()` throws `IOException(ErrorStatus::SQFull)` |
| Batched submission | SQEs are staged in `await_suspend` and flushed to the kernel in a single `io_uring_enter` call at the start of each `drain()` tick |
| Blocking drain | `Scheduler::drain()` issues `IORING_ENTER_GETEVENTS` when the CQ is empty after submission, avoiding a busy loop |
| Cross-thread posting | `Scheduler::post(std::function<void()>)` is thread-safe; any thread (including a V8 isolate thread) can queue work onto the scheduler. An `eventfd` is checked non-blocking at the start of each `drain()` tick to drain the inbox without blocking the event loop |
| Cooperative scheduler | `Scheduler::run()` loops drain → reap until a `std::stop_token` fires; `shutdown()` flushes all pending tasks |
| Linux-only | Requires Linux 5.1+ (`io_uring`) and `linux/io_uring.h` kernel headers; no platform abstraction layer |

[↑ Top](#table-of-contents)

## Default Values

| Field / Parameter | Default | Notes |
|-------------------|---------|-------|
| `IO::IO(entries)` | `256` | SQ ring depth; CQ is always twice this size to prevent overflow |
| `IO::fd` | `-1` | Set to the live `io_uring` fd on successful construction |
| `Read::offset` | `0` | Byte offset into the file; pass explicitly for positioned reads |
| `Write::offset` | `0` | Byte offset into the file; pass explicitly for positioned writes |
| `Open::mode` | `0644` | File creation mode; ignored when flags do not include `O_CREAT` |
| `Open::dfd` | `AT_FDCWD` | Directory fd for relative path resolution |
| `Stat::dfd` | `AT_FDCWD` | Directory fd for relative path resolution |
| `Stat::flags` | `0` | `statx` flags; `0` follows symlinks |
| `Stat::mask` | `STATX_BASIC_STATS` | Fields requested from the kernel |
| `Recv::flags` | `0` | `MSG_*` flags passed to the underlying `recv` |
| `Send::flags` | `0` | `MSG_*` flags passed to the underlying `send` |
| `Accept` socket flags | `SOCK_NONBLOCK \| SOCK_CLOEXEC` | Always applied; not overridable via the constructor |
| `Awaitable::result` | `0` | Overwritten by the CQE result on completion |

[↑ Top](#table-of-contents)

## Core API

### IO struct

```cpp
slim::common::IO io;
slim::common::IO io(512); // custom ring depth
```

`IO` is the root resource. It owns the `io_uring` file descriptor and all `mmap` regions for the SQ ring, SQ entries array, and CQ ring. All operations and the scheduler hold a reference to it.

`IO` is non-copyable. Move is not defined — construct one instance per thread or io context and pass it by reference.

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
    MyOp(IO& io) : Awaitable(io) {}
protected:
    void prepare(io_uring_sqe* sqe) noexcept override { /* fill sqe */ }
};
```

`Awaitable` implements the C++20 awaiter protocol and handles all SQE acquisition boilerplate. Subclasses only override `prepare()` to fill in the opcode and operation-specific fields. SQEs are staged in the ring on suspension and flushed to the kernel in batch by `Scheduler::drain()`.

| Method | Description |
|--------|-------------|
| `await_ready()` | Always returns `false` — every operation suspends the coroutine |
| `await_suspend(handle)` | Acquires an SQE, calls `prepare()`, and sets `user_data` to `this`. If the SQ is full, sets a thread-local error flag and returns `false` to resume the coroutine immediately without suspending; `Scheduler::spawn()` detects this flag after `resume()` returns and throws `IOException(ErrorStatus::SQFull)` |
| `await_resume()` | Returns `result` as `int` |
| `prepare(sqe)` | Pure virtual. Called with a zeroed SQE slot; subclass fills opcode and fields |

[↑ Top](#table-of-contents)

### Operations

All operations live in `slim::common::io` and derive from `Awaitable`. All return `int` when `co_await`ed: a non-negative value on success (semantics match the underlying syscall), or a negative errno on failure.

| Operation | Constructor | `io_uring` opcode | Notes |
|-----------|-------------|-------------------|-------|
| `Accept` | `Accept(IO&, int server_fd)` | `IORING_OP_ACCEPT` | Yields the accepted client fd. Socket created with `SOCK_NONBLOCK \| SOCK_CLOEXEC`. Peer address available via `addr` / `addr_len` members |
| `Close` | `Close(IO&, int fd)` | `IORING_OP_CLOSE` | Async fd close |
| `Open` | `Open(IO&, const char* path, int flags, mode_t mode = 0644, int dfd = AT_FDCWD)` | `IORING_OP_OPENAT` | Yields the opened fd on success |
| `Read` | `Read(IO&, int fd, void* buf, size_t len, uint64_t offset = 0)` | `IORING_OP_READ` | Yields bytes read |
| `Recv` | `Recv(IO&, int fd, void* buf, size_t len, int flags = 0)` | `IORING_OP_RECV` | Yields bytes received |
| `Send` | `Send(IO&, int fd, const void* buf, size_t len, int flags = 0)` | `IORING_OP_SEND` | Yields bytes sent |
| `Stat` | `Stat(IO&, const char* path, int flags = 0, uint32_t mask = STATX_BASIC_STATS, int dfd = AT_FDCWD)` | `IORING_OP_STATX` | Result in `buf` member (`struct statx`) on success |
| `Write` | `Write(IO&, int fd, const void* buf, size_t len, uint64_t offset = 0)` | `IORING_OP_WRITE` | Yields bytes written |

[↑ Top](#table-of-contents)

### Scheduler

```cpp
slim::common::IO io;
slim::common::io::Scheduler sched(io);
```

`Scheduler` drives the coroutine event loop. It holds a `std::vector<Task<void>>` of live tasks, an `IO&` reference, and an `eventfd` for cross-thread wakeup. It is non-copyable.

| Method | Description |
|--------|-------------|
| `spawn(Task<T>&&)` | Resumes the task to its first suspension point, then takes ownership. Throws `IOException(ErrorStatus::SQFull)` if `await_suspend` found the SQ full, or `IOException(ErrorStatus::BadAllocation)` if the internal task vector cannot grow. **Do not pass an inline temporary lambda directly** — bind it to a named variable first to avoid GCC coroutine UB (see note below) |
| `post(std::function<void()>)` | Thread-safe. Enqueues a callable onto the scheduler's inbox and writes to the `eventfd` to wake the event loop. Safe to call from any thread including a V8 isolate thread |
| `run(std::stop_token)` | Loops `drain()` → `reap()` until the stop token is signalled |
| `shutdown()` | Flushes all remaining tasks by looping `drain()` → `reap()` until the task list is empty. Called automatically by the destructor if tasks remain |
| `eventfd()` | Returns the scheduler's `eventfd` file descriptor. Exposed for advanced use cases |

**`drain()`** — first checks the `eventfd` non-blocking and drains the inbox if the flag is set, then submits all SQEs staged since the last tick via a single `io_uring_enter` call, then processes all available CQEs. If the CQ is still empty after submission and the scheduler is not shutting down, it blocks in the kernel via `IORING_ENTER_GETEVENTS` until at least one event arrives, then resumes the associated coroutine for each CQE. The CQ head is advanced after each CQE.

**`reap()`** — sweeps `tasks_` and erases completed entries. Currently O(n); an intrusive list is the recommended optimization for high-IOPS paths.

**`spawn()` lambda lifetime note:** GCC may elide the closure copy into the coroutine frame when a lambda is passed as a prvalue, leaving the frame with a dangling reference after the temporary is destroyed. Always name the lambda before calling it:

```cpp
auto coro = [&]() -> Task<void> { /* ... */ };
sched.spawn(coro());  // correct — named variable
// sched.spawn([&]() -> Task<void> { ... }()); // UB — temporary lambda
```

[↑ Top](#table-of-contents)

## Error Model

Constructor failures on `IO` and spawn-time failures on `Scheduler` are reported via `IOException`, a `std::runtime_error` subclass that carries an `ErrorStatus`.

| `ErrorStatus` | String | Meaning |
|---------------|--------|---------|
| `OK` | `"OK"` | No error |
| `BadAllocation` | `"Bad allocation"` | `Scheduler::spawn()` could not grow the internal task vector |
| `EventFdCreateFailed` | `"eventfd create failed"` | `Scheduler` could not create its wakeup `eventfd` |
| `EventFdReadFailed` | `"eventfd read failed"` | Unexpected error reading the wakeup `eventfd` |
| `EventFdWriteFailed` | `"eventfd write failed"` | `Scheduler::post()` could not write to the wakeup `eventfd` |
| `IOInvalidEntries` | `"IO invalid entries"` | `IO` constructed with zero entries |
| `IOMmapCqFailed` | `"IO mmap CQ ring failed"` | CQ ring `mmap` failed (legacy kernels without `IORING_FEAT_SINGLE_MMAP`) |
| `IOMmapSqesFailed` | `"IO mmap SQEs failed"` | SQEs array `mmap` failed |
| `IOMmapSqFailed` | `"IO mmap SQ ring failed"` | SQ ring `mmap` failed |
| `IOSetupFailed` | `"IO setup failed"` | `SYS_io_uring_setup` returned a negative fd |
| `SQFull` | `"Submission queue full"` | `Scheduler::spawn()` was called while every SQ slot was occupied |

`IOException::status()` returns the `ErrorStatus` value. `what()` returns the corresponding string from `error_status_str[]`.

I/O operations (all `Awaitable` subclasses) do **not** throw — they return a negative errno as an `int` result from `co_await`, following the Linux syscall convention.

[↑ Top](#table-of-contents)

## Building

This library is built using [SlimLibraryPackager](https://codeberg.org/greergan/SlimLibraryPackager). See that repository for build instructions.

Requires Linux 5.1+ and kernel headers providing `linux/io_uring.h`. A C++20-capable compiler with coroutine support is required.

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
// Construct the io_uring instance (256-entry SQ, 512-entry CQ)
slim::common::IO io;

// Or with a custom ring depth
slim::common::IO io(512);
```

```cpp
// Read a file asynchronously
auto read_file = [&]() -> slim::common::io::Task<void> {
    slim::common::io::Open open(io, "/etc/hostname", O_RDONLY);
    int fd = co_await open;
    if (fd < 0) co_return;

    char buf[256]{};
    slim::common::io::Read read(io, fd, buf, sizeof(buf));
    int n = co_await read;

    slim::common::io::Close close(io, fd);
    co_await close;
};

slim::common::io::Scheduler sched(io);
auto coro = read_file();
sched.spawn(std::move(coro));
sched.shutdown();
```

```cpp
// Accept loop — serve connections until stopped
std::stop_source stop;

auto accept_loop = [&]() -> slim::common::io::Task<void> {
    while (true) {
        slim::common::io::Accept accept(io, server_fd);
        int client_fd = co_await accept;
        if (client_fd < 0) break;

        slim::common::io::Send send(io, client_fd, "hello\n", 6);
        co_await send;

        slim::common::io::Close close(io, client_fd);
        co_await close;
    }
};

slim::common::io::Scheduler sched(io);
auto coro = accept_loop();
sched.spawn(std::move(coro));
sched.run(stop.get_token()); // blocks until stop.request_stop()
```

```cpp
// Post work from another thread (e.g. a V8 isolate thread)
sched.post([&]() {
    // runs on the scheduler thread during the next drain() tick
    auto send = [&]() -> slim::common::io::Task<void> {
        slim::common::io::Send s(io, client_fd, "pushed\n", 7);
        co_await s;
    };
    auto coro = send();
    sched.spawn(std::move(coro));
});
```

```cpp
// Handle IOException from IO construction
try {
    slim::common::IO io(1024);
    // use io ...
}
catch (const slim::common::io::IOException& e) {
    std::cerr << "io_uring setup failed: " << e.what() << '\n';
    // e.status() == slim::common::io::ErrorStatus::IOSetupFailed
}
```

[↑ Top](#table-of-contents)
