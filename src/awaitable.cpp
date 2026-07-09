#include <sys/syscall.h>
#include <unistd.h>
#include <cstring>
#include <slim/common/io/awaitable.h>
#include <slim/common/io/error_codes.h>
#include <slim/common/io/scheduler.h>

namespace slim::common::io {

// ── SQFull side-channel ──────────────────────────────────────────────────────
// await_suspend() cannot propagate exceptions to spawn() because the coroutine
// ABI intercepts throws from await_suspend and routes them through
// promise_type::unhandled_exception(); handle.resume() then returns normally.
// Instead we store the error here and let spawn() read it after resume().
namespace {
thread_local bool        tl_sq_full        = false;
thread_local IOException tl_sq_full_ex{ErrorStatus::SQFull};
} // namespace

IOException* take_pending_sq_error() noexcept {
    if (!tl_sq_full) return nullptr;
    tl_sq_full = false;
    return &tl_sq_full_ex;
}

// ── Awaitable ────────────────────────────────────────────────────────────────

Awaitable::Awaitable(Scheduler& scheduler) : io_(scheduler.io()) {}

void Awaitable::with_timeout(std::chrono::milliseconds timeout) noexcept {
    auto secs  = std::chrono::duration_cast<std::chrono::seconds>(timeout);
    auto nsecs = std::chrono::duration_cast<std::chrono::nanoseconds>(timeout - secs);

    __kernel_timespec ts{};
    ts.tv_sec  = secs.count();
    ts.tv_nsec = nsecs.count();
    timeout_ts_ = ts;
}

bool Awaitable::await_ready() const noexcept { return false; }

bool Awaitable::await_suspend(std::coroutine_handle<> h) {
    const uint32_t needed = timeout_ts_.has_value() ? 2 : 1;

    if (!has_capacity(needed)) {
        // Signal SQFull through the side-channel instead of throwing.
        // Returning false resumes the coroutine immediately without suspending,
        // leaving result at its default (0). spawn() checks take_pending_sq_error().
        //
        // Checking capacity for BOTH slots before committing either is what
        // prevents an orphaned IOSQE_IO_LINK: if we committed the primary
        // SQE alone on a ring with exactly one free slot, the kernel would
        // be left holding a linked op with no follow-up timeout SQE ever
        // arriving, which is undefined as far as this ring is concerned.
        tl_sq_full = true;
        return false;
    }

    io_uring_sqe* primary_sqe = commit_one();
    prepare(primary_sqe);
    primary_sqe->user_data = reinterpret_cast<uint64_t>(this);

    if (timeout_ts_.has_value()) {
        primary_sqe->flags |= IOSQE_IO_LINK;

        io_uring_sqe* timeout_sqe = commit_one();
        memset(timeout_sqe, 0, sizeof(*timeout_sqe));
        timeout_sqe->opcode   = IORING_OP_LINK_TIMEOUT;
        timeout_sqe->addr     = reinterpret_cast<uint64_t>(&timeout_ts_.value());
        timeout_sqe->len      = 1;
        // user_data left at 0 (io_uring_sqe is zero-initialized above).
        // Scheduler::drain() reinterpret_casts user_data to Awaitable* and
        // checks it for null before dereferencing -- a zero user_data CQE
        // for this companion op is silently skipped, no Scheduler change
        // needed to support this.
    }

    handle = h;
    // submit() is intentionally NOT called here.
    // Scheduler::drain() submits all pending SQEs in one syscall before
    // waiting for completions. This keeps the SQ tail ahead of the kernel's
    // head pointer until drain() flushes, so has_capacity() correctly
    // reports the ring as full when all slots are taken.
    return true;
}

int Awaitable::await_resume() const noexcept { return result; }

bool Awaitable::has_capacity(uint32_t count) const noexcept {
    uint32_t head = io_.sq.head->load(std::memory_order_acquire);
    uint32_t tail = io_.sq.tail->load(std::memory_order_relaxed);
    return (tail - head) + count <= *io_.sq.entries;
}

io_uring_sqe* Awaitable::commit_one() noexcept {
    uint32_t tail     = io_.sq.tail->load(std::memory_order_relaxed);
    uint32_t idx      = tail & *io_.sq.mask;
    io_.sq.array[idx] = idx;
    io_.sq.tail->store(tail + 1, std::memory_order_release);
    return &io_.sq.sqes[idx];
}

void Awaitable::submit() noexcept {
    uint32_t head    = io_.sq.head->load(std::memory_order_acquire);
    uint32_t tail    = io_.sq.tail->load(std::memory_order_relaxed);
    uint32_t pending = tail - head;
    syscall(SYS_io_uring_enter, io_.fd, pending, 0, 0, nullptr, 0);
}

} // namespace slim::common::io
