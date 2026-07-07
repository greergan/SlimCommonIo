#include <sys/syscall.h>
#include <unistd.h>
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
bool Awaitable::await_ready() const noexcept { return false; }
bool Awaitable::await_suspend(std::coroutine_handle<> h) {
    auto* sqe = acquire_sqe();
    if (!sqe) {
        // Signal SQFull through the side-channel instead of throwing.
        // Returning false resumes the coroutine immediately without suspending,
        // leaving result at its default (0). spawn() checks take_pending_sq_error().
        tl_sq_full = true;
        return false;
    }
    handle = h;
    prepare(sqe);
    sqe->user_data = reinterpret_cast<uint64_t>(this);
    // submit() is intentionally NOT called here.
    // Scheduler::drain() submits all pending SQEs in one syscall before
    // waiting for completions. This keeps the SQ tail ahead of the kernel's
    // head pointer until drain() flushes, so acquire_sqe() correctly reports
    // the ring as full when all slots are taken.
    return true;
}
int Awaitable::await_resume() const noexcept { return result; }
io_uring_sqe* Awaitable::acquire_sqe() noexcept {
    uint32_t head = io_.sq.head->load(std::memory_order_acquire);
    uint32_t tail = io_.sq.tail->load(std::memory_order_relaxed);
    if (tail - head >= *io_.sq.entries)
        return nullptr;
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
