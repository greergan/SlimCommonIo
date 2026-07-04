#include <cerrno>
#include <sys/syscall.h>
#include <unistd.h>
#include <slim/common/io/awaitable.h>

namespace slim::common::io {

Awaitable::Awaitable(IO& io_ref) : io(io_ref) {}

bool Awaitable::await_ready() const noexcept { return false; }

void Awaitable::await_suspend(std::coroutine_handle<> h) noexcept {
    handle    = h;
    auto* sqe = acquire_sqe();
    if (!sqe) {
        // SQ is full; complete synchronously with EBUSY so the caller can retry.
        result = -EBUSY;
        handle.resume();
        return;
    }
    prepare(sqe);
    sqe->user_data = reinterpret_cast<uint64_t>(this);
    submit();
}

int Awaitable::await_resume() const noexcept { return result; }

io_uring_sqe* Awaitable::acquire_sqe() noexcept {
    uint32_t head = io.sq.head->load(std::memory_order_acquire);
    uint32_t tail = io.sq.tail->load(std::memory_order_relaxed);
    if (tail - head >= *io.sq.entries)
        return nullptr;
    uint32_t idx     = tail & *io.sq.mask;
    io.sq.array[idx] = idx;
    io.sq.tail->store(tail + 1, std::memory_order_release);
    return &io.sq.sqes[idx];
}

void Awaitable::submit() noexcept {
    // tail was already incremented by acquire_sqe(); read head to find how many
    // SQEs are pending and hand them all to the kernel in one enter call.
    uint32_t head    = io.sq.head->load(std::memory_order_acquire);
    uint32_t tail    = io.sq.tail->load(std::memory_order_relaxed);
    uint32_t pending = tail - head;
    syscall(SYS_io_uring_enter, io.fd, pending, 0, 0, nullptr, 0);
}

} // namespace slim::common::io
