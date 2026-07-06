#include <sys/syscall.h>
#include <unistd.h>
#include <algorithm>
#include <slim/common/io/awaitable.h>
#include <slim/common/io/scheduler.h>
#include <slim/common/io/task.h>
namespace slim::common::io {
Scheduler::Scheduler(IO& io_ref) : io_(io_ref) {}
Scheduler::~Scheduler() {
    if (!tasks_.empty())
        shutdown();
}
void Scheduler::run(std::stop_token stop_token) {
    while (!stop_token.stop_requested()) {
        drain();
        reap();
    }
}
void Scheduler::shutdown() {
    while (!tasks_.empty()) {
        drain();
        reap();
    }
}
void Scheduler::drain() {
    // Submit all SQEs that await_suspend() has staged since the last drain().
    // await_suspend() intentionally does not call submit() so that the SQ tail
    // stays ahead of the kernel head pointer — this makes acquire_sqe() report
    // the ring as full when every slot is occupied, enabling the SQFull path.
    // We flush here, in one syscall, before blocking for completions.
    uint32_t sq_head    = io_.sq.head->load(std::memory_order_acquire);
    uint32_t sq_tail    = io_.sq.tail->load(std::memory_order_relaxed);
    uint32_t sq_pending = sq_tail - sq_head;
    if (sq_pending > 0)
        syscall(SYS_io_uring_enter, io_.fd, sq_pending, 0, 0, nullptr, 0);
    uint32_t head = io_.cq.head->load(std::memory_order_acquire);
    uint32_t tail = io_.cq.tail->load(std::memory_order_acquire);
    uint32_t mask = *io_.cq.mask;
    // If the CQ is empty, block until at least one completion arrives.
    if (head == tail) {
        syscall(SYS_io_uring_enter, io_.fd, 0, 1, IORING_ENTER_GETEVENTS, nullptr, 0);
        head = io_.cq.head->load(std::memory_order_acquire);
        tail = io_.cq.tail->load(std::memory_order_acquire);
    }
    uint32_t current = head;
    while (current != tail) {
        const io_uring_cqe& cqe = io_.cq.cqes[current & mask];
        auto*               awt = reinterpret_cast<Awaitable*>(cqe.user_data);
        if (awt) {
            awt->result = cqe.res;
            awt->handle.resume();
        }
        ++current;
    }
    if (current != head)
        io_.cq.head->store(current, std::memory_order_release);
}
void Scheduler::reap() {
    tasks_.erase(
        std::remove_if(tasks_.begin(), tasks_.end(), [](const Task<void>& t) { return t.done(); }),
        tasks_.end());
}
} // namespace slim::common::io
