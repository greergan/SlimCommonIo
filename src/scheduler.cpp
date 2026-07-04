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
    uint32_t head = io_.cq.head->load(std::memory_order_acquire);
    uint32_t tail = io_.cq.tail->load(std::memory_order_acquire);
    uint32_t mask = *io_.cq.mask;

    // If the CQ is empty, block in the kernel until at least one event arrives.
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
            awt->handle.resume(); // coro runs to next suspend or completion
        }
        ++current;
    }

    // Batch-advance the head pointer once after processing all available CQEs.
    if (current != head)
        io_.cq.head->store(current, std::memory_order_release);
}

void Scheduler::reap() {
    // NOTE: For high-IOPS paths consider an intrusive list to avoid O(n) erase.
    tasks_.erase(
        std::remove_if(tasks_.begin(), tasks_.end(), [](const Task<void>& t) { return t.done(); }),
        tasks_.end());
}

} // namespace slim::common::io
