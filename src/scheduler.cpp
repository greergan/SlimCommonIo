#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <algorithm>
#include <slim/common/io/awaitable.h>
#include <slim/common/io/error_codes.h>
#include <slim/common/io/operations.h>
#include <slim/common/io/scheduler.h>
#include <slim/common/io/task.h>

namespace slim::common::io {

Scheduler::Scheduler(IO& io_ref) : io_(io_ref) {
    eventfd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (eventfd_ < 0)
        throw IOException(ErrorStatus::EventFdCreateFailed);

    // Watch both eventfd_ (new post()ed work) and io_.fd (CQEs ready) so
    // drain() can block on a single wait that wakes for either. Blocking
    // only on io_uring_enter's GETEVENTS is not enough: that call only
    // wakes on completions, so a post() that arrives while the ring is
    // idle (nothing ever submitted) would otherwise never wake the loop.
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
        ::close(eventfd_);
        throw IOException(ErrorStatus::EpollCreateFailed);
    }

    epoll_event ev{};
    ev.events  = EPOLLIN;
    ev.data.fd = eventfd_;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, eventfd_, &ev) < 0) {
        ::close(epoll_fd_);
        ::close(eventfd_);
        throw IOException(ErrorStatus::EpollCtlFailed);
    }

    ev.data.fd = io_.fd;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, io_.fd, &ev) < 0) {
        ::close(epoll_fd_);
        ::close(eventfd_);
        throw IOException(ErrorStatus::EpollCtlFailed);
    }
}

Scheduler::~Scheduler() {
    if (!tasks_.empty())
        shutdown();
    if (epoll_fd_ >= 0)
        ::close(epoll_fd_);
    if (eventfd_ >= 0)
        ::close(eventfd_);
}

void Scheduler::post(std::function<void()> task) {
    {
        std::lock_guard lock(inbox_mutex_);
        inbox_.push(std::move(task));
    }
    uint64_t val = 1;
    if (::write(eventfd_, &val, sizeof(val)) < 0)
        throw IOException(ErrorStatus::EventFdWriteFailed);
}

void Scheduler::run(std::stop_token stop_token) {
    // A thread parked in drain()'s epoll_wait(..., -1) only wakes on
    // eventfd_ activity or a CQE becoming ready -- it has no idea a stop
    // was requested. Without this callback, request_stop() from another
    // thread is invisible to a runner blocked in an idle wait: run()
    // would never re-check stop_token, and run()'s caller (typically
    // joining this thread) would hang forever. Writing to eventfd_ here
    // reuses the existing wakeup path so a stop request always breaks
    // out of the blocking wait promptly.
    std::stop_callback wake_on_stop(stop_token, [this]() {
        uint64_t val = 1;
        ::write(eventfd_, &val, sizeof(val));
    });

    while (!stop_token.stop_requested()) {
        drain();
        reap();
    }
}

void Scheduler::shutdown() {
    shutting_down_ = true;
    while (!tasks_.empty()) {
        drain();
        reap();
    }
}

void Scheduler::drain_inbox() {
    std::queue<std::function<void()>> local;
    {
        std::lock_guard lock(inbox_mutex_);
        std::swap(local, inbox_);
    }
    while (!local.empty()) {
        auto task = std::move(local.front());
        local.pop();
        try {
            task();
        } catch (const IOException& ex) {
            // task() runs here on the run() thread with nothing further
            // up the call chain (drain_inbox -> drain -> run) to catch
            // it -- an uncaught exception here means std::terminate().
            if (ex.status() == ErrorStatus::SQFull) {
                // spawn() checks SQ capacity and throws SQFull strictly
                // BEFORE ever calling resume() on the task -- a
                // coroutine starts suspended at its initial-suspend
                // point, so no user code has run yet when this throws.
                // That makes it safe to retry by re-invoking the
                // caller's original callable (which reconstructs an
                // equally not-yet-started Task via the same factory
                // call): nothing was duplicated, because nothing ran.
                //
                // drain_inbox() only runs when the non-blocking eventfd_
                // read at the top of drain() sees new data, so just
                // pushing back onto inbox_ isn't enough on its own --
                // without another post() happening to arrive, this
                // requeued item could sit unnoticed. Poke eventfd_
                // ourselves to guarantee it's picked up on a later
                // drain() call, once submission has freed up SQ room.
                {
                    std::lock_guard lock(inbox_mutex_);
                    inbox_.push(std::move(task));
                }
                uint64_t val = 1;
                ::write(eventfd_, &val, sizeof(val));
            }
            // Other failures (e.g. BadAllocation) are treated as
            // non-transient: the task is dropped rather than retried
            // forever or left to crash the scheduler thread.
        }
    }
}

void Scheduler::drain() {
    // Check inbox non-blocking before submitting SQEs
    uint64_t val{0};
    if (::read(eventfd_, &val, sizeof(val)) > 0)
        drain_inbox();

    uint32_t sq_head    = io_.sq.head->load(std::memory_order_acquire);
    uint32_t sq_tail    = io_.sq.tail->load(std::memory_order_relaxed);
    uint32_t sq_pending = sq_tail - sq_head;
    if (sq_pending > 0)
        syscall(SYS_io_uring_enter, io_.fd, sq_pending, 0, 0, nullptr, 0);

    uint32_t head = io_.cq.head->load(std::memory_order_acquire);
    uint32_t tail = io_.cq.tail->load(std::memory_order_acquire);
    if (head == tail) {
        if (shutting_down_) return;
        // Block until either a CQE is ready on the io_uring fd or new
        // work lands on the eventfd. Do NOT block solely on
        // io_uring_enter's GETEVENTS here: if the ring is completely
        // idle (nothing ever submitted, so no CQE will ever arrive) and
        // a post() writes to eventfd_ after the non-blocking read at the
        // top of this function, io_uring_enter would never observe that
        // write and this thread would hang forever. epoll_wait watches
        // both fds so either event wakes us.
        epoll_event events[2];
        ::epoll_wait(epoll_fd_, events, 2, -1);
        // Don't try to reap here -- just return and let the next drain()
        // call (from run()'s loop) re-check the inbox and submit/reap
        // normally. This keeps a single code path for both cases instead
        // of duplicating the drain_inbox/submit/reap logic here.
        return;
    }

    uint32_t mask    = *io_.cq.mask;
    uint32_t current = head;
    while (current != tail) {
        const io_uring_cqe& cqe = io_.cq.cqes[current & mask];
        auto*               awt = reinterpret_cast<Awaitable*>(cqe.user_data);
        if (awt) {
            awt->result = cqe.res;
            awt->handle.resume();
            // If this resume advanced an already-in-flight coroutine
            // into a later co_await that itself hit SQFull, the
            // coroutine already raced through to completion with a
            // garbage result by this point (same underlying language
            // behavior as in start_or_defer's comment -- there is no
            // hook to intervene once resume() has been called). We
            // can't undo that here, but we must at least clear the
            // thread-local flag so it doesn't leak into an unrelated
            // spawn()/start_or_defer() check for a different task later
            // on this thread.
            take_pending_sq_error();
        }
        ++current;
        io_.cq.head->store(current, std::memory_order_release);
    }
}

void Scheduler::reap() {
    tasks_.erase(
        std::remove_if(tasks_.begin(), tasks_.end(), [](const Task<void>& t) { return t.done(); }),
        tasks_.end());
}

} // namespace slim::common::io
