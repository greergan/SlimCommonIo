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
#include <slim/common/log.h>

namespace slim::common::io {

Scheduler::Scheduler(IO& io_ref) : io_(io_ref) {
#ifdef ENABLE_LOGGING
    log::trace(log::Message(__func__, "begins", __FILE__, __LINE__));
#endif
    eventfd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (eventfd_ < 0) {
#ifdef ENABLE_LOGGING
        log::error(log::Message(__func__, "eventfd creation failed", __FILE__, __LINE__));
#endif
        throw IOException(ErrorStatus::EventFdCreateFailed);
    }
#ifdef ENABLE_LOGGING
    log::debug(log::Message(__func__, std::format("eventfd_=>{}", eventfd_), __FILE__, __LINE__));
#endif

    // Watch both eventfd_ (new post()ed work) and io_.fd (CQEs ready) so
    // drain() can block on a single wait that wakes for either. Blocking
    // only on io_uring_enter's GETEVENTS is not enough: that call only
    // wakes on completions, so a post() that arrives while the ring is
    // idle (nothing ever submitted) would otherwise never wake the loop.
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
#ifdef ENABLE_LOGGING
        log::error(log::Message(__func__, "epoll_create1 failed", __FILE__, __LINE__));
#endif
        ::close(eventfd_);
        throw IOException(ErrorStatus::EpollCreateFailed);
    }
#ifdef ENABLE_LOGGING
    log::debug(log::Message(__func__, std::format("epoll_fd_=>{}", epoll_fd_), __FILE__, __LINE__));
#endif

    epoll_event ev{};
    ev.events  = EPOLLIN;
    ev.data.fd = eventfd_;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, eventfd_, &ev) < 0) {
#ifdef ENABLE_LOGGING
        log::error(log::Message(__func__, "epoll_ctl add eventfd_ failed", __FILE__, __LINE__));
#endif
        ::close(epoll_fd_);
        ::close(eventfd_);
        throw IOException(ErrorStatus::EpollCtlFailed);
    }

    ev.data.fd = io_.fd;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, io_.fd, &ev) < 0) {
#ifdef ENABLE_LOGGING
        log::error(log::Message(__func__, "epoll_ctl add io_.fd failed", __FILE__, __LINE__));
#endif
        ::close(epoll_fd_);
        ::close(eventfd_);
        throw IOException(ErrorStatus::EpollCtlFailed);
    }
#ifdef ENABLE_LOGGING
    log::trace(log::Message(__func__, "ends", __FILE__, __LINE__));
#endif
}

Scheduler::~Scheduler() {
#ifdef ENABLE_LOGGING
    log::trace(log::Message(__func__, "begins", __FILE__, __LINE__));
#endif
    if (!tasks_.empty()) {
#ifdef ENABLE_LOGGING
        log::debug(log::Message(__func__, std::format("tasks not empty size=>{} => calling shutdown", tasks_.size()), __FILE__, __LINE__));
#endif
        shutdown();
    }
    if (epoll_fd_ >= 0)
        ::close(epoll_fd_);
    if (eventfd_ >= 0)
        ::close(eventfd_);
#ifdef ENABLE_LOGGING
    log::trace(log::Message(__func__, "ends", __FILE__, __LINE__));
#endif
}

void Scheduler::post(std::function<void()> task) {
#ifdef ENABLE_LOGGING
    log::trace(log::Message(__func__, "begins", __FILE__, __LINE__));
#endif
    {
        std::lock_guard lock(inbox_mutex_);
        inbox_.push(std::move(task));
    }
    uint64_t val = 1;
    if (::write(eventfd_, &val, sizeof(val)) < 0) {
#ifdef ENABLE_LOGGING
        log::error(log::Message(__func__, "eventfd write failed", __FILE__, __LINE__));
#endif
        throw IOException(ErrorStatus::EventFdWriteFailed);
    }
#ifdef ENABLE_LOGGING
    log::trace(log::Message(__func__, "ends", __FILE__, __LINE__));
#endif
}

void Scheduler::run(std::stop_token stop_token) {
#ifdef ENABLE_LOGGING
    log::trace(log::Message(__func__, "begins", __FILE__, __LINE__));
#endif
    // A thread parked in drain()'s epoll_wait(..., -1) only wakes on
    // eventfd_ activity or a CQE becoming ready -- it has no idea a stop
    // was requested. Without this callback, request_stop() from another
    // thread is invisible to a runner blocked in an idle wait: run()
    // would never re-check stop_token, and run()'s caller (typically
    // joining this thread) would hang forever. Writing to eventfd_ here
    // reuses the existing wakeup path so a stop request always breaks
    // out of the blocking wait promptly.
    std::stop_callback wake_on_stop(stop_token, [this]() {
#ifdef ENABLE_LOGGING
        log::debug(log::Message(__func__, "stop callback fired, writing to eventfd_", __FILE__, __LINE__));
#endif
        uint64_t val = 1;
        ::write(eventfd_, &val, sizeof(val));
    });

    while (!stop_token.stop_requested()) {
        drain();
        reap();
    }
#ifdef ENABLE_LOGGING
    log::trace(log::Message(__func__, "ends", __FILE__, __LINE__));
#endif
}

void Scheduler::shutdown() {
#ifdef ENABLE_LOGGING
    log::trace(log::Message(__func__, "begins", __FILE__, __LINE__));
#endif
    shutting_down_ = true;
    while (!tasks_.empty()) {
#ifdef ENABLE_LOGGING
        log::debug(log::Message(__func__, std::format("draining tasks size=>{}", tasks_.size()), __FILE__, __LINE__));
#endif
        drain();
        reap();
    }
#ifdef ENABLE_LOGGING
    log::trace(log::Message(__func__, "ends", __FILE__, __LINE__));
#endif
}

void Scheduler::reset() {
#ifdef ENABLE_LOGGING
    log::trace(log::Message(__func__, "begins", __FILE__, __LINE__));
#endif
    tasks_.clear();
    {
        std::lock_guard lock(inbox_mutex_);
        inbox_ = {};
    }
    shutting_down_ = false;
#ifdef ENABLE_LOGGING
    log::trace(log::Message(__func__, "ends", __FILE__, __LINE__));
#endif
}

void Scheduler::drain_inbox() {
#ifdef ENABLE_LOGGING
    log::trace(log::Message(__func__, "begins", __FILE__, __LINE__));
#endif
    std::queue<std::function<void()>> local;
    {
        std::lock_guard lock(inbox_mutex_);
        std::swap(local, inbox_);
    }
#ifdef ENABLE_LOGGING
    log::debug(log::Message(__func__, std::format("inbox tasks=>{}", local.size()), __FILE__, __LINE__));
#endif
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
#ifdef ENABLE_LOGGING
                log::debug(log::Message(__func__, "SQFull => requeueing task", __FILE__, __LINE__));
#endif
                {
                    std::lock_guard lock(inbox_mutex_);
                    inbox_.push(std::move(task));
                }
                uint64_t val = 1;
                ::write(eventfd_, &val, sizeof(val));
            } else {
                // Other failures (e.g. BadAllocation) are treated as
                // non-transient: the task is dropped rather than retried
                // forever or left to crash the scheduler thread.
#ifdef ENABLE_LOGGING
                log::error(log::Message(__func__, std::format("IOException dropped task status=>{}", static_cast<int>(ex.status())), __FILE__, __LINE__));
#endif
            }
        }
    }
#ifdef ENABLE_LOGGING
    log::trace(log::Message(__func__, "ends", __FILE__, __LINE__));
#endif
}

void Scheduler::drain() {
    // Check inbox non-blocking before submitting SQEs
    uint64_t val{0};
    if (::read(eventfd_, &val, sizeof(val)) > 0)
        drain_inbox();

    uint32_t sq_head    = io_.sq.head->load(std::memory_order_acquire);
    uint32_t sq_tail    = io_.sq.tail->load(std::memory_order_relaxed);
    uint32_t sq_pending = sq_tail - sq_head;
    if (sq_pending > 0) {
#ifdef ENABLE_LOGGING
        log::debug(log::Message(__func__, std::format("submitting sq_pending=>{}", sq_pending), __FILE__, __LINE__));
#endif
        syscall(SYS_io_uring_enter, io_.fd, sq_pending, 0, 0, nullptr, 0);
    }

    uint32_t head = io_.cq.head->load(std::memory_order_acquire);
    uint32_t tail = io_.cq.tail->load(std::memory_order_acquire);
    if (head == tail) {
        // Block until either a CQE is ready on the io_uring fd or new
        // work lands on the eventfd. Do NOT block solely on
        // io_uring_enter's GETEVENTS here: if the ring is completely
        // idle (nothing ever submitted, so no CQE will ever arrive) and
        // a post() writes to eventfd_ after the non-blocking read at the
        // top of this function, io_uring_enter would never observe that
        // write and this thread would hang forever. epoll_wait watches
        // both fds so either event wakes us.
#ifdef ENABLE_LOGGING
        log::debug(log::Message(__func__, "cq empty => epoll_wait", __FILE__, __LINE__));
#endif
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
#ifdef ENABLE_LOGGING
            log::debug(log::Message(__func__, std::format("resuming awaitable=>{} result=>{}", (void*)awt, awt->result), __FILE__, __LINE__));
#endif
            awt->handle.resume();
            // awt->handle is an inner frame handle, not a top-level Task
            // handle. Pushing it to done_handles_ and comparing against
            // tasks_ entries was incorrect: the handles never matched
            // top-level tasks, and a recycled address could false-positive
            // reap a live task. Reaping is handled entirely by reap() via
            // t.done() on the top-level Task, whose frame stays alive
            // (owned by tasks_) until tasks_.erase() destroys it.
            take_pending_sq_error();
        }
        ++current;
        io_.cq.head->store(current, std::memory_order_release);
    }
}

void Scheduler::reap() {
#ifdef ENABLE_LOGGING
    log::trace(log::Message(__func__, "begins", __FILE__, __LINE__));
    log::debug(log::Message(__func__, std::format("tasks=>{}", tasks_.size()), __FILE__, __LINE__));
#endif
    tasks_.erase(
        std::remove_if(tasks_.begin(), tasks_.end(), [](const Task<void>& t) {
            // t.done() is always safe here: tasks_ owns the top-level frame,
            // so the frame is alive until tasks_.erase() destroys it below.
            // The former done_handles_ path (matching inner frame handles
            // against top-level task handles) was removed: inner frame handles
            // stored in awt->handle never matched any tasks_ entry, and a
            // recycled inner frame address could false-positive reap a live task.
            bool is_done = t.done();
#ifdef ENABLE_LOGGING
            if (is_done) {
                log::debug(log::Message(__func__, std::format("reaping done handle=>{}", (void*)t.handle().address()), __FILE__, __LINE__));
            }
#endif
            return is_done;
        }),
        tasks_.end());
#ifdef ENABLE_LOGGING
    log::trace(log::Message(__func__, "ends", __FILE__, __LINE__));
#endif
}

} // namespace slim::common::io
