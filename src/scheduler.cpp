#include <algorithm>
#include <cstring>
#include <sys/eventfd.h>
#include <sys/syscall.h>
#include <unistd.h>
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
    // A thread parked in drain()'s io_uring_enter GETEVENTS wait only wakes
    // when a CQE arrives -- it has no idea a stop was requested. Without this
    // callback, request_stop() from another thread is invisible to a runner
    // blocked in the idle wait: run() would never re-check stop_token, and
    // run()'s caller (typically joining this thread) would hang forever.
    // Writing to eventfd_ here triggers the always-in-flight eventfd read CQE,
    // which wakes io_uring_enter and lets the loop re-check stop_token.
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

void Scheduler::arm_eventfd_read() {
    // Submit an IORING_OP_READ for eventfd_ into the ring. The resulting CQE
    // is identified by user_data == &eventfd_buf_ (the sentinel). drain() uses
    // this to distinguish inbox-notification CQEs from real I/O Awaitable CQEs.
    uint32_t tail     = io_.sq.tail->load(std::memory_order_relaxed);
    uint32_t idx      = tail & *io_.sq.mask;
    io_.sq.array[idx] = idx;
    io_uring_sqe* sqe = &io_.sq.sqes[idx];
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode   = IORING_OP_READ;
    sqe->fd       = eventfd_;
    sqe->addr     = reinterpret_cast<uint64_t>(&eventfd_buf_);
    sqe->len      = sizeof(eventfd_buf_);
    sqe->off      = 0;
    sqe->user_data = reinterpret_cast<uint64_t>(&eventfd_buf_);
    io_.sq.tail->store(tail + 1, std::memory_order_release);
    eventfd_armed_ = true;
#ifdef ENABLE_LOGGING
    log::debug(log::Message(__func__, "eventfd read armed", __FILE__, __LINE__));
#endif
}

void Scheduler::drain() {
    // Ensure the eventfd_ read is always in flight so io_uring_enter's
    // GETEVENTS wakes on post() notifications as well as real I/O CQEs.
    if (!eventfd_armed_)
        arm_eventfd_read();

    uint32_t sq_head    = io_.sq.head->load(std::memory_order_acquire);
    uint32_t sq_tail    = io_.sq.tail->load(std::memory_order_relaxed);
    uint32_t sq_pending = sq_tail - sq_head;
#ifdef ENABLE_LOGGING
    log::debug(log::Message(__func__, std::format("submitting sq_pending=>{}", sq_pending), __FILE__, __LINE__));
#endif
    // Submit all pending SQEs and block until at least one CQE arrives.
    // The always-in-flight eventfd read guarantees we wake on post() too.
    uint32_t min_complete = shutting_down_ ? 0 : 1;
    syscall(SYS_io_uring_enter, io_.fd, sq_pending, min_complete, IORING_ENTER_GETEVENTS, nullptr, 0);

#ifdef ENABLE_LOGGING
    log::debug(log::Message(__func__, std::format("submitted sq_pending=>{}", sq_pending), __FILE__, __LINE__));
#endif

    uint32_t head = io_.cq.head->load(std::memory_order_acquire);
    uint32_t tail = io_.cq.tail->load(std::memory_order_acquire);
    uint32_t mask    = *io_.cq.mask;
    uint32_t current = head;
    while (current != tail) {
        const io_uring_cqe& cqe = io_.cq.cqes[current & mask];
        if (cqe.user_data == reinterpret_cast<uint64_t>(&eventfd_buf_)) {
            // Inbox notification from post(). Drain the inbox and rearm
            // so the next drain() call will again wait for post() activity.
#ifdef ENABLE_LOGGING
            log::debug(log::Message(__func__, "eventfd read CQE => drain_inbox", __FILE__, __LINE__));
#endif
            eventfd_armed_ = false;
            drain_inbox();
        } else {
            auto* awt = reinterpret_cast<Awaitable*>(cqe.user_data);
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
