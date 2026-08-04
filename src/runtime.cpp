#include <slim/common/io/runtime.h>
#include <slim/common/io/error_codes.h>
#include <slim/common/log.h>

namespace slim::common::io {
using namespace slim::common;

Runtime::Runtime(size_t worker_count, uint32_t entries) : dispatcher_io_(entries), dispatcher_(dispatcher_io_) {
    workers_.reserve(worker_count);
    for (size_t i = 0; i < worker_count; ++i) {
        workers_.push_back(std::make_unique<WorkerNode>());
    }
}

Runtime::~Runtime() {
    if (state_ == State::Running) stop();
}

void Runtime::start() {
#ifdef ENABLE_LOGGING
    log::trace(log::Message(__func__, "begins", __FILE__, __LINE__));
#endif
    if (state_ != State::Idle && state_ != State::Stopped)
        throw IOException(ErrorStatus::RuntimeNotIdle);
    for (auto& w : workers_) {
        w->start();
    }
#ifdef ENABLE_LOGGING
    log::debug(log::Message(__func__, "workers started => " + std::to_string(workers_.size()), __FILE__, __LINE__));
#endif
    dispatcher_runner_ = std::jthread([this](std::stop_token stop_token) {
        dispatcher_.run(stop_token);
    });
#ifdef ENABLE_LOGGING
    log::debug(log::Message(__func__, "dispatcher_runner_ started", __FILE__, __LINE__));
#endif
    state_ = State::Running;
#ifdef ENABLE_LOGGING
    log::trace(log::Message(__func__, "ends", __FILE__, __LINE__));
#endif
}

void Runtime::stop() {
#ifdef ENABLE_LOGGING
    log::trace(log::Message(__func__, "begins", __FILE__, __LINE__));
#endif
    if (state_ != State::Running) return;
    dispatcher_runner_.request_stop();
#ifdef ENABLE_LOGGING
    log::debug(log::Message(__func__, "dispatcher_runner_ stop requested", __FILE__, __LINE__));
#endif
    if (dispatcher_runner_.joinable()) dispatcher_runner_.join();
#ifdef ENABLE_LOGGING
    log::debug(log::Message(__func__, "dispatcher_runner_ joined", __FILE__, __LINE__));
#endif
    dispatcher_.shutdown();
#ifdef ENABLE_LOGGING
    log::debug(log::Message(__func__, "dispatcher_ shutdown", __FILE__, __LINE__));
#endif
    for (auto& w : workers_) w->stop_and_join();
#ifdef ENABLE_LOGGING
    log::debug(log::Message(__func__, "workers stopped and joined", __FILE__, __LINE__));
#endif
    state_ = State::Stopped;
#ifdef ENABLE_LOGGING
    log::trace(log::Message(__func__, "ends", __FILE__, __LINE__));
#endif
}

void Runtime::post(std::function<void(Scheduler&, size_t)> job) {
#ifdef ENABLE_LOGGING
    log::trace(log::Message(__func__, "begins", __FILE__, __LINE__));
#endif
    dispatcher_.post([this, job = std::move(job)]() {
        size_t idx = next_worker_.fetch_add(1, std::memory_order_relaxed) % workers_.size();
        WorkerNode& w = *workers_[idx];
#ifdef ENABLE_LOGGING
        log::debug(log::Message(__func__, std::format("posting to worker idx=>{}", idx), __FILE__, __LINE__));
#endif
        w.scheduler.post([&w, idx, job]() {
            job(w.scheduler, idx);
        });
    });
#ifdef ENABLE_LOGGING
    log::trace(log::Message(__func__, "ends", __FILE__, __LINE__));
#endif
}

} // namespace slim::common::io
