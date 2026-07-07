#include <slim/common/io/runtime.h>
#include <slim/common/io/error_codes.h>

namespace slim::common::io {

Runtime::Runtime(size_t worker_count, uint32_t entries)
    : dispatcher_io_(entries), dispatcher_(dispatcher_io_) {
    workers_.reserve(worker_count);
    for (size_t i = 0; i < worker_count; ++i) {
        workers_.push_back(std::make_unique<WorkerNode>());
    }
}

Runtime::~Runtime() {
    if (state_ == State::Running) stop();
}

void Runtime::start() {
    if (state_ != State::Idle)
        throw IOException(ErrorStatus::RuntimeNotIdle);

    dispatcher_runner_ = std::jthread([this](std::stop_token) {
        dispatcher_.run(dispatcher_stop_src_.get_token());
    });

    state_ = State::Running;
}

void Runtime::stop() {
    if (state_ != State::Running) return;

    dispatcher_stop_src_.request_stop();
    if (dispatcher_runner_.joinable()) dispatcher_runner_.join();
    dispatcher_.shutdown();

    for (auto& w : workers_) w->stop_and_join();

    state_ = State::Stopped;
}

void Runtime::post(std::function<void(Scheduler&, size_t)> job) {
    dispatcher_.post([this, job = std::move(job)]() {
        size_t idx = next_worker_.fetch_add(1, std::memory_order_relaxed) % workers_.size();
        WorkerNode& w = *workers_[idx];
        w.scheduler.post([&w, idx, job]() {
            job(w.scheduler, idx);
        });
    });
}

} // namespace slim::common::io
