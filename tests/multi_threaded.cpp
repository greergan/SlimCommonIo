// Multi-threaded read test for slim::common::io
//
// Exercises Scheduler::post(), which the header documents as
// "Thread-safe — callable from any thread including V8". Several worker
// threads concurrently post() coroutine tasks that Open/Read/Close files
// under /etc, while a single dedicated thread drives the io_uring event
// loop via Scheduler::run(). This validates that concurrent submission
// from multiple threads into one IO ring / Scheduler works correctly.
//
// Framework: Catch2 v3.
//
// Build (example): link against Catch2::Catch2WithMain and the
// slim-common-io library; requires a Linux kernel with io_uring support.

#include <catch2/catch_test_macros.hpp>

#include <slim/common/io.h>
#include <slim/common/io/awaitable.h>
#include <slim/common/io/operations.h>
#include <slim/common/io/scheduler.h>
#include <slim/common/io/task.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using slim::common::IO;
using slim::common::io::Close;
using slim::common::io::Open;
using slim::common::io::Read;
using slim::common::io::Scheduler;
using slim::common::io::Task;

namespace {

// Thread-safe, timestamped progress logging so a run shows visible signs
// of life instead of going silent for the whole test (helpful for
// distinguishing "slow" from "hung").
void log_progress(const std::string& msg) {
    static std::mutex log_mutex;
    std::lock_guard<std::mutex> lock(log_mutex);
    static const auto start = std::chrono::steady_clock::now();
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - start)
                          .count();
    std::cerr << "[t+" << elapsed_ms << "ms] " << msg << std::endl;
}

} // namespace

namespace {

// Collects up to `max_files` readable regular files directly under /etc.
// Skips directories, symlinks, and anything we can't stat, so the test
// doesn't depend on the exact contents of /etc on any given machine.
std::vector<std::string> list_etc_files(const char* dir_path, size_t max_files) {
    std::vector<std::string> files;
    DIR* dir = opendir(dir_path);
    if (dir == nullptr) return files;

    while (dirent* entry = readdir(dir)) {
        if (files.size() >= max_files) break;
        std::string name = entry->d_name;
        if (name == "." || name == "..") continue;

        std::string full_path = std::string(dir_path) + "/" + name;

        struct stat st{};
        if (lstat(full_path.c_str(), &st) != 0) continue;
        if (!S_ISREG(st.st_mode)) continue; // regular files only

        files.push_back(full_path);
    }
    closedir(dir);
    return files;
}

// Tracks completion so the main thread knows when every spawned read
// has finished (or failed) without needing to join the coroutines
// directly, since Scheduler owns their lifetime once spawned.
struct ReadStats {
    std::atomic<int> attempted{0};
    std::atomic<int> succeeded{0};
    std::atomic<int> completed{0};
};

Task<void> read_one_file(Scheduler& scheduler, std::string path, ReadStats& stats) {
    Open open_op(scheduler, path.c_str(), O_RDONLY);
    int fd = co_await open_op;

    if (fd >= 0) {
        std::vector<uint32_t> buf(4096 / sizeof(uint32_t));
        Read read_op(scheduler, fd, buf);
        int n = co_await read_op;
        if (n >= 0) {
            stats.succeeded.fetch_add(1, std::memory_order_relaxed);
        }

        Close close_op(scheduler, fd);
        co_await close_op;
    }
    // Permission errors (EACCES) on some /etc files are expected and not
    // treated as failures here — we only require that the read either
    // succeeds or fails cleanly without crashing/hanging the scheduler.

    stats.completed.fetch_add(1, std::memory_order_relaxed);
    co_return;
}

} // namespace

TEST_CASE("Concurrent /etc reads from multiple threads", "[scheduler][multithread]") {
    constexpr size_t kMaxFiles    = 64;
    constexpr size_t kNumThreads  = 8;
    constexpr auto   kWaitTimeout = std::chrono::seconds(10);

    std::vector<std::string> files = list_etc_files("/etc", kMaxFiles);
    REQUIRE(!files.empty());
    log_progress("found " + std::to_string(files.size()) + " files under /etc");

    IO        io;
    Scheduler scheduler(io);
    ReadStats stats;
    stats.attempted.store(static_cast<int>(files.size()));

    // Drive the io_uring event loop on its own thread.
    std::stop_source stop_src;
    std::jthread     runner([&scheduler, &stop_src](std::stop_token) {
        scheduler.run(stop_src.get_token());
    });

    // Fan the files out across worker threads; each thread concurrently
    // calls the thread-safe post() to hand a coroutine spawn over to the
    // scheduler thread.
    std::vector<std::thread> workers;
    for (size_t t = 0; t < kNumThreads; ++t) {
        workers.emplace_back([&, t]() {
            for (size_t i = t; i < files.size(); i += kNumThreads) {
                std::string path = files[i];
                scheduler.post([&scheduler, &stats, path]() {
                    scheduler.spawn(read_one_file(scheduler, path, stats));
                });
            }
        });
    }
    for (auto& w : workers) w.join();
    log_progress("all post() calls issued from " + std::to_string(kNumThreads) + " threads, waiting for reads...");

    // Wait for all reads to complete, or time out.
    auto deadline   = std::chrono::steady_clock::now() + kWaitTimeout;
    auto last_log   = std::chrono::steady_clock::now();
    while (stats.completed.load(std::memory_order_relaxed) < static_cast<int>(files.size())) {
        auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            FAIL("Timed out waiting for concurrent /etc reads to complete ("
                 << stats.completed.load() << "/" << files.size() << " done)");
        }
        if (now - last_log >= std::chrono::milliseconds(250)) {
            log_progress(std::to_string(stats.completed.load()) + "/" + std::to_string(files.size()) +
                         " reads completed (" + std::to_string(stats.succeeded.load()) + " succeeded)");
            last_log = now;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    log_progress("all reads completed, shutting down scheduler");

    stop_src.request_stop();
    runner.join();
    scheduler.shutdown();
    log_progress("scheduler shut down cleanly");

    CHECK(stats.completed.load() == static_cast<int>(files.size()));
    // At least some files should be world-readable (e.g. /etc/hostname,
    // /etc/passwd), so we expect a non-zero success count even if a few
    // entries hit EACCES.
    CHECK(stats.succeeded.load() > 0);
}

TEST_CASE("Many threads hammering a single Scheduler::post", "[scheduler][multithread][stress]") {
    // Stress the post() thread-safety more aggressively: many threads,
    // each posting several reads of the same handful of files, to shake
    // out races in the inbox queue / mutex.
    constexpr size_t kNumThreads      = 16;
    constexpr size_t kPostsPerThread  = 20;
    constexpr auto   kWaitTimeout     = std::chrono::seconds(10);

    std::vector<std::string> files = list_etc_files("/etc", 8);
    REQUIRE(!files.empty());
    log_progress("found " + std::to_string(files.size()) + " files under /etc");

    IO        io;
    Scheduler scheduler(io);
    ReadStats stats;
    const int total_posts = static_cast<int>(kNumThreads * kPostsPerThread);
    stats.attempted.store(total_posts);

    std::stop_source stop_src;
    std::jthread     runner([&scheduler, &stop_src](std::stop_token) {
        scheduler.run(stop_src.get_token());
    });

    std::vector<std::thread> workers;
    for (size_t t = 0; t < kNumThreads; ++t) {
        workers.emplace_back([&, t]() {
            for (size_t i = 0; i < kPostsPerThread; ++i) {
                const std::string& path = files[(t + i) % files.size()];
                scheduler.post([&scheduler, &stats, path]() {
                    scheduler.spawn(read_one_file(scheduler, path, stats));
                });
            }
        });
    }
    for (auto& w : workers) w.join();
    log_progress("all " + std::to_string(total_posts) + " post() calls issued from " +
                 std::to_string(kNumThreads) + " threads, waiting for reads...");

    auto deadline = std::chrono::steady_clock::now() + kWaitTimeout;
    auto last_log = std::chrono::steady_clock::now();
    while (stats.completed.load(std::memory_order_relaxed) < total_posts) {
        auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            FAIL("Timed out (" << stats.completed.load() << "/" << total_posts << " done)");
        }
        if (now - last_log >= std::chrono::milliseconds(250)) {
            log_progress(std::to_string(stats.completed.load()) + "/" + std::to_string(total_posts) +
                         " reads completed (" + std::to_string(stats.succeeded.load()) + " succeeded)");
            last_log = now;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    log_progress("all reads completed, shutting down scheduler");

    stop_src.request_stop();
    runner.join();
    scheduler.shutdown();
    log_progress("scheduler shut down cleanly");

    CHECK(stats.completed.load() == total_posts);
    CHECK(stats.succeeded.load() > 0);
}

namespace {

// Per-worker completion tracking for the hand-off scenario: one
// "dispatcher" Scheduler receives work and hands it off to N independent
// worker Schedulers, each owning its own IO ring and its own thread.
struct PerWorkerStats {
    explicit PerWorkerStats(size_t num_workers)
        : succeeded(num_workers), completed(num_workers) {}
    std::vector<std::atomic<int>> succeeded;
    std::vector<std::atomic<int>> completed;
};

Task<void> read_one_file_on_worker(Scheduler& scheduler, std::string path, PerWorkerStats& stats, size_t worker_idx) {
    Open open_op(scheduler, path.c_str(), O_RDONLY);
    int fd = co_await open_op;

    if (fd >= 0) {
        std::vector<uint32_t> buf(4096 / sizeof(uint32_t));
        Read read_op(scheduler, fd, buf);
        int n = co_await read_op;
        if (n >= 0) {
            stats.succeeded[worker_idx].fetch_add(1, std::memory_order_relaxed);
        }

        Close close_op(scheduler, fd);
        co_await close_op;
    }

    stats.completed[worker_idx].fetch_add(1, std::memory_order_relaxed);
    co_return;
}

// A worker owns its own io_uring ring, its own Scheduler, and the thread
// that drives that scheduler's event loop.
struct WorkerNode {
    IO               io;
    Scheduler        scheduler{io};
    std::stop_source stop_src{};
    std::jthread     runner;

    WorkerNode() : runner([this](std::stop_token) { scheduler.run(stop_src.get_token()); }) {}

    void stop_and_join() {
        stop_src.request_stop();
        if (runner.joinable()) runner.join();
        scheduler.shutdown();
    }
};

} // namespace

TEST_CASE("One scheduler hands off work to multiple worker schedulers", "[scheduler][multithread][fanout]") {
    constexpr size_t kMaxFiles    = 64;
    constexpr size_t kNumWorkers  = 4;
    constexpr size_t  kNumClientThreads = 8;
    constexpr auto    kWaitTimeout      = std::chrono::seconds(10);

    std::vector<std::string> files = list_etc_files("/etc", kMaxFiles);
    REQUIRE(!files.empty());
    REQUIRE(files.size() >= kNumWorkers);
    log_progress("found " + std::to_string(files.size()) + " files under /etc");

    // Independent worker schedulers, each with its own IO ring / thread.
    std::vector<std::unique_ptr<WorkerNode>> workers;
    for (size_t i = 0; i < kNumWorkers; ++i) {
        workers.push_back(std::make_unique<WorkerNode>());
    }
    log_progress("spun up " + std::to_string(kNumWorkers) + " worker schedulers");

    // The dispatcher scheduler itself doesn't do any file I/O — it just
    // receives post()ed work from client threads and, on its own thread,
    // round-robins each job out to one of the worker schedulers via
    // their thread-safe post(). This models "a single scheduler that
    // hands off to multiple other schedulers".
    IO        dispatcher_io;
    Scheduler dispatcher(dispatcher_io);

    std::stop_source dispatcher_stop_src;
    std::jthread      dispatcher_runner([&dispatcher, &dispatcher_stop_src](std::stop_token) {
        dispatcher.run(dispatcher_stop_src.get_token());
    });

    PerWorkerStats  stats(kNumWorkers);
    std::atomic<size_t> next_worker{0};

    std::vector<std::thread> clients;
    for (size_t c = 0; c < kNumClientThreads; ++c) {
        clients.emplace_back([&, c]() {
            for (size_t i = c; i < files.size(); i += kNumClientThreads) {
                std::string path = files[i];
                dispatcher.post([&, path]() {
                    size_t idx = next_worker.fetch_add(1, std::memory_order_relaxed) % kNumWorkers;
                    WorkerNode& worker = *workers[idx];
                    worker.scheduler.post([&worker, &stats, path, idx]() {
                        worker.scheduler.spawn(read_one_file_on_worker(worker.scheduler, path, stats, idx));
                    });
                });
            }
        });
    }
    for (auto& c : clients) c.join();
    log_progress("all client posts issued from " + std::to_string(kNumClientThreads) +
                 " threads, waiting for hand-off reads...");

    auto total_completed = [&stats]() {
        int sum = 0;
        for (auto& c : stats.completed) sum += c.load(std::memory_order_relaxed);
        return sum;
    };

    auto deadline = std::chrono::steady_clock::now() + kWaitTimeout;
    auto last_log = std::chrono::steady_clock::now();
    while (total_completed() < static_cast<int>(files.size())) {
        auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            FAIL("Timed out waiting for hand-off reads to complete (" << total_completed() << "/" << files.size() << " done)");
        }
        if (now - last_log >= std::chrono::milliseconds(250)) {
            std::string per_worker;
            for (size_t i = 0; i < kNumWorkers; ++i) {
                per_worker += "w" + std::to_string(i) + "=" + std::to_string(stats.completed[i].load()) + " ";
            }
            log_progress(std::to_string(total_completed()) + "/" + std::to_string(files.size()) +
                         " reads completed [" + per_worker + "]");
            last_log = now;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    log_progress("all reads completed, shutting down dispatcher and workers");

    dispatcher_stop_src.request_stop();
    dispatcher_runner.join();
    dispatcher.shutdown();
    for (auto& w : workers) w->stop_and_join();
    log_progress("all schedulers shut down cleanly");

    CHECK(total_completed() == static_cast<int>(files.size()));

    int total_succeeded = 0;
    for (size_t i = 0; i < kNumWorkers; ++i) {
        // Every worker should have been handed at least one job, since
        // round-robin dispatch spreads files.size() >= kNumWorkers jobs
        // across all workers.
        CHECK(stats.completed[i].load() > 0);
        total_succeeded += stats.succeeded[i].load();
    }
    CHECK(total_succeeded > 0);
}
