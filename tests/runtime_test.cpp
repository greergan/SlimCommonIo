// Runtime test — same scenario as the existing
// "One scheduler hands off work to multiple worker schedulers" test,
// but driven through the Runtime class instead of hand-assembled
// dispatcher/WorkerNode objects.
//
// Framework: Catch2 v3.

#include <catch2/catch_test_macros.hpp>

#include <slim/common/io.h>
#include <slim/common/io/awaitable.h>
#include <slim/common/io/operations.h>
#include <slim/common/io/runtime.h>
#include <slim/common/io/scheduler.h>
#include <slim/common/io/task.h>

#include <atomic>
#include <chrono>
#include <dirent.h>
#include <fcntl.h>
#include <string>
#include <sys/stat.h>
#include <thread>
#include <vector>

using slim::common::io::Close;
using slim::common::io::Open;
using slim::common::io::Read;
using slim::common::io::Runtime;
using slim::common::io::Scheduler;
using slim::common::io::Task;

namespace {

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
        if (!S_ISREG(st.st_mode)) continue;

        files.push_back(full_path);
    }
    closedir(dir);
    return files;
}

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

} // namespace

TEST_CASE("Runtime stops cleanly with no pending work", "[runtime][stop]") {
    // dispatcher is idle, blocked in epoll_wait with nothing submitted —
    // stop() must wake it via the stop callback and return without hanging
    Runtime runtime(1);
    runtime.start();
    runtime.stop();
    SUCCEED();
}

TEST_CASE("Runtime hands off work from multiple client threads to worker schedulers", "[runtime][multithread]") {
    constexpr size_t kMaxFiles         = 64;
    constexpr size_t kNumWorkers       = 4;
    constexpr size_t kNumClientThreads = 8;
    constexpr auto   kWaitTimeout      = std::chrono::seconds(10);

    std::vector<std::string> files = list_etc_files("/etc", kMaxFiles);
    REQUIRE(!files.empty());
    REQUIRE(files.size() >= kNumWorkers);

    Runtime runtime(kNumWorkers);
    runtime.start();

    PerWorkerStats stats(kNumWorkers);

    std::vector<std::thread> clients;
    for (size_t c = 0; c < kNumClientThreads; ++c) {
        clients.emplace_back([&, c]() {
            for (size_t i = c; i < files.size(); i += kNumClientThreads) {
                std::string path = files[i];
                runtime.post([path, &stats](Scheduler& scheduler, size_t idx) {
                    scheduler.spawn(read_one_file_on_worker(scheduler, path, stats, idx));
                });
            }
        });
    }
    for (auto& c : clients) c.join();

    auto total_completed = [&stats]() {
        int sum = 0;
        for (auto& c : stats.completed) sum += c.load(std::memory_order_relaxed);
        return sum;
    };

    auto deadline = std::chrono::steady_clock::now() + kWaitTimeout;
    while (total_completed() < static_cast<int>(files.size())) {
        if (std::chrono::steady_clock::now() >= deadline) {
            FAIL("Timed out waiting for Runtime hand-off reads to complete ("
                 << total_completed() << "/" << files.size() << " done)");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    runtime.stop();

    CHECK(total_completed() == static_cast<int>(files.size()));

    int total_succeeded = 0;
    for (size_t i = 0; i < kNumWorkers; ++i) {
        CHECK(stats.completed[i].load() > 0);
        total_succeeded += stats.succeeded[i].load();
    }
    CHECK(total_succeeded > 0);
}

TEST_CASE("Runtime with a single worker still functions", "[runtime]") {
    Runtime runtime(1);
    runtime.start();

    PerWorkerStats stats(1);
    std::atomic<int> posted{0};

    runtime.post([&stats, &posted](Scheduler& scheduler, size_t idx) {
        scheduler.spawn(read_one_file_on_worker(scheduler, "/etc/hostname", stats, idx));
        posted.fetch_add(1, std::memory_order_relaxed);
    });

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (stats.completed[0].load(std::memory_order_relaxed) < 1) {
        if (std::chrono::steady_clock::now() >= deadline) {
            FAIL("Timed out waiting for single-worker Runtime read to complete");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    runtime.stop();

    CHECK(stats.completed[0].load() == 1);
}



#include <iostream>



TEST_CASE("Runtime can be stopped and restarted", "[runtime][restart]") {
    constexpr size_t kNumWorkers = 4;
    constexpr auto   kTimeout    = std::chrono::seconds(5);

    Runtime runtime(kNumWorkers);
    PerWorkerStats stats1(kNumWorkers);
    PerWorkerStats stats2(kNumWorkers);

    // first run
    runtime.start();
    std::cerr << "first run: posting\n" << std::flush;
    runtime.post([&stats1](Scheduler& scheduler, size_t idx) {
        std::cerr << "first run: job executing on worker " << idx << "\n" << std::flush;
        scheduler.spawn(read_one_file_on_worker(scheduler, "/etc/hostname", stats1, idx));
    });
    auto deadline = std::chrono::steady_clock::now() + kTimeout;
    while (stats1.completed[0].load(std::memory_order_relaxed) +
           stats1.completed[1].load(std::memory_order_relaxed) +
           stats1.completed[2].load(std::memory_order_relaxed) +
           stats1.completed[3].load(std::memory_order_relaxed) < 1) {
        if (std::chrono::steady_clock::now() >= deadline)
            FAIL("Timed out waiting for first run to complete");
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    std::cerr << "first run: completed\n" << std::flush;
    runtime.stop();
    std::cerr << "first run: stopped\n" << std::flush;

    // second run
    runtime.start();
    std::cerr << "second run: posting\n" << std::flush;
    runtime.post([&stats2](Scheduler& scheduler, size_t idx) {
        std::cerr << "second run: job executing on worker " << idx << "\n" << std::flush;
        scheduler.spawn(read_one_file_on_worker(scheduler, "/etc/hostname", stats2, idx));
    });
    deadline = std::chrono::steady_clock::now() + kTimeout;
    while (stats2.completed[0].load(std::memory_order_relaxed) +
           stats2.completed[1].load(std::memory_order_relaxed) +
           stats2.completed[2].load(std::memory_order_relaxed) +
           stats2.completed[3].load(std::memory_order_relaxed) < 1) {
        if (std::chrono::steady_clock::now() >= deadline)
            FAIL("Timed out waiting for second run to complete");
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    std::cerr << "second run: completed\n" << std::flush;
    runtime.stop();

    int total1 = 0, total2 = 0;
    for (size_t i = 0; i < kNumWorkers; ++i) {
        total1 += stats1.completed[i].load();
        total2 += stats2.completed[i].load();
    }
    CHECK(total1 == 1);
    CHECK(total2 == 1);
}
