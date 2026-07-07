// Runtime write test — single worker, single file, 4KB of PRNG data.
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
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using slim::common::IO;
using slim::common::io::Close;
using slim::common::io::Open;
using slim::common::io::Read;
using slim::common::io::Runtime;
using slim::common::io::Scheduler;
using slim::common::io::Task;
using slim::common::io::Write;

namespace {

constexpr size_t kBufSize = 4096;

Task<void> write_random_file(IO& io, std::string path, std::vector<uint8_t> data, std::atomic<int>& completed) {
    Open open_op(io, path.c_str(), O_CREAT | O_WRONLY | O_TRUNC);
    int fd = co_await open_op;

    if (fd >= 0) {
        Write write_op(io, fd, data.data(), data.size());
        co_await write_op;

        Close close_op(io, fd);
        co_await close_op;
    }

    completed.fetch_add(1, std::memory_order_relaxed);
    co_return;
}

} // namespace

TEST_CASE("Runtime writes random data to a file in /tmp", "[runtime][write]") {
    const std::string path = "/tmp/slim_io_runtime_write_test_" + std::to_string(getpid());

    // Generate 4KB of random data with a seeded PRNG.
    std::mt19937                          rng(12345);
    std::uniform_int_distribution<int>    dist(0, 255);
    std::vector<uint8_t>                  data(kBufSize);
    for (auto& byte : data) byte = static_cast<uint8_t>(dist(rng));

    Runtime runtime(1);
    runtime.start();

    std::atomic<int> completed{0};
    runtime.post([path, data, &completed](IO& io, Scheduler& scheduler, size_t /*worker_idx*/) {
        scheduler.spawn(write_random_file(io, path, data, completed));
    });

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (completed.load(std::memory_order_relaxed) < 1) {
        if (std::chrono::steady_clock::now() >= deadline) {
            FAIL("Timed out waiting for Runtime write to complete");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    runtime.stop();

    // Verify via plain read (not io_uring) from the test thread.
    int fd = ::open(path.c_str(), O_RDONLY);
    REQUIRE(fd >= 0);

    std::vector<uint8_t> read_back(kBufSize);
    ssize_t n = ::read(fd, read_back.data(), read_back.size());
    ::close(fd);

    CHECK(n == static_cast<ssize_t>(kBufSize));
    CHECK(std::memcmp(data.data(), read_back.data(), kBufSize) == 0);

    ::unlink(path.c_str());
}

TEST_CASE("Runtime writes random data to per-worker files with multiple workers", "[runtime][write][multithread]") {
    constexpr size_t kNumWorkers  = 8;
    constexpr size_t kFileSize    = 1024 * 1024; // 1MB per worker

    Runtime runtime(kNumWorkers);
    runtime.start();

    std::vector<std::string>          paths(kNumWorkers);
    std::vector<std::vector<uint8_t>> datas(kNumWorkers);
    std::atomic<int>                  completed{0};

    std::mt19937                       rng(67890);
    std::uniform_int_distribution<int> dist(0, 255);

    for (size_t i = 0; i < kNumWorkers; ++i) {
        paths[i] = "/tmp/slim_io_runtime_write_test_" + std::to_string(getpid()) + "_w" + std::to_string(i);
        datas[i].resize(kFileSize);
        for (auto& byte : datas[i]) byte = static_cast<uint8_t>(dist(rng));
    }

    for (size_t i = 0; i < kNumWorkers; ++i) {
        runtime.post([path = paths[i], data = datas[i], &completed](IO& io, Scheduler& scheduler, size_t /*worker_idx*/) {
            scheduler.spawn(write_random_file(io, path, data, completed));
        });
    }

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (completed.load(std::memory_order_relaxed) < static_cast<int>(kNumWorkers)) {
        if (std::chrono::steady_clock::now() >= deadline) {
            FAIL("Timed out waiting for multi-worker Runtime writes to complete ("
                 << completed.load() << "/" << kNumWorkers << " done)");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    runtime.stop();

    for (size_t i = 0; i < kNumWorkers; ++i) {
        int fd = ::open(paths[i].c_str(), O_RDONLY);
        REQUIRE(fd >= 0);

        std::vector<uint8_t> read_back(kFileSize);
        size_t               total_read = 0;
        while (total_read < kFileSize) {
            ssize_t n = ::read(fd, read_back.data() + total_read, kFileSize - total_read);
            if (n <= 0) break;
            total_read += static_cast<size_t>(n);
        }
        ::close(fd);

        CHECK(total_read == kFileSize);
        CHECK(std::memcmp(datas[i].data(), read_back.data(), kFileSize) == 0);

        ::unlink(paths[i].c_str());
    }
}

namespace {

Task<void> write_random_sized_file(IO& io, std::string path, std::vector<uint8_t> data, size_t job_idx,
                                    std::atomic<int>& order_counter, std::vector<int>& completion_order,
                                    std::atomic<int>& completed) {
    Open open_op(io, path.c_str(), O_CREAT | O_WRONLY | O_TRUNC);
    int fd = co_await open_op;

    if (fd >= 0) {
        Write write_op(io, fd, data.data(), data.size());
        co_await write_op;

        Close close_op(io, fd);
        co_await close_op;
    }

    completion_order[job_idx] = order_counter.fetch_add(1, std::memory_order_relaxed);
    completed.fetch_add(1, std::memory_order_relaxed);
    co_return;
}

} // namespace

TEST_CASE("Runtime handles jobs with random write sizes that may complete out of order", "[runtime][timing]") {
    constexpr size_t kNumJobs    = 8;
    constexpr size_t kNumWorkers = 4;
    constexpr size_t kMinSize    = 1024 * 1024;
    constexpr size_t kMaxSize    = 100 * 1024 * 1024;

    Runtime runtime(kNumWorkers);
    runtime.start();

    std::mt19937                        rng(13579);
    std::uniform_int_distribution<size_t> size_dist(kMinSize, kMaxSize);
    std::uniform_int_distribution<int>    byte_dist(0, 255);

    std::vector<std::string>          paths(kNumJobs);
    std::vector<std::vector<uint8_t>> datas(kNumJobs);
    for (size_t i = 0; i < kNumJobs; ++i) {
        paths[i] = "/tmp/slim_io_runtime_order_test_" + std::to_string(getpid()) + "_j" + std::to_string(i);
        datas[i].resize(size_dist(rng));
        for (auto& b : datas[i]) b = static_cast<uint8_t>(byte_dist(rng));
    }

    std::atomic<int> order_counter{0};
    std::vector<int> completion_order(kNumJobs, -1);
    std::atomic<int> completed{0};

    for (size_t i = 0; i < kNumJobs; ++i) {
        runtime.post([path = paths[i], data = datas[i], i, &order_counter, &completion_order, &completed](
                         IO& io, Scheduler& scheduler, size_t /*worker_idx*/) {
            scheduler.spawn(write_random_sized_file(io, path, data, i, order_counter, completion_order, completed));
        });
    }

    while (completed.load(std::memory_order_relaxed) < static_cast<int>(kNumJobs)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    runtime.stop();

    CHECK(completed.load() == static_cast<int>(kNumJobs));

    // Soft check only: log dispatch size vs. completion order, don't
    // assert a specific ordering (real scheduling/OS behavior isn't
    // deterministic enough to require out-of-order completion on every
    // run/machine). Printed unconditionally to stderr so the order is
    // visible on every run, not just on failure or with -s.
    std::cerr << "job_idx | size (bytes) | completion_order\n";
    for (size_t i = 0; i < kNumJobs; ++i) {
        std::cerr << i << " | " << datas[i].size() << " | " << completion_order[i] << "\n";
    }
    std::cerr << std::flush;

    for (size_t i = 0; i < kNumJobs; ++i) {
        CHECK(completion_order[i] >= 0);

        int fd = ::open(paths[i].c_str(), O_RDONLY);
        REQUIRE(fd >= 0);

        std::vector<uint8_t> read_back(datas[i].size());
        size_t               total_read = 0;
        while (total_read < read_back.size()) {
            ssize_t n = ::read(fd, read_back.data() + total_read, read_back.size() - total_read);
            if (n <= 0) break;
            total_read += static_cast<size_t>(n);
        }
        ::close(fd);

        CHECK(total_read == datas[i].size());
        CHECK(std::memcmp(datas[i].data(), read_back.data(), datas[i].size()) == 0);

        ::unlink(paths[i].c_str());
    }
}
