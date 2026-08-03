#include <catch2/catch_test_macros.hpp>
#include <catch2/reporters/catch_reporter_event_listener.hpp>
#include <catch2/reporters/catch_reporter_registrars.hpp>
#include <slim/common/io/runtime.h>
#include <slim/common/io/task.h>
#include <slim/common/io/operations.h>
#include <openssl/ssl.h>
#include <unistd.h>
#include <future>

using slim::common::io::Runtime;
using slim::common::io::Scheduler;
using slim::common::io::Task;
using slim::common::io::Read;
using slim::common::io::Close;

// ── SSL init ─────────────────────────────────────────────────────────────────

struct SslInitListener : Catch::EventListenerBase {
    using EventListenerBase::EventListenerBase;
    void testRunStarting(Catch::TestRunInfo const&) override {
        OPENSSL_init_ssl(OPENSSL_INIT_LOAD_SSL_STRINGS, nullptr);
    }
};
CATCH_REGISTER_LISTENER(SslInitListener)

// ── Helpers ───────────────────────────────────────────────────────────────────

// Coroutine that completes synchronously — no co_await on any io op.
static Task<void> synchronous_task(std::promise<void>& p) {
    p.set_value();
    co_return;
}

// Coroutine that reads from a pipe fd — exercises the CQE path.
static Task<void> pipe_read_task(Scheduler& scheduler, int read_fd, std::promise<int>& p) {
    std::vector<uint32_t> buf(1);
    Read read_op(scheduler, read_fd, buf);
    int result = co_await read_op;
    p.set_value(result);
    Close close_op(scheduler, read_fd);
    co_await close_op;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

TEST_CASE("Scheduler reaps synchronously completed Task without crash", "[scheduler][reap][sync]") {
    Runtime runtime(1);
    runtime.start();

    std::promise<void> p;
    auto future = p.get_future();

    runtime.post([&p](Scheduler& scheduler, size_t) {
        scheduler.spawn(synchronous_task(p));
    });

    // Wait for coroutine to complete
    future.wait();

    // stop() drives shutdown() which calls reap() — must not crash
    REQUIRE_NOTHROW(runtime.stop());
}

TEST_CASE("Scheduler reaps CQE-path Task without accessing freed frame", "[scheduler][reap][cqe]") {
    Runtime runtime(1);
    runtime.start();

    int fds[2];
    REQUIRE(::pipe(fds) == 0);
    int read_fd  = fds[0];
    int write_fd = fds[1];

    std::promise<int> p;
    auto future = p.get_future();

    // Spawn coroutine that blocks on pipe read — exercises CQE path
    runtime.post([&](Scheduler& scheduler, size_t) {
        scheduler.spawn(pipe_read_task(scheduler, read_fd, p));
    });

    // Write one byte to unblock the read
    uint8_t byte = 0x42;
    REQUIRE(::write(write_fd, &byte, 1) == 1);
    ::close(write_fd);

    // Wait for coroutine to complete and get bytes_read result
    int bytes_read = future.get();
    REQUIRE(bytes_read > 0);

    // stop() drives reap() on the now-completed CQE-path coroutine — must not crash
    REQUIRE_NOTHROW(runtime.stop());
}
