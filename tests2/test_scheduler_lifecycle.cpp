#include <catch2/catch_test_macros.hpp>
#include <catch2/reporters/catch_reporter_event_listener.hpp>
#include <catch2/reporters/catch_reporter_registrars.hpp>
#include <slim/common/io/runtime.h>
#include <slim/common/io/task.h>
#include <slim/common/io/operations.h>
#include <slim/common/network/client/tcp.h>
#include <openssl/ssl.h>
#include <unistd.h>
#include <future>

using slim::common::io::Runtime;
using slim::common::io::Scheduler;
using slim::common::io::Task;
using slim::common::io::Read;
using slim::common::io::Close;
using slim::common::network::client::tcp::Connection;

// ── SSL init ─────────────────────────────────────────────────────────────────

struct SslInitListener : Catch::EventListenerBase {
    using EventListenerBase::EventListenerBase;
    void testRunStarting(Catch::TestRunInfo const&) override {
        OPENSSL_init_ssl(OPENSSL_INIT_LOAD_SSL_STRINGS, nullptr);
    }
};
CATCH_REGISTER_LISTENER(SslInitListener)

// ── Helpers ───────────────────────────────────────────────────────────────────

static SSL_CTX* make_ssl_ctx() {
    SSL_CTX* ctx = SSL_CTX_new(TLS_client_method());
    if (ctx) SSL_CTX_set_default_verify_paths(ctx);
    return ctx;
}

// Coroutine that completes synchronously — no co_await on any io op.
static Task<void> synchronous_task(std::promise<void>& p) {
    p.set_value();
    co_return;
}

// Coroutine that opens a TLS connection to example.com:443 — exercises
// do_client_handshake via Connection::create on the CQE path.
static Task<void> tls_connect_task(Scheduler& scheduler, SSL_CTX* ssl_ctx, std::promise<bool>& p) {
    try {
        Connection conn = co_await Connection::create(scheduler, "example.com", 443, ssl_ctx);
        p.set_value(true);
    } catch (...) {
        p.set_value(false);
    }
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

    future.wait();

    REQUIRE_NOTHROW(runtime.stop());
}

TEST_CASE("Scheduler reaps TLS connection Task without accessing freed frame", "[scheduler][reap][tls]") {
    SSL_CTX* ssl_ctx = make_ssl_ctx();
    REQUIRE(ssl_ctx != nullptr);

    Runtime runtime(1);
    runtime.start();

    std::promise<bool> p;
    auto future = p.get_future();

    runtime.post([&](Scheduler& scheduler, size_t) {
        scheduler.spawn(tls_connect_task(scheduler, ssl_ctx, p));
    });

    bool connected = future.get();
    REQUIRE(connected == true);

    REQUIRE_NOTHROW(runtime.stop());

    SSL_CTX_free(ssl_ctx);
}
