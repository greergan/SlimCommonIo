#include <catch2/catch_test_macros.hpp>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <slim/common/io.h>
#include <slim/common/io/awaitable.h>
#include <slim/common/io/error_codes.h>
#include <slim/common/io/operations.h>
#include <slim/common/io/scheduler.h>
#include <slim/common/io/task.h>

using namespace slim::common;
using namespace slim::common::io;

// ─── Helpers ────────────────────────────────────────────────────────────────

static std::pair<int, int> make_socket_pair() {
    int sv[2];
    REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);
    return {sv[0], sv[1]};
}

// ─── Tests ──────────────────────────────────────────────────────────────────

TEST_CASE("Scheduler runs multiple tasks concurrently", "[io][scheduler]") {
    IO        io;
    Scheduler sched{io};

    SECTION("two independent recv operations both complete") {
        auto [r1, w1] = make_socket_pair();
        auto [r2, w2] = make_socket_pair();
        ::write(w1, "a", 1);
        ::write(w2, "b", 1);

        char buf1[4]{}, buf2[4]{};
        int  n1{0}, n2{0};

        // NOTE: Bind lambdas to named variables before spawning.
        // Passing an inline temporary lambda directly to spawn() is UB under GCC:
        // the compiler may elide the closure copy into the coroutine frame, leaving
        // the frame with a dangling reference once the temporary is destroyed.
        auto coro1 = [&]() -> Task<void> {
            n1 = co_await Recv{io, r1, buf1, sizeof(buf1)};
        };
        auto coro2 = [&]() -> Task<void> {
            n2 = co_await Recv{io, r2, buf2, sizeof(buf2)};
        };

        sched.spawn(coro1());
        sched.spawn(coro2());
        sched.shutdown();

        CHECK(n1 == 1);
        CHECK(n2 == 1);
        CHECK(buf1[0] == 'a');
        CHECK(buf2[0] == 'b');

        ::close(r1);
        ::close(w1);
        ::close(r2);
        ::close(w2);
    }
}
