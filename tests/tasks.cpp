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

// ─── Nested co_await of a Task<T> from another coroutine ──────────────────

TEST_CASE("Task<T> awaiter interface", "[io][task][awaiter]") {
    SECTION("Task<int> can be co_await-ed from another coroutine") {
        IO        io;
        Scheduler sched{io};

        auto inner = []() -> Task<int> { co_return 42; };

        int result{0};
        auto outer = [&]() -> Task<void> {
            result = co_await inner();
        };

        sched.spawn(outer());
        sched.shutdown();

        REQUIRE(result == 42);
    }

    SECTION("Task<void> can be co_await-ed from another coroutine") {
        IO        io;
        Scheduler sched{io};

        bool inner_ran{false};
        auto inner = [&]() -> Task<void> {
            inner_ran = true;
            co_return;
        };

        bool outer_ran{false};
        auto outer = [&]() -> Task<void> {
            co_await inner();
            outer_ran = true;
        };

        sched.spawn(outer());
        sched.shutdown();

        REQUIRE(inner_ran);
        REQUIRE(outer_ran);
    }

    SECTION("outer coroutine resumes only after the inner task actually completes") {
        IO        io;
        Scheduler sched{io};

        std::vector<int> order;
        auto inner = [&]() -> Task<int> {
            order.push_back(1);
            co_return 7;
        };

        int result{0};
        auto outer = [&]() -> Task<void> {
            order.push_back(0);
            result = co_await inner();
            order.push_back(2);
        };

        sched.spawn(outer());
        sched.shutdown();

        REQUIRE(result == 7);
        REQUIRE(order == std::vector<int>{0, 1, 2});
    }
}

// ─── Inner task that itself suspends on real I/O before completing ────────

TEST_CASE("Task<T> awaiter with an inner task that suspends on real I/O", "[io][task][awaiter]") {
    SECTION("outer coroutine correctly waits through the inner task's own Recv suspension") {
        IO        io;
        Scheduler sched{io};
        auto [reader, writer] = make_socket_pair();

        ::write(writer, "hello", 5);

        auto inner = [&]() -> Task<int> {
            char buf[16]{};
            int  n = co_await Recv{sched, reader, buf, sizeof(buf)};
            co_return n;
        };

        int result{0};
        auto outer = [&]() -> Task<void> {
            result = co_await inner();
        };

        sched.spawn(outer());
        sched.shutdown();

        REQUIRE(result == 5);

        ::close(reader);
        ::close(writer);
    }

    SECTION("multiple sequential nested I/O-bound tasks all complete in order") {
        IO        io;
        Scheduler sched{io};
        auto [reader, writer] = make_socket_pair();

        ::write(writer, "ab", 2);

        auto recv_one = [&](char* out) -> Task<int> {
            int n = co_await Recv{sched, reader, out, 1};
            co_return n;
        };

        char c1{}, c2{};
        auto outer = [&]() -> Task<void> {
            int n1 = co_await recv_one(&c1);
            int n2 = co_await recv_one(&c2);
            REQUIRE(n1 == 1);
            REQUIRE(n2 == 1);
        };

        sched.spawn(outer());
        sched.shutdown();

        REQUIRE(c1 == 'a');
        REQUIRE(c2 == 'b');

        ::close(reader);
        ::close(writer);
    }
}

// ─── Exception propagation through nested co_await ─────────────────────────

TEST_CASE("Task<T> awaiter propagates exceptions from the inner task", "[io][task][awaiter]") {
    SECTION("exception thrown inside a nested Task<int> propagates to the outer awaiter") {
        IO        io;
        Scheduler sched{io};

        auto inner = []() -> Task<int> {
            throw std::runtime_error("inner boom");
            co_return 0;
        };

        bool        caught{false};
        std::string what;
        auto outer = [&]() -> Task<void> {
            try {
                co_await inner();
            } catch (const std::runtime_error& e) {
                caught = true;
                what   = e.what();
            }
        };

        sched.spawn(outer());
        sched.shutdown();

        REQUIRE(caught);
        REQUIRE(what == "inner boom");
    }

    SECTION("exception thrown inside a nested Task<void> propagates to the outer awaiter") {
        IO        io;
        Scheduler sched{io};

        auto inner = []() -> Task<void> {
            throw std::runtime_error("void boom");
            co_return;
        };

        bool caught{false};
        auto outer = [&]() -> Task<void> {
            try {
                co_await inner();
            } catch (const std::runtime_error&) {
                caught = true;
            }
        };

        sched.spawn(outer());
        sched.shutdown();

        REQUIRE(caught);
    }

    SECTION("uncaught nested exception surfaces via the outer task's result()") {
        IO        io;
        Scheduler sched{io};

        auto inner = []() -> Task<int> {
            throw std::runtime_error("uncaught");
            co_return 0;
        };

        auto outer = [&]() -> Task<int> {
            int v = co_await inner();
            co_return v;
        };

        auto task = outer();
        task.resume();

        REQUIRE(task.done());
        REQUIRE_THROWS_AS(task.result(), std::runtime_error);
    }
}

// ─── Existing external-driver path still works unchanged ──────────────────

TEST_CASE("Task<T> external-driver path is unaffected by the new awaiter interface", "[io][task][awaiter]") {
    SECTION("a Task<int> never co_await-ed by anything still works via spawn()/resume()/result()") {
        auto task = []() -> Task<int> { co_return 99; }();
        task.resume();
        REQUIRE(task.done());
        REQUIRE(task.result() == 99);
    }

    SECTION("a Task<void> never co_await-ed by anything still works via spawn()/resume()/result()") {
        bool ran{false};
        auto task = [&]() -> Task<void> {
            ran = true;
            co_return;
        }();
        task.resume();
        REQUIRE(task.done());
        REQUIRE(ran);
        REQUIRE_NOTHROW(task.result());
    }

    SECTION("Scheduler::spawn() still drives a plain (non-nested) task to completion via I/O") {
        IO        io;
        Scheduler sched{io};
        auto [reader, writer] = make_socket_pair();

        ::write(writer, "x", 1);
        char buf[4]{};
        int  n{0};

        auto coro = [&]() -> Task<void> {
            n = co_await Recv{sched, reader, buf, sizeof(buf)};
        };
        sched.spawn(coro());
        sched.shutdown();

        REQUIRE(n == 1);
        REQUIRE(buf[0] == 'x');

        ::close(reader);
        ::close(writer);
    }
}

// ─── Multiple levels of nesting ────────────────────────────────────────────

TEST_CASE("Task<T> supports multiple levels of nested co_await", "[io][task][awaiter]") {
    SECTION("three levels deep: outer awaits middle awaits inner") {
        IO        io;
        Scheduler sched{io};

        auto inner = []() -> Task<int> { co_return 3; };

        auto middle = [&]() -> Task<int> {
            int v = co_await inner();
            co_return v + 1;
        };

        int result{0};
        auto outer = [&]() -> Task<void> {
            result = co_await middle();
        };

        sched.spawn(outer());
        sched.shutdown();

        REQUIRE(result == 4);
    }

    SECTION("three levels deep with real I/O suspension at the innermost level") {
        IO        io;
        Scheduler sched{io};
        auto [reader, writer] = make_socket_pair();

        ::write(writer, "z", 1);

        auto inner = [&]() -> Task<int> {
            char c{};
            int  n = co_await Recv{sched, reader, &c, 1};
            co_return n > 0 ? static_cast<int>(c) : -1;
        };

        auto middle = [&]() -> Task<int> {
            int v = co_await inner();
            co_return v;
        };

        int result{0};
        auto outer = [&]() -> Task<void> {
            result = co_await middle();
        };

        sched.spawn(outer());
        sched.shutdown();

        REQUIRE(result == static_cast<int>('z'));

        ::close(reader);
        ::close(writer);
    }

    SECTION("exception at the innermost level propagates through all three levels") {
        IO        io;
        Scheduler sched{io};

        auto inner = []() -> Task<int> {
            throw std::runtime_error("deep boom");
            co_return 0;
        };

        auto middle = [&]() -> Task<int> {
            int v = co_await inner();
            co_return v;
        };

        bool caught{false};
        auto outer = [&]() -> Task<void> {
            try {
                co_await middle();
            } catch (const std::runtime_error&) {
                caught = true;
            }
        };

        sched.spawn(outer());
        sched.shutdown();

        REQUIRE(caught);
    }
}
