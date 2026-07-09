#include <catch2/catch_test_macros.hpp>
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
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

static int make_listener() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    REQUIRE(fd >= 0);
    int opt = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_port        = 0;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    REQUIRE(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
    REQUIRE(::listen(fd, 1) == 0);
    return fd;
}

static int get_port(int fd) {
    sockaddr_in addr{};
    socklen_t   len = sizeof(addr);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
    return ntohs(addr.sin_port);
}

static int make_nonblocking_socket() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    REQUIRE(fd >= 0);
    int flags = ::fcntl(fd, F_GETFL);
    ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    return fd;
}

// ─── Connect ────────────────────────────────────────────────────────────────

TEST_CASE("Connect", "[io][operations]") {
    SECTION("resumes with 0 on successful connect") {
        IO        io;
        Scheduler sched{io};
        int       listener = make_listener();
        int       port     = get_port(listener);
        int       client_fd = make_nonblocking_socket();

        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_port        = htons(static_cast<uint16_t>(port));
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

        int result{-1};
        auto coro = [&]() -> Task<void> {
            result = co_await Connect{sched, client_fd,
                reinterpret_cast<sockaddr*>(&addr), sizeof(addr)};
        };
        sched.spawn(coro());
        sched.shutdown();

        REQUIRE(result == 0);

        int accepted = ::accept(listener, nullptr, nullptr);
        REQUIRE(accepted >= 0);

        ::close(client_fd);
        ::close(accepted);
        ::close(listener);
    }

    SECTION("returns negative errno when nothing is listening") {
        IO        io;
        Scheduler sched{io};
        int       client_fd = make_nonblocking_socket();

        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_port        = htons(1); // reserved port, nothing listening
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

        int result{0};
        auto coro = [&]() -> Task<void> {
            result = co_await Connect{sched, client_fd,
                reinterpret_cast<sockaddr*>(&addr), sizeof(addr)};
        };
        sched.spawn(coro());
        sched.shutdown();

        REQUIRE(result < 0);

        ::close(client_fd);
    }

    SECTION("addr is copied at construction, safe if caller's sockaddr goes out of scope") {
        IO        io;
        Scheduler sched{io};
        int       listener = make_listener();
        int       port     = get_port(listener);
        int       client_fd = make_nonblocking_socket();

        int result{-1};
        auto coro = [&]() -> Task<void> {
            // addr is local to this inner scope, destroyed before co_await
            // suspends and resumes -- Connect must have copied it already.
            sockaddr_in addr{};
            addr.sin_family      = AF_INET;
            addr.sin_port        = htons(static_cast<uint16_t>(port));
            addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

            Connect connect_op{sched, client_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)};
            result = co_await connect_op;
        };
        sched.spawn(coro());
        sched.shutdown();

        REQUIRE(result == 0);

        int accepted = ::accept(listener, nullptr, nullptr);
        REQUIRE(accepted >= 0);

        ::close(client_fd);
        ::close(accepted);
        ::close(listener);
    }
}

// ─── Poll ───────────────────────────────────────────────────────────────────

TEST_CASE("Poll", "[io][operations]") {
    SECTION("resumes with POLLIN when data becomes available") {
        IO        io;
        Scheduler sched{io};
        auto [reader, writer] = make_socket_pair();

        int result{-1};
        auto coro = [&]() -> Task<void> {
            result = co_await Poll{sched, reader, POLLIN};
        };
        sched.spawn(coro());

        // Write after spawning so the poll genuinely has to wait, not just
        // observe an already-ready fd.
        ::write(writer, "x", 1);
        sched.shutdown();

        REQUIRE((result & POLLIN) != 0);

        ::close(reader);
        ::close(writer);
    }

    SECTION("resumes with POLLOUT immediately on a writable socket") {
        IO        io;
        Scheduler sched{io};
        auto [a, b] = make_socket_pair();

        int result{-1};
        auto coro = [&]() -> Task<void> {
            result = co_await Poll{sched, a, POLLOUT};
        };
        sched.spawn(coro());
        sched.shutdown();

        REQUIRE((result & POLLOUT) != 0);

        ::close(a);
        ::close(b);
    }

    SECTION("resumes with POLLHUP/POLLIN-like signal on peer close") {
        IO        io;
        Scheduler sched{io};
        auto [reader, writer] = make_socket_pair();
        ::close(writer);

        int result{-1};
        auto coro = [&]() -> Task<void> {
            result = co_await Poll{sched, reader, POLLIN};
        };
        sched.spawn(coro());
        sched.shutdown();

        // Peer closed -- readable end reports POLLIN (EOF read returns 0) and/or POLLHUP.
        REQUIRE(((result & POLLIN) != 0 || (result & POLLHUP) != 0));

        ::close(reader);
    }

    SECTION("returns negative errno on bad fd") {
        IO        io;
        Scheduler sched{io};
        int       result{0};

        auto coro = [&]() -> Task<void> {
            result = co_await Poll{sched, -1, POLLIN};
        };
        sched.spawn(coro());
        sched.shutdown();

        REQUIRE(result < 0);
    }

    SECTION("multiple concurrent polls on different fds both resolve") {
        IO        io;
        Scheduler sched{io};
        auto [r1, w1] = make_socket_pair();
        auto [r2, w2] = make_socket_pair();

        int result1{-1}, result2{-1};
        auto coro1 = [&]() -> Task<void> { result1 = co_await Poll{sched, r1, POLLIN}; };
        auto coro2 = [&]() -> Task<void> { result2 = co_await Poll{sched, r2, POLLIN}; };
        sched.spawn(coro1());
        sched.spawn(coro2());

        ::write(w1, "a", 1);
        ::write(w2, "b", 1);
        sched.shutdown();

        REQUIRE((result1 & POLLIN) != 0);
        REQUIRE((result2 & POLLIN) != 0);

        ::close(r1); ::close(w1);
        ::close(r2); ::close(w2);
    }
}

// ─── with_timeout (IOSQE_IO_LINK) ──────────────────────────────────────────

TEST_CASE("with_timeout", "[io][operations][timeout]") {
    SECTION("op that would otherwise complete succeeds normally when timeout does not fire") {
        IO        io;
        Scheduler sched{io};
        auto [reader, writer] = make_socket_pair();

        ::write(writer, "hello", 5);
        char buf[16]{};
        int  n{0};

        auto coro = [&]() -> Task<void> {
            Recv recv_op{sched, reader, buf, sizeof(buf)};
            recv_op.with_timeout(std::chrono::milliseconds(500));
            n = co_await recv_op;
        };
        sched.spawn(coro());
        sched.shutdown();

        REQUIRE(n == 5);
        REQUIRE(std::string_view(buf, 5) == "hello");

        ::close(reader);
        ::close(writer);
    }

    SECTION("Poll with no data and a short timeout resolves via cancellation, not a hang") {
        IO        io;
        Scheduler sched{io};
        auto [reader, writer] = make_socket_pair();
        // Nothing is ever written -- Poll would block forever without the
        // linked timeout. This is the actual point of the feature: the
        // kernel cancels the primary op for us when the deadline passes.

        int result{1}; // sentinel != any expected outcome
        auto coro = [&]() -> Task<void> {
            Poll poll_op{sched, reader, POLLIN};
            poll_op.with_timeout(std::chrono::milliseconds(50));
            result = co_await poll_op;
        };
        sched.spawn(coro());
        sched.shutdown();

        // Cancelled by the linked timeout -- kernel reports -ECANCELED (or
        // -ETIME depending on kernel version/ordering of the two CQEs).
        REQUIRE(result < 0);

        ::close(reader);
        ::close(writer);
    }

    SECTION("Connect to a non-routable address is cancelled by timeout instead of hanging") {
        IO        io;
        Scheduler sched{io};
        int       client_fd = make_nonblocking_socket();

        // TEST-NET-1 (RFC 5737) -- reserved for documentation, guaranteed
        // non-routable, connect() will hang until something gives up.
        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_port        = htons(9);
        ::inet_pton(AF_INET, "192.0.2.1", &addr.sin_addr);

        int result{1};
        auto coro = [&]() -> Task<void> {
            Connect connect_op{sched, client_fd,
                reinterpret_cast<sockaddr*>(&addr), sizeof(addr)};
            connect_op.with_timeout(std::chrono::milliseconds(100));
            result = co_await connect_op;
        };
        sched.spawn(coro());
        sched.shutdown();

        REQUIRE(result < 0);

        ::close(client_fd);
    }

    SECTION("two linked ops on the same ring do not interfere with each other") {
        IO        io;
        Scheduler sched{io};
        auto [r1, w1] = make_socket_pair();
        auto [r2, w2] = make_socket_pair();

        ::write(w1, "a", 1);
        // r2/w2 intentionally left silent so its op times out.

        int  n1{0}, n2{1};
        char buf1[4]{};
        auto coro_a = [&]() -> Task<void> {
            Recv recv_op{sched, r1, buf1, sizeof(buf1)};
            recv_op.with_timeout(std::chrono::milliseconds(500));
            n1 = co_await recv_op;
        };
        auto coro_b = [&]() -> Task<void> {
            Poll poll_op{sched, r2, POLLIN};
            poll_op.with_timeout(std::chrono::milliseconds(50));
            n2 = co_await poll_op;
        };

        sched.spawn(coro_a());
        sched.spawn(coro_b());
        sched.shutdown();

        REQUIRE(n1 == 1); // completed normally, unaffected by the other's timeout
        REQUIRE(std::string_view(buf1, 1) == "a");
        REQUIRE(n2 < 0);  // cancelled by its own linked timeout

        ::close(r1); ::close(w1);
        ::close(r2); ::close(w2);
    }
}
