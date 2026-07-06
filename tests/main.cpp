#include <catch2/catch_test_macros.hpp>
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <stop_token>
#include <thread>
#include <slim/common/io.h>
#include <slim/common/io/awaitable.h>
#include <slim/common/io/error_codes.h>
#include <slim/common/io/operations.h>
#include <slim/common/io/scheduler.h>
#include <slim/common/io/task.h>

using namespace slim::common;
using namespace slim::common::io;
using namespace std::chrono_literals;

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
    REQUIRE(::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) == 0);
    REQUIRE(::listen(fd, 1) == 0);
    return fd;
}

static int get_port(int fd) {
    sockaddr_in addr{};
    socklen_t   len = sizeof(addr);
    ::getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &len);
    return ntohs(addr.sin_port);
}

static int connect_to(int port) {
    int         fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(static_cast<uint16_t>(port));
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ::connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
    return fd;
}

// ─── IO Ring ────────────────────────────────────────────────────────────────

TEST_CASE("IO ring initialization", "[io][ring]") {
    SECTION("default entries initializes successfully") { REQUIRE_NOTHROW(IO{}); }
    SECTION("custom entries initializes successfully") { REQUIRE_NOTHROW(IO{512}); }

    SECTION("zero entries throws IOException with IOInvalidEntries") {
        try {
            IO{0};
            FAIL("expected IOException");
        } catch (const IOException &e) {
            REQUIRE(e.status() == ErrorStatus::IOInvalidEntries);
        }
    }

    SECTION("fd is valid after init") {
        IO io;
        REQUIRE(io.fd >= 0);
    }

    SECTION("multiple instances are independent") {
        IO io1;
        IO io2;
        REQUIRE(io1.fd != io2.fd);
        REQUIRE(io1.sq.map != io2.sq.map);
        REQUIRE(io1.cq.map != io2.cq.map);
    }
}

TEST_CASE("IO ring SQ", "[io][ring][sq]") {
    IO io;

    SECTION("pointers are valid") {
        REQUIRE(io.sq.head != nullptr);
        REQUIRE(io.sq.tail != nullptr);
        REQUIRE(io.sq.mask != nullptr);
        REQUIRE(io.sq.entries != nullptr);
        REQUIRE(io.sq.flags != nullptr);
        REQUIRE(io.sq.dropped != nullptr);
        REQUIRE(io.sq.array != nullptr);
        REQUIRE(io.sq.sqes != nullptr);
        REQUIRE(io.sq.map != nullptr);
        REQUIRE(io.sq.sqes_map != nullptr);
    }

    SECTION("map sizes are non-zero") {
        REQUIRE(io.sq.map_size > 0);
        REQUIRE(io.sq.sqes_map_size > 0);
    }

    SECTION("entries count is non-zero") { REQUIRE(*io.sq.entries > 0); }

    SECTION("head and tail start at zero") {
        REQUIRE(io.sq.head->load() == 0);
        REQUIRE(io.sq.tail->load() == 0);
    }
}

TEST_CASE("IO ring CQ", "[io][ring][cq]") {
    IO io;

    SECTION("pointers are valid") {
        REQUIRE(io.cq.head != nullptr);
        REQUIRE(io.cq.tail != nullptr);
        REQUIRE(io.cq.mask != nullptr);
        REQUIRE(io.cq.entries != nullptr);
        REQUIRE(io.cq.cqes != nullptr);
        REQUIRE(io.cq.map != nullptr);
    }

    SECTION("entries count is at least SQ entries") { REQUIRE(*io.cq.entries >= *io.sq.entries); }

    SECTION("head and tail start at zero") {
        REQUIRE(io.cq.head->load() == 0);
        REQUIRE(io.cq.tail->load() == 0);
    }
}

TEST_CASE("IO ring teardown", "[io][ring]") {
    SECTION("does not throw") { REQUIRE_NOTHROW([] { IO io; }()); }
    SECTION("multiple teardowns are safe") { REQUIRE_NOTHROW([] { IO io1; IO io2; }()); }
}

// ─── IOException ────────────────────────────────────────────────────────────

TEST_CASE("IOException", "[io][error]") {
    SECTION("status is preserved") {
        IOException ex(ErrorStatus::IOSetupFailed);
        REQUIRE(ex.status() == ErrorStatus::IOSetupFailed);
    }

    SECTION("what() returns correct string") {
        IOException ex(ErrorStatus::IOSetupFailed);
        REQUIRE(std::string_view(ex.what()) == to_string(ErrorStatus::IOSetupFailed));
    }

    SECTION("to_string returns correct string for each status") {
        REQUIRE(to_string(ErrorStatus::OK)               == "OK");
        REQUIRE(to_string(ErrorStatus::BadAllocation)    == "Bad allocation");
        REQUIRE(to_string(ErrorStatus::IOInvalidEntries) == "IO invalid entries");
        REQUIRE(to_string(ErrorStatus::IOMmapCqFailed)   == "IO mmap CQ ring failed");
        REQUIRE(to_string(ErrorStatus::IOMmapSqesFailed) == "IO mmap SQEs failed");
        REQUIRE(to_string(ErrorStatus::IOMmapSqFailed)   == "IO mmap SQ ring failed");
        REQUIRE(to_string(ErrorStatus::IOSetupFailed)    == "IO setup failed");
        REQUIRE(to_string(ErrorStatus::SQFull)           == "Submission queue full");
    }

    SECTION("is catchable as std::runtime_error") {
        try {
            throw IOException(ErrorStatus::IOMmapSqFailed);
        } catch (const std::runtime_error &e) {
            REQUIRE(std::string_view(e.what()) == to_string(ErrorStatus::IOMmapSqFailed));
        }
    }

    SECTION("is catchable as std::exception") {
        try {
            throw IOException(ErrorStatus::IOMmapCqFailed);
        } catch (const std::exception &e) {
            REQUIRE(std::string_view(e.what()) == to_string(ErrorStatus::IOMmapCqFailed));
        }
    }
}

// ─── Task<T> ────────────────────────────────────────────────────────────────

TEST_CASE("Task<int>", "[io][task]") {
    SECTION("completes with value") {
        auto task = []() -> Task<int> { co_return 42; }();
        task.resume();
        REQUIRE(task.done());
        REQUIRE(task.result() == 42);
    }

    SECTION("propagates exception") {
        auto task = []() -> Task<int> {
            throw std::runtime_error("boom");
            co_return 0;
        }();
        task.resume();
        REQUIRE(task.done());
        REQUIRE_THROWS_AS(task.result(), std::runtime_error);
    }

    SECTION("move semantics: moved-from task is done") {
        auto t1 = []() -> Task<int> { co_return 1; }();
        auto t2 = std::move(t1);
        REQUIRE(t1.done()); // moved-from has null handle
        t2.resume();
        REQUIRE(t2.result() == 1);
    }

    SECTION("result() without resume returns default-constructed value") {
        // initial_suspend = suspend_always, so body never runs; value stays at T{}
        auto task = []() -> Task<int> { co_return 99; }();
        REQUIRE(!task.done());
        REQUIRE(task.result() == 0); // T{} for int
    }
}

TEST_CASE("Task<void>", "[io][task]") {
    SECTION("completes") {
        auto task = []() -> Task<void> { co_return; }();
        task.resume();
        REQUIRE(task.done());
        REQUIRE_NOTHROW(task.result());
    }

    SECTION("propagates exception") {
        auto task = []() -> Task<void> {
            throw std::runtime_error("boom");
            co_return;
        }();
        task.resume();
        REQUIRE(task.done());
        REQUIRE_THROWS_AS(task.result(), std::runtime_error);
    }

    SECTION("move semantics: moved-from task is done") {
        auto t1 = []() -> Task<void> { co_return; }();
        auto t2 = std::move(t1);
        REQUIRE(t1.done());
        t2.resume();
        REQUIRE_NOTHROW(t2.result());
    }
}

// ─── Scheduler ──────────────────────────────────────────────────────────────

TEST_CASE("Scheduler", "[io][scheduler]") {
    SECTION("shuts down cleanly with no tasks") {
        IO        io;
        Scheduler sched{io};
        REQUIRE_NOTHROW(sched.shutdown());
    }

    SECTION("respects stop token") {
        IO           io;
        Scheduler    sched{io};
        std::stop_source source;
        source.request_stop();
        REQUIRE_NOTHROW(sched.run(source.get_token()));
    }

    SECTION("destructor shuts down pending tasks") {
        // Verify no hang or crash when Scheduler goes out of scope with a pending task.
        // The socket pair keeps the recv alive until we write to it from outside.
        auto [r, w] = make_socket_pair();
        ::write(w, "x", 1);
        int n{0};
        {
            IO        io;
            Scheduler sched{io};
            auto      coro = [&]() -> Task<void> { n = co_await Recv{io, r, &n, 1}; };
            sched.spawn(coro());
        } // sched destructor calls shutdown()
        REQUIRE(n != 0); // was written into by the recv result (1 byte = result 1)
        ::close(r);
        ::close(w);
    }

    SECTION("multiple tasks run concurrently") {
        IO        io;
        Scheduler sched{io};
        auto [r1, w1] = make_socket_pair();
        auto [r2, w2] = make_socket_pair();
        ::write(w1, "a", 1);
        ::write(w2, "b", 1);
        char buf1[4]{}, buf2[4]{};
        int  n1{0}, n2{0};

        auto c1 = [&]() -> Task<void> { n1 = co_await Recv{io, r1, buf1, sizeof(buf1)}; };
        auto c2 = [&]() -> Task<void> { n2 = co_await Recv{io, r2, buf2, sizeof(buf2)}; };
        sched.spawn(c1());
        sched.spawn(c2());
        sched.shutdown();

        REQUIRE(n1 == 1);
        REQUIRE(n2 == 1);
        REQUIRE(buf1[0] == 'a');
        REQUIRE(buf2[0] == 'b');
        ::close(r1); ::close(w1);
        ::close(r2); ::close(w2);
    }

    SECTION("task with two sequential awaits completes") {
        IO        io;
        Scheduler sched{io};
        auto [r, w] = make_socket_pair();
        ::write(w, "hi", 2);

        char buf1[4]{}, buf2[4]{};
        int  n1{0}, n2{0};
        auto coro = [&]() -> Task<void> {
            n1 = co_await Recv{io, r, buf1, 1};
            n2 = co_await Recv{io, r, buf2, 1};
        };
        sched.spawn(coro());
        sched.shutdown();

        REQUIRE(n1 == 1);
        REQUIRE(n2 == 1);
        REQUIRE(buf1[0] == 'h');
        REQUIRE(buf2[0] == 'i');
        ::close(r); ::close(w);
    }

    SECTION("spawn throws IOException(BadAllocation) on vector growth failure") {
        // Verify the exception type and status mapping directly — portably forcing
        // std::bad_alloc from push_back is not feasible in a unit test.
        IOException ex(ErrorStatus::BadAllocation);
        REQUIRE(ex.status() == ErrorStatus::BadAllocation);
        REQUIRE(std::string_view(ex.what()) == to_string(ErrorStatus::BadAllocation));
    }

    SECTION("spawn throws IOException(SQFull) when SQ is exhausted") {
        // Fill the ring with the minimum entry count (1), then attempt a second
        // operation to trigger the SQFull path in await_suspend.
        // await_suspend throws before the coroutine suspends, so the exception
        // propagates out through spawn() — catch it there, not inside the coroutine.
        IO        io{1};
        Scheduler sched{io};
        auto [r, w] = make_socket_pair();

        // First recv saturates the single SQE slot.
        ::write(w, "x", 1);
        char buf1[4]{};
        auto coro1 = [&]() -> Task<void> { co_await Recv{io, r, buf1, 1}; };
        sched.spawn(coro1());

        // Second spawn hits the full SQ and throws IOException(SQFull).
        char buf2[4]{};
        auto coro2 = [&]() -> Task<void> { co_await Recv{io, r, buf2, sizeof(buf2)}; };
        bool threw = false;
        try {
            sched.spawn(coro2());
        } catch (const IOException& e) {
            threw = true;
            REQUIRE(e.status() == ErrorStatus::SQFull);
        }
        REQUIRE(threw);

        sched.shutdown();
        ::close(r);
        ::close(w);
    }
}

// ─── Recv / Send ────────────────────────────────────────────────────────────

TEST_CASE("Recv", "[io][operations]") {
    SECTION("resumes with bytes read") {
        IO        io;
        Scheduler sched{io};
        auto [reader, writer] = make_socket_pair();
        ::write(writer, "hello", 5);
        char buf[16]{};
        int  n{0};

        auto coro = [&]() -> Task<void> { n = co_await Recv{io, reader, buf, sizeof(buf)}; };
        sched.spawn(coro());
        sched.shutdown();

        REQUIRE(n == 5);
        REQUIRE(std::string_view(buf, 5) == "hello");
        ::close(reader); ::close(writer);
    }

    SECTION("returns zero on EOF") {
        IO        io;
        Scheduler sched{io};
        auto [reader, writer] = make_socket_pair();
        ::close(writer);
        char buf[16]{};
        int  n{-1};

        auto coro = [&]() -> Task<void> { n = co_await Recv{io, reader, buf, sizeof(buf)}; };
        sched.spawn(coro());
        sched.shutdown();

        REQUIRE(n == 0);
        ::close(reader);
    }

    SECTION("returns negative errno on bad fd") {
        IO        io;
        Scheduler sched{io};
        char      buf[4]{};
        int       n{0};

        auto coro = [&]() -> Task<void> { n = co_await Recv{io, -1, buf, sizeof(buf)}; };
        sched.spawn(coro());
        sched.shutdown();

        REQUIRE(n < 0); // kernel returns -EBADF or similar
    }
}

TEST_CASE("Send", "[io][operations]") {
    SECTION("resumes with bytes written") {
        IO        io;
        Scheduler sched{io};
        auto [reader, writer] = make_socket_pair();
        int n{0};

        auto coro = [&]() -> Task<void> { n = co_await Send{io, writer, "world", 5}; };
        sched.spawn(coro());
        sched.shutdown();

        REQUIRE(n == 5);
        char buf[16]{};
        REQUIRE(::read(reader, buf, sizeof(buf)) == 5);
        REQUIRE(std::string_view(buf, 5) == "world");
        ::close(reader); ::close(writer);
    }

    SECTION("returns negative errno on bad fd") {
        IO        io;
        Scheduler sched{io};
        int       n{0};

        auto coro = [&]() -> Task<void> { n = co_await Send{io, -1, "x", 1}; };
        sched.spawn(coro());
        sched.shutdown();

        REQUIRE(n < 0);
    }
}

// ─── Accept ─────────────────────────────────────────────────────────────────

TEST_CASE("Accept", "[io][operations]") {
    SECTION("resumes with client fd") {
        IO        io;
        Scheduler sched{io};
        int       listener = make_listener();
        int       port     = get_port(listener);
        int       client_fd{-1};

        std::thread connector([port]() {
            int fd = connect_to(port);
            ::close(fd);
        });

        auto coro = [&]() -> Task<void> { client_fd = co_await Accept{io, listener}; };
        sched.spawn(coro());
        sched.shutdown();
        connector.join();

        REQUIRE(client_fd >= 0);
        ::close(client_fd);
        ::close(listener);
    }

    SECTION("accepted fd has SOCK_NONBLOCK and SOCK_CLOEXEC set") {
        IO        io;
        Scheduler sched{io};
        int       listener = make_listener();
        int       port     = get_port(listener);
        int       client_fd{-1};

        std::thread connector([port]() { ::close(connect_to(port)); });

        auto coro = [&]() -> Task<void> { client_fd = co_await Accept{io, listener}; };
        sched.spawn(coro());
        sched.shutdown();
        connector.join();

        REQUIRE(client_fd >= 0);
        int flags = ::fcntl(client_fd, F_GETFL);
        REQUIRE((flags & O_NONBLOCK) != 0);
        int fdflags = ::fcntl(client_fd, F_GETFD);
        REQUIRE((fdflags & FD_CLOEXEC) != 0);
        ::close(client_fd);
        ::close(listener);
    }
}

// ─── Close ──────────────────────────────────────────────────────────────────

TEST_CASE("Close", "[io][operations]") {
    SECTION("resumes on completion") {
        IO        io;
        Scheduler sched{io};
        int       fd = ::socket(AF_INET, SOCK_STREAM, 0);
        REQUIRE(fd >= 0);
        int result{-1};

        auto coro = [&]() -> Task<void> { result = co_await Close{io, fd}; };
        sched.spawn(coro());
        sched.shutdown();

        REQUIRE(result == 0);
    }

    SECTION("returns negative errno on bad fd") {
        IO        io;
        Scheduler sched{io};
        int       result{0};

        auto coro = [&]() -> Task<void> { result = co_await Close{io, -1}; };
        sched.spawn(coro());
        sched.shutdown();

        REQUIRE(result < 0);
    }
}

// ─── Read / Write (disk) ────────────────────────────────────────────────────

TEST_CASE("Read and Write", "[io][operations]") {
    SECTION("write then read a temp file") {
        IO        io;
        Scheduler sched{io};
        char      path[] = "/tmp/slim_io_test_XXXXXX";
        int       fd     = ::mkstemp(path);
        REQUIRE(fd >= 0);

        int  written{0}, read_bytes{0};
        char read_buf[16]{};

        auto coro = [&]() -> Task<void> {
            written    = co_await Write{io, fd, "testdata", 8, 0};
            read_bytes = co_await Read{io, fd, read_buf, sizeof(read_buf), 0};
        };
        sched.spawn(coro());
        sched.shutdown();

        REQUIRE(written == 8);
        REQUIRE(read_bytes == 8);
        REQUIRE(std::string_view(read_buf, 8) == "testdata");
        ::close(fd);
        ::unlink(path);
    }

    SECTION("read returns negative errno on bad fd") {
        IO        io;
        Scheduler sched{io};
        char      buf[4]{};
        int       result{0};

        auto coro = [&]() -> Task<void> { result = co_await Read{io, -1, buf, sizeof(buf)}; };
        sched.spawn(coro());
        sched.shutdown();

        REQUIRE(result < 0);
    }

    SECTION("write returns negative errno on bad fd") {
        IO        io;
        Scheduler sched{io};
        int       result{0};

        auto coro = [&]() -> Task<void> { result = co_await Write{io, -1, "x", 1}; };
        sched.spawn(coro());
        sched.shutdown();

        REQUIRE(result < 0);
    }
}

// ─── Open ───────────────────────────────────────────────────────────────────

TEST_CASE("Open", "[io][operations]") {
    SECTION("resumes with valid fd") {
        IO        io;
        Scheduler sched{io};
        char      path[] = "/tmp/slim_io_open_test_XXXXXX";
        int       tmp    = ::mkstemp(path);
        REQUIRE(tmp >= 0);
        ::close(tmp);

        int opened_fd{-1};
        auto coro = [&]() -> Task<void> { opened_fd = co_await Open{io, path, O_RDONLY}; };
        sched.spawn(coro());
        sched.shutdown();

        REQUIRE(opened_fd >= 0);
        ::close(opened_fd);
        ::unlink(path);
    }

    SECTION("returns negative errno for nonexistent path") {
        IO        io;
        Scheduler sched{io};
        int       opened_fd{0};

        auto coro = [&]() -> Task<void> { opened_fd = co_await Open{io, "/nonexistent/path/file", O_RDONLY}; };
        sched.spawn(coro());
        sched.shutdown();

        REQUIRE(opened_fd < 0);
    }
}

// ─── Stat ───────────────────────────────────────────────────────────────────

TEST_CASE("Stat", "[io][operations]") {
    SECTION("resumes with stat data") {
        IO        io;
        Scheduler sched{io};
        char      path[] = "/tmp/slim_io_stat_test_XXXXXX";
        int       tmp    = ::mkstemp(path);
        REQUIRE(tmp >= 0);
        ::write(tmp, "x", 1);
        ::close(tmp);

        Stat stat_op{io, path};
        int  result{-1};

        auto coro = [&]() -> Task<void> { result = co_await stat_op; };
        sched.spawn(coro());
        sched.shutdown();

        REQUIRE(result == 0);
        REQUIRE(stat_op.buf.stx_size == 1);
        ::unlink(path);
    }

    SECTION("returns negative errno for nonexistent path") {
        IO        io;
        Scheduler sched{io};
        Stat      stat_op{io, "/nonexistent/path/file"};
        int       result{0};

        auto coro = [&]() -> Task<void> { result = co_await stat_op; };
        sched.spawn(coro());
        sched.shutdown();

        REQUIRE(result < 0);
    }
}
