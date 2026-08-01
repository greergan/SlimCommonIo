#include <cstring>
#include <slim/common/io/operations.h>

namespace slim::common::io {

// ─── Accept ─────────────────────────────────────────────────────────────────

Accept::Accept(Scheduler& scheduler, int server_fd) : Awaitable(scheduler), server_fd_(server_fd) {}

void Accept::prepare(io_uring_sqe* sqe) noexcept {
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode       = IORING_OP_ACCEPT;
    sqe->fd           = server_fd_;
    sqe->addr         = reinterpret_cast<uint64_t>(&addr_);
    sqe->addr2        = reinterpret_cast<uint64_t>(&addr_len_);
    sqe->accept_flags = SOCK_NONBLOCK | SOCK_CLOEXEC;
}

// ─── Close ──────────────────────────────────────────────────────────────────

Close::Close(Scheduler& scheduler, int fd) : Awaitable(scheduler), fd_(fd) {}

void Close::prepare(io_uring_sqe* sqe) noexcept {
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode = IORING_OP_CLOSE;
    sqe->fd     = fd_;
}

// ─── Connect ────────────────────────────────────────────────────────────────

Connect::Connect(Scheduler& scheduler, int fd, const sockaddr* addr, socklen_t addr_len)
    : Awaitable(scheduler), fd_(fd), addr_len_(addr_len) {
    // Copy the caller's sockaddr into our own storage. The Connect object
    // (like Accept) must own its addr buffer for the full lifetime of the
    // operation, since the kernel reads it asynchronously and the caller's
    // original sockaddr may go out of scope before the coroutine resumes.
    memcpy(&addr_, addr, addr_len);
}

void Connect::prepare(io_uring_sqe* sqe) noexcept {
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode = IORING_OP_CONNECT;
    sqe->fd     = fd_;
    sqe->addr   = reinterpret_cast<uint64_t>(&addr_);
    sqe->off    = addr_len_;
}

// ─── Open ───────────────────────────────────────────────────────────────────

Open::Open(Scheduler& scheduler, std::string_view path, int flags, mode_t mode, int dfd)
    : Awaitable(scheduler), dfd_(dfd), path_(path), flags_(flags), mode_(mode) {}

void Open::prepare(io_uring_sqe* sqe) noexcept {
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode     = IORING_OP_OPENAT;
    sqe->fd         = dfd_;
    sqe->addr       = reinterpret_cast<uint64_t>(path_.data());
    sqe->open_flags = static_cast<uint32_t>(flags_);
    sqe->len        = mode_;
}

// ─── Poll ───────────────────────────────────────────────────────────────────

Poll::Poll(Scheduler& scheduler, int fd, uint32_t poll_mask)
    : Awaitable(scheduler), fd_(fd), poll_mask_(poll_mask) {}

void Poll::prepare(io_uring_sqe* sqe) noexcept {
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode        = IORING_OP_POLL_ADD;
    sqe->fd            = fd_;
    sqe->poll32_events = poll_mask_;
}

// ─── Read ───────────────────────────────────────────────────────────────────

Read::Read(Scheduler& scheduler, int fd, std::span<uint32_t> buf, uint64_t offset)
    : Awaitable(scheduler), fd_(fd), buf_(buf), offset_(offset) {}

void Read::prepare(io_uring_sqe* sqe) noexcept {
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode = IORING_OP_READ;
    sqe->fd     = fd_;
    sqe->addr   = reinterpret_cast<uint64_t>(buf_.data());
    sqe->len    = static_cast<uint32_t>(buf_.size() * sizeof(uint32_t));
    sqe->off    = offset_;
}

// ─── Recv ───────────────────────────────────────────────────────────────────

Recv::Recv(Scheduler& scheduler, int fd, void* buf, size_t len, int flags)
    : Awaitable(scheduler), fd_(fd), buf_(buf), len_(len), flags_(flags) {}

void Recv::prepare(io_uring_sqe* sqe) noexcept {
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode    = IORING_OP_RECV;
    sqe->fd        = fd_;
    sqe->addr      = reinterpret_cast<uint64_t>(buf_);
    sqe->len       = static_cast<uint32_t>(len_);
    sqe->msg_flags = static_cast<uint32_t>(flags_);
}

// ─── Send ───────────────────────────────────────────────────────────────────

Send::Send(Scheduler& scheduler, int fd, const void* buf, size_t len, int flags)
    : Awaitable(scheduler), fd_(fd), buf_(buf), len_(len), flags_(flags) {}

void Send::prepare(io_uring_sqe* sqe) noexcept {
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode    = IORING_OP_SEND;
    sqe->fd        = fd_;
    sqe->addr      = reinterpret_cast<uint64_t>(buf_);
    sqe->len       = static_cast<uint32_t>(len_);
    sqe->msg_flags = static_cast<uint32_t>(flags_);
}

// ─── Stat ───────────────────────────────────────────────────────────────────

Stat::Stat(Scheduler& scheduler, std::string_view path, int flags, uint32_t mask, int dfd)
    : Awaitable(scheduler), dfd_(dfd), path_(path), flags_(flags), mask_(mask) {}

void Stat::prepare(io_uring_sqe* sqe) noexcept {
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode      = IORING_OP_STATX;
    sqe->fd          = dfd_;
    sqe->addr        = reinterpret_cast<uint64_t>(path_.data());
    sqe->addr2       = reinterpret_cast<uint64_t>(&buf_);
    sqe->statx_flags = static_cast<uint32_t>(flags_);
    sqe->len         = mask_;
}

// ─── Write ──────────────────────────────────────────────────────────────────

Write::Write(Scheduler& scheduler, int fd, std::span<const uint32_t> buf, uint64_t offset)
    : Awaitable(scheduler), fd_(fd), buf_(buf), offset_(offset) {}

void Write::prepare(io_uring_sqe* sqe) noexcept {
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode = IORING_OP_WRITE;
    sqe->fd     = fd_;
    sqe->addr   = reinterpret_cast<uint64_t>(buf_.data());
    sqe->len    = static_cast<uint32_t>(buf_.size() * sizeof(uint32_t));
    sqe->off    = offset_;
}

} // namespace slim::common::io
