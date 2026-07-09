#include <cstring>
#include <slim/common/io/operations.h>

namespace slim::common::io {

// ─── Accept ─────────────────────────────────────────────────────────────────

Accept::Accept(Scheduler& scheduler, int sfd) : Awaitable(scheduler), server_fd(sfd) {}

void Accept::prepare(io_uring_sqe* sqe) noexcept {
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode       = IORING_OP_ACCEPT;
    sqe->fd           = server_fd;
    sqe->addr         = reinterpret_cast<uint64_t>(&addr);
    sqe->addr2        = reinterpret_cast<uint64_t>(&addr_len);
    sqe->accept_flags = SOCK_NONBLOCK | SOCK_CLOEXEC;
}

// ─── Close ──────────────────────────────────────────────────────────────────

Close::Close(Scheduler& scheduler, int fd_) : Awaitable(scheduler), fd(fd_) {}

void Close::prepare(io_uring_sqe* sqe) noexcept {
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode = IORING_OP_CLOSE;
    sqe->fd     = fd;
}

// ─── Connect ────────────────────────────────────────────────────────────────

Connect::Connect(Scheduler& scheduler, int fd_, const sockaddr* addr_, socklen_t addr_len_)
    : Awaitable(scheduler), fd(fd_), addr_len(addr_len_) {
    // Copy the caller's sockaddr into our own storage. The Connect object
    // (like Accept) must own its addr buffer for the full lifetime of the
    // operation, since the kernel reads it asynchronously and the caller's
    // original sockaddr may go out of scope before the coroutine resumes.
    memcpy(&addr, addr_, addr_len_);
}

void Connect::prepare(io_uring_sqe* sqe) noexcept {
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode = IORING_OP_CONNECT;
    sqe->fd     = fd;
    sqe->addr   = reinterpret_cast<uint64_t>(&addr);
    sqe->off    = addr_len;
}

// ─── Open ───────────────────────────────────────────────────────────────────

Open::Open(Scheduler& scheduler, const char* path_, int flags_, mode_t mode_, int dfd_)
    : Awaitable(scheduler), dfd(dfd_), path(path_), flags(flags_), mode(mode_) {}

void Open::prepare(io_uring_sqe* sqe) noexcept {
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode     = IORING_OP_OPENAT;
    sqe->fd         = dfd;
    sqe->addr       = reinterpret_cast<uint64_t>(path);
    sqe->open_flags = static_cast<uint32_t>(flags);
    sqe->len        = mode;
}

// ─── Poll ───────────────────────────────────────────────────────────────────

Poll::Poll(Scheduler& scheduler, int fd_, uint32_t poll_mask_)
    : Awaitable(scheduler), fd(fd_), poll_mask(poll_mask_) {}

void Poll::prepare(io_uring_sqe* sqe) noexcept {
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode        = IORING_OP_POLL_ADD;
    sqe->fd            = fd;
    sqe->poll32_events = poll_mask;
}

// ─── Read ───────────────────────────────────────────────────────────────────

Read::Read(Scheduler& scheduler, int fd_, std::span<uint32_t> buf_, uint64_t offset_)
    : Awaitable(scheduler), fd(fd_), buf(buf_), offset(offset_) {}

void Read::prepare(io_uring_sqe* sqe) noexcept {
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode = IORING_OP_READ;
    sqe->fd     = fd;
    sqe->addr   = reinterpret_cast<uint64_t>(buf.data());
    sqe->len    = static_cast<uint32_t>(buf.size() * sizeof(uint32_t));
    sqe->off    = offset;
}

// ─── Recv ───────────────────────────────────────────────────────────────────

Recv::Recv(Scheduler& scheduler, int fd_, void* buf_, size_t len_, int flags_)
    : Awaitable(scheduler), fd(fd_), buf(buf_), len(len_), flags(flags_) {}

void Recv::prepare(io_uring_sqe* sqe) noexcept {
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode    = IORING_OP_RECV;
    sqe->fd        = fd;
    sqe->addr      = reinterpret_cast<uint64_t>(buf);
    sqe->len       = static_cast<uint32_t>(len);
    sqe->msg_flags = static_cast<uint32_t>(flags);
}

// ─── Send ───────────────────────────────────────────────────────────────────

Send::Send(Scheduler& scheduler, int fd_, const void* buf_, size_t len_, int flags_)
    : Awaitable(scheduler), fd(fd_), buf(buf_), len(len_), flags(flags_) {}

void Send::prepare(io_uring_sqe* sqe) noexcept {
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode    = IORING_OP_SEND;
    sqe->fd        = fd;
    sqe->addr      = reinterpret_cast<uint64_t>(buf);
    sqe->len       = static_cast<uint32_t>(len);
    sqe->msg_flags = static_cast<uint32_t>(flags);
}

// ─── Stat ───────────────────────────────────────────────────────────────────

Stat::Stat(Scheduler& scheduler, const char* path_, int flags_, uint32_t mask_, int dfd_)
    : Awaitable(scheduler), dfd(dfd_), path(path_), flags(flags_), mask(mask_) {}

void Stat::prepare(io_uring_sqe* sqe) noexcept {
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode      = IORING_OP_STATX;
    sqe->fd          = dfd;
    sqe->addr        = reinterpret_cast<uint64_t>(path);
    sqe->addr2       = reinterpret_cast<uint64_t>(&buf);
    sqe->statx_flags = static_cast<uint32_t>(flags);
    sqe->len         = mask;
}

// ─── Write ──────────────────────────────────────────────────────────────────

Write::Write(Scheduler& scheduler, int fd_, std::span<const uint32_t> buf_, uint64_t offset_)
    : Awaitable(scheduler), fd(fd_), buf(buf_), offset(offset_) {}

void Write::prepare(io_uring_sqe* sqe) noexcept {
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode = IORING_OP_WRITE;
    sqe->fd     = fd;
    sqe->addr   = reinterpret_cast<uint64_t>(buf.data());
    sqe->len    = static_cast<uint32_t>(buf.size() * sizeof(uint32_t));
    sqe->off    = offset;
}

} // namespace slim::common::io
