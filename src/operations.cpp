#include <cstring>
#include <slim/common/io/operations.h>

namespace slim::common::io {

// ─── Accept ─────────────────────────────────────────────────────────────────

Accept::Accept(IO& io_ref, int sfd) : Awaitable(io_ref), server_fd(sfd) {}

void Accept::prepare(io_uring_sqe* sqe) noexcept {
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode       = IORING_OP_ACCEPT;
    sqe->fd           = server_fd;
    sqe->addr         = reinterpret_cast<uint64_t>(&addr);
    sqe->addr2        = reinterpret_cast<uint64_t>(&addr_len);
    sqe->accept_flags = SOCK_NONBLOCK | SOCK_CLOEXEC;
}

// ─── Close ──────────────────────────────────────────────────────────────────

Close::Close(IO& io_ref, int fd_) : Awaitable(io_ref), fd(fd_) {}

void Close::prepare(io_uring_sqe* sqe) noexcept {
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode = IORING_OP_CLOSE;
    sqe->fd     = fd;
}

// ─── Open ───────────────────────────────────────────────────────────────────

Open::Open(IO& io_ref, const char* path_, int flags_, mode_t mode_, int dfd_)
    : Awaitable(io_ref), dfd(dfd_), path(path_), flags(flags_), mode(mode_) {}

void Open::prepare(io_uring_sqe* sqe) noexcept {
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode     = IORING_OP_OPENAT;
    sqe->fd         = dfd;
    sqe->addr       = reinterpret_cast<uint64_t>(path);
    sqe->open_flags = static_cast<uint32_t>(flags);
    sqe->len        = mode;
}

// ─── Read ───────────────────────────────────────────────────────────────────

Read::Read(IO& io_ref, int fd_, void* buf_, size_t len_, uint64_t offset_)
    : Awaitable(io_ref), fd(fd_), buf(buf_), len(len_), offset(offset_) {}

void Read::prepare(io_uring_sqe* sqe) noexcept {
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode = IORING_OP_READ;
    sqe->fd     = fd;
    sqe->addr   = reinterpret_cast<uint64_t>(buf);
    sqe->len    = static_cast<uint32_t>(len);
    sqe->off    = offset;
}

// ─── Recv ───────────────────────────────────────────────────────────────────

Recv::Recv(IO& io_ref, int fd_, void* buf_, size_t len_, int flags_)
    : Awaitable(io_ref), fd(fd_), buf(buf_), len(len_), flags(flags_) {}

void Recv::prepare(io_uring_sqe* sqe) noexcept {
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode    = IORING_OP_RECV;
    sqe->fd        = fd;
    sqe->addr      = reinterpret_cast<uint64_t>(buf);
    sqe->len       = static_cast<uint32_t>(len);
    sqe->msg_flags = static_cast<uint32_t>(flags);
}

// ─── Send ───────────────────────────────────────────────────────────────────

Send::Send(IO& io_ref, int fd_, const void* buf_, size_t len_, int flags_)
    : Awaitable(io_ref), fd(fd_), buf(buf_), len(len_), flags(flags_) {}

void Send::prepare(io_uring_sqe* sqe) noexcept {
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode    = IORING_OP_SEND;
    sqe->fd        = fd;
    sqe->addr      = reinterpret_cast<uint64_t>(buf);
    sqe->len       = static_cast<uint32_t>(len);
    sqe->msg_flags = static_cast<uint32_t>(flags);
}

// ─── Stat ───────────────────────────────────────────────────────────────────

Stat::Stat(IO& io_ref, const char* path_, int flags_, uint32_t mask_, int dfd_)
    : Awaitable(io_ref), dfd(dfd_), path(path_), flags(flags_), mask(mask_) {}

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

Write::Write(IO& io_ref, int fd_, const void* buf_, size_t len_, uint64_t offset_)
    : Awaitable(io_ref), fd(fd_), buf(buf_), len(len_), offset(offset_) {}

void Write::prepare(io_uring_sqe* sqe) noexcept {
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode = IORING_OP_WRITE;
    sqe->fd     = fd;
    sqe->addr   = reinterpret_cast<uint64_t>(buf);
    sqe->len    = static_cast<uint32_t>(len);
    sqe->off    = offset;
}

} // namespace slim::common::io
