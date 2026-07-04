#include <sys/syscall.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cstring>
#include <slim/common/io.h>
#include <slim/common/io/error_codes.h>

// Fallback definitions if compiling against older header packages
#ifndef IORING_FEAT_SINGLE_MMAP
#define IORING_FEAT_SINGLE_MMAP (1U << 0)
#endif

namespace slim::common {

IO::IO(uint32_t entries) {
    io_uring_params params{};
    std::memset(&params, 0, sizeof(params));

    // Core configurations: Clamp size and double CQ size to prevent overflows
    //params.flags = IORING_SETUP_CQSIZE | IORING_SETUP_CLAMP | IORING_SETUP_COOP_TASKRUN | IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN;
    params.flags      = IORING_SETUP_CQSIZE | IORING_SETUP_CLAMP;
    params.cq_entries = entries * 2;

    fd = static_cast<int>(syscall(SYS_io_uring_setup, entries, &params));
    if (fd < 0) throw io::IOException(io::ErrorStatus::IOSetupFailed);

    // 1. Map the SQ Ring
    sq.map_size = params.sq_off.array + params.sq_entries * sizeof(uint32_t);
    sq.map      = ::mmap(nullptr, sq.map_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_SQ_RING);
    if (sq.map == MAP_FAILED) {
        ::close(fd);
        throw io::IOException(io::ErrorStatus::IOMmapSqFailed);
    }

    // 2. Map the SQEs Array (Always separate from ring structures)
    sq.sqes_map_size = params.sq_entries * sizeof(io_uring_sqe);
    sq.sqes_map      = ::mmap(nullptr, sq.sqes_map_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_SQES);
    if (sq.sqes_map == MAP_FAILED) {
        ::munmap(sq.map, sq.map_size);
        ::close(fd);
        throw io::IOException(io::ErrorStatus::IOMmapSqesFailed);
    }

    // 3. Map the CQ Ring (Check for Single MMAP Optimization)
    if (params.features & IORING_FEAT_SINGLE_MMAP) {
        // Reuse the SQ mapping pointer! The CQ structures occupy the same shared memory allocation.
        cq.map = sq.map;
        cq.map_size = 0; // We don't track size for unmapping since sq.map owns it
    } else {
        // Fallback for legacy kernels
        cq.map_size = params.cq_off.cqes + params.cq_entries * sizeof(io_uring_cqe);
        cq.map      = ::mmap(nullptr, cq.map_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_CQ_RING);
        if (cq.map == MAP_FAILED) {
            ::munmap(sq.sqes_map, sq.sqes_map_size);
            ::munmap(sq.map, sq.map_size);
            ::close(fd);
            throw io::IOException(io::ErrorStatus::IOMmapCqFailed);
        }
    }

    // Wire SQ pointers
    auto* sqb  = static_cast<uint8_t*>(sq.map);
    sq.head    = reinterpret_cast<std::atomic<uint32_t>*>(sqb + params.sq_off.head);
    sq.tail    = reinterpret_cast<std::atomic<uint32_t>*>(sqb + params.sq_off.tail);
    sq.mask    = reinterpret_cast<uint32_t*>(sqb + params.sq_off.ring_mask);
    sq.entries = reinterpret_cast<uint32_t*>(sqb + params.sq_off.ring_entries);
    sq.flags   = reinterpret_cast<std::atomic<uint32_t>*>(sqb + params.sq_off.flags);
    sq.dropped = reinterpret_cast<uint32_t*>(sqb + params.sq_off.dropped);
    sq.array   = reinterpret_cast<uint32_t*>(sqb + params.sq_off.array);
    sq.sqes    = static_cast<io_uring_sqe*>(sq.sqes_map);

    // Wire CQ pointers
    auto* cqb  = static_cast<uint8_t*>(cq.map);
    cq.head    = reinterpret_cast<std::atomic<uint32_t>*>(cqb + params.cq_off.head);
    cq.tail    = reinterpret_cast<std::atomic<uint32_t>*>(cqb + params.cq_off.tail);
    cq.mask    = reinterpret_cast<uint32_t*>(cqb + params.cq_off.ring_mask);
    cq.entries = reinterpret_cast<uint32_t*>(cqb + params.cq_off.ring_entries);
    cq.cqes    = reinterpret_cast<io_uring_cqe*>(cqb + params.cq_off.cqes);
}

IO::~IO() {
    // Only unmap cq.map if it was explicitly allocated separately from sq.map
    if (cq.map && cq.map_size > 0) {
        ::munmap(cq.map, cq.map_size);
    }
    if (sq.sqes_map) ::munmap(sq.sqes_map, sq.sqes_map_size);
    if (sq.map)      ::munmap(sq.map,      sq.map_size);
    if (fd >= 0)     ::close(fd);
}

} // namespace slim::common
