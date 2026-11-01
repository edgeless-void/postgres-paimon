// SPDX-License-Identifier: Apache-2.0
/*
 * paimon_ring.cpp — ring buffer write (producer) and read (consumer).
 */

extern "C" {
#include "postgres.h"
#include "storage/lwlock.h"
#include "storage/condition_variable.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "miscadmin.h"
#include "utils/wait_event.h"
}

#include "paimon_shmem.h"
#include "paimon_ring.h"

static uint32 g_wait_event = 0;

static uint32
ring_wait_event(void)
{
    if (!g_wait_event)
        g_wait_event = WaitEventExtensionNew("PaimonRingData");
    return g_wait_event;
}

/* Copy bytes into ring with wrap-around. */
static void
ring_copy_in(uint8_t *ring, uint32_t ring_size,
             uint64_t pos, const void *src, size_t len)
{
    uint32_t offset = (uint32_t)(pos % ring_size);
    size_t   tail   = ring_size - offset;
    if (len <= tail)
        memcpy(ring + offset, src, len);
    else
    {
        memcpy(ring + offset, src, tail);
        memcpy(ring, (const uint8_t *)src + tail, len - tail);
    }
}

/* Copy bytes out of ring with wrap-around. */
static void
ring_copy_out(const uint8_t *ring, uint32_t ring_size,
              uint64_t pos, void *dst, size_t len)
{
    uint32_t offset = (uint32_t)(pos % ring_size);
    size_t   tail   = ring_size - offset;
    if (len <= tail)
        memcpy(dst, ring + offset, len);
    else
    {
        memcpy(dst, ring + offset, tail);
        memcpy((uint8_t *)dst + tail, ring, len - tail);
    }
}

bool
paimon_ring_write(const void *data, size_t len)
{
    PaimonRingHdr *hdr  = paimon_ring_hdr();
    LWLock        *lock = paimon_ring_lock();
    uint8_t       *ring = paimon_ring_data();

    if (!hdr || !lock || !ring || len == 0 || len > (size_t)UINT32_MAX)
        return false;

    uint32_t ring_size = hdr->ring_size;

    LWLockAcquire(lock, LW_EXCLUSIVE);

    uint64_t used  = hdr->write_pos - hdr->read_pos;
    uint64_t avail = ring_size - used;

    if (avail < len)
    {
        LWLockRelease(lock);
        pg_atomic_fetch_add_u32(&hdr->dropped_msgs, 1);
        return false;
    }

    ring_copy_in(ring, ring_size, hdr->write_pos, data, len);
    hdr->write_pos += len;

    /* Read bgworker_pid before releasing the lock so we see a consistent value. */
    uint32 bgw_pid = pg_atomic_read_u32(&hdr->bgworker_pid);

    LWLockRelease(lock);
    ConditionVariableBroadcast(&hdr->has_data_cv);

    /* Wake the bgworker's latch so paimon_ring_read_msg_timeout returns promptly. */
    if (bgw_pid != 0) {
        PGPROC *proc = BackendPidGetProc((int) bgw_pid);
        if (proc != NULL)
            SetLatch(&proc->procLatch);
    }

    return true;
}

bool
paimon_ring_write_batch(const void *const *datas, const size_t *lens, int n)
{
    PaimonRingHdr *hdr  = paimon_ring_hdr();
    LWLock        *lock = paimon_ring_lock();
    uint8_t       *ring = paimon_ring_data();

    if (n <= 0)
        return true;
    if (!hdr || !lock || !ring)
        return false;

    uint64_t total = 0;
    for (int i = 0; i < n; i++)
    {
        if (lens[i] == 0 || lens[i] > (size_t)UINT32_MAX)
            return false;
        total += lens[i];
    }

    uint32_t ring_size = hdr->ring_size;

    LWLockAcquire(lock, LW_EXCLUSIVE);

    uint64_t used  = hdr->write_pos - hdr->read_pos;
    uint64_t avail = ring_size - used;

    /*
     * All-or-nothing: a transaction's messages either all reach the ring or
     * none do, so the bgworker never applies half of a transaction.
     */
    if (avail < total)
    {
        LWLockRelease(lock);
        pg_atomic_fetch_add_u32(&hdr->dropped_msgs, (uint32) n);
        return false;
    }

    for (int i = 0; i < n; i++)
    {
        ring_copy_in(ring, ring_size, hdr->write_pos, datas[i], lens[i]);
        hdr->write_pos += lens[i];
    }

    uint32 bgw_pid = pg_atomic_read_u32(&hdr->bgworker_pid);

    LWLockRelease(lock);
    ConditionVariableBroadcast(&hdr->has_data_cv);

    if (bgw_pid != 0) {
        PGPROC *proc = BackendPidGetProc((int) bgw_pid);
        if (proc != NULL)
            SetLatch(&proc->procLatch);
    }

    return true;
}

size_t
paimon_ring_read_msg(char **out_buf)
{
    PaimonRingHdr *hdr  = paimon_ring_hdr();
    LWLock        *lock = paimon_ring_lock();
    uint8_t       *ring = paimon_ring_data();

    if (!hdr || !lock || !ring)
        return 0;

    uint32_t ring_size = hdr->ring_size;
    uint32_t wait_ev   = ring_wait_event();

    for (;;)
    {
        CHECK_FOR_INTERRUPTS();

        LWLockAcquire(lock, LW_EXCLUSIVE);

        uint64_t avail = hdr->write_pos - hdr->read_pos;

        if (avail >= 4)
        {
            uint32_t total_len;
            ring_copy_out(ring, ring_size, hdr->read_pos,
                          &total_len, sizeof(total_len));

            if (total_len < 5 || total_len > ring_size)
            {
                hdr->read_pos++;    /* resync: skip one byte */
                LWLockRelease(lock);
                continue;
            }

            if (avail >= total_len)
            {
                char *buf = (char *) palloc(total_len);
                ring_copy_out(ring, ring_size, hdr->read_pos, buf, total_len);
                hdr->read_pos += total_len;
                LWLockRelease(lock);
                *out_buf = buf;
                return total_len;
            }
        }

        LWLockRelease(lock);
        ConditionVariableSleep(&hdr->has_data_cv, wait_ev);
    }
}

size_t
paimon_ring_read_msg_timeout(char **out_buf, long timeout_ms)
{
    PaimonRingHdr *hdr  = paimon_ring_hdr();
    LWLock        *lock = paimon_ring_lock();
    uint8_t       *ring = paimon_ring_data();

    if (!hdr || !lock || !ring)
        return 0;

    uint32_t ring_size = hdr->ring_size;
    uint32_t wait_ev   = ring_wait_event();

    for (;;)
    {
        CHECK_FOR_INTERRUPTS();

        LWLockAcquire(lock, LW_EXCLUSIVE);

        uint64_t avail = hdr->write_pos - hdr->read_pos;

        if (avail >= 4)
        {
            uint32_t total_len;
            ring_copy_out(ring, ring_size, hdr->read_pos,
                          &total_len, sizeof(total_len));

            if (total_len < 5 || total_len > ring_size)
            {
                hdr->read_pos++;
                LWLockRelease(lock);
                continue;
            }

            if (avail >= total_len)
            {
                char *buf = (char *) palloc(total_len);
                ring_copy_out(ring, ring_size, hdr->read_pos, buf, total_len);
                hdr->read_pos += total_len;
                LWLockRelease(lock);
                *out_buf = buf;
                return total_len;
            }
        }

        LWLockRelease(lock);

        int rc = WaitLatch(MyLatch,
                           WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                           timeout_ms,
                           wait_ev);
        ResetLatch(MyLatch);

        if (rc & WL_TIMEOUT)         return 0;
        if (rc & WL_EXIT_ON_PM_DEATH) return 0;
        /* WL_LATCH_SET: loop to check for messages */
    }
}
