// SPDX-License-Identifier: Apache-2.0
#pragma once
/*
 * paimon_ring.h — ring buffer producer / consumer API.
 *
 * Producers (backends) call paimon_ring_write() after building a complete
 * framed message in the same wire format used by the old socket protocol.
 * The write is protected by an LWLock and completes in microseconds.
 *
 * The consumer (bgworker) calls paimon_ring_read_msg() which blocks via
 * ConditionVariable until data is available, then returns one message.
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include <stdbool.h>

/*
 * Write one complete framed message to the ring.
 * data must be a framed message: [uint32 total_len][uint8 type][payload].
 * Non-blocking: returns false and increments dropped_msgs if the ring is full.
 */
bool   paimon_ring_write(const void *data, size_t len);

/*
 * Write n framed messages atomically: either all of them are appended to the
 * ring (contiguously, under one lock acquisition) or none are.  Used at
 * COMMIT so a transaction is never delivered partially.
 */
bool   paimon_ring_write_batch(const void *const *datas, const size_t *lens, int n);

/*
 * Read one complete framed message from the ring.
 * Blocks until data is available or shutdown is requested.
 * On success fills *out_buf (palloc'd, caller must pfree) and returns len > 0.
 * Returns 0 on shutdown.
 */
size_t paimon_ring_read_msg(char **out_buf);

/*
 * Like paimon_ring_read_msg but wakes up after timeout_ms milliseconds even
 * if no message is available.  Returns 0 on timeout or shutdown; the caller
 * should perform any periodic work (time-based flush, etc.) and retry.
 * Also wakes immediately when the ring writer signals via the bgworker_pid latch.
 */
size_t paimon_ring_read_msg_timeout(char **out_buf, long timeout_ms);

#ifdef __cplusplus
}
#endif
