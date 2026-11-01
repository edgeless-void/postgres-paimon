// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <stdint.h>

/*
 * Wire protocol between the PostgreSQL TAM (client) and the standalone
 * Paimon daemon.  All multi-byte integers are little-endian unless noted.
 *
 * Every message starts with:
 *   [4 bytes: total message length including these 4 bytes]
 *   [1 byte:  PaimonMsgType]
 *   [payload...]
 *
 * All messages are fire-and-forget: a backend queues them for the duration
 * of its transaction and writes them to the ring at COMMIT, followed by a
 * PMSG_SYNC (see paimon_client.h).  Nothing is sent for aborted work.
 *
 * Table identifiers ("table_name" below) are "<paimon db>\0<relname>":
 * PostgreSQL schema "public" → Paimon database "default", any other schema →
 * the database of the same name.  A name without '\0' (older builds,
 * recovery journals) means database "default".
 *
 * PMSG_DDL_CREATE carries the table's full current schema; the bgworker
 * creates the table or reconciles an existing one to it.  ADD_COL / DROP_COL
 * / RENAME / TYPE are no longer produced; the bgworker still accepts them.
 */

typedef enum PaimonMsgType {
    PMSG_DML_BATCH     = 1,  /* batch of INSERT/UPDATE/DELETE rows     */
    PMSG_DDL_CREATE    = 2,  /* CREATE TABLE — initial schema           */
    PMSG_DDL_ADD_COL   = 3,  /* ADD COLUMN                              */
    PMSG_DDL_DROP_COL  = 4,  /* DROP COLUMN                             */
    PMSG_DDL_RENAME    = 5,  /* RENAME COLUMN                           */
    PMSG_DDL_TYPE      = 6,  /* ALTER COLUMN TYPE (widening only)       */
    PMSG_DDL_TRUNCATE  = 7,  /* TRUNCATE TABLE                          */
    PMSG_DDL_DROP_TBL  = 8,  /* DROP TABLE                              */
    PMSG_DDL_PK_CHANGE = 9,  /* PK constraint change — always blocked   */
    PMSG_SYNC          = 10  /* flush all pending writes, wait for ack  */
} PaimonMsgType;

typedef enum PaimonAckStatus {
    PACK_OK      = 0,
    PACK_ERROR   = 1,
    PACK_BLOCK   = 2   /* operation not allowed (e.g. PK change)       */
} PaimonAckStatus;

/* Row kind — matches Paimon's RowKind enum */
typedef enum PaimonRowKind {
    PROW_INSERT         = 0,
    PROW_UPDATE_BEFORE  = 1,
    PROW_UPDATE_AFTER   = 2,
    PROW_DELETE         = 3
} PaimonRowKind;

/*
 * Paimon field type codes sent over the wire.
 * The daemon maps these to Arrow data types and Paimon type strings.
 */
typedef enum PaimonTypeCode {
    PTYPE_BOOLEAN  =  1,
    PTYPE_SMALLINT =  2,
    PTYPE_INT      =  3,
    PTYPE_BIGINT   =  4,
    PTYPE_FLOAT    =  5,
    PTYPE_DOUBLE   =  6,
    PTYPE_DECIMAL  =  7,  /* typmod encodes (precision<<16)|scale */
    PTYPE_STRING   =  8,
    PTYPE_BYTES    =  9,
    PTYPE_DATE     = 10,  /* int32: days since 1970-01-01          */
    PTYPE_TIMESTAMP= 11,  /* int64: microseconds since 1970-01-01  */
    PTYPE_TIMESTAMPTZ=12,
    PTYPE_TEXT     = 13   /* alias for STRING                      */
} PaimonTypeCode;

/*
 * ── PMSG_DDL_CREATE payload ─────────────────────────────────────────────
 *
 *  u32  table_oid
 *  u16  name_len
 *  [name_len bytes]  table_name
 *  u32  field_count
 *  For each field:
 *    u16  attnum          (= Paimon field_id)
 *    u8   type_code       (PaimonTypeCode)
 *    u32  typmod          (type modifier, e.g. precision/scale for DECIMAL)
 *    u8   not_null
 *    u16  col_name_len
 *    [col_name_len bytes]
 *  u32  pk_count
 *  [pk_count * u16]  pk_attnums
 *
 * ── PMSG_DML_BATCH payload ──────────────────────────────────────────────
 *
 *  u32  table_oid
 *  u16  name_len
 *  [name_len bytes]  table_name
 *  u32  schema_version   (hash of TupleDesc; daemon re-checks on change)
 *  u32  row_count
 *  For each row:
 *    u8   row_kind        (PaimonRowKind)
 *    u64  seq             ((blkno << 16) | offset — used as sequence field)
 *    u16  ncols
 *    For each column:
 *      u16  attnum
 *      u8   type_code
 *      i32  val_len       (-1 = NULL; >=0 = byte length of value)
 *      [val_len bytes]    value in native little-endian binary
 *
 * ── PMSG_DDL_ADD_COL payload ────────────────────────────────────────────
 *  u32  table_oid
 *  u16  attnum
 *  u8   type_code
 *  u32  typmod
 *  u8   not_null
 *  u16  col_name_len
 *  [col_name_len bytes]
 *
 * ── PMSG_DDL_DROP_COL payload ───────────────────────────────────────────
 *  u32  table_oid
 *  u16  attnum
 *
 * ── PMSG_DDL_RENAME payload ─────────────────────────────────────────────
 *  u32  table_oid
 *  u16  attnum
 *  u16  new_name_len
 *  [new_name_len bytes]
 *
 * ── PMSG_DDL_TYPE payload ───────────────────────────────────────────────
 *  u32  table_oid
 *  u16  attnum
 *  u8   new_type_code
 *  u32  new_typmod
 *
 * ── PMSG_DDL_TRUNCATE / PMSG_DDL_DROP_TBL / PMSG_DDL_PK_CHANGE payload ─
 *  u32  table_oid
 *  u16  name_len
 *  [name_len bytes]
 *
 * ── ACK (daemon → TAM, for all DDL and PMSG_SYNC) ───────────────────────
 *  u32  total_len (= 8)
 *  u8   PMSG_ACK  (= 0)
 *  u8   status    (PaimonAckStatus)
 *  u16  msg_len
 *  [msg_len bytes]  error message (empty on OK)
 */

#define PAIMON_SOCK_ENV    "CLOUDDB_PAIMON_SOCK"
#define PAIMON_SOCK_DEFAULT "/tmp/clouddb-paimon.sock"
#define PAIMON_SOCK_BACKLOG 16

/* Max rows buffered per relation before forcing a flush to the daemon */
#define PAIMON_CLIENT_FLUSH_ROWS  1024
/* Max byte size of the in-memory row buffer before forcing flush */
#define PAIMON_CLIENT_FLUSH_BYTES (4 * 1024 * 1024)
/* Row buffer target for Parquet row groups in the daemon */
#define PAIMON_ROWGROUP_ROWS  65536
