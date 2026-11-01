// SPDX-License-Identifier: Apache-2.0
#pragma once
/*
 * paimon_relmeta.h — relation → lake metadata helpers shared by the TAM
 * (paimon_heap.cpp) and the DDL hooks (paimon_ddl.cpp).
 *
 * Include after the PostgreSQL headers (needs Relation / TupleDesc).
 */
#include <string>
#include <vector>
#include <cstdint>

#include "paimon_client.h"

/* Fingerprint of the mirrored part of a TupleDesc (+ metadata-field config). */
uint32_t paimon_schema_fingerprint(TupleDesc td);

/* Column list (incl. configured metadata fields) and PK attnums of rel. */
void paimon_build_col_meta(Relation rel,
                           std::vector<PaimonColumn> &cols,
                           std::vector<uint16_t>     &pk_attnums);

/*
 * Wire identifier of the lake table for rel: "<paimon db>\0<relname>", see
 * paimon_event.h.  PostgreSQL schema "public" maps to Paimon database
 * "default"; any other schema maps to the database of the same name.
 */
std::string paimon_rel_ident(Relation rel);
std::string paimon_rel_ident_oid(Oid relid);

/*
 * False for relations whose changes must not reach the lake: temporary
 * tables, and the transient "pg_temp_<oid>" heaps PostgreSQL builds while
 * rewriting a table (ALTER COLUMN TYPE, SET ACCESS METHOD, ...).
 */
bool paimon_rel_is_mirrored(Relation rel);
bool paimon_relid_is_mirrored(Oid relid);
