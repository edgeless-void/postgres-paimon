// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <string>
#include <vector>
#include <cstdint>
#include "paimon_event.h"

namespace paimon {

struct FieldDef {
    uint16_t       field_id;        /* = pg attnum, stable across renames */
    std::string    name;
    PaimonTypeCode type_code;
    uint32_t       typmod  = 0;
    bool           not_null = false;
};

struct SchemaVersion {
    int64_t              id = 0;
    std::vector<FieldDef>      fields;
    int32_t              highest_field_id = 0;
    std::vector<std::string>   partition_keys;   /* empty = unpartitioned */
    std::vector<std::string>   primary_keys;     /* column names */
    /* options written verbatim into the schema JSON */
    /* merge-engine, sequence.field, changelog-producer are fixed */
};

/*
 * Manages the schema/ subdirectory inside a Paimon table root.
 *
 * On first use for a relation, schema-0 is created.  DDL operations
 * write schema-(N+1) and atomically bump LATEST.
 */
class PaimonSchemaManager {
public:
    explicit PaimonSchemaManager(const std::string& table_root);

    /* Load current schema from disk (or return cached). */
    const SchemaVersion& current() const { return current_; }

    /* True if schema-0 has been written. */
    bool exists() const { return current_.id >= 0 && !current_.fields.empty(); }

    /*
     * Bootstrap: write schema-0 from a CREATE event.  If the schema already
     * exists, reconcile() it to the incoming definition instead.
     */
    bool bootstrap(const std::vector<FieldDef>& fields,
                   const std::vector<std::string>& pk_names);

    /*
     * Make the current schema match `fields` (the full PostgreSQL column
     * list, matched by field id = attnum): add missing fields, drop fields
     * PostgreSQL no longer has, apply renames / type / nullability changes.
     * The hidden sequence field is kept; metadata fields (id >= 0xFF00) are
     * added but never dropped.  Writes schema-(id+1) only if something
     * changed.  With dry_run, only reports whether anything would change.
     */
    bool reconcile(const std::vector<FieldDef>& fields,
                   const std::vector<std::string>& pk_names,
                   bool dry_run = false);

    /* Evolve: write schema-(id+1) atomically. */
    bool addField(const FieldDef& f);
    bool dropField(uint16_t field_id);
    bool renameField(uint16_t field_id, const std::string& new_name);
    bool changeFieldType(uint16_t field_id, PaimonTypeCode new_type, uint32_t new_typmod);

    /* Serialise current schema to JSON string (for snapshot embedding). */
    std::string toJson() const;

private:
    std::string    root_;        /* <warehouse>/<db>/<table>             */
    SchemaVersion  current_;

    bool writeSchema(const SchemaVersion& sv);
    bool loadLatest();
    static SchemaVersion parseJson(const std::string& json);
    static std::string   serializeJson(const SchemaVersion& sv);
};

} // namespace paimon
