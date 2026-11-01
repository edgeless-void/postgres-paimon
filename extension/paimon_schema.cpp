// SPDX-License-Identifier: Apache-2.0
#include "paimon_schema.h"
#include "type_map.h"

#include <fstream>
#include <sstream>
#include <filesystem>
#include <stdexcept>
#include <algorithm>

/* Configurable system column names set by bgworker GUCs. */
extern char *g_paimon_seq_col_guc;
#define SEQ_COL (g_paimon_seq_col_guc && *g_paimon_seq_col_guc ? g_paimon_seq_col_guc : "_seq")

namespace fs = std::filesystem;
namespace paimon {

/* ── tiny JSON helpers (no external dependency) ─────────────────────── */
namespace {

std::string jstr(const std::string& s) {
    /* Minimal JSON string escaping */
    std::string out = "\"";
    for (char c : s) {
        if (c == '"')  out += "\\\"";
        else if (c == '\\') out += "\\\\";
        else if (c == '\n') out += "\\n";
        else           out += c;
    }
    return out + "\"";
}

/* Parse a JSON string value from a flat object — finds "key" : "value" */
std::string parseStrField(const std::string& json, const std::string& key) {
    std::string kpat = "\"" + key + "\"";
    auto pos = json.find(kpat);
    if (pos == std::string::npos) return "";
    pos = json.find(':', pos + kpat.size());
    if (pos == std::string::npos) return "";
    pos = json.find('"', pos + 1);
    if (pos == std::string::npos) return "";
    auto end = json.find('"', pos + 1);
    if (end == std::string::npos) return "";
    return json.substr(pos + 1, end - pos - 1);
}

int64_t parseIntField(const std::string& json, const std::string& key,
                      int64_t def = 0) {
    std::string kpat = "\"" + key + "\"";
    auto pos = json.find(kpat);
    if (pos == std::string::npos) return def;
    pos = json.find(':', pos + kpat.size());
    if (pos == std::string::npos) return def;
    while (pos < json.size() && (json[pos] == ':' || json[pos] == ' ')) pos++;
    return std::stoll(json.substr(pos));
}

} // anonymous namespace

/* ── PaimonSchemaManager ─────────────────────────────────────────────── */

PaimonSchemaManager::PaimonSchemaManager(const std::string& table_root)
    : root_(table_root) {
    current_.id = -1;
    loadLatest();
}

bool PaimonSchemaManager::loadLatest() {
    fs::path schema_dir = fs::path(root_) / "schema";
    if (!fs::exists(schema_dir)) return false;

    /* Find the highest schema-N file */
    int64_t max_id = -1;
    for (auto& entry : fs::directory_iterator(schema_dir)) {
        std::string fname = entry.path().filename().string();
        if (fname.rfind("schema-", 0) == 0) {
            try {
                int64_t id = std::stoll(fname.substr(7));
                if (id > max_id) max_id = id;
            } catch (...) {}
        }
    }
    if (max_id < 0) return false;

    std::ifstream f(fs::path(schema_dir) / ("schema-" + std::to_string(max_id)));
    if (!f) return false;
    std::string json((std::istreambuf_iterator<char>(f)),
                      std::istreambuf_iterator<char>());
    current_ = parseJson(json);
    return true;
}

bool PaimonSchemaManager::writeSchema(const SchemaVersion& sv) {
    fs::path schema_dir = fs::path(root_) / "schema";
    fs::create_directories(schema_dir);

    std::string fname = "schema-" + std::to_string(sv.id);
    fs::path tmp = schema_dir / (fname + ".tmp");
    {
        std::ofstream f(tmp);
        if (!f) return false;
        f << serializeJson(sv);
    }
    fs::rename(tmp, schema_dir / fname);
    current_ = sv;
    return true;
}

bool PaimonSchemaManager::bootstrap(const std::vector<FieldDef>& fields,
                                     const std::vector<std::string>& pk_names) {
    if (exists()) {
        reconcile(fields, pk_names);
        return true;
    }

    SchemaVersion sv;
    sv.id = 0;
    sv.fields = fields;
    /* Add hidden sequence field (field_id 32767 is reserved for it). */
    FieldDef seq_field;
    seq_field.field_id  = 32767;
    seq_field.name      = SEQ_COL;
    seq_field.type_code = PTYPE_BIGINT;
    seq_field.not_null  = true;
    sv.fields.push_back(seq_field);

    sv.highest_field_id = 32767;
    for (const auto& f : fields)
        if ((int32_t)f.field_id > sv.highest_field_id)
            sv.highest_field_id = (int32_t)f.field_id;
    sv.primary_keys = pk_names;
    return writeSchema(sv);
}

bool PaimonSchemaManager::reconcile(const std::vector<FieldDef>& fields,
                                     const std::vector<std::string>& pk_names,
                                     bool dry_run) {
    if (!exists()) return !fields.empty();

    SchemaVersion sv = current_;
    bool changed = false;

    /* Drop fields PostgreSQL no longer has (keep _seq and metadata fields). */
    auto keep = [&](const FieldDef& f) {
        if (f.field_id == 32767 || f.field_id >= 0xFF00u) return true;
        for (const auto& in : fields)
            if (in.field_id == f.field_id) return true;
        return false;
    };
    std::vector<FieldDef> kept;
    for (const auto& f : sv.fields) {
        if (keep(f)) kept.push_back(f);
        else changed = true;
    }
    sv.fields.swap(kept);

    /* Update existing fields in place and append new ones, like addField()
       does (Paimon resolves columns by field id, not by position). */
    for (const auto& in : fields) {
        bool found = false;
        for (auto& f : sv.fields) {
            if (f.field_id != in.field_id) continue;
            found = true;
            if (f.name != in.name || f.type_code != in.type_code ||
                f.typmod != in.typmod || f.not_null != in.not_null) {
                f.name      = in.name;
                f.type_code = in.type_code;
                f.typmod    = in.typmod;
                f.not_null  = in.not_null;
                changed = true;
            }
            break;
        }
        if (!found) {
            sv.fields.push_back(in);
            if ((int32_t)in.field_id > sv.highest_field_id)
                sv.highest_field_id = (int32_t)in.field_id;
            changed = true;
        }
    }

    /* Primary key is immutable in PostgreSQL (blocked), but its column may
       have been renamed. */
    if (!pk_names.empty() && pk_names != sv.primary_keys) {
        sv.primary_keys = pk_names;
        changed = true;
    }

    if (!changed || dry_run) return changed;
    sv.id++;
    return writeSchema(sv);
}

bool PaimonSchemaManager::addField(const FieldDef& f) {
    SchemaVersion sv = current_;
    sv.id++;
    sv.fields.push_back(f);
    if (f.field_id > sv.highest_field_id)
        sv.highest_field_id = f.field_id;
    return writeSchema(sv);
}

bool PaimonSchemaManager::dropField(uint16_t field_id) {
    SchemaVersion sv = current_;
    sv.id++;
    sv.fields.erase(std::remove_if(sv.fields.begin(), sv.fields.end(),
        [field_id](const FieldDef& f){ return f.field_id == field_id; }),
        sv.fields.end());
    return writeSchema(sv);
}

bool PaimonSchemaManager::renameField(uint16_t field_id,
                                       const std::string& new_name) {
    SchemaVersion sv = current_;
    sv.id++;
    for (auto& f : sv.fields)
        if (f.field_id == field_id) { f.name = new_name; break; }
    /* Update pk names if renamed */
    std::string old_name;
    for (const auto& f : current_.fields)
        if (f.field_id == field_id) { old_name = f.name; break; }
    for (auto& pk : sv.primary_keys)
        if (pk == old_name) { pk = new_name; break; }
    return writeSchema(sv);
}

bool PaimonSchemaManager::changeFieldType(uint16_t field_id,
                                           PaimonTypeCode new_type,
                                           uint32_t new_typmod) {
    SchemaVersion sv = current_;
    sv.id++;
    for (auto& f : sv.fields)
        if (f.field_id == field_id) {
            f.type_code = new_type;
            f.typmod    = new_typmod;
            break;
        }
    return writeSchema(sv);
}

/* ── JSON serialisation ─────────────────────────────────────────────── */

std::string PaimonSchemaManager::serializeJson(const SchemaVersion& sv) {
    std::ostringstream o;
    o << "{\n";
    o << "  \"id\" : " << sv.id << ",\n";
    o << "  \"fields\" : [\n";
    for (size_t i = 0; i < sv.fields.size(); ++i) {
        const auto& f = sv.fields[i];
        o << "    {\"id\" : " << f.field_id
          << ", \"name\" : " << jstr(f.name)
          << ", \"type\" : "
          << jstr(paimon_type_string(f.type_code, f.typmod, f.not_null))
          << "}";
        if (i + 1 < sv.fields.size()) o << ",";
        o << "\n";
    }
    o << "  ],\n";
    o << "  \"highestFieldId\" : " << sv.highest_field_id << ",\n";
    o << "  \"partitionKeys\" : [],\n";
    o << "  \"primaryKeys\" : [";
    for (size_t i = 0; i < sv.primary_keys.size(); ++i) {
        o << jstr(sv.primary_keys[i]);
        if (i + 1 < sv.primary_keys.size()) o << ", ";
    }
    o << "],\n";
    o << "  \"options\" : {\n";
    o << "    \"sequence.field\" : " << jstr(SEQ_COL) << ",\n";
    o << "    \"merge-engine\" : \"deduplicate\",\n";
    o << "    \"changelog-producer\" : \"input\"\n";
    o << "  },\n";
    o << "  \"comment\" : \"\"\n";
    o << "}\n";
    return o.str();
}

std::string PaimonSchemaManager::toJson() const {
    return serializeJson(current_);
}

SchemaVersion PaimonSchemaManager::parseJson(const std::string& json) {
    /* Minimal parser — sufficient for round-trip of our own output */
    SchemaVersion sv;
    sv.id = parseIntField(json, "id", 0);
    sv.highest_field_id = (int32_t)parseIntField(json, "highestFieldId", 0);

    /* Parse fields array */
    auto fields_pos = json.find("\"fields\"");
    if (fields_pos != std::string::npos) {
        auto arr_start = json.find('[', fields_pos);
        auto arr_end   = json.find(']', arr_start);
        if (arr_start != std::string::npos && arr_end != std::string::npos) {
            std::string arr = json.substr(arr_start, arr_end - arr_start);
            size_t p = 0;
            while ((p = arr.find('{', p)) != std::string::npos) {
                auto obj_end = arr.find('}', p);
                std::string obj = arr.substr(p, obj_end - p + 1);
                FieldDef f;
                f.field_id  = (uint16_t)parseIntField(obj, "id", 0);
                f.name      = parseStrField(obj, "name");
                {
                    std::string ts = parseStrField(obj, "type");
                    f.not_null  = (ts.find("NOT NULL") != std::string::npos);
                    if      (ts.find("BIGINT")    != std::string::npos) f.type_code = PTYPE_BIGINT;
                    else if (ts.find("INT")       != std::string::npos) f.type_code = PTYPE_INT;
                    else if (ts.find("SMALLINT")  != std::string::npos) f.type_code = PTYPE_SMALLINT;
                    else if (ts.find("DOUBLE")    != std::string::npos) f.type_code = PTYPE_DOUBLE;
                    else if (ts.find("FLOAT")     != std::string::npos) f.type_code = PTYPE_FLOAT;
                    else if (ts.find("BOOLEAN")   != std::string::npos) f.type_code = PTYPE_BOOLEAN;
                    else if (ts.find("DECIMAL")   != std::string::npos) f.type_code = PTYPE_DECIMAL;
                    else if (ts.find("TIMESTAMP WITH LOCAL") != std::string::npos) f.type_code = PTYPE_TIMESTAMPTZ;
                    else if (ts.find("TIMESTAMP") != std::string::npos) f.type_code = PTYPE_TIMESTAMP;
                    else if (ts.find("DATE")      != std::string::npos) f.type_code = PTYPE_DATE;
                    else if (ts.find("BYTES")     != std::string::npos) f.type_code = PTYPE_BYTES;
                    else                                                 f.type_code = PTYPE_STRING;
                }
                sv.fields.push_back(f);
                p = obj_end + 1;
            }
        }
    }

    /* Parse primaryKeys array */
    auto pk_pos = json.find("\"primaryKeys\"");
    if (pk_pos != std::string::npos) {
        auto arr_start = json.find('[', pk_pos);
        auto arr_end   = json.find(']', arr_start);
        if (arr_start != std::string::npos && arr_end != std::string::npos) {
            std::string arr = json.substr(arr_start + 1, arr_end - arr_start - 1);
            size_t p = 0;
            while ((p = arr.find('"', p)) != std::string::npos) {
                auto end = arr.find('"', p + 1);
                sv.primary_keys.push_back(arr.substr(p + 1, end - p - 1));
                p = end + 1;
            }
        }
    }
    return sv;
}

} // namespace paimon
