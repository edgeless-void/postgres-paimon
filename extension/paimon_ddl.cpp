// SPDX-License-Identifier: Apache-2.0
/*
 * paimon_ddl.cpp — DDL capture on paimon_heap tables.
 *
 * All captured DDL is queued in the backend's per-transaction message list
 * (PaimonClient) and reaches the bgworker only if, and when, the transaction
 * commits — in order with the transaction's DML.  A failed or rolled-back
 * DDL statement therefore never touches the lake.
 *
 * object_access_hook (sees every drop / truncate, including cascades):
 *   DROP TABLE, DROP SCHEMA ... CASCADE, DROP OWNED, DROP EXTENSION, ...
 *                                → queueDropTable
 *   TRUNCATE (incl. CASCADE and inheritance children) → queueTruncate
 *
 * ProcessUtility hook:
 *   CREATE TABLE                 → queueSchema (post-execute)
 *   ALTER TABLE (any subcommand) → queueSchema (post-execute); the bgworker
 *                                  reconciles add / drop / rename / retype /
 *                                  nullability by field id
 *   ALTER TABLE RENAME COLUMN    → queueSchema (post-execute)
 *   ALTER TABLE ADD/DROP PRIMARY KEY, DEFAULT clauses   → blocked with ERROR
 *   ALTER TABLE RENAME TO / SET SCHEMA, ALTER SCHEMA RENAME (when the schema
 *   holds paimon_heap tables)   → blocked with ERROR (the lake table path is
 *                                  derived from schema + table name)
 *
 * Non-paimon_heap relations are passed through untouched.
 */

extern "C" {
#include "postgres.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/relation.h"
#include "access/skey.h"
#include "access/table.h"
#include "catalog/dependency.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_class.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "tcop/utility.h"
#include "nodes/parsenodes.h"
#include "commands/tablecmds.h"
#include "catalog/namespace.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "catalog/pg_am.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_constraint.h"

void paimon_ddl_init(void);
}

#include "paimon_client.h"
#include "paimon_relmeta.h"

/* ── hook chain ──────────────────────────────────────────────────────── */
static ProcessUtility_hook_type prev_ProcessUtility = NULL;
static object_access_hook_type  prev_object_access  = NULL;

/* ── AM OID cache ────────────────────────────────────────────────────── */
static Oid s_paimon_am_oid = InvalidOid;

static Oid
get_paimon_am_oid(void)
{
    if (!OidIsValid(s_paimon_am_oid)) {
        HeapTuple ht = SearchSysCache1(AMNAME, CStringGetDatum("paimon_heap"));
        if (HeapTupleIsValid(ht)) {
            s_paimon_am_oid = ((Form_pg_am) GETSTRUCT(ht))->oid;
            ReleaseSysCache(ht);
        }
    }
    return s_paimon_am_oid;
}

static bool
is_paimon_heap_rel(Oid relid)
{
    Oid am = get_rel_relam(relid);
    if (!OidIsValid(am))
        return false;
    Oid pam = get_paimon_am_oid();
    return OidIsValid(pam) && am == pam;
}

/* Queue the relation's current full schema for the lake (transactional). */
static void
queue_rel_schema(Oid relid)
{
    if (!OidIsValid(relid) || !is_paimon_heap_rel(relid))
        return;

    Relation rel = relation_open(relid, AccessShareLock);
    if (paimon_rel_is_mirrored(rel)) {
        std::vector<PaimonColumn> cols;
        std::vector<uint16_t>     pk_attnums;
        paimon_build_col_meta(rel, cols, pk_attnums);
        PaimonClient::instance().queueSchema(
            (uint32_t) relid, paimon_rel_ident(rel),
            paimon_schema_fingerprint(RelationGetDescr(rel)),
            cols, pk_attnums);
    }
    relation_close(rel, AccessShareLock);
}

static void
error_lake_path_change(const char *what)
{
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("paimon_heap: %s is not supported", what),
             errdetail("The Paimon table location is derived from the schema and "
                       "table name; changing them would detach the table from "
                       "its lake data."),
             errhint("Create a new table with the desired name and copy the data.")));
}

/* True if namespace nspid contains a (mirrored) paimon_heap table. */
static bool
namespace_has_paimon_tables(Oid nspid)
{
    Oid pam = get_paimon_am_oid();
    if (!OidIsValid(pam))
        return false;

    bool        found = false;
    ScanKeyData key;
    ScanKeyInit(&key, Anum_pg_class_relnamespace,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(nspid));

    Relation    pg_class = table_open(RelationRelationId, AccessShareLock);
    SysScanDesc scan = systable_beginscan(pg_class, InvalidOid, false,
                                          NULL, 1, &key);
    HeapTuple   tup;
    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        Form_pg_class c = (Form_pg_class) GETSTRUCT(tup);
        if (c->relam == pam && c->relkind == RELKIND_RELATION &&
            c->relpersistence != RELPERSISTENCE_TEMP) {
            found = true;
            break;
        }
    }
    systable_endscan(scan);
    table_close(pg_class, AccessShareLock);
    return found;
}

/* ── helpers ─────────────────────────────────────────────────────────── */

/*
 * At hook time we see the raw parse tree before transformColumnDef() runs.
 * DEFAULT is stored in ColumnDef->constraints as a CONSTR_DEFAULT node, NOT
 * in ColumnDef->raw_default (which is only set after transformation).
 */
static bool
cdef_has_default(ColumnDef *cdef)
{
    if (cdef->raw_default != NULL)
        return true;
    ListCell *lc;
    foreach(lc, cdef->constraints) {
        Constraint *con = castNode(Constraint, lfirst(lc));
        if (con->contype == CONSTR_DEFAULT)
            return true;
    }
    return false;
}

static void
error_no_default(const char *colname)
{
    if (colname)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("paimon_heap: column default values are not supported"),
                 errdetail("Column \"%s\" has a DEFAULT clause.", colname),
                 errhint("Remove the DEFAULT clause; existing rows will read NULL "
                         "for newly added columns.")));
    else
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("paimon_heap: column default values are not supported"),
                 errhint("Remove the DEFAULT clause; existing rows will read NULL "
                         "for newly added columns.")));
}

/* ── pre-execute handlers ────────────────────────────────────────────── */

/*
 * Block CREATE TABLE USING paimon_heap when any column has a DEFAULT clause.
 * We cannot backfill defaults into historical Parquet files, so defaults
 * would silently produce NULLs for older rows in Spark — it is better to
 * refuse early than to silently mislead.
 */
static void
handle_create_pre(CreateStmt *stmt)
{
    if (!stmt->accessMethod ||
        strcmp(stmt->accessMethod, "paimon_heap") != 0)
        return;

    ListCell *lc;
    foreach(lc, stmt->tableElts) {
        if (!IsA(lfirst(lc), ColumnDef))
            continue;
        ColumnDef *cdef = castNode(ColumnDef, lfirst(lc));
        if (cdef_has_default(cdef))
            error_no_default(cdef->colname);
    }
}

/*
 * object_access_hook: catches every relation drop and truncate — explicit,
 * cascaded (DROP SCHEMA ... CASCADE, DROP OWNED, DROP EXTENSION, TRUNCATE
 * ... CASCADE, inheritance children) — while the catalog entry still exists.
 */
static void
paimon_object_access(ObjectAccessType access, Oid classId, Oid objectId,
                     int subId, void *arg)
{
    if (prev_object_access)
        prev_object_access(access, classId, objectId, subId, arg);

    if (classId != RelationRelationId || subId != 0)
        return;

    switch (access) {
        case OAT_DROP: {
            ObjectAccessDrop *drop = (ObjectAccessDrop *) arg;
            /* Internal drops: transient heaps of a table rewrite, etc. */
            if (drop && (drop->dropflags & PERFORM_DELETION_INTERNAL))
                break;
            if (!is_paimon_heap_rel(objectId) ||
                !paimon_relid_is_mirrored(objectId))
                break;
            std::string ident = paimon_rel_ident_oid(objectId);
            if (!ident.empty())
                PaimonClient::instance().queueDropTable((uint32_t) objectId, ident);
            break;
        }
        case OAT_TRUNCATE: {
            if (!is_paimon_heap_rel(objectId) ||
                !paimon_relid_is_mirrored(objectId))
                break;
            std::string ident = paimon_rel_ident_oid(objectId);
            if (!ident.empty())
                PaimonClient::instance().queueTruncate((uint32_t) objectId, ident);
            break;
        }
        default:
            break;
    }
}

/*
 * If conname on relid is a PRIMARY KEY constraint, raise an error.
 * Used by AT_DropConstraint to block dropping the PK.
 */
static void
block_if_pk_drop(Oid relid, const char *conname, bool missing_ok)
{
    Oid con_oid = get_relation_constraint_oid(relid, conname, missing_ok);
    if (!OidIsValid(con_oid))
        return;
    HeapTuple tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(con_oid));
    if (!HeapTupleIsValid(tup))
        return;
    char contype = ((Form_pg_constraint) GETSTRUCT(tup))->contype;
    ReleaseSysCache(tup);
    if (contype == CONSTRAINT_PRIMARY)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("paimon_heap: changing the primary key is not supported"),
                 errhint("Recreate the table with the desired primary key.")));
}

static void
handle_alter_pre(AlterTableStmt *stmt)
{
    Oid relid = RangeVarGetRelid(stmt->relation, NoLock, stmt->missing_ok);
    if (!OidIsValid(relid) || !is_paimon_heap_rel(relid))
        return;

    ListCell *lc;
    foreach(lc, stmt->cmds) {
        AlterTableCmd *cmd = castNode(AlterTableCmd, lfirst(lc));

        switch (cmd->subtype) {
            case AT_AddColumn: {
                ColumnDef *cdef = castNode(ColumnDef, cmd->def);
                if (cdef_has_default(cdef))
                    error_no_default(cdef->colname);
                break;
            }
            case AT_ColumnDefault:
                /* cmd->def == NULL means DROP DEFAULT (harmless); non-NULL is SET DEFAULT */
                if (cmd->def != NULL)
                    error_no_default(cmd->name);
                break;

            case AT_AddConstraint: {
                Constraint *con = castNode(Constraint, cmd->def);
                if (con->contype == CONSTR_PRIMARY)
                    ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                             errmsg("paimon_heap: changing the primary key is not supported"),
                             errhint("Recreate the table with the desired primary key.")));
                break;
            }
            case AT_DropConstraint:
                block_if_pk_drop(relid, cmd->name, cmd->missing_ok);
                break;

            default:
                break;
        }
    }
}

/*
 * RenameStmt: RENAME COLUMN is mirrored post-execute; renaming the table or
 * its schema would change the lake path and is blocked.
 */
static void
handle_rename_pre(RenameStmt *stmt)
{
    switch (stmt->renameType) {
        case OBJECT_TABLE: {
            Oid relid = RangeVarGetRelid(stmt->relation, NoLock, stmt->missing_ok);
            if (OidIsValid(relid) && is_paimon_heap_rel(relid) &&
                paimon_relid_is_mirrored(relid))
                error_lake_path_change("renaming a paimon_heap table");
            break;
        }
        case OBJECT_SCHEMA: {
            Oid nspid = get_namespace_oid(stmt->subname, true);
            if (OidIsValid(nspid) && namespace_has_paimon_tables(nspid))
                error_lake_path_change("renaming a schema that contains paimon_heap tables");
            break;
        }
        default:
            break;
    }
}

static void
handle_alter_schema_pre(AlterObjectSchemaStmt *stmt)
{
    if (stmt->objectType != OBJECT_TABLE || stmt->relation == NULL)
        return;
    Oid relid = RangeVarGetRelid(stmt->relation, NoLock, stmt->missing_ok);
    if (OidIsValid(relid) && is_paimon_heap_rel(relid) &&
        paimon_relid_is_mirrored(relid))
        error_lake_path_change("moving a paimon_heap table to another schema");
}

/* ── post-execute handlers ───────────────────────────────────────────── */

/*
 * After CREATE TABLE / ALTER TABLE / RENAME COLUMN, queue the relation's
 * full schema.  The bgworker creates the Paimon table or reconciles it to
 * this schema, so the lake table exists from CREATE on and every schema
 * change (including ones PostgreSQL applied by recursion to inheritance
 * children, which are re-sent on their next DML) converges.
 */
static void
handle_create_post(CreateStmt *stmt)
{
    queue_rel_schema(RangeVarGetRelid(stmt->relation, NoLock, true));
}

static void
handle_alter_post(AlterTableStmt *stmt)
{
    queue_rel_schema(RangeVarGetRelid(stmt->relation, NoLock, true));
}

static void
handle_rename_post(RenameStmt *stmt)
{
    if (stmt->renameType != OBJECT_COLUMN || stmt->relation == NULL)
        return;
    queue_rel_schema(RangeVarGetRelid(stmt->relation, NoLock, true));
}

/* ── main hook ───────────────────────────────────────────────────────── */

static void
paimon_process_utility(PlannedStmt *pstmt,
                        const char *queryString,
                        bool readOnlyTree,
                        ProcessUtilityContext context,
                        ParamListInfo params,
                        QueryEnvironment *queryEnv,
                        DestReceiver *dest,
                        QueryCompletion *qc)
{
    Node *parsetree = pstmt->utilityStmt;

    /*
     * Pre-execute: capture names/attnums before the DDL modifies the catalog,
     * and block forbidden operations (PK changes) before they happen.
     */
    switch (nodeTag(parsetree)) {
        case T_CreateStmt:
            handle_create_pre(castNode(CreateStmt, parsetree));
            break;
        case T_AlterTableStmt:
            handle_alter_pre(castNode(AlterTableStmt, parsetree));
            break;
        case T_RenameStmt:
            handle_rename_pre(castNode(RenameStmt, parsetree));
            break;
        case T_AlterObjectSchemaStmt:
            handle_alter_schema_pre(castNode(AlterObjectSchemaStmt, parsetree));
            break;
        default:
            break;
    }

    /* Execute the DDL (chain to any prior hook or the standard function). */
    if (prev_ProcessUtility)
        prev_ProcessUtility(pstmt, queryString, readOnlyTree,
                            context, params, queryEnv, dest, qc);
    else
        standard_ProcessUtility(pstmt, queryString, readOnlyTree,
                                context, params, queryEnv, dest, qc);

    /*
     * Post-execute: read the updated catalog and queue the resulting schema.
     * Nothing is sent yet — it goes out with the transaction at COMMIT.
     */
    switch (nodeTag(parsetree)) {
        case T_CreateStmt:
            handle_create_post(castNode(CreateStmt, parsetree));
            break;
        case T_AlterTableStmt:
            handle_alter_post(castNode(AlterTableStmt, parsetree));
            break;
        case T_RenameStmt:
            handle_rename_post(castNode(RenameStmt, parsetree));
            break;
        default:
            break;
    }
}

/* ── registration (called from _PG_init) ────────────────────────────── */

extern "C" void
paimon_ddl_init(void)
{
    prev_ProcessUtility = ProcessUtility_hook;
    ProcessUtility_hook = paimon_process_utility;

    prev_object_access = object_access_hook;
    object_access_hook = paimon_object_access;
}
