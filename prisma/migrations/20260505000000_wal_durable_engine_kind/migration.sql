PRAGMA foreign_keys = OFF;

DROP INDEX "exports_name_key";

CREATE TABLE "exports_new" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "name" TEXT NOT NULL,
    "engine_kind" TEXT NOT NULL CHECK (
        "engine_kind" IN ('memory', 'simple_durable', 'wal_durable')
    ),
    "block_size" INTEGER NOT NULL,
    "state" TEXT NOT NULL CHECK ("state" IN ('active', 'deleted')),
    "created_at" TEXT NOT NULL,
    "updated_at" TEXT NOT NULL,
    "deleted_at" TEXT,
    CHECK ("block_size" > 0)
);

INSERT INTO "exports_new" (
    "id", "name", "engine_kind", "block_size", "state",
    "created_at", "updated_at", "deleted_at"
)
SELECT
    "id", "name", "engine_kind", "block_size", "state",
    "created_at", "updated_at", "deleted_at"
FROM "exports";

DROP TABLE "exports";

ALTER TABLE "exports_new" RENAME TO "exports";

CREATE UNIQUE INDEX "exports_name_key" ON "exports"("name");

CREATE TABLE "export_heads_new" (
    "export_id" TEXT NOT NULL PRIMARY KEY,
    "layout_kind" TEXT NOT NULL CHECK (
        "layout_kind" IN (
            'memory_empty',
            'simple_mutable_tree',
            'cow_immutable_tree'
        )
    ),
    "root_node_id" TEXT,
    "size_bytes" INTEGER NOT NULL,
    "checkpoint_wal_seq" INTEGER NOT NULL,
    "updated_at" TEXT NOT NULL,
    CHECK ("size_bytes" > 0),
    CHECK ("checkpoint_wal_seq" >= 0),
    CONSTRAINT "export_heads_export_id_fkey"
        FOREIGN KEY ("export_id")
        REFERENCES "exports" ("id")
        ON DELETE RESTRICT
        ON UPDATE CASCADE,
    CONSTRAINT "export_heads_root_node_id_fkey"
        FOREIGN KEY ("root_node_id")
        REFERENCES "tree_nodes" ("id")
        ON DELETE RESTRICT
        ON UPDATE CASCADE
);

INSERT INTO "export_heads_new" (
    "export_id", "layout_kind", "root_node_id", "size_bytes",
    "checkpoint_wal_seq", "updated_at"
)
SELECT
    "export_id", "layout_kind", "root_node_id", "size_bytes",
    "checkpoint_wal_seq", "updated_at"
FROM "export_heads";

DROP TABLE "export_heads";

ALTER TABLE "export_heads_new" RENAME TO "export_heads";

PRAGMA foreign_keys = ON;
