PRAGMA foreign_keys = OFF;

CREATE TABLE "tree_nodes_new" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "layout_kind" TEXT NOT NULL CHECK (
        "layout_kind" IN ('simple_mutable_tree', 'cow_immutable_tree')
    ),
    "owner_export_id" TEXT,
    "kind" TEXT NOT NULL CHECK ("kind" IN ('internal', 'leaf')),
    "level" INTEGER NOT NULL,
    "span_start_bytes" INTEGER NOT NULL,
    "span_len_bytes" INTEGER NOT NULL,
    "created_at" TEXT NOT NULL,
    CHECK ("level" >= 0),
    CHECK ("span_start_bytes" >= 0),
    CHECK ("span_len_bytes" > 0),
    CONSTRAINT "tree_nodes_owner_export_id_fkey"
        FOREIGN KEY ("owner_export_id")
        REFERENCES "exports" ("id")
        ON DELETE RESTRICT
        ON UPDATE CASCADE
);

INSERT INTO "tree_nodes_new" (
    "id", "layout_kind", "owner_export_id", "kind", "level",
    "span_start_bytes", "span_len_bytes", "created_at"
)
SELECT
    "id", "layout_kind", "owner_export_id", "kind", "level",
    "span_start_bytes", "span_len_bytes", "created_at"
FROM "tree_nodes";

DROP TABLE "tree_nodes";

ALTER TABLE "tree_nodes_new" RENAME TO "tree_nodes";

CREATE TABLE "tree_leaf_refs_new" (
    "node_id" TEXT NOT NULL PRIMARY KEY,
    "storage_kind" TEXT NOT NULL CHECK (
        "storage_kind" IN ('mutable_blob', 'immutable_blob')
    ),
    "storage_key" TEXT NOT NULL,
    "len_bytes" INTEGER NOT NULL,
    "created_at" TEXT NOT NULL,
    CHECK (length("storage_key") > 0),
    CHECK ("len_bytes" > 0),
    CONSTRAINT "tree_leaf_refs_node_id_fkey"
        FOREIGN KEY ("node_id")
        REFERENCES "tree_nodes" ("id")
        ON DELETE RESTRICT
        ON UPDATE CASCADE
);

INSERT INTO "tree_leaf_refs_new" (
    "node_id", "storage_kind", "storage_key", "len_bytes", "created_at"
)
SELECT
    "node_id", "storage_kind", "storage_key", "len_bytes", "created_at"
FROM "tree_leaf_refs";

DROP TABLE "tree_leaf_refs";

ALTER TABLE "tree_leaf_refs_new" RENAME TO "tree_leaf_refs";

PRAGMA foreign_keys = ON;
