CREATE TABLE "tree_nodes" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "layout_kind" TEXT NOT NULL CHECK (
        "layout_kind" IN ('simple_mutable_tree')
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

CREATE TABLE "tree_edges" (
    "parent_node_id" TEXT NOT NULL,
    "slot" INTEGER NOT NULL,
    "child_node_id" TEXT NOT NULL,
    CHECK ("slot" >= 0),
    PRIMARY KEY ("parent_node_id", "slot"),
    CONSTRAINT "tree_edges_parent_node_id_fkey"
        FOREIGN KEY ("parent_node_id")
        REFERENCES "tree_nodes" ("id")
        ON DELETE RESTRICT
        ON UPDATE CASCADE,
    CONSTRAINT "tree_edges_child_node_id_fkey"
        FOREIGN KEY ("child_node_id")
        REFERENCES "tree_nodes" ("id")
        ON DELETE RESTRICT
        ON UPDATE CASCADE
);

CREATE TABLE "tree_leaf_refs" (
    "node_id" TEXT NOT NULL PRIMARY KEY,
    "storage_kind" TEXT NOT NULL CHECK ("storage_kind" IN ('mutable_blob')),
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

CREATE TABLE "export_heads" (
    "export_id" TEXT NOT NULL PRIMARY KEY,
    "layout_kind" TEXT NOT NULL CHECK (
        "layout_kind" IN ('memory_empty', 'simple_mutable_tree')
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

INSERT INTO "export_heads" (
    "export_id",
    "layout_kind",
    "root_node_id",
    "size_bytes",
    "checkpoint_wal_seq",
    "updated_at"
)
SELECT
    e."id",
    'memory_empty',
    g."root_node_id",
    g."size_bytes",
    g."checkpoint_wal_seq",
    e."updated_at"
FROM "exports" e
JOIN "export_generations" g
    ON g."export_id" = e."id"
   AND g."generation" = (
       SELECT MAX(g2."generation")
       FROM "export_generations" g2
       WHERE g2."export_id" = e."id"
   );
