CREATE TABLE "exports" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "name" TEXT NOT NULL,
    "size_bytes" INTEGER NOT NULL,
    "block_size" INTEGER NOT NULL,
    "state" TEXT NOT NULL CHECK ("state" IN ('active', 'deleted')),
    "created_at" TEXT NOT NULL,
    "updated_at" TEXT NOT NULL,
    "deleted_at" TEXT,
    CHECK ("size_bytes" > 0),
    CHECK ("block_size" > 0)
);

CREATE TABLE "export_generations" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "export_id" TEXT NOT NULL,
    "generation" INTEGER NOT NULL,
    "root_node_id" TEXT,
    "checkpoint_wal_seq" INTEGER NOT NULL,
    "created_at" TEXT NOT NULL,
    CHECK ("generation" >= 0),
    CHECK ("checkpoint_wal_seq" >= 0),
    CONSTRAINT "export_generations_export_id_fkey"
        FOREIGN KEY ("export_id")
        REFERENCES "exports" ("id")
        ON DELETE RESTRICT
        ON UPDATE CASCADE
);

CREATE UNIQUE INDEX "exports_name_key" ON "exports"("name");
CREATE UNIQUE INDEX "export_generations_export_id_generation_key"
    ON "export_generations"("export_id", "generation");
