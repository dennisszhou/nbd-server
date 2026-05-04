PRAGMA foreign_keys = OFF;

DROP INDEX "exports_name_key";

CREATE TABLE "exports_new" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "name" TEXT NOT NULL,
    "engine_kind" TEXT NOT NULL CHECK (
        "engine_kind" IN ('memory', 'simple_durable')
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

PRAGMA foreign_keys = ON;
