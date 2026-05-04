CREATE TABLE "exports" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "name" TEXT NOT NULL,
    "engine_kind" TEXT NOT NULL CHECK ("engine_kind" IN ('memory')),
    "block_size" INTEGER NOT NULL,
    "state" TEXT NOT NULL CHECK ("state" IN ('active', 'deleted')),
    "created_at" TEXT NOT NULL,
    "updated_at" TEXT NOT NULL,
    "deleted_at" TEXT,
    CHECK ("block_size" > 0)
);

CREATE UNIQUE INDEX "exports_name_key" ON "exports"("name");
