ALTER TABLE "export_heads"
ADD COLUMN "tree_format" TEXT CHECK (
    (
        "layout_kind" = 'memory_empty'
        AND "tree_format" IS NULL
    )
    OR (
        "layout_kind" IN ('simple_mutable_tree', 'cow_immutable_tree')
        AND "tree_format" IS NOT NULL
        AND "tree_format" IN ('bounded_32_v1')
    )
);
