use nbd_control_plane::{
    BlobKey, CatalogError, CatalogUrl, ChunkIndex, CloneExport, CowChunkRef, CowTreeMetadataStore,
    CreateExport, DeleteExport, ExportCatalog, ExportEngineKind, ExportLayoutKind, ExportName,
    ExportRecord, ExportState, InspectExport, ListExports, NodeId, PublishCompaction,
    PublishCompactionOutcome, SQLiteExportCatalog, SimpleChunkRef, SimpleTreeMetadataStore, WalSeq,
    SIMPLE_CHUNK_BYTES, TREE_CHUNK_BYTES,
};
use nbd_test_support::TestRuntime;
use sqlx::Row;

const MIGRATIONS: &[&str] = &[include_str!(
    "../../../prisma/migrations/20260506000000_baseline/migration.sql"
)];

#[tokio::test]
async fn create_export_initializes_memory_head() {
    let (_runtime, catalog) = migrated_catalog().await;

    let created = catalog
        .create_export(create_export("disk-a", 1024 * 1024, 4096))
        .await
        .expect("create export");

    assert_eq!(created.name().as_str(), "disk-a");
    assert_eq!(created.size_bytes(), 1024 * 1024);
    assert_eq!(created.block_size(), 4096);
    assert_eq!(created.engine_kind(), ExportEngineKind::Memory);
    assert_eq!(created.state(), ExportState::Active);
    assert_eq!(created.head().layout_kind(), ExportLayoutKind::MemoryEmpty);
    assert!(created.head().root_node_id().is_none());
    assert_eq!(created.head().size_bytes(), 1024 * 1024);
    assert_eq!(created.head().base_wal_seq(), WalSeq::zero());

    let inspected = catalog
        .inspect_export(InspectExport::new(export_name("disk-a")))
        .await
        .expect("inspect export");
    assert_eq!(inspected, created);
}

#[tokio::test]
async fn create_export_initializes_simple_durable_head() {
    let (_runtime, catalog) = migrated_catalog().await;

    let created = catalog
        .create_export(create_export_with_engine(
            "disk-durable",
            128 * 1024 * 1024,
            4096,
            ExportEngineKind::SimpleDurable,
        ))
        .await
        .expect("create export");

    assert_eq!(created.engine_kind(), ExportEngineKind::SimpleDurable);
    assert_eq!(
        created.head().layout_kind(),
        ExportLayoutKind::SimpleMutableTree,
    );
    assert!(created.head().root_node_id().is_none());
    assert_eq!(created.head().base_wal_seq(), WalSeq::zero());

    let snapshot = catalog
        .load_simple_tree(created.id())
        .await
        .expect("load simple tree");
    assert_eq!(snapshot.export_id(), created.id());
    assert!(snapshot.root_node_id().is_none());
    assert!(snapshot.chunks().is_empty());
}

#[tokio::test]
async fn create_export_initializes_wal_durable_head() {
    let (_runtime, catalog) = migrated_catalog().await;

    let created = catalog
        .create_export(create_export_with_engine(
            "disk-wal",
            128 * 1024 * 1024,
            4096,
            ExportEngineKind::WalDurable,
        ))
        .await
        .expect("create export");

    assert_eq!(created.engine_kind(), ExportEngineKind::WalDurable);
    assert_eq!(
        created.head().layout_kind(),
        ExportLayoutKind::CowImmutableTree
    );
    assert!(created.head().root_node_id().is_none());
    assert_eq!(created.head().base_wal_seq(), WalSeq::zero());

    let snapshot = catalog
        .load_cow_tree(created.id())
        .await
        .expect("load cow tree");
    assert_eq!(snapshot.export_id(), created.id());
    assert!(snapshot.root_node_id().is_none());
    assert_eq!(snapshot.base_wal_seq(), WalSeq::zero());
    assert!(snapshot.chunks().is_empty());
}

#[tokio::test]
async fn descriptor_and_head_load_separate_export_identity_from_serving_head() {
    let (_runtime, catalog) = migrated_catalog().await;
    let created = catalog
        .create_export(create_export_with_engine(
            "disk-wal",
            128 * 1024 * 1024,
            4096,
            ExportEngineKind::WalDurable,
        ))
        .await
        .expect("create export");

    let descriptor = catalog
        .load_export_descriptor(export_name("disk-wal"))
        .await
        .expect("load descriptor");
    assert_eq!(descriptor.id(), created.id());
    assert_eq!(descriptor.name().as_str(), "disk-wal");
    assert_eq!(descriptor.engine_kind(), ExportEngineKind::WalDurable);
    assert_eq!(descriptor.block_size(), 4096);

    let initial_head = catalog
        .load_export_head(created.id())
        .await
        .expect("load initial head");
    assert_eq!(initial_head, created.head().clone());

    assert_eq!(descriptor.state(), ExportState::Active);
    assert!(descriptor.deleted_at().is_none());
}

#[tokio::test]
async fn duplicate_create_fails_clearly() {
    let (_runtime, catalog) = migrated_catalog().await;

    catalog
        .create_export(create_export("disk-a", 1024, 4096))
        .await
        .expect("create export");

    let error = catalog
        .create_export(create_export("disk-a", 1024, 4096))
        .await
        .unwrap_err();

    assert!(matches!(error, CatalogError::ExportAlreadyExists { .. }));
}

#[tokio::test]
async fn export_head_owns_serving_size() {
    let (_runtime, catalog) = migrated_catalog().await;

    let created = catalog
        .create_export(create_export("disk-a", 1024, 4096))
        .await
        .expect("create export");

    sqlx::query(
        r#"
        UPDATE export_heads
        SET size_bytes = 3072
        WHERE export_id = ?
        "#,
    )
    .bind(created.id().as_str())
    .execute(catalog.pool())
    .await
    .expect("update export head");

    let inspected = catalog
        .inspect_export(InspectExport::new(export_name("disk-a")))
        .await
        .expect("inspect export");

    assert_eq!(inspected.size_bytes(), 3072);
    assert_eq!(inspected.head().size_bytes(), 3072);
    assert_eq!(
        inspected.head().layout_kind(),
        ExportLayoutKind::MemoryEmpty
    );
}

#[tokio::test]
async fn load_export_head_rejects_layout_invalid_rows() {
    let (_runtime, catalog) = migrated_catalog().await;

    let created = catalog
        .create_export(create_export("disk-a", 1024, 4096))
        .await
        .expect("create export");

    sqlx::query(
        r#"
        UPDATE export_heads
        SET base_wal_seq = 1
        WHERE export_id = ?
        "#,
    )
    .bind(created.id().as_str())
    .execute(catalog.pool())
    .await
    .expect("corrupt head");

    let error = catalog
        .load_export_head(created.id())
        .await
        .expect_err("invalid memory head must fail to decode");
    assert!(matches!(error, CatalogError::InvalidField { .. }));
}

#[tokio::test]
async fn migration_does_not_create_export_generations() {
    let (_runtime, catalog) = migrated_catalog().await;

    let table_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM sqlite_master
        WHERE type = 'table' AND name = 'export_generations'
        "#,
    )
    .fetch_one(catalog.pool())
    .await
    .expect("inspect sqlite schema");

    assert_eq!(table_count, 0);
}

#[tokio::test]
async fn baseline_migration_supports_all_engine_kinds() {
    let (_runtime, catalog) = migrated_catalog().await;

    let memory = catalog
        .create_export(create_export("disk-memory", 1024, 4096))
        .await
        .expect("create memory export");
    assert_eq!(memory.engine_kind(), ExportEngineKind::Memory);
    assert_eq!(memory.head().layout_kind(), ExportLayoutKind::MemoryEmpty);

    let durable = catalog
        .create_export(create_export_with_engine(
            "disk-durable",
            1024,
            4096,
            ExportEngineKind::SimpleDurable,
        ))
        .await
        .expect("create simple durable export");
    assert_eq!(durable.engine_kind(), ExportEngineKind::SimpleDurable);
    assert_eq!(
        durable.head().layout_kind(),
        ExportLayoutKind::SimpleMutableTree,
    );

    let wal = catalog
        .create_export(create_export_with_engine(
            "disk-wal",
            1024,
            4096,
            ExportEngineKind::WalDurable,
        ))
        .await
        .expect("create wal durable export");
    assert_eq!(wal.engine_kind(), ExportEngineKind::WalDurable);
    assert_eq!(wal.head().layout_kind(), ExportLayoutKind::CowImmutableTree);
}

#[tokio::test]
async fn delete_hides_export_from_load_and_default_list() {
    let (_runtime, catalog) = migrated_catalog().await;

    catalog
        .create_export(create_export("disk-a", 1024, 4096))
        .await
        .expect("create export");
    catalog
        .delete_export(DeleteExport::new(export_name("disk-a")))
        .await
        .expect("delete export");

    let load_error = catalog
        .load_export(export_name("disk-a"))
        .await
        .unwrap_err();
    assert!(matches!(load_error, CatalogError::ExportDeleted { .. }));
    let descriptor_error = catalog
        .load_export_descriptor(export_name("disk-a"))
        .await
        .unwrap_err();
    assert!(matches!(
        descriptor_error,
        CatalogError::ExportDeleted { .. }
    ));

    let inspected = catalog
        .inspect_export(InspectExport::new(export_name("disk-a")))
        .await
        .expect("inspect deleted export");
    assert_eq!(inspected.state(), ExportState::Deleted);
    assert!(inspected.deleted_at().is_some());
    assert_eq!(
        inspected.head().layout_kind(),
        ExportLayoutKind::MemoryEmpty
    );

    assert!(catalog
        .list_exports(ListExports::active_only())
        .await
        .expect("list active")
        .is_empty());

    let all = catalog
        .list_exports(ListExports::include_deleted())
        .await
        .expect("list all");
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].name().as_str(), "disk-a");
}

#[tokio::test]
async fn list_exports_orders_active_exports_by_name() {
    let (_runtime, catalog) = migrated_catalog().await;

    catalog
        .create_export(create_export("disk-b", 1024, 4096))
        .await
        .expect("create disk-b");
    catalog
        .create_export(create_export("disk-a", 1024, 4096))
        .await
        .expect("create disk-a");

    let names: Vec<String> = catalog
        .list_exports(ListExports::active_only())
        .await
        .expect("list active")
        .iter()
        .map(|meta| meta.name().as_str().to_owned())
        .collect();

    assert_eq!(names, ["disk-a", "disk-b"]);
}

#[tokio::test]
async fn migration_rejects_zero_sized_heads() {
    let (_runtime, catalog) = migrated_catalog().await;

    sqlx::query(
        r#"
        INSERT INTO exports (
          id, name, engine_kind, block_size, state, created_at, updated_at
        )
        VALUES ('export-zero', 'zero', 'memory', 4096, 'active', 'now', 'now')
        "#,
    )
    .execute(catalog.pool())
    .await
    .expect("insert export row");

    let error = sqlx::query(
        r#"
        INSERT INTO export_heads (
          export_id, layout_kind, root_node_id, size_bytes,
          base_wal_seq, updated_at
        )
        VALUES (
          'export-zero', 'memory_empty', NULL, 0, 0, 'now'
        )
        "#,
    )
    .execute(catalog.pool())
    .await
    .expect_err("zero-sized head should violate migration constraints");

    assert!(error.to_string().contains("CHECK constraint failed"));
}

#[tokio::test]
async fn simple_tree_loads_empty_sparse_head() {
    let (_runtime, catalog) = migrated_catalog().await;
    let created = catalog
        .create_export(create_export_with_engine(
            "disk-a",
            128 * 1024 * 1024,
            4096,
            ExportEngineKind::SimpleDurable,
        ))
        .await
        .expect("create export");

    let snapshot = catalog
        .load_simple_tree(created.id())
        .await
        .expect("load simple tree");

    assert_eq!(snapshot.export_id(), created.id());
    assert_eq!(snapshot.size_bytes(), 128 * 1024 * 1024);
    assert!(snapshot.root_node_id().is_none());
    assert!(snapshot.chunks().is_empty());
}

#[tokio::test]
async fn simple_tree_commits_new_leaf_metadata() {
    let (_runtime, catalog) = migrated_catalog().await;
    let created = catalog
        .create_export(create_export_with_engine(
            "disk-a",
            128 * 1024 * 1024,
            4096,
            ExportEngineKind::SimpleDurable,
        ))
        .await
        .expect("create export");
    let chunk = SimpleChunkRef::new(
        ChunkIndex::new(2),
        BlobKey::new("blob-two").expect("valid blob key"),
        SIMPLE_CHUNK_BYTES,
    )
    .expect("valid chunk");

    let snapshot = catalog
        .commit_simple_chunks(created.id(), vec![chunk.clone()])
        .await
        .expect("commit simple chunk");

    let root_node_id = snapshot.root_node_id().expect("root node should exist");
    assert_eq!(
        snapshot
            .chunk(ChunkIndex::new(2))
            .expect("chunk should be materialized"),
        &chunk
    );

    let reloaded = catalog
        .load_simple_tree(created.id())
        .await
        .expect("reload simple tree");
    assert_eq!(reloaded.root_node_id(), Some(root_node_id));
    assert_eq!(reloaded.chunk(ChunkIndex::new(2)), Some(&chunk));
    assert!(reloaded.chunk(ChunkIndex::new(1)).is_none());
}

#[tokio::test]
async fn simple_tree_rejects_foreign_root() {
    let (_runtime, catalog) = migrated_catalog().await;
    let source = catalog
        .create_export(create_export_with_engine(
            "source",
            128 * 1024 * 1024,
            4096,
            ExportEngineKind::SimpleDurable,
        ))
        .await
        .expect("create source export");
    let destination = catalog
        .create_export(create_export_with_engine(
            "destination",
            128 * 1024 * 1024,
            4096,
            ExportEngineKind::SimpleDurable,
        ))
        .await
        .expect("create destination export");
    let source_snapshot = catalog
        .commit_simple_chunks(
            source.id(),
            vec![SimpleChunkRef::new(
                ChunkIndex::new(2),
                BlobKey::new("source-simple").expect("valid blob key"),
                SIMPLE_CHUNK_BYTES,
            )
            .expect("valid simple chunk")],
        )
        .await
        .expect("commit source chunk");
    let source_root = source_snapshot.root_node_id().expect("source root");

    sqlx::query(
        r#"
        UPDATE export_heads
        SET root_node_id = ?
        WHERE export_id = ?
        "#,
    )
    .bind(source_root.as_str())
    .bind(destination.id().as_str())
    .execute(catalog.pool())
    .await
    .expect("point destination at source root");

    let error = catalog
        .load_simple_tree(destination.id())
        .await
        .expect_err("simple roots must remain export-private");
    assert!(error.to_string().contains("not owned by export"));
}

#[tokio::test]
async fn simple_tree_rejects_existing_leaf_metadata() {
    let (_runtime, catalog) = migrated_catalog().await;
    let created = catalog
        .create_export(create_export_with_engine(
            "disk-a",
            128 * 1024 * 1024,
            4096,
            ExportEngineKind::SimpleDurable,
        ))
        .await
        .expect("create export");
    let first = SimpleChunkRef::new(
        ChunkIndex::new(1),
        BlobKey::new("blob-one").expect("valid blob key"),
        SIMPLE_CHUNK_BYTES,
    )
    .expect("valid chunk");
    catalog
        .commit_simple_chunks(created.id(), vec![first])
        .await
        .expect("commit first chunk");

    let second = SimpleChunkRef::new(
        ChunkIndex::new(1),
        BlobKey::new("blob-one-replacement").expect("valid blob key"),
        SIMPLE_CHUNK_BYTES,
    )
    .expect("valid chunk");
    let error = catalog
        .commit_simple_chunks(created.id(), vec![second])
        .await
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("chunk 1 is already materialized"));
}

#[tokio::test]
async fn cow_tree_publish_creates_checkpoint_root() {
    let (_runtime, catalog) = migrated_catalog().await;
    let created = catalog
        .create_export(create_export_with_engine(
            "disk-wal",
            128 * 1024 * 1024,
            4096,
            ExportEngineKind::WalDurable,
        ))
        .await
        .expect("create export");
    let chunk = cow_chunk(2, "blob-two");

    let outcome = catalog
        .publish_compaction(
            PublishCompaction::new(
                created.id().clone(),
                created.head().clone(),
                WalSeq::new(4),
                vec![chunk.clone()],
            )
            .expect("publish request"),
        )
        .await
        .expect("publish compaction");
    let published = match outcome {
        PublishCompactionOutcome::Published(meta) => meta,
        outcome => panic!("expected Published, got {outcome:?}"),
    };
    assert_eq!(published.head().base_wal_seq(), WalSeq::new(4));
    assert!(published.head().root_node_id().is_some());

    let snapshot = catalog
        .load_cow_tree(created.id())
        .await
        .expect("load cow tree");
    assert_eq!(snapshot.root_node_id(), published.head().root_node_id());
    assert_eq!(snapshot.base_wal_seq(), WalSeq::new(4));
    assert_eq!(snapshot.chunk(ChunkIndex::new(2)), Some(&chunk));
    assert!(snapshot.chunk(ChunkIndex::new(1)).is_none());
}

#[tokio::test]
async fn cow_tree_allows_shared_immutable_root() {
    let (_runtime, catalog) = migrated_catalog().await;
    let source = catalog
        .create_export(create_export_with_engine(
            "source",
            128 * 1024 * 1024,
            4096,
            ExportEngineKind::WalDurable,
        ))
        .await
        .expect("create source export");
    let destination = catalog
        .create_export(create_export_with_engine(
            "destination",
            128 * 1024 * 1024,
            4096,
            ExportEngineKind::WalDurable,
        ))
        .await
        .expect("create destination export");
    let chunk = cow_chunk(2, "source-cow");
    let published = catalog
        .publish_compaction(
            PublishCompaction::new(
                source.id().clone(),
                source.head().clone(),
                WalSeq::new(4),
                vec![chunk.clone()],
            )
            .expect("publish request"),
        )
        .await
        .expect("publish compaction")
        .into_record();
    let source_root = published.head().root_node_id().expect("source root");

    sqlx::query(
        r#"
        UPDATE export_heads
        SET root_node_id = ?
        WHERE export_id = ?
        "#,
    )
    .bind(source_root.as_str())
    .bind(destination.id().as_str())
    .execute(catalog.pool())
    .await
    .expect("point destination at source root");

    let snapshot = catalog
        .load_cow_tree(destination.id())
        .await
        .expect("load shared cow tree");
    assert_eq!(snapshot.export_id(), destination.id());
    assert_eq!(snapshot.root_node_id(), Some(source_root));
    assert_eq!(snapshot.base_wal_seq(), WalSeq::zero());
    assert_eq!(snapshot.chunk(ChunkIndex::new(2)), Some(&chunk));
}

#[tokio::test]
async fn clone_export_copies_root_and_reuses_unchanged_nodes() {
    let (_runtime, catalog) = migrated_catalog().await;
    let source = catalog
        .create_export(create_export_with_engine(
            "source",
            128 * 1024 * 1024,
            4096,
            ExportEngineKind::WalDurable,
        ))
        .await
        .expect("create source export");
    let source_zero = cow_chunk(0, "source-zero");
    let source_one = cow_chunk(1, "source-one");
    let source = publish_cow_root(
        &catalog,
        &source,
        4,
        vec![source_zero.clone(), source_one.clone()],
    )
    .await;
    let source_root = source.head().root_node_id().expect("source root");

    let cloned = catalog
        .clone_export(
            CloneExport::new(export_name("source"), export_name("destination"))
                .expect("clone request"),
        )
        .await
        .expect("clone export");
    let destination = cloned.destination();
    assert_eq!(cloned.source().id(), source.id());
    assert_ne!(destination.id(), source.id());
    assert_eq!(destination.engine_kind(), ExportEngineKind::WalDurable);
    assert_eq!(
        destination.head().layout_kind(),
        ExportLayoutKind::CowImmutableTree
    );
    assert_eq!(destination.head().root_node_id(), Some(source_root));
    assert_eq!(destination.head().base_wal_seq(), WalSeq::zero());
    assert_eq!(destination.size_bytes(), source.size_bytes());
    assert_eq!(destination.block_size(), source.block_size());

    let child_zero = cow_chunk(0, "child-zero");
    let published_child = catalog
        .publish_compaction(
            PublishCompaction::new(
                destination.id().clone(),
                destination.head().clone(),
                WalSeq::new(1),
                vec![child_zero.clone(), source_one.clone()],
            )
            .expect("publish child request"),
        )
        .await
        .expect("publish child compaction")
        .into_record();
    let child_root = published_child.head().root_node_id().expect("child root");
    assert_ne!(child_root, source_root);

    assert_ne!(
        cow_child_node(&catalog, source_root, 0).await,
        cow_child_node(&catalog, child_root, 0).await,
    );
    assert_eq!(
        cow_child_node(&catalog, source_root, 1).await,
        cow_child_node(&catalog, child_root, 1).await,
    );

    let source_snapshot = catalog
        .load_cow_tree(source.id())
        .await
        .expect("load source tree");
    let destination_snapshot = catalog
        .load_cow_tree(destination.id())
        .await
        .expect("load destination tree");
    assert_eq!(
        source_snapshot.chunk(ChunkIndex::new(0)),
        Some(&source_zero)
    );
    assert_eq!(
        destination_snapshot.chunk(ChunkIndex::new(0)),
        Some(&child_zero)
    );
    assert_eq!(
        source_snapshot.chunk(ChunkIndex::new(1)),
        destination_snapshot.chunk(ChunkIndex::new(1)),
    );
}

#[tokio::test]
async fn clone_export_rejects_empty_cow_source() {
    let (_runtime, catalog) = migrated_catalog().await;
    catalog
        .create_export(create_export_with_engine(
            "empty",
            128 * 1024 * 1024,
            4096,
            ExportEngineKind::WalDurable,
        ))
        .await
        .expect("create empty source");

    let error = catalog
        .clone_export(
            CloneExport::new(export_name("empty"), export_name("destination"))
                .expect("clone request"),
        )
        .await
        .expect_err("empty source must be rejected");
    assert!(error
        .to_string()
        .contains("source committed snapshot is empty"));
}

#[tokio::test]
async fn clone_export_rejects_invalid_sources_and_destinations() {
    let (_runtime, catalog) = migrated_catalog().await;
    let missing = catalog
        .clone_export(
            CloneExport::new(export_name("missing"), export_name("destination"))
                .expect("clone request"),
        )
        .await
        .expect_err("missing source should fail");
    assert!(matches!(missing, CatalogError::ExportNotFound { .. }));

    catalog
        .create_export(create_export("memory", TREE_CHUNK_BYTES, 4096))
        .await
        .expect("create memory source");
    let memory = catalog
        .clone_export(
            CloneExport::new(export_name("memory"), export_name("memory-clone"))
                .expect("clone request"),
        )
        .await
        .expect_err("memory source should fail");
    assert!(memory.to_string().contains("wal_durable"));

    catalog
        .create_export(create_export_with_engine(
            "simple",
            TREE_CHUNK_BYTES,
            4096,
            ExportEngineKind::SimpleDurable,
        ))
        .await
        .expect("create simple source");
    let simple = catalog
        .clone_export(
            CloneExport::new(export_name("simple"), export_name("simple-clone"))
                .expect("clone request"),
        )
        .await
        .expect_err("simple source should fail");
    assert!(simple.to_string().contains("wal_durable"));

    let _deleted = catalog
        .create_export(create_export_with_engine(
            "deleted",
            TREE_CHUNK_BYTES,
            4096,
            ExportEngineKind::WalDurable,
        ))
        .await
        .expect("create deleted source");
    catalog
        .delete_export(DeleteExport::new(export_name("deleted")))
        .await
        .expect("delete source");
    let deleted_error = catalog
        .clone_export(
            CloneExport::new(export_name("deleted"), export_name("deleted-clone"))
                .expect("clone request"),
        )
        .await
        .expect_err("deleted source should fail");
    assert!(matches!(deleted_error, CatalogError::ExportDeleted { .. }));

    let source = catalog
        .create_export(create_export_with_engine(
            "dupe-source",
            TREE_CHUNK_BYTES,
            4096,
            ExportEngineKind::WalDurable,
        ))
        .await
        .expect("create duplicate source");
    publish_cow_root(&catalog, &source, 1, vec![cow_chunk(0, "dupe-source")]).await;
    catalog
        .create_export(create_export("existing", TREE_CHUNK_BYTES, 4096))
        .await
        .expect("create existing destination");
    let duplicate = catalog
        .clone_export(
            CloneExport::new(export_name("dupe-source"), export_name("existing"))
                .expect("clone request"),
        )
        .await
        .expect_err("duplicate destination should fail");
    assert!(matches!(
        duplicate,
        CatalogError::ExportAlreadyExists { .. }
    ));
}

#[tokio::test]
async fn cow_tree_publish_is_idempotent_for_covered_checkpoint() {
    let (_runtime, catalog) = migrated_catalog().await;
    let created = catalog
        .create_export(create_export_with_engine(
            "disk-wal",
            128 * 1024 * 1024,
            4096,
            ExportEngineKind::WalDurable,
        ))
        .await
        .expect("create export");
    let base = created.head().clone();
    catalog
        .publish_compaction(
            PublishCompaction::new(
                created.id().clone(),
                base.clone(),
                WalSeq::new(4),
                vec![cow_chunk(0, "blob-zero")],
            )
            .expect("first publish"),
        )
        .await
        .expect("publish first checkpoint");

    let outcome = catalog
        .publish_compaction(
            PublishCompaction::new(
                created.id().clone(),
                base,
                WalSeq::new(2),
                vec![cow_chunk(0, "blob-duplicate")],
            )
            .expect("duplicate publish"),
        )
        .await
        .expect("publish covered checkpoint");

    let covered = match outcome {
        PublishCompactionOutcome::AlreadyCovered(meta) => meta,
        outcome => panic!("expected AlreadyCovered, got {outcome:?}"),
    };
    assert_eq!(covered.head().base_wal_seq(), WalSeq::new(4));
    let snapshot = catalog
        .load_cow_tree(created.id())
        .await
        .expect("load cow tree");
    assert_eq!(
        snapshot
            .chunk(ChunkIndex::new(0))
            .unwrap()
            .blob_key()
            .as_str(),
        "blob-zero"
    );
}

#[tokio::test]
async fn cow_tree_publish_rejects_stale_base() {
    let (_runtime, catalog) = migrated_catalog().await;
    let created = catalog
        .create_export(create_export_with_engine(
            "disk-wal",
            128 * 1024 * 1024,
            4096,
            ExportEngineKind::WalDurable,
        ))
        .await
        .expect("create export");
    let base = created.head().clone();
    catalog
        .publish_compaction(
            PublishCompaction::new(
                created.id().clone(),
                base.clone(),
                WalSeq::new(4),
                vec![cow_chunk(0, "blob-zero")],
            )
            .expect("first publish"),
        )
        .await
        .expect("publish first checkpoint");

    let outcome = catalog
        .publish_compaction(
            PublishCompaction::new(
                created.id().clone(),
                base,
                WalSeq::new(6),
                vec![cow_chunk(1, "blob-one")],
            )
            .expect("stale publish"),
        )
        .await
        .expect("publish stale checkpoint");

    let stale = match outcome {
        PublishCompactionOutcome::StalePlan(meta) => meta,
        outcome => panic!("expected StalePlan, got {outcome:?}"),
    };
    assert_eq!(stale.head().base_wal_seq(), WalSeq::new(4));
    let snapshot = catalog
        .load_cow_tree(created.id())
        .await
        .expect("load cow tree");
    assert!(snapshot.chunk(ChunkIndex::new(1)).is_none());
}

#[tokio::test]
async fn cow_tree_concurrent_publish_allows_only_one_winner() {
    let (_runtime, catalog) = migrated_catalog().await;
    let created = catalog
        .create_export(create_export_with_engine(
            "disk-wal",
            128 * 1024 * 1024,
            4096,
            ExportEngineKind::WalDurable,
        ))
        .await
        .expect("create export");
    let base = created.head().clone();
    let left = catalog.clone();
    let right = catalog.clone();
    let left_request = PublishCompaction::new(
        created.id().clone(),
        base.clone(),
        WalSeq::new(4),
        vec![cow_chunk(0, "blob-left")],
    )
    .expect("left publish request");
    let right_request = PublishCompaction::new(
        created.id().clone(),
        base,
        WalSeq::new(6),
        vec![cow_chunk(1, "blob-right")],
    )
    .expect("right publish request");

    let (left_result, right_result) = tokio::join!(
        left.publish_compaction(left_request),
        right.publish_compaction(right_request),
    );
    let outcomes = [left_result, right_result];
    // The losing publisher may observe a stale/covered head or a backend
    // contention error. The catalog contract is one winner and a consistent
    // final serving head, not a SQLite-specific loser error.
    let published = outcomes
        .iter()
        .filter_map(|outcome| match outcome {
            Ok(PublishCompactionOutcome::Published(meta)) => Some(meta),
            Ok(
                PublishCompactionOutcome::AlreadyCovered(_)
                | PublishCompactionOutcome::StalePlan(_),
            )
            | Err(_) => None,
        })
        .collect::<Vec<_>>();
    let non_published = outcomes
        .iter()
        .filter(|outcome| !matches!(outcome, Ok(PublishCompactionOutcome::Published(_))))
        .count();

    assert_eq!(
        published.len(),
        1,
        "exactly one concurrent compaction publish should win: {outcomes:?}",
    );
    assert_eq!(
        non_published, 1,
        "the competing publish must fail or observe a covered/stale head: {outcomes:?}",
    );

    let snapshot = catalog
        .load_cow_tree(created.id())
        .await
        .expect("load cow tree");
    assert_eq!(snapshot.root_node_id(), published[0].head().root_node_id());
    assert_eq!(snapshot.base_wal_seq(), published[0].head().base_wal_seq(),);
}

#[tokio::test]
async fn cow_tree_publish_rejects_deleted_and_wrong_layout_exports() {
    let (_runtime, catalog) = migrated_catalog().await;
    let memory = catalog
        .create_export(create_export("disk-memory", TREE_CHUNK_BYTES, 4096))
        .await
        .expect("create memory export");
    let wal = catalog
        .create_export(create_export_with_engine(
            "disk-wal",
            TREE_CHUNK_BYTES,
            4096,
            ExportEngineKind::WalDurable,
        ))
        .await
        .expect("create wal export");
    catalog
        .delete_export(DeleteExport::new(export_name("disk-wal")))
        .await
        .expect("delete wal export");

    let wrong_layout = catalog
        .publish_compaction(
            PublishCompaction::new(
                memory.id().clone(),
                wal.head().clone(),
                WalSeq::new(1),
                vec![cow_chunk(0, "blob-memory")],
            )
            .expect("wrong layout publish"),
        )
        .await
        .unwrap_err();
    assert!(wrong_layout.to_string().contains("engine_kind"));

    let deleted = catalog
        .publish_compaction(
            PublishCompaction::new(
                wal.id().clone(),
                wal.head().clone(),
                WalSeq::new(1),
                vec![cow_chunk(0, "blob-deleted")],
            )
            .expect("deleted publish"),
        )
        .await
        .unwrap_err();
    assert!(matches!(deleted, CatalogError::ExportDeleted { .. }));
}

async fn migrated_catalog() -> (TestRuntime, SQLiteExportCatalog) {
    let runtime = TestRuntime::new().expect("test runtime");
    let url = CatalogUrl::parse(runtime.catalog_url()).expect("catalog URL");
    let catalog = SQLiteExportCatalog::connect(&url)
        .await
        .expect("connect catalog");

    for migration in MIGRATIONS {
        sqlx::raw_sql(migration)
            .execute(catalog.pool())
            .await
            .expect("apply migration");
    }

    (runtime, catalog)
}

fn export_name(name: &str) -> ExportName {
    ExportName::new(name).expect("valid export name")
}

async fn publish_cow_root(
    catalog: &SQLiteExportCatalog,
    export: &ExportRecord,
    checkpoint: u64,
    chunks: Vec<CowChunkRef>,
) -> ExportRecord {
    catalog
        .publish_compaction(
            PublishCompaction::new(
                export.id().clone(),
                export.head().clone(),
                WalSeq::new(checkpoint),
                chunks,
            )
            .expect("publish request"),
        )
        .await
        .expect("publish compaction")
        .into_record()
}

async fn cow_child_node(catalog: &SQLiteExportCatalog, root: &NodeId, slot: u64) -> NodeId {
    let row = sqlx::query(
        r#"
        SELECT child_node_id
        FROM tree_edges
        WHERE parent_node_id = ? AND slot = ?
        "#,
    )
    .bind(root.as_str())
    .bind(slot as i64)
    .fetch_one(catalog.pool())
    .await
    .expect("load child edge");

    NodeId::new(
        row.try_get::<String, _>("child_node_id")
            .expect("child node id"),
    )
    .expect("valid child node id")
}

fn create_export(name: &str, size_bytes: u64, block_size: u64) -> CreateExport {
    create_export_with_engine(name, size_bytes, block_size, ExportEngineKind::Memory)
}

fn create_export_with_engine(
    name: &str,
    size_bytes: u64,
    block_size: u64,
    engine_kind: ExportEngineKind,
) -> CreateExport {
    CreateExport::new(export_name(name), size_bytes, block_size, engine_kind)
        .expect("valid create export request")
}

fn cow_chunk(index: u64, blob_key: &str) -> CowChunkRef {
    CowChunkRef::new(
        ChunkIndex::new(index),
        BlobKey::new(blob_key).expect("valid blob key"),
        TREE_CHUNK_BYTES,
    )
    .expect("valid cow chunk")
}
