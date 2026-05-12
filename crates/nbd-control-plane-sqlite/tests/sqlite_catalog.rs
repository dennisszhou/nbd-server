use nbd_control_plane_core::{
    BlobKey, CatalogError, ChunkIndex, CloneExport, CowChunkRef, CreateExport, DeleteExport,
    ExportCatalog, ExportEngineKind, ExportHead, ExportLayoutKind, ExportName, ExportRecord,
    ExportState, InspectExport, ListExports, NodeId, PublishTreeUpdate, PublishTreeUpdateOutcome,
    SIMPLE_CHUNK_BYTES, TREE_CHUNK_BYTES, TreeEdgeLookup, TreeEdgeRecord, TreeFormat,
    TreeLeafRefRecord, TreeNodeKind, TreeNodeRecord, TreeRecordBatch, TreeRecordStore,
    TreeStorageKind, WalSeq,
};
use nbd_control_plane_sqlite::SQLiteExportCatalog;
use nbd_test_support::TestRuntime;
use std::error::Error as _;
use std::fs;

const MIGRATIONS: &[&str] = &[
    include_str!("../../../prisma/migrations/20260506000000_baseline/migration.sql"),
    include_str!("../../../prisma/migrations/20260512000000_tree_format/migration.sql"),
];

#[tokio::test]
async fn sqlite_errors_preserve_database_source() {
    let (_runtime, catalog) = migrated_catalog().await;
    catalog.pool().close().await;

    let error = catalog
        .list_exports(ListExports::active_only())
        .await
        .expect_err("closed pool should fail");

    assert!(matches!(error, CatalogError::Database { .. }));
    assert!(error.source().is_some());
    assert!(error.to_string().contains("database error:"));
}

#[tokio::test]
async fn connect_path_rejects_missing_catalog_file() {
    let runtime = TestRuntime::new().expect("test runtime");

    let error = SQLiteExportCatalog::connect_path(runtime.catalog_path())
        .await
        .expect_err("missing catalog should fail");

    assert!(matches!(error, CatalogError::Database { .. }));
    assert!(!runtime.catalog_path().exists());
}

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
    assert_eq!(created.head().tree_format(), None);

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
    assert_eq!(created.head().tree_format(), Some(TreeFormat::Bounded32V1));

    let head = catalog
        .load_export_head(created.id())
        .await
        .expect("load simple tree head");
    assert_eq!(head, created.head().clone());
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
    assert_eq!(created.head().tree_format(), Some(TreeFormat::Bounded32V1));

    let head = catalog
        .load_export_head(created.id())
        .await
        .expect("load cow tree head");
    assert_eq!(head, created.head().clone());
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

    assert!(
        catalog
            .list_exports(ListExports::active_only())
            .await
            .expect("list active")
            .is_empty()
    );

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
async fn tree_record_store_reads_rows_and_rolls_back_stale_publish() {
    let (_runtime, catalog) = migrated_catalog().await;
    let export = catalog
        .create_export(create_export_with_engine(
            "lazy-tree",
            128 * 1024 * 1024,
            4096,
            ExportEngineKind::SimpleDurable,
        ))
        .await
        .expect("create export");
    let root = node_id("lazy-root");
    let leaf = node_id("lazy-leaf");
    let missing = node_id("missing-node");
    let next_head = ExportHead::new_with_tree_format(
        ExportLayoutKind::SimpleMutableTree,
        Some(root.clone()),
        export.size_bytes(),
        WalSeq::zero(),
        export.head().tree_format(),
    )
    .expect("next head");

    let published = catalog
        .publish_tree_update(PublishTreeUpdate {
            export_id: export.id().clone(),
            expected_head: export.head().clone(),
            next_head: next_head.clone(),
            records: TreeRecordBatch {
                nodes: vec![
                    TreeNodeRecord {
                        id: root.clone(),
                        layout_kind: ExportLayoutKind::SimpleMutableTree,
                        owner_export_id: Some(export.id().clone()),
                        kind: TreeNodeKind::Internal,
                        level: 1,
                        span_start_bytes: 0,
                        span_len_bytes: export.size_bytes(),
                    },
                    TreeNodeRecord {
                        id: leaf.clone(),
                        layout_kind: ExportLayoutKind::SimpleMutableTree,
                        owner_export_id: Some(export.id().clone()),
                        kind: TreeNodeKind::Leaf,
                        level: 0,
                        span_start_bytes: 0,
                        span_len_bytes: SIMPLE_CHUNK_BYTES,
                    },
                ],
                edges: vec![TreeEdgeRecord {
                    parent_node_id: root.clone(),
                    slot: 0,
                    child_node_id: leaf.clone(),
                }],
                leaf_refs: vec![TreeLeafRefRecord {
                    node_id: leaf.clone(),
                    storage_kind: TreeStorageKind::MutableBlob,
                    storage_key: blob_key("lazy-blob"),
                    len_bytes: SIMPLE_CHUNK_BYTES,
                }],
            },
        })
        .await
        .expect("publish tree update");

    assert!(matches!(published, PublishTreeUpdateOutcome::Published(_)));
    assert_eq!(published.record().head(), &next_head);
    assert_eq!(catalog.load_node(&root).await.unwrap().unwrap().id, root);
    assert_eq!(
        catalog
            .load_nodes(&[root.clone(), missing.clone()])
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        catalog
            .load_child_edges(&[TreeEdgeLookup {
                parent_node_id: root.clone(),
                slots: vec![0, 1],
            }])
            .await
            .unwrap(),
        vec![TreeEdgeRecord {
            parent_node_id: root.clone(),
            slot: 0,
            child_node_id: leaf.clone(),
        }]
    );
    assert_eq!(
        catalog
            .load_leaf_refs(&[leaf.clone(), missing.clone()])
            .await
            .unwrap(),
        vec![TreeLeafRefRecord {
            node_id: leaf.clone(),
            storage_kind: TreeStorageKind::MutableBlob,
            storage_key: blob_key("lazy-blob"),
            len_bytes: SIMPLE_CHUNK_BYTES,
        }]
    );

    let stale_root = node_id("stale-root");
    let stale_head = ExportHead::new_with_tree_format(
        ExportLayoutKind::SimpleMutableTree,
        Some(stale_root.clone()),
        export.size_bytes(),
        WalSeq::zero(),
        export.head().tree_format(),
    )
    .expect("stale head");
    let stale = catalog
        .publish_tree_update(PublishTreeUpdate {
            export_id: export.id().clone(),
            expected_head: export.head().clone(),
            next_head: stale_head,
            records: TreeRecordBatch {
                nodes: vec![TreeNodeRecord {
                    id: stale_root.clone(),
                    layout_kind: ExportLayoutKind::SimpleMutableTree,
                    owner_export_id: Some(export.id().clone()),
                    kind: TreeNodeKind::Internal,
                    level: 1,
                    span_start_bytes: 0,
                    span_len_bytes: export.size_bytes(),
                }],
                edges: Vec::new(),
                leaf_refs: Vec::new(),
            },
        })
        .await
        .expect("stale publish");

    assert!(matches!(stale, PublishTreeUpdateOutcome::StaleHead(_)));
    assert_eq!(stale.record().head(), &next_head);
    assert!(catalog.load_node(&stale_root).await.unwrap().is_none());
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

    let published = publish_cow_root(&catalog, &created, 4, vec![chunk.clone()]).await;
    assert_eq!(published.head().base_wal_seq(), WalSeq::new(4));
    let root = published.head().root_node_id().expect("root should exist");
    assert_eq!(load_cow_chunk(&catalog, root, 2).await, Some(chunk));
    assert_eq!(load_cow_chunk(&catalog, root, 1).await, None);
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
    let published = publish_cow_root(&catalog, &source, 4, vec![chunk.clone()]).await;
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

    let destination_head = catalog
        .load_export_head(destination.id())
        .await
        .expect("load destination head");
    assert_eq!(destination_head.root_node_id(), Some(source_root));
    assert_eq!(destination_head.base_wal_seq(), WalSeq::zero());
    assert_eq!(load_cow_chunk(&catalog, source_root, 2).await, Some(chunk));
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
    assert_eq!(
        destination.head().tree_format(),
        source.head().tree_format()
    );
    assert_eq!(destination.size_bytes(), source.size_bytes());
    assert_eq!(destination.block_size(), source.block_size());

    assert_eq!(
        load_cow_chunk(&catalog, source_root, 0).await,
        Some(source_zero)
    );
    assert_eq!(
        load_cow_chunk(&catalog, source_root, 1).await,
        Some(source_one)
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
    assert!(
        error
            .to_string()
            .contains("source committed snapshot is empty")
    );
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
    let left_request = cow_publish_request(&created, &base, 4, vec![cow_chunk(0, "blob-left")]);
    let right_request = cow_publish_request(&created, &base, 6, vec![cow_chunk(1, "blob-right")]);

    let (left_result, right_result) = tokio::join!(
        left.publish_tree_update(left_request),
        right.publish_tree_update(right_request),
    );
    let outcomes = [left_result, right_result];
    // The losing publisher may observe a stale head or a backend
    // contention error. The catalog contract is one winner and a consistent
    // final serving head, not a SQLite-specific loser error.
    let published = outcomes
        .iter()
        .filter_map(|outcome| match outcome {
            Ok(PublishTreeUpdateOutcome::Published(meta)) => Some(meta),
            Ok(PublishTreeUpdateOutcome::StaleHead(_)) | Err(_) => None,
        })
        .collect::<Vec<_>>();
    let non_published = outcomes
        .iter()
        .filter(|outcome| !matches!(outcome, Ok(PublishTreeUpdateOutcome::Published(_))))
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

    let head = catalog
        .load_export_head(created.id())
        .await
        .expect("load cow tree head");
    assert_eq!(head.root_node_id(), published[0].head().root_node_id());
    assert_eq!(head.base_wal_seq(), published[0].head().base_wal_seq(),);
}

#[tokio::test]
async fn tree_record_publish_rejects_deleted_exports() {
    let (_runtime, catalog) = migrated_catalog().await;
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

    let deleted = catalog
        .publish_tree_update(cow_publish_request(
            &wal,
            wal.head(),
            1,
            vec![cow_chunk(0, "blob-deleted")],
        ))
        .await
        .unwrap_err();
    assert!(matches!(deleted, CatalogError::ExportDeleted { .. }));
}

async fn migrated_catalog() -> (TestRuntime, SQLiteExportCatalog) {
    let runtime = TestRuntime::new().expect("test runtime");
    fs::File::create(runtime.catalog_path()).expect("create catalog file");
    let catalog = SQLiteExportCatalog::connect_path(runtime.catalog_path())
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
    match catalog
        .publish_tree_update(cow_publish_request(
            export,
            export.head(),
            checkpoint,
            chunks,
        ))
        .await
        .expect("publish tree update")
    {
        PublishTreeUpdateOutcome::Published(record) => record,
        outcome => panic!("expected published tree update, got {outcome:?}"),
    }
}

fn cow_publish_request(
    export: &ExportRecord,
    expected_head: &ExportHead,
    checkpoint: u64,
    chunks: Vec<CowChunkRef>,
) -> PublishTreeUpdate {
    let root = NodeId::new(format!("{}-root-{checkpoint}", export.id())).expect("root node");
    let mut nodes = vec![TreeNodeRecord {
        id: root.clone(),
        layout_kind: ExportLayoutKind::CowImmutableTree,
        owner_export_id: None,
        kind: TreeNodeKind::Internal,
        level: 1,
        span_start_bytes: 0,
        span_len_bytes: export.size_bytes(),
    }];
    let mut edges = Vec::new();
    let mut leaf_refs = Vec::new();
    for chunk in chunks {
        let slot = u16::try_from(chunk.chunk_index().get()).expect("test chunk slot");
        let leaf = NodeId::new(format!(
            "{}-leaf-{checkpoint}-{}",
            export.id(),
            chunk.chunk_index()
        ))
        .expect("leaf node");
        nodes.push(TreeNodeRecord {
            id: leaf.clone(),
            layout_kind: ExportLayoutKind::CowImmutableTree,
            owner_export_id: None,
            kind: TreeNodeKind::Leaf,
            level: 0,
            span_start_bytes: chunk.chunk_index().get() * TREE_CHUNK_BYTES,
            span_len_bytes: TREE_CHUNK_BYTES
                .min(export.size_bytes() - chunk.chunk_index().get() * TREE_CHUNK_BYTES),
        });
        edges.push(TreeEdgeRecord {
            parent_node_id: root.clone(),
            slot,
            child_node_id: leaf.clone(),
        });
        leaf_refs.push(TreeLeafRefRecord {
            node_id: leaf,
            storage_kind: TreeStorageKind::ImmutableBlob,
            storage_key: chunk.blob_key().clone(),
            len_bytes: chunk.len_bytes(),
        });
    }
    let next_head = ExportHead::new_with_tree_format(
        ExportLayoutKind::CowImmutableTree,
        Some(root),
        expected_head.size_bytes(),
        WalSeq::new(checkpoint),
        expected_head.tree_format(),
    )
    .expect("next head");
    PublishTreeUpdate {
        export_id: export.id().clone(),
        expected_head: expected_head.clone(),
        next_head,
        records: TreeRecordBatch {
            nodes,
            edges,
            leaf_refs,
        },
    }
}

async fn load_cow_chunk(
    catalog: &SQLiteExportCatalog,
    root: &NodeId,
    slot: u64,
) -> Option<CowChunkRef> {
    let slot = u16::try_from(slot).expect("test slot");
    let edge = catalog
        .load_child_edges(&[TreeEdgeLookup {
            parent_node_id: root.clone(),
            slots: vec![slot],
        }])
        .await
        .expect("load child edge")
        .into_iter()
        .next()?;
    let leaf_ref = catalog
        .load_leaf_refs(&[edge.child_node_id])
        .await
        .expect("load leaf ref")
        .into_iter()
        .next()?;

    assert_eq!(leaf_ref.storage_kind, TreeStorageKind::ImmutableBlob);
    Some(
        CowChunkRef::new(
            ChunkIndex::new(u64::from(slot)),
            leaf_ref.storage_key,
            leaf_ref.len_bytes,
        )
        .expect("cow chunk"),
    )
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

fn node_id(id: &str) -> NodeId {
    NodeId::new(id).expect("valid node id")
}

fn blob_key(key: &str) -> BlobKey {
    BlobKey::new(key).expect("valid blob key")
}

fn cow_chunk(index: u64, blob_key: &str) -> CowChunkRef {
    CowChunkRef::new(
        ChunkIndex::new(index),
        self::blob_key(blob_key),
        TREE_CHUNK_BYTES,
    )
    .expect("valid cow chunk")
}
