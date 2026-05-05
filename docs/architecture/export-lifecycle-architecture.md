Title: Export Lifecycle Architecture
Date: 2026-05-01
Status: draft

# Problem

Some export operations need to coordinate durable catalog metadata with the
etcd lease model. If open and delete each talk directly to only one side of
that boundary, they can race:

```text
open loads export as active
delete sees no active lease
delete marks export deleted
open acquires lease and starts serving stale metadata
```

The architecture needs a small lifecycle boundary that composes
`ExportCatalog` and `ExportLeaseStore` without making either component
own the other's state.

# Goal

Define export lifecycle orchestration for:

- opening an export for serving;
- deleting an export from `nbdcli`;
- preventing open/delete races;
- keeping `ExportCatalog` as durable metadata truth;
- keeping etcd leases as lifecycle exclusion truth.

# Responsibilities

`ExportLifecycleManager` is the control-plane orchestration boundary for
operations that need both catalog state and export leases.

It does not store metadata by itself. It coordinates:

- `ExportCatalog` for durable export state;
- `ExportLeaseStore` for per-export lifecycle exclusion;
- `LocalExportRegistry` only after an NBD server has acquired a serving lease.

This is the target open/delete race-prevention boundary. The current local
prototype has catalog delete and process-local open exclusion, but it does not
yet implement `ExportLifecycleManager` or `ExportLeaseStore`.

# Lease Model

Open and delete contend on the same per-export lease namespace.

A lease holder may be:

```rust
enum ExportLeasePurpose {
    Serve,
    Delete,
}
```

Serving leases are held and renewed by the NBD server while the export is
mounted. Delete leases are held by `nbdcli` only long enough to mark the export
deleted in the catalog.

The exact etcd key layout is a design-phase detail. The architectural
requirement is that a delete lease and a serving lease for the same export name
cannot coexist.

# API Shape

Use structured requests and results.

```rust
struct BeginOpenExport {
    name: ExportName,
    holder: ServerId,
}

struct OpenExportLease {
    export: ExportMeta,
    lease: ExportLeaseSnapshot,
}

struct DeleteExportLifecycle {
    name: ExportName,
    holder: ManagementClientId,
}

trait ExportLifecycleManager {
    async fn begin_open(&self, request: BeginOpenExport)
        -> Result<OpenExportLease>;

    async fn delete_export(&self, request: DeleteExportLifecycle)
        -> Result<()>;
}
```

`begin_open` returns both the catalog metadata and the lease snapshot that must
be registered with `LocalExportRegistry`.

# Open Flow

```text
NBD_OPT_GO(export_name)
  -> ExportLifecycleManager.begin_open(export_name)
       -> acquire per-export lease with purpose = Serve
       -> load export from ExportCatalog
       -> reject missing/deleted export
       -> return ExportMeta + lease
  -> LocalExportRegistry.register(..., lease, state = Opening)
  -> initialize Export components
  -> replay WAL into ExportReadView
  -> transition local record to Active
  -> enter transmission phase
```

Acquiring the lease before loading the export ensures a concurrent delete
cannot mark the export deleted while open is moving toward serving.

If `begin_open` acquires the lease and then discovers the export is missing or
deleted, it must release the lease before returning failure.

If initialization or WAL replay fails after `begin_open` succeeds, the open
path must unregister the local record and release the lease before returning
failure.

# Delete Flow

```text
nbdcli delete name
  -> ExportLifecycleManager.delete_export(name)
       -> acquire per-export lease with purpose = Delete
       -> if lease acquisition fails: return ExportBusy
       -> ExportCatalog.delete_export(name)
       -> release delete lease
```

Taking the lease is the active check. It proves no server currently holds the
serving lease and blocks the next open from loading and serving the export
while deletion is in progress.

After the catalog is marked deleted, the delete lease can be released. A later
open may acquire the lease, but it will load the deleted catalog state and fail
before serving.

If `nbdcli` crashes while holding the delete lease, the lease expires. If the
catalog was not marked deleted, later opens may proceed. If the catalog was
marked deleted, later opens fail from catalog state.

# Invariants

- `ExportCatalog` remains durable export metadata truth.
- `ExportLeaseStore` remains lifecycle exclusion truth.
- Open and delete contend on the same per-export lease.
- A delete operation marks the catalog deleted only while holding the lease.
- A serving export registers locally only after acquiring the serving lease.
- A serving export releases or stops renewing its lease on close.
- `LocalExportRegistry` does not decide cross-process delete eligibility.
- `nbdcli delete` acquires the lease; it does not merely inspect it.
- A deleted catalog state prevents future serving even after the delete lease
  is released.

# Design-Phase Details

These are intentionally not architecture blockers:

- exact etcd key layout;
- exact lease holder encoding;
- exact error type mapping for `ExportBusy`, missing export, and deleted
  export.
