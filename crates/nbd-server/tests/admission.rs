use nbd_server::{AdmissionOp, ByteRange, ExportAdmissionCtl};

#[tokio::test]
async fn admission_orders_later_read_behind_waiting_write() {
    let admission = ExportAdmissionCtl::new();
    let read_1 = admission
        .register(read(0, 4096))
        .expect("register first read")
        .wait()
        .await
        .expect("first read permit");
    let write_2 = admission.register(write(0, 4096)).expect("register write");
    let read_3 = admission
        .register(read(0, 4096))
        .expect("register second read");

    let write_task = tokio::spawn(async move { write_2.wait().await.expect("write permit") });
    let read_task = tokio::spawn(async move { read_3.wait().await.expect("second read permit") });
    tokio::task::yield_now().await;
    assert!(!write_task.is_finished());
    assert!(!read_task.is_finished());

    drop(read_1);
    tokio::task::yield_now().await;
    assert!(write_task.is_finished());
    assert!(!read_task.is_finished());

    let write_permit = write_task.await.expect("write task");
    drop(write_permit);
    let read_permit = read_task.await.expect("read task");
    drop(read_permit);
}

#[tokio::test]
async fn admission_allows_non_overlapping_work() {
    let admission = ExportAdmissionCtl::new();
    let read_permit = admission
        .register(read(0, 4096))
        .expect("register read")
        .wait()
        .await
        .expect("read permit");
    let write_permit = admission
        .register(write(4096, 4096))
        .expect("register non-overlapping write")
        .wait()
        .await
        .expect("write permit");

    drop(read_permit);
    drop(write_permit);
}

#[tokio::test]
async fn admission_serializes_overlapping_writes() {
    let admission = ExportAdmissionCtl::new();
    let write_1 = admission
        .register(write(0, 4096))
        .expect("register first write")
        .wait()
        .await
        .expect("first write permit");
    let write_2 = admission
        .register(write(2048, 4096))
        .expect("register second write");

    let write_task = tokio::spawn(async move { write_2.wait().await.expect("second write") });
    tokio::task::yield_now().await;
    assert!(!write_task.is_finished());

    drop(write_1);
    let write_permit = write_task.await.expect("write task");
    drop(write_permit);
}

#[tokio::test]
async fn admission_flush_waits_and_blocks_later_work() {
    let admission = ExportAdmissionCtl::new();
    let read_permit = admission
        .register(read(0, 4096))
        .expect("register read")
        .wait()
        .await
        .expect("read permit");
    let flush = admission
        .register(AdmissionOp::Flush)
        .expect("register flush");
    let later_read = admission
        .register(read(4096, 4096))
        .expect("register later read");

    let flush_task = tokio::spawn(async move { flush.wait().await.expect("flush permit") });
    let read_task = tokio::spawn(async move { later_read.wait().await.expect("later read") });
    tokio::task::yield_now().await;
    assert!(!flush_task.is_finished());
    assert!(!read_task.is_finished());

    drop(read_permit);
    tokio::task::yield_now().await;
    assert!(flush_task.is_finished());
    assert!(!read_task.is_finished());

    let flush_permit = flush_task.await.expect("flush task");
    drop(flush_permit);
    let read_permit = read_task.await.expect("read task");
    drop(read_permit);
}

#[tokio::test]
async fn admission_cancelled_waiter_does_not_block_later_work() {
    let admission = ExportAdmissionCtl::new();
    let write_1 = admission
        .register(write(0, 4096))
        .expect("register active write")
        .wait()
        .await
        .expect("active write permit");
    let cancelled_write = admission
        .register(write(0, 4096))
        .expect("register cancelled write");
    let later_read = admission
        .register(read(0, 4096))
        .expect("register later read");

    drop(cancelled_write);
    let read_task = tokio::spawn(async move { later_read.wait().await.expect("later read") });
    tokio::task::yield_now().await;
    assert!(!read_task.is_finished());

    drop(write_1);
    let read_permit = read_task.await.expect("read task");
    drop(read_permit);
}

#[tokio::test]
async fn admission_dropping_granted_waiter_releases_active_permit() {
    let admission = ExportAdmissionCtl::new();
    let granted_read = admission
        .register(read(0, 4096))
        .expect("register granted read");

    drop(granted_read);
    let write_permit = admission
        .register(write(0, 4096))
        .expect("register write after dropped read")
        .wait()
        .await
        .expect("write permit");
    drop(write_permit);
}

#[tokio::test]
async fn admission_has_no_lost_wake_between_register_and_release() {
    let admission = ExportAdmissionCtl::new();

    let write_before_register = admission
        .register(write(0, 4096))
        .expect("register write")
        .wait()
        .await
        .expect("write permit");
    let read_after_register = admission
        .register(read(0, 4096))
        .expect("register waiting read");
    let read_task =
        tokio::spawn(async move { read_after_register.wait().await.expect("waiting read") });
    tokio::task::yield_now().await;
    assert!(!read_task.is_finished());

    drop(write_before_register);
    let read_permit = read_task.await.expect("waiting read task");
    drop(read_permit);

    let write_before_release = admission
        .register(write(0, 4096))
        .expect("register second write")
        .wait()
        .await
        .expect("second write permit");
    drop(write_before_release);
    let read_after_release = admission
        .register(read(0, 4096))
        .expect("register ready read")
        .wait()
        .await
        .expect("ready read permit");
    drop(read_after_release);
}

fn read(start: u64, len: u32) -> AdmissionOp {
    AdmissionOp::Read(ByteRange::new(start, len))
}

fn write(start: u64, len: u32) -> AdmissionOp {
    AdmissionOp::Write(ByteRange::new(start, len))
}
