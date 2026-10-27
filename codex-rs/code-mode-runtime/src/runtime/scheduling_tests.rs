use super::*;
use pretty_assertions::assert_eq;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc as std_mpsc;
use std::time::Duration;

const WATCHDOG: Duration = Duration::from_secs(5);

struct BlockingDrop {
    entered: std_mpsc::Sender<()>,
    release: std_mpsc::Receiver<()>,
    drops: Arc<AtomicUsize>,
}

impl Drop for BlockingDrop {
    fn drop(&mut self) {
        let _ = self.entered.send(());
        let _ = self.release.recv_timeout(WATCHDOG);
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn cancelled_queued_work_cleans_up_without_blocking_other_cells() {
    let executor = RuntimeExecutor::new().unwrap();
    let cancellation = CancellationToken::new();
    let (ready_tx, ready_rx) = std_mpsc::channel();
    let (worker_release, worker_wait) = std_mpsc::channel();
    let (drop_entered, drop_started) = std_mpsc::channel();
    let (drop_release, drop_wait) = std_mpsc::channel();
    let (completed_tx, completed_rx) = std_mpsc::channel();
    let drops = Arc::new(AtomicUsize::new(0));
    let executed = Arc::new(AtomicBool::new(false));
    let probe = BlockingDrop {
        entered: drop_entered,
        release: drop_wait,
        drops: Arc::clone(&drops),
    };
    let worker_executed = Arc::clone(&executed);
    let coordinator = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        ready_tx.send(runtime.handle().clone()).unwrap();
        runtime.block_on(async move {
            let (worker_started, worker_ready) = oneshot::channel();
            let worker = tokio::task::spawn_blocking(move || {
                let _ = worker_started.send(());
                let _ = worker_wait.recv_timeout(WATCHDOG);
            });
            worker_ready.await.unwrap();
            let (events, _) = mpsc::unbounded_channel();
            let job = run_runtime_job(&executor, events, None, &cancellation, move || {
                worker_executed.store(true, Ordering::SeqCst);
                drop(probe);
            });
            tokio::pin!(job);
            assert!(futures::poll!(&mut job).is_pending());
            cancellation.cancel();
            let result = job.await;
            let _ = completed_tx.send(result);
            worker.await.unwrap();
        });
    });

    // Observe progress from outside the coordinator so a blocking destructor
    // produces a bounded failure instead of preventing its own timeout.
    let handle = ready_rx.recv_timeout(WATCHDOG);
    let cleanup_started = drop_started.recv_timeout(WATCHDOG);
    let (progress_tx, progress_rx) = std_mpsc::channel();
    if let Ok(handle) = &handle {
        handle.spawn(async move {
            let _ = progress_tx.send(());
        });
    }
    let progress = progress_rx.recv_timeout(WATCHDOG);
    let waiting_for_cleanup = matches!(completed_rx.try_recv(), Err(std_mpsc::TryRecvError::Empty));
    let _ = drop_release.send(());
    let completed = completed_rx.recv_timeout(WATCHDOG);
    let _ = worker_release.send(());
    let joined = coordinator.join();

    handle.unwrap();
    cleanup_started.unwrap();
    progress.unwrap();
    assert!(waiting_for_cleanup);
    assert_eq!(completed.unwrap(), None);
    joined.unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(!executed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn already_cancelled_work_never_executes() {
    let executor = RuntimeExecutor::new().unwrap();
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let (events, _) = mpsc::unbounded_channel();
    let (dropped, observed_drop) = oneshot::channel::<()>();
    let executed = Arc::new(AtomicBool::new(false));
    let worker_executed = Arc::clone(&executed);
    let result = run_runtime_job(&executor, events, None, &cancellation, move || {
        worker_executed.store(true, Ordering::SeqCst);
        drop(dropped);
    })
    .await;

    assert_eq!(result, None);
    assert!(!executed.load(Ordering::SeqCst));
    assert!(observed_drop.await.is_err());
}

#[tokio::test]
async fn cleanup_panic_is_reported_and_later_cleanup_still_runs() {
    struct PanickingDrop;
    impl Drop for PanickingDrop {
        fn drop(&mut self) {
            panic!("cleanup failure");
        }
    }

    let executor = RuntimeExecutor::new().unwrap();
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let (events, mut received) = mpsc::unbounded_channel();
    let probe = PanickingDrop;
    assert_eq!(
        run_runtime_job(&executor, events, None, &cancellation, move || drop(probe)).await,
        None
    );
    assert!(matches!(
        received.recv().await,
        Some(RuntimeEvent::ThreadPanicked)
    ));
    executor.dispose(()).await.unwrap();
}

#[tokio::test]
async fn runtime_thread_panic_before_initialization_is_reported_directly() {
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    drop(event_rx);
    let (failure_tx, mut failure_rx) = mpsc::unbounded_channel();
    let executor = RuntimeExecutor::new().unwrap();
    run_runtime_job(
        &executor,
        event_tx,
        Some(std::sync::Arc::new(move |reason| {
            let _ = failure_tx.send(reason);
        })),
        &CancellationToken::new(),
        || panic!("runtime thread panic probe"),
    )
    .await;

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), failure_rx.recv())
            .await
            .expect("runtime failure timeout")
            .expect("runtime failure"),
        "code-mode V8 runtime thread panicked"
    );
}

#[tokio::test]
async fn runtime_thread_panic_is_forwarded_without_owner_supervision() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let executor = RuntimeExecutor::new().unwrap();
    run_runtime_job(
        &executor,
        event_tx,
        /*task_failure_handler*/ None,
        &CancellationToken::new(),
        || panic!("runtime thread panic probe"),
    )
    .await;

    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("runtime panic event timeout"),
        Some(RuntimeEvent::ThreadPanicked)
    ));
}
