use futures::FutureExt;
use tasuki::{BackEnd, Client, InsertJob, JobData, JobResult, WorkerBuilder, WorkerContext};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .compact()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let token = tokio_util::sync::CancellationToken::new();

    let pool = sqlx::PgPool::connect("postgres://root:password@postgres:5432/app")
        .await
        .unwrap();

    let backend = BackEnd::new(pool.clone());
    let mut listener = backend.listener().await.unwrap();

    let worker = WorkerBuilder::new()
        .tick(futures::stream::pending())
        .build(backend, job_handler)
        .subscribe(&mut listener)
        .with_graceful_shutdown(token.clone().cancelled_owned());

    let client = Client::<u64>::new(pool.clone());
    let client_token = token.clone();
    let client_handle = async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
        let mut n = 0;
        loop {
            tokio::select! {
                _ = client_token.cancelled()=>{
                    break;
                }
                _ = interval.tick()=>{
                    let job = InsertJob::new(n);
                    match client.insert(job).await {
                        Ok(_) => {
                            tracing::info!("Enqueue job {}", n);
                            n += 1
                        }
                        Err(error) => {
                            tracing::error!(error = %error, "Failed to enqueue job")
                        }
                    };
                }
            }
        }
    };

    let worker_fut = worker.run();
    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(client_handle);
    tasks.spawn(worker_fut);
    // Stop the listener when the cancellation token is triggered (e.g., Ctrl+C)
    tasks.spawn(
        listener
            .listen_until(token.clone().cancelled_owned())
            .map(|_| ()),
    );
    tasks.spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        token.cancel();
    });
    tasks.join_all().await;
}

async fn job_handler(
    JobData(count): JobData<u64>,
    WorkerContext(_): WorkerContext<()>,
) -> JobResult {
    let handle = tokio::spawn(async move {
        tracing::info!("-start: job {}", count);
        tokio::time::sleep(std::time::Duration::from_secs(count % 5 + 1)).await;

        tracing::info!("--end: job {}", count)
    });
    match handle.await {
        Ok(_) => JobResult::Cancel,
        Err(_) => JobResult::Retry(None),
    }
}
