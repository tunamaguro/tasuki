use pin_project_lite::pin_project;
use tasuki::{
    BackEnd, Client, InsertJob, JobData, JobResult, TokioSpawner, WorkerBuilder, WorkerContext,
    worker::JobSpawner,
};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .compact()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let pool = sqlx::PgPool::connect("postgres://root:password@postgres:5432/app")
        .await
        .unwrap();

    let backend = BackEnd::new(pool.clone());
    let worker = WorkerBuilder::new(std::time::Duration::from_secs(1))
        .handler(job_handler)
        .job_spawner(TokioSpawner)
        .build(backend);

    let client = Client::<u64>::new(pool.clone());
    let client_handle = async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
        let mut n = 0;
        loop {
            interval.tick().await;
            let job = InsertJob::new(n);
            match client.insert(&job).await {
                Ok(_) => {
                    tracing::info!("Enqueue job {}", n);
                    n += 1
                }
                Err(error) => {
                    tracing::error!(error = %error, "Failed to enqueue job")
                }
            };
        }
    };

    let worker_fut = worker.run();
    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(client_handle);
    tasks.spawn(worker_fut);

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
