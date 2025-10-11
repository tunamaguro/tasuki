use tasuki_core::{JobData, JobResult, TokioSpawner, WorkerBuilder, WorkerContext};
use tasuki_postgres::backend::BackEnd;
use tasuki_postgres::client::{Client, InsertJob};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .compact()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let (raw_client, connection) = tokio_postgres::connect(
        "postgres://root:password@postgres:5432/app",
        tokio_postgres::NoTls,
    )
    .await?;

    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::error!(%error, "postgres connection terminated");
        }
    });

    let client = std::sync::Arc::new(raw_client);

    let backend = BackEnd::<_, u64>::new(client.clone());
    let worker = WorkerBuilder::new(std::time::Duration::from_secs(1))
        .handler(job_handler)
        .job_spawner(TokioSpawner)
        .build(backend);

    let producer_client = Client::<_, u64>::new(client.clone());

    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
        let mut n = 0u64;
        loop {
            interval.tick().await;
            let job = InsertJob::new(n);
            match producer_client.insert(&job).await {
                Ok(()) => tracing::info!(job_id = n, "queued job"),
                Err(error) => tracing::error!(%error, "failed to enqueue job"),
            }
            n = n.wrapping_add(1);
        }
    });

    tasks.spawn(worker.run());

    tasks.join_all().await;

    Ok(())
}

async fn job_handler(
    JobData(count): JobData<u64>,
    WorkerContext(_): WorkerContext<()>,
) -> JobResult {
    let handle = tokio::spawn(async move {
        tracing::info!(job_id = count, "start job");
        tokio::time::sleep(std::time::Duration::from_secs(count % 5 + 1)).await;
        tracing::info!(job_id = count, "finish job");
    });

    match handle.await {
        Ok(()) => JobResult::Complete,
        Err(_) => JobResult::Retry(None),
    }
}
