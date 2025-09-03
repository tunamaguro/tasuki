// Insert a few jobs and list them.
//
// This example enqueues several jobs into the default queue and then
// fetches a single page using the client listing API.
use sqlx::types::Uuid;
use tasuki_sqlx::client::{Client, InsertJob, ListJobsOptions};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .compact()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    // Reuse the same DSN as other examples for consistency.
    let pool = sqlx::PgPool::connect("postgres://root:password@postgres:5432/app")
        .await
        .unwrap();

    let client = Client::<u64>::new(pool.clone());

    // Enqueue 200 jobs.
    for n in 0..=200 {
        let job = InsertJob::new(n);
        client.insert(&job).await.unwrap();
        tracing::info!("Enqueued job {}", n);
    }

    // List jobs 30 per page using keyset pagination.
    let mut cursor: Option<Uuid> = None;
    let page_size = 30;
    let mut total = 0;
    let mut page = 0;
    loop {
        let opts = ListJobsOptions {
            cursor_job_id: cursor,
            page_size,
        };

        let rows = client.list_jobs(&opts).await.unwrap();
        if rows.is_empty() {
            break;
        }

        println!("-- page {} --", page);
        println!("id\tstatus");
        for r in &rows {
            println!("{}\t{:?}", r.id, r.status);
        }

        cursor = rows.last().map(|r| r.id);
        page += 1;
        total += rows.len();
    }

    println!("total_rows = {}, total_pages = {}", total, page);

    // If not exists, return 0 rows
    let opts = ListJobsOptions {
        cursor_job_id: Uuid::parse_str("a1a2a3a4b1b2c1c2d1d2d3d4d5d6d7d8").ok(),
        page_size: 20,
    };

    let rows = client.list_jobs(&opts).await.unwrap();
    if rows.is_empty() {
        println!("if id not exists, return 0 rows")
    } else {
        panic!("unexpected")
    }
}

