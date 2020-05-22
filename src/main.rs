use tokio_postgres::NoTls;
use warp::{http::StatusCode, Filter, Rejection, Reply};

type Result<T> = std::result::Result<T, Rejection>;
type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("migrations");
}

#[tokio::main]
async fn main() {
    run_migrations().await.expect("can run DB migrations: {}");

    let health_route = warp::path!("health").and_then(health_handler);

    let routes = health_route.with(warp::cors().allow_any_origin());

    println!("Started server at localhost:8000");
    warp::serve(routes).run(([0, 0, 0, 0], 8000)).await;
}

async fn health_handler() -> Result<impl Reply> {
    Ok(StatusCode::OK)
}

async fn run_migrations() -> std::result::Result<(), Error> {
    println!("Running DB migrations...");
    let (mut client, con) = tokio_postgres::connect("host=localhost user=postgres", NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = con.await {
            eprintln!("connection error: {}", e);
        }
    });
    let migration_report = embedded::migrations::runner()
        .run_async(&mut client)
        .await?;
    for migration in migration_report.applied_migrations() {
        println!(
            "Migration Applied -  Name: {}, Version: {}",
            migration.name(),
            migration.version()
        );
    }
    println!("DB migrations finished!");

    Ok(())
}
