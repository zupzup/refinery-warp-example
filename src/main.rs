use tokio_postgres::{Client, NoTls, Row};
use warp::{http::StatusCode, Filter, Rejection, Reply};

type Result<T> = std::result::Result<T, Rejection>;
type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("migrations");
}

#[derive(Debug)]
struct MigrationRow {
    version: i32,
    name: String,
    applied_on: String,
    checksum: String,
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

    let migration_state_before = fetch_migration_state(&client).await?;
    embedded::migrations::runner()
        .run_async(&mut client)
        .await?;
    let migration_state_after = fetch_migration_state(&client).await?;

    if migration_state_after.len() > migration_state_before.len() {
        let deployed_versions: Vec<i32> = migration_state_before
            .into_iter()
            .map(|m| m.version)
            .collect();
        let delta: Vec<MigrationRow> = migration_state_after
            .into_iter()
            .filter(|m| !deployed_versions.contains(&m.version))
            .collect();

        for m in delta.iter() {
            println!("Migration applied: {:?}", m);
        }
    } else {
        println!("No migrations to apply");
    }

    println!("DB migrations finished!");

    Ok(())
}

async fn fetch_migration_state(
    client: &Client,
) -> std::result::Result<Vec<MigrationRow>, tokio_postgres::Error> {
    let rows = match client
        .query("SELECT * from refinery_schema_history", &[])
        .await
    {
        Ok(v) => v,
        Err(e) => {
            eprintln!("could not fetch migration state - first run? {}", e);
            return Ok(vec![]);
        }
    };

    Ok(rows.iter().map(|r| to_migration_row(&r)).collect())
}

fn to_migration_row(row: &Row) -> MigrationRow {
    let version: i32 = row.get(0);
    let name: String = row.get(1);
    let applied_on: String = row.get(2);
    let checksum: String = row.get(3);
    MigrationRow {
        version,
        name,
        applied_on,
        checksum,
    }
}
