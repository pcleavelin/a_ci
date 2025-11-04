use a_ci_lib::{inbound, domain::ci::BaseService};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let http_server = inbound::http::HttpServer::new(BaseService {})?;

    http_server.run().await;

    Ok(())
}
