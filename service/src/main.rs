use a_ci_lib::{domain::ci::BaseService, inbound};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let http_server = inbound::http::HttpServer::new(BaseService::new())?;

    http_server.run().await;

    Ok(())
}
