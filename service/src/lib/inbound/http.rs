mod handlers;

use std::{net::SocketAddr, sync::Arc};

use axum::routing::{get, post};
use reqwest::Method;
use tower_http::cors::CorsLayer;

use crate::domain::ci::interface::CiService;

#[derive(Clone)]
pub(crate) struct ApiState<S>
where
    S: CiService,
{
    ci_service: Arc<S>,
}

pub struct HttpServer {
    make_service: axum::routing::IntoMakeService<axum::Router>,
}

impl HttpServer {
    pub fn new(ci_service: impl CiService) -> anyhow::Result<Self> {
        let state = ApiState {
            ci_service: Arc::new(ci_service),
        };

        let router = routes()
            .layer(
                CorsLayer::new()
                    .allow_headers(tower_http::cors::Any)
                    .allow_methods([Method::GET, Method::POST, Method::DELETE]),
            )
            .with_state(state);

        Ok(Self {
            make_service: router.into_make_service(),
        })
    }

    pub async fn run(self) {
        let addr = SocketAddr::from(([0, 0, 0, 0], 8100));
        eprintln!("socket listening on {addr}");

        axum::Server::bind(&addr)
            .serve(self.make_service)
            .await
            .expect("couldn't start http server");
    }
}

fn routes<S>() -> axum::Router<ApiState<S>>
where
    S: CiService,
{
    axum::Router::<ApiState<S>>::new()
        .route("/health", get(handlers::health))
        .route("/repos", get(handlers::all_repos))
}
