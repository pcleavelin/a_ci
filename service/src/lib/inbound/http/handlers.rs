use axum::extract::{Json, State};
use serde::Serialize;

use crate::domain::ci::interface::CiService;
use crate::domain::ci::models::repo::Repository;
use crate::inbound::http::ApiState;

pub(super) async fn health() -> String {
    "OK".to_string()
}

#[derive(Serialize)]
pub(super) struct RepositoryListResponse {
    repos: Vec<RepositoryResponse>,
}
#[derive(Serialize)]
pub(super) struct RepositoryResponse {
    name: String,
}
impl From<Repository> for RepositoryResponse {
    fn from(value: Repository) -> Self {
        Self { name: value.name }
    }
}

pub(super) async fn all_repos<S: CiService>(
    State(state): State<ApiState<S>>,
) -> Json<RepositoryListResponse> {
    let repos = RepositoryListResponse {
        repos: state
            .ci_service
            .repos()
            .into_iter()
            .map(Into::into)
            .collect(),
    };

    Json(repos)
}
