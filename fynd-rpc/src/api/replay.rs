//! `POST /v1/replay/advance[?to_block=N]`: step a recorded market served with `--replay`.

use std::sync::Arc;

use actix_web::{web, HttpResponse};
use fynd_core::feed::recorded::ReplayControl;
use serde::Deserialize;

use crate::api::{error::ErrorResponse, RouteConfigurator};

/// Where to step: the next block, or on to the first block at or after `to_block`.
#[derive(Deserialize)]
struct AdvanceQuery {
    to_block: Option<u64>,
}

/// Adds the advance route ahead of any caller routes, which still run after it.
pub(crate) fn with_advance_route(
    control: ReplayControl,
    overrides: Option<RouteConfigurator>,
) -> RouteConfigurator {
    Arc::new(move |scope, state| {
        let control = control.clone();
        let scope = scope.route(
            "/replay/advance",
            web::post().to(move |query: web::Query<AdvanceQuery>| {
                let control = control.clone();
                async move { advance(&control, query.to_block).await }
            }),
        );
        match &overrides {
            Some(overrides) => overrides(scope, state),
            None => scope,
        }
    })
}

/// Steps the replay and returns the block it then serves, once quotes see its derived data.
async fn advance(control: &ReplayControl, to_block: Option<u64>) -> HttpResponse {
    match control.advance(to_block).await {
        Ok(position) => HttpResponse::Ok().json(position),
        Err(error) => HttpResponse::ServiceUnavailable()
            .json(ErrorResponse::new(error, "REPLAY_STOPPED".into())),
    }
}
