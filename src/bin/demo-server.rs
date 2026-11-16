use std::{
    env, io,
    time::{Duration, Instant},
};

use axum::{
    extract::State,
    http::StatusCode,
    response::Html,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::{net::TcpListener, time::timeout};

const REDIS_TIMEOUT: Duration = Duration::from_secs(2);

// HTTP 요청들이 공유할 연결 설정
#[derive(Clone)]
struct AppState {
    mini_url: String,
    official_url: String,
}

#[derive(Deserialize)]
struct PingRequest {
    target: String,
}

#[derive(Deserialize)]
struct KeyRequest {
    target: String,
    key: String,
}

#[derive(Deserialize)]
struct SetRequest {
    target: String,
    key: String,
    value: String,
}

type ApiError = (StatusCode, Json<Value>);

fn api_error(status: StatusCode, message: &str) -> ApiError {
    (status, Json(json!({ "status": "error", "error": message })))
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../../web/index.html"))
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

// 각 요청에서 새 연결을 만들고 연결 시간과 명령 왕복 시간을 나누어 측정합니다.
async fn execute_command<T: redis::FromRedisValue>(
    state: &AppState,
    target: &str,
    command: redis::Cmd,
) -> Result<(T, f64, f64), ApiError> {
    let url = match target {
        "mini" => &state.mini_url,
        "redis" => &state.official_url,
        _ => {
            return Err(api_error(
                StatusCode::BAD_REQUEST,
                "target must be mini or redis",
            ))
        }
    };
    let client = redis::Client::open(url.as_str()).map_err(|_| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "invalid Redis URL configuration",
        )
    })?;

    let connect_started = Instant::now();
    let mut connection = timeout(REDIS_TIMEOUT, client.get_multiplexed_async_connection())
        .await
        .map_err(|_| api_error(StatusCode::GATEWAY_TIMEOUT, "Redis connection timed out"))?
        .map_err(|_| api_error(StatusCode::BAD_GATEWAY, "Redis connection failed"))?;
    let connect_ms = connect_started.elapsed().as_secs_f64() * 1000.0;

    let command_started = Instant::now();
    let reply = timeout(REDIS_TIMEOUT, command.query_async::<T>(&mut connection))
        .await
        .map_err(|_| api_error(StatusCode::GATEWAY_TIMEOUT, "Redis command timed out"))?
        .map_err(|_| api_error(StatusCode::BAD_GATEWAY, "Redis command failed"))?;
    let command_ms = command_started.elapsed().as_secs_f64() * 1000.0;

    Ok((reply, connect_ms, command_ms))
}

async fn ping(
    State(state): State<AppState>,
    Json(request): Json<PingRequest>,
) -> Result<Json<Value>, ApiError> {
    let (reply, connect_ms, ping_ms) =
        execute_command::<String>(&state, &request.target, redis::cmd("PING")).await?;
    if reply != "PONG" {
        return Err(api_error(
            StatusCode::BAD_GATEWAY,
            "unexpected Redis PING response",
        ));
    }
    Ok(Json(json!({
        "target": request.target, "status": "ok", "reply": reply,
        "connect_ms": connect_ms, "ping_ms": ping_ms,
    })))
}

async fn get_key(
    State(state): State<AppState>,
    Json(request): Json<KeyRequest>,
) -> Result<Json<Value>, ApiError> {
    let mut command = redis::cmd("GET");
    command.arg(&request.key);
    let (reply, connect_ms, command_ms) =
        execute_command::<Option<String>>(&state, &request.target, command).await?;
    Ok(Json(json!({
        "target": request.target, "status": "ok", "key": request.key, "reply": reply,
        "connect_ms": connect_ms, "command_ms": command_ms,
    })))
}

async fn set_key(
    State(state): State<AppState>,
    Json(request): Json<SetRequest>,
) -> Result<Json<Value>, ApiError> {
    let mut command = redis::cmd("SET");
    command.arg(&request.key).arg(&request.value);
    let (reply, connect_ms, command_ms) =
        execute_command::<String>(&state, &request.target, command).await?;
    Ok(Json(json!({
        "target": request.target, "status": "ok", "key": request.key, "reply": reply,
        "connect_ms": connect_ms, "command_ms": command_ms,
    })))
}

async fn delete_key(
    State(state): State<AppState>,
    Json(request): Json<KeyRequest>,
) -> Result<Json<Value>, ApiError> {
    let mut command = redis::cmd("DEL");
    command.arg(&request.key);
    let (reply, connect_ms, command_ms) =
        execute_command::<u64>(&state, &request.target, command).await?;
    Ok(Json(json!({
        "target": request.target, "status": "ok", "key": request.key, "reply": reply,
        "connect_ms": connect_ms, "command_ms": command_ms,
    })))
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let state = AppState {
        mini_url: env::var("MINI_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379/?protocol=resp2".to_owned()),
        official_url: env::var("OFFICIAL_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6380/?protocol=resp2".to_owned()),
    };
    let app = Router::new()
        .route("/", get(index))
        .route("/api/health", get(health))
        .route("/api/ping", post(ping))
        .route("/api/get", post(get_key))
        .route("/api/set", post(set_key))
        .route("/api/del", post(delete_key))
        .with_state(state);

    let listener = TcpListener::bind("0.0.0.0:8083").await?;
    println!("demo server: http://localhost:8083");
    axum::serve(listener, app).await
}
