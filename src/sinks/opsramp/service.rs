use super::config::OpsRampSinkConfig;
use crate::config::ProxyConfig;
use crate::http::{Auth, HttpClient};
use crate::sinks::util::UriSerde;
use crate::tls::{TlsConfig, TlsOptions, TlsSettings};
// use assert_cmd::prelude::OutputOkExt;
use futures::future::BoxFuture;
use http::StatusCode;
use prost::Message;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use std::sync::Arc;
use std::sync::RwLock;
use std::task::{Context, Poll};
use std::time::Duration;
use tonic::Status;
use tonic::{metadata::MetadataValue, transport::Channel, Request};
use tower::Service;
use tracing_futures::Instrument;
use vector_core::buffers::Ackable;
use vector_core::event::{EventFinalizers, EventStatus, Finalizable};
use vector_core::internal_event::EventsSent;
use vector_core::stream::DriverResponse;

pub use super::pb::opentelemetry::proto::collector::logs::v1 as logsService;
pub use super::pb::opentelemetry::proto::common::v1 as logsCommon;
pub use super::pb::opentelemetry::proto::logs::v1 as logsStructures;
use logsStructures::LogRecord as OpsRampRecord;

use logsStructures::ResourceLogs as OpsRampBatch;
// use super::event::OpsRampBatch;

use logsService::logs_service_client::LogsServiceClient;
use logsService::ExportLogsServiceRequest;

#[derive(Debug, Snafu)]
pub enum OpsRampError {
    #[snafu(display("Server responded with an error: {}", code))]
    ServerError { code: StatusCode },
    #[snafu(display("Failed to make HTTP(S) request: {}", error))]
    HttpError { error: tonic::Status },
}

#[derive(Debug, Snafu)]
pub struct OpsRampResponse {
    batch_size: usize,
    events_byte_size: usize,
}

impl DriverResponse for OpsRampResponse {
    fn event_status(&self) -> EventStatus {
        EventStatus::Delivered
    }

    fn events_sent(&self) -> EventsSent {
        EventsSent {
            count: self.batch_size,
            byte_size: self.events_byte_size,
        }
    }
}

pub struct OpsRampRequest {
    pub batch_size: usize,
    pub finalizers: EventFinalizers,
    pub payload: Vec<u8>,
    pub tenant_id: String,
    pub events_byte_size: usize,
    pub opsramp_records: Vec<OpsRampRecord>,
}

impl Ackable for OpsRampRequest {
    fn ack_size(&self) -> usize {
        self.batch_size
    }
}

impl Finalizable for OpsRampRequest {
    fn take_finalizers(&mut self) -> EventFinalizers {
        std::mem::take(&mut self.finalizers)
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpsRampAuthTokenResponse {
    #[serde(rename = "access_token")]
    pub access_token: String,
    #[serde(rename = "token_type")]
    pub token_type: String,
    #[serde(rename = "expires_in")]
    pub expires_in: i64,
    pub scope: String,
}

#[derive(Debug, Clone)]
pub struct OpsRampService {
    endpoint: UriSerde,
    grpc_channel: tonic::transport::Channel,
    access_token: Arc<RwLock<String>>,
    client_key: Arc<RwLock<String>>,
    client_secret: Arc<RwLock<String>>,
    proxy: Option<ProxyConfig>,
    proxy_username: String,
    proxy_password: String,
    tls: Option<TlsOptions>,
}

impl OpsRampService {
    pub fn new(
        grpc_channel: tonic::transport::Channel,
        config: OpsRampSinkConfig,
    ) -> crate::Result<Self> {
        Ok(Self {
            endpoint: config.endpoint.clone(),
            grpc_channel,
            access_token: Arc::new(RwLock::new("".to_string())),
            client_key: Arc::new(RwLock::new(config.client_key.clone())),
            client_secret: Arc::new(RwLock::new(config.client_secret.clone())),
            proxy: config.proxy.clone(),
            proxy_username: config.proxy_username.clone(),
            proxy_password: config.proxy_password.clone(),
            tls: config.tls.clone(),
        })
    }

    async fn get_opsramp_auth_token(&mut self) -> String {
        if !self.access_token.clone().read().unwrap().is_empty() {
            info!(
                "making use of saved opsramp auth token {}",
                self.access_token.clone().read().unwrap().to_string()
            );
            return self.access_token.clone().read().unwrap().to_string();
        }

        let opsramp_auth_token_request_url = format!("{}auth/oauth/token", self.endpoint.uri);

        let opsramp_auth_body = format!(
            "client_id={}&client_secret={}&grant_type=client_credentials",
            self.client_key.clone().read().unwrap().to_string(),
            self.client_secret.clone().read().unwrap().to_string()
        );

        let mut client = reqwest::Client::builder().timeout(Duration::from_secs(10));

        let proxy_config = self.proxy.clone().unwrap_or_default();
        if proxy_config.enabled {
            info!("proxy used for opsramp auth token is {:?}", proxy_config);
            let mut raw_proxy = "".to_string();
            let mut proxy: reqwest::Proxy;
            if proxy_config.https.is_some() {
                raw_proxy = proxy_config.https.unwrap_or_default();
                proxy = reqwest::Proxy::all(&raw_proxy).unwrap();
                client = client.proxy(proxy);
            } else if proxy_config.http.is_some() {
                raw_proxy = proxy_config.http.unwrap_or_default();
                proxy = reqwest::Proxy::all(&raw_proxy).unwrap();
                client = client.proxy(proxy);
            }

            if raw_proxy != "" && self.proxy_username != "" && self.proxy_password != "" {
                proxy = reqwest::Proxy::all(&raw_proxy).unwrap().basic_auth(
                    self.proxy_username.clone().as_str(),
                    self.proxy_password.clone().as_str(),
                );
                client = client.proxy(proxy);
            }
        }

        let client = client.build().unwrap();

        let response = client
            .post(opsramp_auth_token_request_url)
            .body(opsramp_auth_body)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Accept", "application/json")
            .send()
            .await
            .unwrap();

        if response.status() != http::StatusCode::OK {
            error!("opsramp auth token fetch failed {}", response.status());
        }

        let auth_token_response: OpsRampAuthTokenResponse =
            serde_json::from_str(response.text().await.unwrap().as_str()).unwrap();

        *self.access_token.write().unwrap() = auth_token_response.access_token.clone();

        auth_token_response.access_token
    }
}

impl Service<OpsRampRequest> for OpsRampService {
    type Response = OpsRampResponse;
    type Error = OpsRampError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: OpsRampRequest) -> Self::Future {
        // Getting auth token from OpsRamp

        println!("Reached HERE IN OPSRAMP SERVICE");

        let grpc_channel = self.grpc_channel.clone();

        let mut auth_token = self.clone();

        let batch_size = request.batch_size;
        let events_byte_size = request.events_byte_size;
        Box::pin(async move {
            let access_token = auth_token.get_opsramp_auth_token().await;
            let token =
                MetadataValue::from_str(format!("Bearer {}", access_token).as_str()).unwrap();

            let mut client =
                LogsServiceClient::with_interceptor(grpc_channel, move |mut req: Request<()>| {
                    req.metadata_mut().insert("authorization", token.clone());
                    Ok(req)
                });

            let req = tonic::Request::new(ExportLogsServiceRequest {
                resource_logs: vec![OpsRampBatch::from(request.opsramp_records)],
            });

            match client.export(req).await {
                Ok(response) => {
                    // let status = respons;

                    // match status {
                    //     StatusCode::NO_CONTENT => Ok(OpsRampResponse {
                    //         batch_size,
                    //         events_byte_size,
                    //     }),
                    //     code => Err(OpsRampError::ServerError { code }),
                    // }
                    info!("opsramp grpc sent {:?}", response);
                    Ok(OpsRampResponse {
                        batch_size,
                        events_byte_size,
                    })
                }
                Err(error) => Err(OpsRampError::HttpError { error }),
            }
        })
    }
}
