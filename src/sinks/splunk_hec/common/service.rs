use std::{
    sync::Arc,
    task::{Context, Poll},
};

use crate::sinks::{
    splunk_hec::common::{build_uri, request::HecRequest, response::HecResponse},
    util::http::HttpBatchService,
};
use futures_util::future::BoxFuture;
use http::Request;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use tokio::sync::{
    mpsc::{self, UnboundedSender},
    oneshot::{self, Sender},
};
use tower::{Service, ServiceExt};
use uuid::Uuid;
use vector_core::event::EventStatus;

use crate::sinks::UriParseError;
use crate::{http::HttpClient, sinks::util::Compression};

use super::acknowledgements::{run_acknowledgements, HecClientAcknowledgementsConfig};

// A Splunk channel must be a GUID/UUID formatted value
// https://docs.splunk.com/Documentation/Splunk/8.2.3/Data/AboutHECIDXAck#About_channels_and_sending_data
lazy_static::lazy_static! {
    static ref SPLUNK_CHANNEL: String = {
        let mut buf = Uuid::encode_buffer();
        Uuid::new_v4().to_hyphenated().encode_lower(&mut buf).to_string()
    };
}

#[derive(Clone)]
pub struct HecService {
    pub batch_service:
        HttpBatchService<BoxFuture<'static, Result<Request<Vec<u8>>, crate::Error>>, HecRequest>,
    // Optional to turn off indexer acknowledgement behavior in Humio sinks
    ack_finalizer_tx: Option<UnboundedSender<(u64, Sender<EventStatus>)>>,
}

#[derive(Deserialize, Serialize, Debug)]
struct HecAckResponseBody {
    text: String,
    code: u8,
    #[serde(alias = "ackId")]
    ack_id: Option<u64>,
}

impl HecService {
    pub fn new(
        client: HttpClient,
        http_request_builder: HttpRequestBuilder,
        indexer_acknowledgements: Option<HecClientAcknowledgementsConfig>,
    ) -> Self {
        let event_client = client.clone();
        let ack_client = client;
        let http_request_builder = Arc::new(http_request_builder);
        let tx = indexer_acknowledgements.map(|indexer_acknowledgements| {
            let (tx, rx) = mpsc::unbounded_channel();
            tokio::spawn(run_acknowledgements(
                rx,
                ack_client,
                Arc::clone(&http_request_builder),
                indexer_acknowledgements,
            ));
            tx
        });

        let batch_service = HttpBatchService::new(event_client, move |req: HecRequest| {
            let request_builder = Arc::clone(&http_request_builder);
            let future: BoxFuture<'static, Result<http::Request<Vec<u8>>, crate::Error>> =
                Box::pin(async move {
                    request_builder.build_request(req.body, "/services/collector/event")
                });
            future
        });
        Self {
            batch_service,
            ack_finalizer_tx: tx,
        }
    }
}

impl Service<HecRequest> for HecService {
    type Response = HecResponse;
    type Error = crate::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: HecRequest) -> Self::Future {
        let mut http_service = self.batch_service.clone();
        let ack_finalizer_tx = self.ack_finalizer_tx.clone();
        Box::pin(async move {
            http_service.ready().await?;
            let events_count = req.events_count;
            let events_byte_size = req.events_byte_size;
            let response = http_service.call(req).await?;
            let event_status = if response.status().is_success() {
                if let Some(ack_finalizer_tx) = ack_finalizer_tx {
                    let body = serde_json::from_slice::<HecAckResponseBody>(response.body());
                    match body {
                        Ok(body) => {
                            if let Some(ack_id) = body.ack_id {
                                let (tx, rx) = oneshot::channel();
                                match ack_finalizer_tx.send((ack_id, tx)) {
                                    Ok(_) => rx.await.unwrap_or(EventStatus::Failed),
                                    // If we cannot send ack ids to the ack client, fall back to default behavior
                                    Err(_) => EventStatus::Delivered,
                                }
                            } else {
                                // Default behavior if indexer acknowledgements is disabled
                                EventStatus::Delivered
                            }
                        }
                        Err(error) => {
                            // This may occur if Splunk changes the response format in future versions.
                            error!(message = "Unable to parse Splunk HEC response", ?error);
                            EventStatus::Delivered
                        }
                    }
                } else {
                    EventStatus::Delivered
                }
            } else if response.status().is_server_error() {
                EventStatus::Errored
            } else {
                EventStatus::Failed
            };

            Ok(HecResponse {
                http_response: response,
                event_status,
                events_count,
                events_byte_size,
            })
        })
    }
}

pub struct HttpRequestBuilder {
    pub endpoint: String,
    pub token: String,
    pub compression: Compression,
}

impl HttpRequestBuilder {
    pub fn build_request(
        &self,
        body: Vec<u8>,
        path: &str,
    ) -> Result<Request<Vec<u8>>, crate::Error> {
        let uri = build_uri(self.endpoint.as_str(), path).context(UriParseError)?;

        let mut builder = Request::post(uri)
            .header("Content-Type", "application/json")
            .header("Authorization", format!("Splunk {}", self.token.as_str()))
            .header("X-Splunk-Request-Channel", SPLUNK_CHANNEL.as_str());

        if let Some(ce) = self.compression.content_encoding() {
            builder = builder.header("Content-Encoding", ce);
        }

        builder.body(body).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        num::NonZeroU8,
        sync::atomic::{AtomicU64, Ordering},
    };

    use futures_util::{stream::FuturesUnordered, StreamExt};
    use tower::Service;
    use vector_core::{
        config::proxy::ProxyConfig,
        event::{EventFinalizers, EventStatus},
    };
    use wiremock::{
        matchers::{header, header_exists, method, path},
        Mock, MockServer, Request, Respond, ResponseTemplate,
    };

    use crate::{
        http::HttpClient,
        sinks::{
            splunk_hec::common::{
                acknowledgements::{
                    HecAckStatusRequest, HecAckStatusResponse, HecClientAcknowledgementsConfig,
                },
                request::HecRequest,
                service::{HecAckResponseBody, HecService, HttpRequestBuilder},
            },
            util::Compression,
        },
    };

    const TOKEN: &str = "token";
    static ACK_ID: AtomicU64 = AtomicU64::new(0);

    fn get_hec_service(
        endpoint: String,
        acknowledgements_config: HecClientAcknowledgementsConfig,
    ) -> HecService {
        let client = HttpClient::new(None, &ProxyConfig::default()).unwrap();
        let http_request_builder = HttpRequestBuilder {
            endpoint,
            token: String::from(TOKEN),
            compression: Compression::default(),
        };
        HecService::new(client, http_request_builder, Some(acknowledgements_config))
    }

    fn get_hec_request() -> HecRequest {
        let body = String::from("test-message").into_bytes();
        let events_byte_size = body.len();
        HecRequest {
            body,
            events_count: 1,
            events_byte_size,
            finalizers: EventFinalizers::default(),
        }
    }

    async fn get_hec_mock_server<R>(acknowledgements_enabled: bool, ack_response: R) -> MockServer
    where
        R: Respond + 'static,
    {
        // Authorization tokens and channels are required
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/services/collector/event"))
            .and(header(
                "Authorization",
                format!("Splunk {}", TOKEN).as_str(),
            ))
            .and(header_exists("X-Splunk-Request-Channel"))
            .respond_with(move |_: &Request| {
                let ack_id =
                    acknowledgements_enabled.then(|| ACK_ID.fetch_add(1, Ordering::Relaxed));
                ResponseTemplate::new(200).set_body_json(HecAckResponseBody {
                    text: String::from("Success"),
                    code: 0,
                    ack_id,
                })
            })
            .mount(&mock_server)
            .await;

        Mock::given(method("POST"))
            .and(path("/services/collector/ack"))
            .and(header(
                "Authorization",
                format!("Splunk {}", TOKEN).as_str(),
            ))
            .and(header_exists("X-Splunk-Request-Channel"))
            .respond_with(ack_response)
            .mount(&mock_server)
            .await;

        mock_server
    }

    #[tokio::test]
    async fn acknowledgements_enabled_on_server() {
        let ack_response = |req: &Request| {
            let req = serde_json::from_slice::<HecAckStatusRequest>(req.body.as_slice()).unwrap();
            ResponseTemplate::new(200).set_body_json(HecAckStatusResponse {
                acks: req
                    .acks
                    .into_iter()
                    .map(|ack_id| (ack_id, true))
                    .collect::<HashMap<_, _>>(),
            })
        };
        let mock_server = get_hec_mock_server(true, ack_response).await;

        let acknowledgements_config = HecClientAcknowledgementsConfig {
            query_interval: NonZeroU8::new(1).unwrap(),
            ..Default::default()
        };
        let mut service = get_hec_service(mock_server.uri(), acknowledgements_config);

        let mut responses = (0..10)
            .map(|_| service.call(get_hec_request()))
            .collect::<FuturesUnordered<_>>();
        while let Some(response) = responses.next().await {
            assert_eq!(EventStatus::Delivered, response.unwrap().event_status)
        }
    }

    #[tokio::test]
    async fn acknowledgements_disabled_on_server() {
        let ack_response = |_: &Request| ResponseTemplate::new(400);
        let mock_server = get_hec_mock_server(false, ack_response).await;

        let acknowledgements_config = HecClientAcknowledgementsConfig {
            query_interval: NonZeroU8::new(1).unwrap(),
            ..Default::default()
        };
        let mut service = get_hec_service(mock_server.uri(), acknowledgements_config);

        let request = get_hec_request();
        let response = service.call(request).await.unwrap();
        assert_eq!(EventStatus::Delivered, response.event_status)
    }

    #[tokio::test]
    async fn acknowledgements_enabled_on_server_retry_limit_exceeded() {
        let ack_response = |req: &Request| {
            let req = serde_json::from_slice::<HecAckStatusRequest>(req.body.as_slice()).unwrap();
            ResponseTemplate::new(200).set_body_json(HecAckStatusResponse {
                acks: req
                    .acks
                    .into_iter()
                    .map(|ack_id| (ack_id, false))
                    .collect::<HashMap<_, _>>(),
            })
        };
        let mock_server = get_hec_mock_server(true, ack_response).await;

        let acknowledgements_config = HecClientAcknowledgementsConfig {
            query_interval: NonZeroU8::new(1).unwrap(),
            retry_limit: NonZeroU8::new(1).unwrap(),
        };
        let mut service = get_hec_service(mock_server.uri(), acknowledgements_config);

        let request = get_hec_request();
        let response = service.call(request).await.unwrap();
        assert_eq!(EventStatus::Failed, response.event_status)
    }

    #[tokio::test]
    async fn acknowledgements_server_changed_ack_response_format() {
        let ack_response = |_: &Request| {
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!(r#"{ "new": "a new response body" }"#))
        };
        let mock_server = get_hec_mock_server(true, ack_response).await;

        let acknowledgements_config = HecClientAcknowledgementsConfig {
            query_interval: NonZeroU8::new(1).unwrap(),
            retry_limit: NonZeroU8::new(3).unwrap(),
        };
        let mut service = get_hec_service(mock_server.uri(), acknowledgements_config);

        let request = get_hec_request();
        let response = service.call(request).await.unwrap();
        assert_eq!(EventStatus::Delivered, response.event_status)
    }

    #[tokio::test]
    async fn acknowledgements_enabled_on_server_ack_endpoint_failing() {
        let ack_response = |_: &Request| ResponseTemplate::new(503);
        let mock_server = get_hec_mock_server(true, ack_response).await;

        let acknowledgements_config = HecClientAcknowledgementsConfig {
            query_interval: NonZeroU8::new(1).unwrap(),
            retry_limit: NonZeroU8::new(3).unwrap(),
        };
        let mut service = get_hec_service(mock_server.uri(), acknowledgements_config);

        let request = get_hec_request();
        let response = service.call(request).await.unwrap();
        assert_eq!(EventStatus::Failed, response.event_status)
    }

    #[tokio::test]
    async fn acknowledgements_server_changed_event_response_format() {
        let ack_response = |req: &Request| {
            let req = serde_json::from_slice::<HecAckStatusRequest>(req.body.as_slice()).unwrap();
            ResponseTemplate::new(200).set_body_json(HecAckStatusResponse {
                acks: req
                    .acks
                    .into_iter()
                    .map(|ack_id| (ack_id, true))
                    .collect::<HashMap<_, _>>(),
            })
        };
        let mock_server = get_hec_mock_server(true, ack_response).await;
        // Override the usual event endpoint
        Mock::given(method("POST"))
            .and(path("/services/collector/event"))
            .and(header(
                "Authorization",
                format!("Splunk {}", TOKEN).as_str(),
            ))
            .and(header_exists("X-Splunk-Request-Channel"))
            .respond_with(move |_: &Request| {
                ResponseTemplate::new(200).set_body_json(r#"{ "new": "a new response body" }"#)
            })
            .mount(&mock_server)
            .await;

        let acknowledgements_config = HecClientAcknowledgementsConfig {
            query_interval: NonZeroU8::new(1).unwrap(),
            retry_limit: NonZeroU8::new(1).unwrap(),
        };
        let mut service = get_hec_service(mock_server.uri(), acknowledgements_config);

        let request = get_hec_request();
        let response = service.call(request).await.unwrap();
        assert_eq!(EventStatus::Delivered, response.event_status)
    }
}
