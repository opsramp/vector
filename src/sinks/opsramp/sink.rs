pub use super::pb::opentelemetry::proto::collector::logs::v1 as logsService;
pub use super::pb::opentelemetry::proto::common::v1 as logsCommon;
pub use super::pb::opentelemetry::proto::logs::v1 as logsStructures;

use super::event::{OpsRampBatchEncoder, OpsRampLogRecord, PartitionKey};
use crate::config::log_schema;
use crate::config::SinkContext;
use crate::sinks::opsramp::config::OpsRampSinkConfig;
use crate::sinks::util::SinkBuilderExt;
use crate::sinks::util::{Compression, RequestBuilder};
use crate::template::Template;
use futures::stream::BoxStream;
use futures_util::StreamExt;
pub use logsService::logs_service_client::LogsServiceClient;
pub use logsService::ExportLogsServiceRequest;
pub use logsStructures::ResourceLogs;
use std::collections::HashMap;
use std::fmt::Debug;
use std::num::NonZeroUsize;
use vector_core::{
    buffers::Acker,
    event::{self, Event, EventFinalizers, Finalizable},
    partition::Partitioner,
    sink::StreamSink,
    stream::BatcherSettings,
    ByteSizeOf,
};

use super::service::OpsRampRequest;
use super::service::OpsRampService;
use logsCommon::AnyValue;
use logsCommon::KeyValue;
use logsStructures::LogRecord as OpsRampRecord;
use prost::Message;
use snafu::Snafu;

#[async_trait::async_trait]
impl StreamSink for OpsRampSink {
    async fn run(mut self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}

#[derive(Default)]
struct RecordPartitionner;

impl Partitioner for RecordPartitionner {
    type Item = OpsRampLogRecord;
    type Key = PartitionKey;

    fn partition(&self, item: &Self::Item) -> Self::Key {
        item.partition.clone()
    }
}

impl ByteSizeOf for OpsRampRecord {
    fn allocated_bytes(&self) -> usize {
        self.encode_to_vec().size_of()
    }
}

#[derive(Debug, Snafu)]
pub enum RequestBuildError {
    #[snafu(display("Encoded payload is greater than the max limit."))]
    PayloadTooBig,
    #[snafu(display("Failed to build payload with error: {}", error))]
    Io { error: std::io::Error },
}

impl From<std::io::Error> for RequestBuildError {
    fn from(error: std::io::Error) -> RequestBuildError {
        RequestBuildError::Io { error }
    }
}

// #[derive(Clone)]
// pub struct KeyPartitioner(Option<Template>);

// impl KeyPartitioner {
//     pub const fn new(template: Option<Template>) -> Self {
//         Self(template)
//     }
// }

// impl Partitioner for KeyPartitioner {
//     type Item = Event;
//     type Key = Option<String>;

//     fn partition(&self, item: &Self::Item) -> Self::Key {
//         self.0.as_ref().and_then(|t| {
//             t.render_string(item)
//                 .map_err(|error| {
//                     emit!(&TemplateRenderingFailed {
//                         error,
//                         field: Some("tenant_id"),
//                         drop_event: false,
//                     })
//                 })
//                 .ok()
//         })
//     }
// }

#[derive(Clone)]
pub struct OpsRampRequestBuilder {
    compression: Compression,
    encoder: OpsRampBatchEncoder,
}

impl Default for OpsRampRequestBuilder {
    fn default() -> Self {
        Self {
            compression: Compression::None,
            encoder: OpsRampBatchEncoder::default(),
        }
    }
}

impl RequestBuilder<(PartitionKey, Vec<OpsRampLogRecord>)> for OpsRampRequestBuilder {
    type Metadata = (String, usize, EventFinalizers, usize, Vec<OpsRampLogRecord>);
    type Events = Vec<OpsRampLogRecord>;
    type Encoder = OpsRampBatchEncoder;
    type Payload = Vec<u8>;
    type Request = OpsRampRequest;
    type Error = RequestBuildError;

    fn compression(&self) -> Compression {
        self.compression
    }

    fn encoder(&self) -> &Self::Encoder {
        &self.encoder
    }

    fn split_input(
        &self,
        input: (PartitionKey, Vec<OpsRampLogRecord>),
    ) -> (Self::Metadata, Self::Events) {
        let (key, mut events) = input;
        let batch_size = events.len();
        let events_byte_size = events.size_of();
        let finalizers = events
            .iter_mut()
            .fold(EventFinalizers::default(), |mut acc, x| {
                acc.merge(x.take_finalizers());
                acc
            });

        (
            (
                key.tenant_id,
                batch_size,
                finalizers,
                events_byte_size,
                events.clone(),
            ),
            events,
        )
    }

    fn build_request(&self, metadata: Self::Metadata, payload: Self::Payload) -> Self::Request {
        let (tenant_id, batch_size, finalizers, events_byte_size, opsramp_records) = metadata;

        OpsRampRequest {
            batch_size,
            finalizers,
            payload,
            tenant_id,
            events_byte_size,
            opsramp_records,
        }
    }
}

#[derive(Clone)]
pub(super) struct EventEncoder {
    labels: HashMap<Template, Template>,
    remove_label_fields: bool,
    remove_timestamp: bool,
}

impl EventEncoder {
    fn build_labels(&self, event: &Event) -> Vec<(String, String)> {
        self.labels
            .iter()
            .filter_map(|(key_template, value_template)| {
                if let (Ok(key), Ok(value)) = (
                    key_template.render_string(event),
                    value_template.render_string(event),
                ) {
                    Some((key, value))
                } else {
                    None
                }
            })
            .collect()
    }

    fn remove_label_fields(&self, event: &mut Event) {
        if self.remove_label_fields {
            for template in self.labels.values() {
                if let Some(fields) = template.get_fields() {
                    for field in fields {
                        event.as_mut_log().remove(&field);
                    }
                }
            }
        }
    }

    pub(super) fn encode_event(&self, mut event: Event, tenant_id: String) -> OpsRampLogRecord {
        let mut labels = self.build_labels(&event);
        self.remove_label_fields(&mut event);

        let finalizers = event.take_finalizers();

        let schema = log_schema();
        let timestamp_key = schema.timestamp_key();
        let timestamp = match event.as_log().get(timestamp_key) {
            Some(event::Value::Timestamp(ts)) => ts.timestamp_nanos(),
            _ => chrono::Utc::now().timestamp_nanos(),
        };

        if self.remove_timestamp {
            event.as_mut_log().remove(timestamp_key);
        }

        // If no labels are provided we set our own default
        // `{agent="opsramp-logs"}` label. This can happen if the only
        // label is a templatable one but the event doesn't match.
        if labels.is_empty() {
            labels = vec![("agent".to_string(), "opsramp-logs".to_string())]
        }

        let log = event.into_log();

        let mut attributes: Vec<KeyValue> = Vec::new();
        let mut severity_number: i32 = 0;
        let mut severity_text = "Unknown".to_string();

        for (key, value) in labels.clone() {
            let pair = KeyValue {
                key: key,
                value: Some(AnyValue {
                    value: Some(logsCommon::any_value::Value::StringValue(value)),
                }),
            };
            attributes.push(pair);
        }

        let body: Option<AnyValue> = Some(AnyValue {
            value: Some(logsCommon::any_value::Value::StringValue(
                serde_json::to_string(&log.clone()).expect("json encoding should never fail.")
            )),
        });

        for (key, value) in log.as_map() {
            let pair = KeyValue {
                key: key.to_string(),
                value: Some(AnyValue {
                    value: Some(logsCommon::any_value::Value::StringValue(
                        value.to_string_lossy(),
                    )),
                }),
            };
            attributes.push(pair);

            if key == "level" {
                severity_text = value.to_string_lossy();

                if value.to_string_lossy() == "Fatal" {
                    severity_number = 21;
                } else if value.to_string_lossy() == "Error" {
                    severity_number = 17;
                } else if value.to_string_lossy() == "Warn" {
                    severity_number = 13;
                } else if value.to_string_lossy() == "Info" {
                    severity_number = 9;
                } else if value.to_string_lossy() == "Debug" {
                    severity_number = 5;
                } else if value.to_string_lossy() == "Trace" {
                    severity_number = 1;
                } else {
                    severity_text = "Unknown".to_string();
                    severity_number = 0;
                }
            }
        }

        let partition = PartitionKey::new(tenant_id, &mut labels);

        let event = OpsRampRecord {
            time_unix_nano: timestamp as u64,
            severity_number: severity_number,
            severity_text: severity_text,
            name: "".to_string(),
            body: body,
            attributes: attributes,
            dropped_attributes_count: 0,
            flags: Default::default(),
            trace_id: Default::default(),
            span_id: Default::default(),
        };

        OpsRampLogRecord {
            labels,
            event: event,
            partition,
            finalizers,
        }
    }
}

#[derive(Clone)]
pub struct OpsRampSink {
    acker: Acker,
    request_builder: OpsRampRequestBuilder,
    batch_settings: BatcherSettings,
    pub(super) encoder: EventEncoder,
    service: OpsRampService,
    tenant_id: String,
}

impl OpsRampSink {
    pub fn new(
        config: OpsRampSinkConfig,
        grpc_channel: tonic::transport::Channel,
        cx: SinkContext,
    ) -> crate::Result<Self> {
        Ok(Self {
            acker: cx.acker(),
            request_builder: OpsRampRequestBuilder::default(),
            batch_settings: config.batch.into_batcher_settings()?,
            encoder: EventEncoder {
                labels: config.clone().labels,
                remove_label_fields: config.clone().remove_label_fields,
                remove_timestamp: config.clone().remove_timestamp,
            },
            service: OpsRampService::new(grpc_channel.clone(), config.clone())?,
            tenant_id: config.tenant_id.clone(),
        })
    }

    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let service = tower::ServiceBuilder::new()
            .concurrency_limit(1)
            .service(self.service);

        let encoder = self.encoder.clone();

        println!("here in opsramp/sink.rs run_inner function");

        let sink = input
            .map(|event| encoder.encode_event(event, self.tenant_id.clone()))
            .batched_partitioned(RecordPartitionner::default(), self.batch_settings)
            .request_builder(NonZeroUsize::new(1), self.request_builder)
            .filter_map(|request| async move {
                match request {
                    Err(e) => {
                        error!("Failed to build OpsRamp request: {:?}.", e);
                        None
                    }
                    Ok(req) => Some(req),
                }
            })
            .into_driver(service, self.acker);

        println!("here after the sink is created in opsramp/sink.rs run_inner function");

        sink.run().await
    }
}
