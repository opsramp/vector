use crate::sinks::util::encoding::Encoder;
use crate::sinks::util::SinkBatchSettings;
use serde::{ser::SerializeSeq, Serialize};
use std::io;
use std::num::NonZeroU64;
use vector_core::{
    event::{EventFinalizers, Finalizable},
    ByteSizeOf,
};

use super::pb::opentelemetry::proto::logs::v1 as logsStructures;

pub use super::pb::opentelemetry::proto::collector::logs::v1 as logsService;
use super::pb::opentelemetry::proto::common::v1 as logsCommon;
use super::pb::opentelemetry::proto::resource::v1 as logsResource;
use logsCommon::AnyValue;
use logsCommon::KeyValue;
use logsStructures::LogRecord as OpsRampRecord;
use logsStructures::ResourceLogs as OpsRampBatch;
use prost::Message;

pub type Labels = Vec<(String, String)>;

#[derive(Clone, Debug)]
pub struct OpsRampLogRecord {
    pub partition: PartitionKey,
    pub labels: Labels,
    pub event: OpsRampRecord,
    pub finalizers: EventFinalizers,
}

impl ByteSizeOf for OpsRampLogRecord {
    fn allocated_bytes(&self) -> usize {
        self.partition.allocated_bytes()
            + self.labels.iter().fold(0, |res, item| {
                res + item.0.allocated_bytes() + item.1.allocated_bytes()
            })
            + self.event.allocated_bytes()
    }
}

impl Finalizable for OpsRampLogRecord {
    fn take_finalizers(&mut self) -> EventFinalizers {
        std::mem::take(&mut self.finalizers)
    }
}

impl ByteSizeOf for OpsRampBatch {
    fn allocated_bytes(&self) -> usize {
        self.encode_to_vec().size_of()
    }
}

#[derive(Clone, Default)]
pub struct OpsRampBatchEncoder;

impl Encoder<Vec<OpsRampLogRecord>> for OpsRampBatchEncoder {
    fn encode_input(
        &self,
        input: Vec<OpsRampLogRecord>,
        _: &mut dyn io::Write,
    ) -> io::Result<usize> {
        let batch = vec![OpsRampBatch::from(input)];
        Ok(batch.size_of())
    }
}

impl From<Vec<OpsRampLogRecord>> for OpsRampBatch {
    fn from(events: Vec<OpsRampLogRecord>) -> Self {
        let mut resource_logs = Self::default();

        let mut attributes: Vec<KeyValue> = Vec::new();

        if events.len() > 0 {
            for (key, value) in events[0].clone().labels {
                let pair = KeyValue {
                    key: key,
                    value: Some(AnyValue {
                        value: Some(logsCommon::any_value::Value::StringValue(value)),
                    }),
                };
                attributes.push(pair);
            }
        }

        resource_logs.resource = Some(logsResource::Resource {
            attributes: attributes,
            dropped_attributes_count: 0,
        });

        resource_logs.instrumentation_library_logs =
            vec![logsStructures::InstrumentationLibraryLogs {
                instrumentation_library: Some(logsCommon::InstrumentationLibrary {
                    name: "opsramp-logs".to_string(),
                    version: "1.0.0".to_string(),
                }),
                logs: Default::default(),
                schema_url: Default::default(),
            }];

        let result = events.into_iter().fold(resource_logs, |mut res, item| {
            res.instrumentation_library_logs[0].logs.push(item.event);
            res
        });
        result
    }
}

#[derive(Clone, Debug)]
pub struct OpsRampEvent {
    pub timestamp: i64,
    pub event: String,
}

impl ByteSizeOf for OpsRampEvent {
    fn allocated_bytes(&self) -> usize {
        self.timestamp.allocated_bytes() + self.event.allocated_bytes()
    }
}

impl Serialize for OpsRampEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(2))?;
        seq.serialize_element(&self.timestamp.to_string())?;
        seq.serialize_element(&self.event)?;
        seq.end()
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct OpsRampDefaultBatchSettings;

impl SinkBatchSettings for OpsRampDefaultBatchSettings {
    const MAX_EVENTS: Option<usize> = None;
    const MAX_BYTES: Option<usize> = Some(950000);
    const TIMEOUT_SECS: NonZeroU64 = unsafe { NonZeroU64::new_unchecked(1) };
}
#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub struct PartitionKey {
    pub tenant_id: String,
    labels: String,
}

impl ByteSizeOf for PartitionKey {
    fn allocated_bytes(&self) -> usize {
        self.tenant_id.allocated_bytes() + self.labels.allocated_bytes()
    }
}

impl PartitionKey {
    pub fn new(tenant_id: String, labels: &mut Labels) -> Self {
        // Let's join all of the labels to single string so that
        // cloning requires only single allocation.
        // That requires sorting to ensure uniqueness, but
        // also choosing a separator that isn't likely to be
        // used in either name or value.
        labels.sort();
        PartitionKey {
            tenant_id,
            labels: labels.iter().flat_map(|(a, b)| [a, "→", b, "∇"]).collect(),
        }
    }
}
