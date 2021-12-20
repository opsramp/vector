use crate::sinks::util::encoding::Encoder;
use serde::{ser::SerializeSeq, Serialize};
use std::io;
use vector_core::ByteSizeOf;

use super::pb::opentelemetry::proto::logs::v1 as logsStructures;

use super::pb::opentelemetry::proto::common::v1 as logsCommon;
use logsStructures::LogRecord as OpsRampRecord;
use logsStructures::ResourceLogs as OpsRampBatch;
use prost::Message;


impl ByteSizeOf for OpsRampBatch {
    fn allocated_bytes(&self) -> usize {
        self.encode_to_vec().size_of()
    }
}

pub type Labels = Vec<(String, String)>;

#[derive(Clone, Default)]
pub struct OpsRampBatchEncoder;

impl Encoder<Vec<OpsRampRecord>> for OpsRampBatchEncoder {
    fn encode_input(
        &self,
        input: Vec<OpsRampRecord>,
        writer: &mut dyn io::Write,
    ) -> io::Result<usize> {
        let batch = vec![OpsRampBatch::from(input)];
        Ok(batch.size_of())
    }
}

// #[derive(Debug, Default, Serialize)]
// pub struct OpsRampBatch {
//     stream: HashMap<String, String>,
//     values: Vec<OpsRampEvent>,
//     #[serde(skip)]
//     finalizers: EventFinalizers,
// }

impl From<Vec<OpsRampRecord>> for OpsRampBatch {
    fn from(events: Vec<OpsRampRecord>) -> Self {
        let mut resource_logs = Self::default();
        // res.resource = logsResource::Resource {
        //     attributes: Default::default(),
        //     dropped_attributes_count: Default::default(),
        // };
        resource_logs.instrumentation_library_logs =
            vec![logsStructures::InstrumentationLibraryLogs {
                instrumentation_library: Some(logsCommon::InstrumentationLibrary {
                    name: "opsramp-logs".to_string(),
                    version: "1.0.0".to_string(),
                }),
                logs: Default::default(),
                schema_url: Default::default(),
            }];

        let mut result = events.into_iter().fold(resource_logs, |mut res, mut item| {
            res.instrumentation_library_logs[0].logs.push(item);
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

// #[derive(Clone, Debug)]
// pub struct OpsRampRecord {
//     pub partition: PartitionKey,
//     pub labels: Labels,
//     pub event: OpsRampEvent,
//     pub finalizers: EventFinalizers,
// }

// impl ByteSizeOf for OpsRampRecord {
//     fn allocated_bytes(&self) -> usize {
//         self.partition.allocated_bytes()
//             + self.labels.iter().fold(0, |res, item| {
//                 res + item.0.allocated_bytes() + item.1.allocated_bytes()
//             })
//             + self.event.allocated_bytes()
//     }
// }

// impl Finalizable for OpsRampRecord {
//     fn take_finalizers(&mut self) -> EventFinalizers {
//         std::mem::take(&mut self.finalizers)
//     }
// }

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub struct PartitionKey {
    pub tenant_id: Option<String>,
    labels: String,
}

impl ByteSizeOf for PartitionKey {
    fn allocated_bytes(&self) -> usize {
        self.tenant_id
            .as_ref()
            .map(|value| value.allocated_bytes())
            .unwrap_or(0)
            + self.labels.allocated_bytes()
    }
}

impl PartitionKey {
    pub fn new(tenant_id: Option<String>, labels: &mut Labels) -> Self {
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
