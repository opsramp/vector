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

#[derive(Clone, Default)]
pub struct OpsRampBatchEncoder;

impl Encoder<Vec<OpsRampRecord>> for OpsRampBatchEncoder {
    fn encode_input(&self, input: Vec<OpsRampRecord>, _: &mut dyn io::Write) -> io::Result<usize> {
        let batch = vec![OpsRampBatch::from(input)];
        Ok(batch.size_of())
    }
}

impl From<Vec<OpsRampRecord>> for OpsRampBatch {
    fn from(events: Vec<OpsRampRecord>) -> Self {
        let mut resource_logs = Self::default();
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
