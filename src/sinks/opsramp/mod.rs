pub mod pb {
    pub mod opentelemetry {
        pub mod proto {
            pub mod common {
                pub mod v1 {
                    tonic::include_proto!("opentelemetry.proto.common.v1");
                }
            }

            pub mod resource {
                pub mod v1 {
                    tonic::include_proto!("opentelemetry.proto.resource.v1");
                }
            }

            pub mod logs {
                pub mod v1 {
                    tonic::include_proto!("opentelemetry.proto.logs.v1");
                }
            }

            pub mod collector {
                pub mod logs {
                    pub mod v1 {
                        tonic::include_proto!("opentelemetry.proto.collector.logs.v1");
                    }
                }
            }
        }
    }
}

mod config;
mod healthcheck;
mod sink;
mod event;
mod service;

use crate::config::SinkDescription;
use config::OpsRampSinkConfig;

inventory::submit! {
    SinkDescription::new::<OpsRampSinkConfig>("opsramp")
}

// use mod opentelemetry_proto;

