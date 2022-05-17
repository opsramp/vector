//! This module pools the kubernetes events and sends them as log records
#![deny(missing_docs)]

use futures::SinkExt;
use futures::{future::FutureExt, sink::Sink};
use std::fs;
use std::task::Poll;
use tokio::time::Duration;
use vector_core::event::LogEvent;

use std::path::PathBuf;

use crate::config::AcknowledgementsConfig;
use crate::kubernetes::api_watcher::invocation;
use crate::kubernetes::watcher;
use crate::{
    config::{
        ComponentKey, DataType, GenerateConfig, GlobalOptions, ProxyConfig, SourceConfig,
        SourceContext, SourceDescription,
    },
    event::Event,
    kubernetes as k8s,
    serde::bool_or_struct,
    shutdown::ShutdownSignal,
    sources,
};
use k8s_openapi::api::core::v1::Event as K8sEvent;
use k8s_openapi::ListOptional;
use serde::{Deserialize, Serialize};

/// Configuration for the `kubernetes_events` source.
#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// Optional path to a kubeconfig file readable by Vector. If not set,
    /// Vector will try to connect to Kubernetes using in-cluster configuration.
    kube_config_file: Option<PathBuf>,

    /// Override global data_dir
    data_dir: Option<PathBuf>,

    #[serde(default, deserialize_with = "bool_or_struct")]
    acknowledgements: AcknowledgementsConfig,
}

const COMPONENT_ID: &str = "kubernetes_events";

inventory::submit! {
    SourceDescription::new::<Config>(COMPONENT_ID)
}

impl GenerateConfig for Config {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(&Self {
            ..Default::default()
        })
        .unwrap()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            kube_config_file: None,
            data_dir: None,
            acknowledgements: AcknowledgementsConfig::default(),
        }
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "kubernetes_events")]
impl SourceConfig for Config {
    async fn build(&self, cx: SourceContext) -> crate::Result<sources::Source> {
        let source = Source::new(self, &cx.globals, &cx.key, &cx.proxy)?;
        Ok(Box::pin(source.run(cx.out, cx.shutdown).map(|result| {
            result.map_err(|error| {
                error!(message = "Source future failed.", %error);
            })
        })))
    }

    fn output_type(&self) -> DataType {
        DataType::Log
    }

    fn source_type(&self) -> &'static str {
        COMPONENT_ID
    }
}

#[derive(Clone)]
struct Source {
    client: k8s::client::Client,
    data_dir: PathBuf,
}

impl Source {
    fn new(
        config: &Config,
        globals: &GlobalOptions,
        key: &ComponentKey,
        proxy: &ProxyConfig,
    ) -> crate::Result<Self> {
        let k8s_config = match &config.kube_config_file {
            Some(kc) => k8s::client::config::Config::kubeconfig(kc)?,
            None => k8s::client::config::Config::in_cluster()?,
        };
        let client = k8s::client::Client::new(k8s_config, proxy)?;

        let data_dir = globals.resolve_and_make_data_subdir(config.data_dir.as_ref(), key.id())?;

        Ok(Self { client, data_dir })
    }

    async fn run<O>(self, mut out: O, mut global_shutdown: ShutdownSignal) -> crate::Result<()>
    where
        O: Sink<Event> + Send + 'static + Unpin,
        <O as Sink<Event>>::Error: std::error::Error,
    {
        let Self {
            mut client,
            data_dir,
        } = self;

        // TODO: Implement checkpoint to save the resource version to avoid duplicates.
        let resource_version_file_path = format!(
            "{}/{}",
            data_dir.into_os_string().into_string().unwrap_or_default(),
            "resource_version"
        );

        info!(
            "data dir for kubernetes_events {}",
            resource_version_file_path
        );

        let mut resource_version = match fs::read_to_string(resource_version_file_path.clone()) {
            Ok(resource_version) => resource_version,
            Err(_err) => "0".to_string(),
        };

        info!("resource_version initialized for kubernetes_events");

        loop {
            info!(
                "kubernetes events resource version: {:#?}",
                resource_version,
            );

            let list_optional = ListOptional {
                resource_version: Some(&resource_version[..]),
                // resource_version_match: Some("0"),
                ..ListOptional::default()
            };

            let (request, response_body) = K8sEvent::list_event_for_all_namespaces(list_optional)?;

            // // Send request, get response.
            // let response = client.send(request).await?;
            let response = match client.send(request).await {
                Ok(response) => response,
                Err(source) => {
                    return Err(Box::new(watcher::invocation::Error::other(
                        invocation::Error::Request { source },
                    )))
                }
            };

            // Got a status code from executing the request.
            let status_code = response.status();

            let buf = hyper::body::to_bytes(response).await?;

            // Construct the `ResponseBody<ListResponse<Pod>>` using the
            // constructor returned by the API function.
            let mut response_body = response_body(status_code);

            let event_list = loop {
                response_body.append_slice(&buf[..]);
                let response = response_body.parse();
                match response {
                    // Successful response (HTTP 200 and parsed successfully)
                    Ok(k8s_openapi::ListResponse::Ok(event_list)) => break event_list,

                    // Some unexpected response
                    // (not HTTP 200, but still parsed successfully)
                    Ok(other) => {
                        return Err(
                            format!("expected Ok but got {} {:?}", status_code, other).into()
                        )
                    }

                    // Need more response data.
                    // Read more bytes from the response into the `ResponseBody`
                    Err(k8s_openapi::ResponseError::NeedMoreData) => continue,

                    // Some other error, like the response body being
                    // malformed JSON or invalid UTF-8.
                    Err(err) => return Err(format!("error: {} {:?}", status_code, err).into()),
                }
            };

            for event in event_list.items {
                let mut log_event = LogEvent::from(event.message.unwrap_or_default());

                log_event.insert("app", "kubernetes_events");

                if event.action.is_some() {
                    log_event.insert("action", event.action);
                }
                if event.count.is_some() {
                    log_event.insert("count", event.count);
                }

                // considering last timestamp if its present or else considering event time
                if event.last_timestamp.is_some() {
                    let last_timestamp = event.last_timestamp.unwrap().0;
                    log_event.insert("last_timestamp", last_timestamp.clone());
                    log_event.insert("timestamp", last_timestamp);
                }
                if event.event_time.is_some() {
                    let event_time = event.event_time.unwrap().0;
                    log_event.insert("event_time", event_time);
                    log_event.try_insert("timestamp", event_time);
                }
                if event.first_timestamp.is_some() {
                    log_event.insert("first_timestamp", event.first_timestamp.unwrap().0);
                }

                if event.involved_object.api_version.is_some() {
                    log_event.insert("api_version", event.involved_object.api_version);
                }
                if event.involved_object.field_path.is_some() {
                    log_event.insert("field_path", event.involved_object.field_path);
                }
                if event.involved_object.kind.is_some() {
                    log_event.insert("kind", event.involved_object.kind);
                }
                if event.involved_object.name.is_some() {
                    log_event.insert("name", event.involved_object.name);
                }
                if event.involved_object.namespace.is_some() {
                    log_event.insert("namespace", event.involved_object.namespace);
                }
                if event.involved_object.resource_version.is_some() {
                    log_event.insert("resource_version", event.involved_object.resource_version);
                }
                if event.involved_object.uid.is_some() {
                    log_event.insert("uid", event.involved_object.uid);
                }
                if event.source.is_some() {
                    let event_source = event.source.unwrap();

                    if event_source.component.is_some() {
                        log_event.insert("source_component", event_source.component);
                    }
                    if event_source.host.is_some() {
                        log_event.insert("source_host", event_source.host.clone());
                        log_event.try_insert("host", event_source.host);
                    }
                }
                let log_event = Event::from(log_event);

                out.send(log_event).await;
            }

            // updating resource_version
            if event_list.metadata.resource_version.is_some() {
                resource_version = event_list.metadata.resource_version.unwrap();
            }
            if matches!(futures::poll!(&mut global_shutdown), Poll::Ready(_)) {
                fs::write(resource_version_file_path.clone(), resource_version);
                break;
            }
            tokio::time::sleep(Duration::from_secs(30)).await;
        }

        Ok(())
    }
}
