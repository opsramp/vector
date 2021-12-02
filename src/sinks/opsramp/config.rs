use super::healthcheck::healthcheck;
use super::sink::OpsRampSink;
use crate::http::HttpClient;
use crate::{
    config::{DataType, GenerateConfig, ProxyConfig, SinkConfig, SinkContext},
    sinks::{
        util::{
            encoding::EncodingConfig, retries::RetryLogic, BatchConfig, BatchSettings, BatchSink,
            Compression, RealtimeEventBasedDefaultBatchSettings, ServiceBuilderExt,
            TowerRequestConfig, UriSerde,
        },
        Healthcheck, VectorSink,
    },
    template::Template,
    tls::{tls_connector_builder, MaybeTlsSettings, TlsConfig, TlsOptions, TlsSettings},
};
use futures::future::FutureExt;
use http::Uri;
use hyper::client::HttpConnector;
use hyper_openssl::HttpsConnector;
use hyper_proxy::ProxyConnector;
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use std::{collections::HashMap, env};
use tonic::body::BoxBody;
use tonic::{metadata::MetadataValue, transport::Channel};
use tonic::transport::Endpoint;
use std::time::Duration;

use super::pb::opentelemetry::proto::collector::logs::v1 as logsService;

use logsService::logs_service_client::LogsServiceClient;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OpsRampSinkConfig {
    #[serde(default)]
    pub endpoint: UriSerde,

    // pub tenant_id: Option<Template>,
    #[serde(default)]
    pub tenant_id: String,
    #[serde(default)]
    pub client_key: String,
    #[serde(default)]
    pub client_secret: String,

    pub labels: HashMap<Template, Template>,
    pub proxy: Option<ProxyConfig>,
    #[serde(default)]
    pub proxy_username: String,
    #[serde(default)]
    pub proxy_password: String,

    #[serde(default = "crate::serde::default_false")]
    pub remove_label_fields: bool,
    #[serde(default = "crate::serde::default_true")]
    pub remove_timestamp: bool,

    #[serde(default)]
    pub compression: Compression,
    pub encoding: EncodingConfig<Encoding>,
    #[serde(default)]
    pub batch: BatchConfig<RealtimeEventBasedDefaultBatchSettings>,
    #[serde(default)]
    pub request: TowerRequestConfig,
    #[serde(default)]
    pub tls: Option<TlsOptions>,
}

// #[cfg(test)]
// fn default_config(e: Encoding) -> OpsRampSinkConfig {
//     OpsRampSinkConfig {
//         endpoint: Default::default(),
//         tenant_id: Default::default(),
//         client_key: "".to_string(),
//         client_secret: "".to_string(),
//         labels: Default::default(),
//         proxy: Default::default(),
//         proxy_username: "".to_string(),
//         proxy_password: "".to_string(),
//         compression: Default::default(),
//         batch: Default::default(),
//         encoding: e.into(),
//         request: Default::default(),
//         tls: Default::default(),
//         remove_label_fields: Default::default(),
//         remove_timestamp: Default::default(),
//     }
// }

// TODO: change this later
impl GenerateConfig for OpsRampSinkConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(
            r#"endpoint = "http://localhost:3100"
            encoding = "json"
            labels = {}"#,
        )
        .unwrap()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Encoding {
    Json,
    Text,
}

impl OpsRampSinkConfig {
    pub(super) fn build_http_client(&self, cx: SinkContext) -> crate::Result<HttpClient> {
        let tls = TlsSettings::from_options(&self.tls).unwrap_or_default();
        let client = HttpClient::new(tls, cx.proxy())?;
        Ok(client)
    }

    // pub(super) fn build_grpc_client(
    //     &self,
    //     tls_settings: &MaybeTlsSettings,
    //     proxy_config: &ProxyConfig,
    // ) -> crate::Result<hyper::Client<ProxyConnector<HttpsConnector<HttpConnector>>, BoxBody>> {
    //     let mut http = HttpConnector::new();
    //     http.enforce_http(false);

    //     let tls = tls_connector_builder(tls_settings)?;
    //     let mut https = HttpsConnector::with_connector(http, tls)?;

    //     let settings = tls_settings.tls().cloned();
    //     https.set_callback(move |c, _uri| {
    //         if let Some(settings) = &settings {
    //             settings.apply_connect_configuration(c);
    //         }

    //         Ok(())
    //     });

    //     let mut proxy = ProxyConnector::new(https).unwrap();
    //     proxy_config.configure(&mut proxy)?;

    //     Ok(hyper::Client::builder().http2_only(true).build(proxy))
    // }
}

#[async_trait::async_trait]
#[typetag::serde(name = "opsramp")]
impl SinkConfig for OpsRampSinkConfig {
    async fn build(
        &self,
        cx: SinkContext,
    ) -> crate::Result<(VectorSink, crate::sinks::Healthcheck)> {
        if self.labels.is_empty() {
            return Err("`labels` must include at least one label.".into());
        }

        for label in self.labels.keys() {
            if !valid_label_name(label) {
                return Err(format!("Invalid label name {:?}", label.get_ref()).into());
            }
        }

        let endpoint = match env::var("ENDPOINT") {
            Ok(val) => UriSerde {
                uri: val.parse::<Uri>().unwrap_or_default(),
                auth: self.endpoint.auth.clone(),
            },
            Err(..) => self.endpoint.clone(),
        };
        let tenant_id = match env::var("TENANT_ID") {
            Ok(val) => val,
            // Ok(val) => Option::from(Template::try_from(val).unwrap_or_default()),
            Err(..) => self.tenant_id.clone(),
        };
        let client_key = match env::var("CLIENT_KEY") {
            Ok(val) => val,
            Err(..) => self.client_key.clone(),
        };
        let client_secret = match env::var("CLIENT_SECRET") {
            Ok(val) => val,
            Err(..) => self.client_secret.clone(),
        };

        let proxy_protocol = env::var("PROXY_PROTO").unwrap_or_default();
        let proxy_url = env::var("PROXY_URL").unwrap_or_default();
        let proxy_username = env::var("PROXY_USERNAME").unwrap_or_default();
        let proxy_password = env::var("PROXY_PASSWORD").unwrap_or_default();
        let mut proxy = Option::from(cx.proxy().clone());
        if proxy_protocol != "" && proxy_url != "" {
            let proxy_url_with_protocol = format!("{}://{}", proxy_protocol, proxy_url);
            proxy = match proxy_protocol.as_str() {
                "http" => Option::from(ProxyConfig {
                    enabled: true,
                    http: Option::from(proxy_url_with_protocol),
                    ..cx.proxy().clone()
                }),
                "https" => Option::from(ProxyConfig {
                    enabled: true,
                    https: Option::from(proxy_url_with_protocol),
                    ..cx.proxy().clone()
                }),
                _ => Option::from(cx.proxy().clone()),
            };
        }

        let config = OpsRampSinkConfig {
            endpoint: endpoint.clone(),
            tenant_id: tenant_id.clone(),
            client_key: client_key,
            client_secret: client_secret,
            proxy: proxy.clone(),
            proxy_username: proxy_username.to_string(),
            proxy_password: proxy_password.to_string(),
            ..self.clone()
        };

        let tls = TlsSettings::from_options(&self.tls).unwrap_or_default();

        // let client = self.build_grpc_client(&tls, &config.proxy.clone().unwrap_or_default())?;

        let endpoint = config.endpoint.clone();

        println!("endpoint is {:?}", endpoint);

        let grpc_channel = Channel::builder(endpoint.uri).connect().await?;

        println!("here after grpc channel");

        // let grpc_channel = Channel::from_static("0.0.0.0:50051").connect().await.unwrap();

        // let grpc_client = LogsServiceClient::connect(endpoint_string).await?;

        let sink = OpsRampSink::new(config.clone(), grpc_channel, cx.clone())?;

        println!("here after creating the sink");

        let healthcheck = healthcheck(config.clone(), self.build_http_client(cx.clone())?).boxed();

        println!("here in opsramp/config.rs after creating healthcheck and sink.");

        Ok((VectorSink::Stream(Box::new(sink)), healthcheck))
    }

    fn input_type(&self) -> DataType {
        DataType::Log
    }

    fn sink_type(&self) -> &'static str {
        "opsramp"
    }
}

pub fn valid_label_name(label: &Template) -> bool {
    label.is_dynamic() || {
        let label_trim = label.get_ref().trim();
        let mut label_chars = label_trim.chars();
        if let Some(ch) = label_chars.next() {
            (ch.is_ascii_alphabetic() || ch == '_')
                && label_chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        } else {
            false
        }
    }
}

#[derive(Debug, Clone)]
struct OpsRampGrpcRetryLogic;

impl RetryLogic for OpsRampGrpcRetryLogic {
    type Error = OpsRampSinkError;
    type Response = ();

    fn is_retriable_error(&self, err: &Self::Error) -> bool {
        use tonic::Code::*;

        match err {
            OpsRampSinkError::Request { source } => !matches!(
                source.code(),
                // List taken from
                //
                // <https://github.com/grpc/grpc/blob/ed1b20777c69bd47e730a63271eafc1b299f6ca0/doc/statuscodes.md>
                NotFound
                    | InvalidArgument
                    | AlreadyExists
                    | PermissionDenied
                    | OutOfRange
                    | Unimplemented
                    | Unauthenticated
            ),
            _ => true,
        }
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum OpsRampSinkError {
    #[snafu(display("Request failed: {}", source))]
    Request { source: tonic::Status },

    #[snafu(display("Vector source unhealthy"))]
    Health,

    #[snafu(display("URL has no host."))]
    NoHost,
}
