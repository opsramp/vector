use super::config::OpsRampSinkConfig;
use crate::http::HttpClient;

pub async fn healthcheck(_: OpsRampSinkConfig, _: HttpClient) -> crate::Result<()> {
    Ok(())
}
