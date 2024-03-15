use crate::auth::Token;
use crate::Content;
use anyhow::{anyhow, Result};
use chrono::TimeDelta;
use chrono::Utc;
use pulsar_admin_sdk::apis::configuration::Configuration;
use pulsar_admin_sdk::apis::namespaces_api::namespaces_get_tenant_namespaces;
use pulsar_admin_sdk::apis::namespaces_api::namespaces_get_topics;
use pulsar_admin_sdk::apis::persistent_topic_api::persistent_topics_get_subscriptions;
use pulsar_admin_sdk::apis::persistent_topic_api::persistent_topics_reset_cursor;

pub async fn fetch_anything(url: String, token: &Token) -> Result<Vec<String>, reqwest::Error> {
    let client = reqwest::Client::new();
    let response = client
        .get(url)
        .bearer_auth(&token.access_token)
        .send()
        .await?;

    let array: Vec<String> = response.json().await?;

    Ok(array)
}

pub async fn fetch_tenants(token: &Token) -> Result<Vec<Content>, reqwest::Error> {
    let addr = "https://pc-2c213b69.euw1-turtle.streamnative.g.snio.cloud";
    let tenant = fetch_anything(format!("{}/admin/v2/tenants", addr), token).await?;
    let content = tenant
        .iter()
        .map(|tenant| Content::Tenant {
            name: tenant.to_string(),
        })
        .collect();

    Ok(content)
}

pub async fn reset_subscription(
    tenant: &str,
    namespace: &str,
    topic: &str,
    sub_name: &str,
    cfg: &Configuration,
    time_delta: TimeDelta,
) -> anyhow::Result<()> {
    let now = Utc::now();
    let one_hour_before = now - time_delta;
    let timestamp = one_hour_before.timestamp_millis();

    persistent_topics_reset_cursor(cfg, tenant, namespace, topic, sub_name, timestamp, None)
        .await
        .map_err(|err| anyhow!("Failed to seek back subscription {}", err))
}

pub async fn fetch_namespaces(tenant: &str, cfg: &Configuration) -> anyhow::Result<Vec<Content>> {
    let result = namespaces_get_tenant_namespaces(&cfg, tenant)
        .await
        .map_err(|err| anyhow!("Failed to fetch namespaces {}", err))?;

    let perfix_dropped = result
        .iter()
        .map(|namespace| Content::Namespace {
            name: namespace
                .strip_prefix(format!("{}/", tenant.to_string()).as_str())
                // .strip_prefix("flowie/")
                .map(|stripped| stripped.to_string())
                .unwrap_or(namespace.clone()),
        })
        .collect();

    Ok(perfix_dropped)
}

pub async fn fetch_topics(
    tenant: &str,
    namespace: &str,
    cfg: &Configuration,
) -> anyhow::Result<Vec<Content>> {
    let result = namespaces_get_topics(&cfg, tenant, namespace, None, None)
        .await
        .map_err(|err| anyhow!("Failed to fetch topics {}", err))?;

    let perfix_dropped = result
        .iter()
        .map(|topic| Content::Topic {
            name: topic
                .split('/')
                .last()
                .map(|stripped| stripped.to_string())
                .unwrap_or(topic.clone()),
            fqn: topic.to_string(),
        })
        .collect();

    Ok(perfix_dropped)
}

pub async fn fetch_subscriptions(
    tenant: &str,
    namespace: &str,
    topic: &str,
    cfg: &Configuration,
) -> anyhow::Result<Vec<Content>> {
    let result = persistent_topics_get_subscriptions(&cfg, tenant, namespace, topic, None)
        .await
        .map_err(|err| {
            anyhow!(
                "Failed to fetch subscriptions {} {} {} {}",
                tenant,
                namespace,
                topic,
                err
            )
        })?
        .iter()
        .map(|sub| Content::Subscription {
            name: sub.to_string(),
        })
        .collect();

    Ok(result)
}
