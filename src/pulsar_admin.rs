use crate::auth::Token;
use crate::update::Namespace;
use crate::update::Subscription;
use crate::update::Tenant;
use crate::update::Topic;
use anyhow::{anyhow, Result};
use chrono::TimeDelta;
use chrono::Utc;
use itertools::Itertools;
use pulsar_admin_sdk::apis::configuration::Configuration;
use pulsar_admin_sdk::apis::namespaces_api::namespaces_get_tenant_namespaces;
use pulsar_admin_sdk::apis::namespaces_api::namespaces_get_topics;
use pulsar_admin_sdk::apis::persistent_topic_api::persistent_topics_delete_subscription;
use pulsar_admin_sdk::apis::persistent_topic_api::persistent_topics_get_stats;
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

pub async fn fetch_subs(
    tenant: &str,
    namespace: &str,
    topic: &str,
    cfg: &Configuration,
) -> anyhow::Result<Vec<Subscription>> {
    let res = persistent_topics_get_stats(
        cfg,
        tenant,
        namespace,
        topic,
        None,
        None,
        Some(true),
        None,
        None,
        None,
    );
    let result = res
        .await
        .map_err(|err| {
            anyhow!(
                "Failed to fetch subscriptions (topic stats) {} {} {} {}",
                tenant,
                namespace,
                topic,
                err
            )
        })?
        .subscriptions
        .map(|subs| {
            subs.iter()
                .map(|(key, value)| Subscription {
                    name: key.to_string(),
                    sub_type: value
                        .r#type
                        .clone()
                        .unwrap_or("no_type".to_string()),
                    backlog_size: value.msg_backlog.unwrap_or(0),
                })
                .collect_vec()
        })
        .unwrap_or(vec![]);

    Ok(result)
}

pub async fn fetch_tenants(token: &Token) -> Result<Vec<Tenant>, reqwest::Error> {
    let addr = "https://pc-2c213b69.euw1-turtle.streamnative.g.snio.cloud";
    let tenant = fetch_anything(format!("{}/admin/v2/tenants", addr), token).await?;
    let content = tenant
        .iter()
        .map(|tenant| Tenant { name: tenant.to_string() })
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

pub async fn delete_subscription(
    tenant: &str,
    namespace: &str,
    topic: &str,
    sub_name: &str,
    cfg: &Configuration,
) -> anyhow::Result<()> {
    persistent_topics_delete_subscription(cfg, tenant, namespace, topic, sub_name, Some(true), None)
        .await
        .map_err(|err| anyhow!("Failed to delete subscription {}", err))
}

pub async fn fetch_namespaces(tenant: &str, cfg: &Configuration) -> anyhow::Result<Vec<Namespace>> {
    let result = namespaces_get_tenant_namespaces(&cfg, tenant)
        .await
        .map_err(|err| anyhow!("Failed to fetch namespaces {}", err))?;

    let perfix_dropped = result
        .iter()
        .map(|namespace| Namespace {
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
) -> anyhow::Result<Vec<Topic>> {
    let result = namespaces_get_topics(&cfg, tenant, namespace, None, None)
        .await
        .map_err(|err| anyhow!("Failed to fetch topics {}", err))?;

    let perfix_dropped = result
        .iter()
        .map(|topic| Topic {
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
