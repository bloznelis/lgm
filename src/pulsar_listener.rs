use futures::TryStreamExt;
use pulsar::consumer::InitialPosition;
use pulsar::{Consumer, DeserializeMessage, Payload, Pulsar, SubType, TokioExecutor};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::panic;
use std::sync::{mpsc::Sender, Arc};

use crate::AppEvent;

#[derive(Serialize, Deserialize)]
pub struct TopicEvent {
    pub body: Value,
    pub properties: Vec<String>,
}

impl DeserializeMessage for TopicEvent {
    type Output = Result<TopicEvent, serde_json::Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        let props = payload
            .metadata
            .properties
            .iter()
            .map(|keyvalue| format!("{}:{}", keyvalue.key, keyvalue.value))
            .collect::<Vec<String>>();

        serde_json::from_slice::<Value>(&payload.data).map(|content| TopicEvent {
            body: content,
            properties: props,
        })
    }
}

pub async fn listen_to_topic(
    sub_name: String,
    topic_fqn: String,
    event_sender: Sender<AppEvent>,
    pulsar: Arc<Pulsar<TokioExecutor>>,
    mut control_channel: tokio::sync::oneshot::Receiver<()>,
) -> anyhow::Result<()> {
    let mut consumer: Consumer<TopicEvent, TokioExecutor> = pulsar
        .consumer()
        .with_options(
            pulsar::ConsumerOptions::default()
                .durable(false)
                .with_initial_position(InitialPosition::Latest),
        )
        .with_topic(topic_fqn)
        .with_subscription_type(SubType::Exclusive)
        .with_subscription(sub_name)
        .build()
        .await?;

    loop {
        tokio::select! {
            msg = consumer.try_next() => {
                match msg {
                    Ok(Some(message)) => {
                        let topic_event = message.deserialize()?;
                        let _ = event_sender.send(AppEvent::SubscriptionEvent(topic_event));

                        consumer.ack(&message).await?;

                    },
                    Ok(None) => break,
                    Err(e) => {
                        println!("Error in topic consumer, {:?}", e);
                        break;
                    }
                }
            },
            _ = &mut control_channel => {
                // cancel!
                break;
            }
        }
    }

    consumer.close().await?;

    Ok(())
}
