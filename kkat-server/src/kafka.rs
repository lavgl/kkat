use futures::sync::mpsc;
// use std::sync::mpsc;

use bytes::Bytes;

use std::{thread, time};

use rdkafka::{
  config::RDKafkaLogLevel,
  consumer::{BaseConsumer, CommitMode, Consumer},
  message::Message,
  topic_partition_list::{Offset::Offset, TopicPartitionList},
  ClientConfig,
};

use super::params::QueryParams;

fn adjust_offset(offset: i64, (min_offset, max_offset): (i64, i64)) -> i64 {
  let mut offset = offset;

  if offset < 0 {
    offset += max_offset;
  }

  if offset < min_offset {
    offset = min_offset;
  }

  if offset > max_offset {
    offset = max_offset;
  }

  offset
}

pub fn read_messages(query: QueryParams) -> mpsc::UnboundedReceiver<Bytes> {
  let offset = query.offset.unwrap_or(0);

  let consumer: BaseConsumer = ClientConfig::new()
    .set("group.id", "kkat-2")
    .set("bootstrap.servers", "localhost:9092")
    .set("auto.offset.reset", "earliest")
    .set("enable.auto.commit", "false")
    .set_log_level(RDKafkaLogLevel::Debug)
    .create()
    .expect("consumer to be created");

  let meta = consumer
    .fetch_metadata(Some("test"), time::Duration::from_millis(1000))
    .expect("to fetch meta");

  let mut tpl = TopicPartitionList::new();

  for topic in meta.topics() {
    for partition in topic.partitions() {
      let topic = topic.name();
      let partition = partition.id();

      let watermarks = consumer
        .fetch_watermarks(topic, partition, time::Duration::from_millis(1000))
        .expect("fetching offset fails");

      println!("watermarks for {}[{}]: {:?}", topic, partition, watermarks);

      let offset = adjust_offset(offset, watermarks);

      tpl.add_partition_offset(topic, partition, Offset(offset));
    }
  }

  println!("tpl {:?}", tpl);

  let (sender, receiver) = mpsc::unbounded();

  consumer.assign(&tpl).expect("assign partitions");

  thread::spawn(move || {
    println!("start consumer");

    loop {
      let message = match consumer.poll(time::Duration::from_millis(1000)) {
        Some(Ok(message)) => message,
        Some(Err(err)) => {
          println!("error! {}", err);
          break;
        }
        None => continue,
      };

      let payload = match message.payload_view::<str>() {
        None => "",
        Some(Ok(s)) => s,
        Some(Err(e)) => {
          println!("error while deserealizing value");
          ""
        }
      };

      println!("payload: {}", payload);

      sender
        .unbounded_send(Bytes::from(format!("{}\n", payload)))
        .expect("sended correctly");

      consumer
        .commit_message(&message, CommitMode::Async)
        .expect("commiting message fails");
    }
  });

  receiver
}
