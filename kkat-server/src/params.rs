use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct QueryParams {
  pub topic: String,
  pub offset: Option<i64>,
}

impl QueryParams {
  pub fn new(topic: String, offset: i64) -> QueryParams {
    QueryParams {
      topic,
      offset: Some(offset),
    }
  }
}
