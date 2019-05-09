use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct QueryParams {
  pub topic: String,
  pub offset: Option<i64>,
  pub format: Option<String>,
}

impl QueryParams {
  pub fn new(topic: String, offset: i64, format: String) -> QueryParams {
    QueryParams {
      topic,
      offset: Some(offset),
      format: Some(format),
    }
  }
}
