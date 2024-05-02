use chrono::{DateTime, Utc};

#[derive(Debug)]
pub struct Benchmark {
    pub id: String,
    pub cid: Option<String>,
    pub timestamp: Option<DateTime<Utc>>,
}
