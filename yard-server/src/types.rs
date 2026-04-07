use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PrState {
    Open,
    Merged,
    Closed,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PlanResult {
    Pass,
    Fail,
    Pending,
    None,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PrRow {
    pub number: u64,
    pub title: String,
    pub author: String,
    pub state: PrState,
    pub plan_result: PlanResult,
    pub updated: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DashboardData {
    pub prs: Vec<PrRow>,
    pub open_prs: u32,
    pub jobs_tracked: u32,
    pub page: u32,
    pub per_page: u32,
    pub has_more: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JobInfo {
    pub name: String,
    pub path: String,
    pub environment: String,
    pub region: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JobsData {
    pub jobs: Vec<JobInfo>,
}
