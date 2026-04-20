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
    pub url: String,
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

/// Cached dashboard data — full unpaginated PR list + metadata.
/// Stored in DynamoDB, paginated on read to produce DashboardData.
/// Only consumed by the native API layer; on wasm32 the struct is still
/// reachable via `types::*` glob imports but no caller exists. Narrow
/// `dead_code` allow keeps wasm32 clippy green without cfg-gating the type.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(target_arch = "wasm32", allow(dead_code))]
pub struct DashboardCache {
    pub prs: Vec<PrRow>,
    pub open_prs: u32,
    pub jobs_tracked: u32,
}

impl DashboardCache {
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    pub fn paginate(&self, page: u32, per_page: u32) -> DashboardData {
        let start = ((page - 1) * per_page) as usize;
        let end = (start + per_page as usize).min(self.prs.len());
        let prs = if start < self.prs.len() {
            self.prs[start..end].to_vec()
        } else {
            vec![]
        };
        let has_more = end < self.prs.len();

        DashboardData {
            prs,
            open_prs: self.open_prs,
            jobs_tracked: self.jobs_tracked,
            page,
            per_page,
            has_more,
        }
    }
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum DriftType {
    Modified,
    New,
    Deleted,
    ResourceMissing,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DriftItem {
    pub name: String,
    pub environment: String,
    pub region: String,
    pub drift_type: DriftType,
    pub fields_changed: Vec<String>,
    pub old_config: Option<String>,
    pub new_config: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DriftData {
    pub items: Vec<DriftItem>,
    pub in_sync: u32,
    pub drifted: u32,
}
