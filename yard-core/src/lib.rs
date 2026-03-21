use anyhow::{Context, Result};
use yard_structs::{StateBackend, YardAction};
mod state;
mod storage;
pub mod utils;

pub async fn dispatch(action: YardAction) -> Result<()> {
    match action {
        YardAction::Init { manifest } => {
            state::initialize_backend(&manifest.project, &manifest.state, &manifest.jobs).await?;
        }
        YardAction::Plan { manifest } => {
            let storage = storage::get_storage(&manifest.state).await?;
            let actual_state = storage.read().await.context("Run init first!")?;
            let proposed_state = state::calculate_proposed_state(&manifest);

            let changes = state::calculate_diff(&actual_state, &proposed_state);

            println!("--- Plan for {} ---", manifest.project);
            for c in changes {
                println!("{:?}", c);
            }
        }
        _ => {}
    }
    Ok(())
}
