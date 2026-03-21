use anyhow::Result;
use yard_structs::{StateBackend, YardAction};
mod state;
mod storage;

pub async fn dispatch(action: YardAction) -> Result<()> {
    match action {
        YardAction::Init { manifest } => {
            state::initialize_backend(&manifest.project, &manifest.state).await?;
        }
        _ => {}
    }
    Ok(())
}
