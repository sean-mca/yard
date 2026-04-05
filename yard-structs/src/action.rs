use crate::config::ProjectManifest;

#[derive(Debug)]
pub enum YardAction {
    Init { manifest: ProjectManifest },
    Plan { manifest: ProjectManifest },
    Apply { manifest: ProjectManifest },
    Destroy { resource_id: String },
}
