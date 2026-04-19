use async_trait::async_trait;
use octocrab::Octocrab;

/// Trait for GitHub API operations, enabling mock implementations in tests.
#[allow(dead_code)]
#[async_trait]
pub trait GitHubApi: Send + Sync {
    /// Post a plan comment on a pull request.
    async fn post_plan_comment(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
        plan_output: &str,
    ) -> Result<(), octocrab::Error>;

    /// List files changed in a pull request.
    async fn get_pr_changed_files(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
    ) -> Result<Vec<String>, octocrab::Error>;

    /// Get the head SHA of a pull request.
    async fn get_pr_head_sha(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
    ) -> Result<String, octocrab::Error>;
}

/// Wrapper around octocrab for yard-specific GitHub operations.
pub struct GitHubClient {
    octo: Octocrab,
}

impl GitHubClient {
    pub fn new(token: &str) -> Result<Self, octocrab::Error> {
        let octo = Octocrab::builder()
            .personal_token(token.to_string())
            .build()?;
        Ok(Self { octo })
    }
}

#[async_trait]
impl GitHubApi for GitHubClient {
    async fn post_plan_comment(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
        plan_output: &str,
    ) -> Result<(), octocrab::Error> {
        let body = format!(
            "### yard plan\n\n\
             <details>\n\
             <summary>Plan output</summary>\n\n\
             ```\n{plan_output}\n```\n\n\
             </details>\n\n\
             > To apply these changes, merge this PR."
        );

        self.octo
            .issues(owner, repo)
            .create_comment(pr_number, body)
            .await?;

        Ok(())
    }

    async fn get_pr_changed_files(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
    ) -> Result<Vec<String>, octocrab::Error> {
        let files = self.octo
            .pulls(owner, repo)
            .list_files(pr_number)
            .await?;

        let paths: Vec<String> = files
            .into_iter()
            .map(|f| f.filename)
            .collect();

        Ok(paths)
    }

    async fn get_pr_head_sha(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
    ) -> Result<String, octocrab::Error> {
        let pr = self.octo
            .pulls(owner, repo)
            .get(pr_number)
            .await?;

        Ok(pr.head.sha)
    }
}

#[cfg(test)]
pub mod test_support {
    use super::*;
    use tokio::sync::Mutex;

    #[derive(Debug, Clone)]
    pub struct RecordedCall {
        pub method: String,
        pub args: Vec<String>,
    }

    pub struct MockGitHubClient {
        pub calls: Mutex<Vec<RecordedCall>>,
    }

    impl MockGitHubClient {
        pub fn new() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl GitHubApi for MockGitHubClient {
        async fn post_plan_comment(
            &self,
            owner: &str,
            repo: &str,
            pr_number: u64,
            plan_output: &str,
        ) -> Result<(), octocrab::Error> {
            self.calls.lock().await.push(RecordedCall {
                method: "post_plan_comment".to_string(),
                args: vec![
                    owner.to_string(),
                    repo.to_string(),
                    pr_number.to_string(),
                    plan_output.to_string(),
                ],
            });
            Ok(())
        }

        async fn get_pr_changed_files(
            &self,
            owner: &str,
            repo: &str,
            pr_number: u64,
        ) -> Result<Vec<String>, octocrab::Error> {
            self.calls.lock().await.push(RecordedCall {
                method: "get_pr_changed_files".to_string(),
                args: vec![
                    owner.to_string(),
                    repo.to_string(),
                    pr_number.to_string(),
                ],
            });
            Ok(vec![])
        }

        async fn get_pr_head_sha(
            &self,
            owner: &str,
            repo: &str,
            pr_number: u64,
        ) -> Result<String, octocrab::Error> {
            self.calls.lock().await.push(RecordedCall {
                method: "get_pr_head_sha".to_string(),
                args: vec![
                    owner.to_string(),
                    repo.to_string(),
                    pr_number.to_string(),
                ],
            });
            Ok("mock-sha-abc123".to_string())
        }
    }
}
