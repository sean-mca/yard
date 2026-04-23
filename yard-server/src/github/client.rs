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
