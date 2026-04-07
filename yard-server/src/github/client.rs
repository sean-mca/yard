use octocrab::Octocrab;

/// Wrapper around octocrab for yard-specific GitHub operations.
pub struct GitHubClient {
    octo: Octocrab,
}

#[allow(dead_code)]
impl GitHubClient {
    pub fn new(token: &str) -> Result<Self, octocrab::Error> {
        let octo = Octocrab::builder()
            .personal_token(token.to_string())
            .build()?;
        Ok(Self { octo })
    }

    /// Post a plan comment on a pull request.
    pub async fn post_plan_comment(
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

    /// List files changed in a pull request.
    pub async fn get_pr_changed_files(
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

    /// Get the head SHA of a pull request.
    pub async fn get_pr_head_sha(
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
