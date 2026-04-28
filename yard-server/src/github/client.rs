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

// ---- Test Support ----

#[cfg(test)]
pub(crate) mod test_support {
    use super::GitHubApi;
    use async_trait::async_trait;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    /// Record of a single `post_plan_comment` invocation captured by
    /// `InMemoryGitHubApi`. Fields beyond `body` are kept for diagnostic
    /// value when an assertion fails — a regression that posts to the
    /// wrong owner/repo/pr is informative against the recorded record.
    #[derive(Debug, Clone)]
    #[allow(dead_code)]
    pub struct PostedComment {
        pub owner: String,
        pub repo: String,
        pub pr_number: u64,
        pub body: String,
    }

    /// In-memory `GitHubApi` impl: captures `post_plan_comment` calls into
    /// `posts` (so the test can assert on them after the handler returns)
    /// and returns safe defaults from the two unused-by-Plan-path methods
    /// (D-15: full method record keeps the impl complete + future-proofs
    /// against the test ever exercising the Apply path).
    pub struct InMemoryGitHubApi {
        pub posts: Arc<Mutex<Vec<PostedComment>>>,
    }

    impl InMemoryGitHubApi {
        pub fn new() -> Self {
            Self {
                posts: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl GitHubApi for InMemoryGitHubApi {
        async fn post_plan_comment(
            &self,
            owner: &str,
            repo: &str,
            pr_number: u64,
            plan_output: &str,
        ) -> Result<(), octocrab::Error> {
            self.posts.lock().await.push(PostedComment {
                owner: owner.to_string(),
                repo: repo.to_string(),
                pr_number,
                body: plan_output.to_string(),
            });
            Ok(())
        }

        async fn get_pr_changed_files(
            &self,
            _owner: &str,
            _repo: &str,
            _pr_number: u64,
        ) -> Result<Vec<String>, octocrab::Error> {
            // Not exercised by the Plan path; returned default (D-15).
            Ok(Vec::new())
        }

        async fn get_pr_head_sha(
            &self,
            _owner: &str,
            _repo: &str,
            _pr_number: u64,
        ) -> Result<String, octocrab::Error> {
            // Not exercised by the pull_request webhook path (head_sha is
            // in the payload). Exercised only by the issue_comment "yard
            // apply" path, out of scope for Phase 27. Returned safe
            // default per D-15.
            Ok(String::new())
        }
    }
}
