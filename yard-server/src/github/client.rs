use async_trait::async_trait;
use octocrab::Octocrab;

/// Determines the header/footer text injected around plan/apply output.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub enum CommentMode {
    Plan,
    Apply,
}

/// A PR/issue comment returned by the GitHub API.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PrComment {
    pub id: u64,
    pub body: String,
}

/// Trait for GitHub API operations, enabling mock implementations in tests.
#[allow(dead_code)]
#[async_trait]
pub trait GitHubApi: Send + Sync {
    /// Post a plan or apply comment on a pull request.
    async fn post_comment(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
        output: &str,
        mode: CommentMode,
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

    /// List comments on a PR/issue.
    async fn list_comments(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
    ) -> Result<Vec<PrComment>, octocrab::Error>;

    /// Update an existing comment by ID.
    async fn update_comment(
        &self,
        owner: &str,
        repo: &str,
        comment_id: u64,
        body: &str,
    ) -> Result<(), octocrab::Error>;

    /// Post a comment with the body used verbatim (no template wrapping).
    async fn post_comment_raw(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
        body: &str,
    ) -> Result<(), octocrab::Error>;

    /// Health check -- proves token validity and API connectivity via GET /rate_limit.
    async fn health_check(&self) -> Result<(), octocrab::Error>;
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
    async fn post_comment(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
        output: &str,
        mode: CommentMode,
    ) -> Result<(), octocrab::Error> {
        // Sanitize triple backticks to prevent markdown fence breakout in GitHub comments.
        // Trades visual fidelity (``` → `` `) for injection safety.
        let safe_output = output.replace("```", "`` `");

        let (header, footer) = match mode {
            CommentMode::Plan => (
                "### yard plan",
                "> To apply these changes, merge this PR.",
            ),
            CommentMode::Apply => (
                "### yard apply",
                "> Applied successfully.",
            ),
        };

        let body = format!(
            "{header}\n\n\
             <details>\n\
             <summary>Output</summary>\n\n\
             ```\n{safe_output}\n```\n\n\
             </details>\n\n\
             {footer}"
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

    async fn list_comments(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
    ) -> Result<Vec<PrComment>, octocrab::Error> {
        let page = self.octo
            .issues(owner, repo)
            .list_comments(pr_number)
            .per_page(100)
            .send()
            .await?;

        Ok(page.items.into_iter().map(|c| PrComment {
            id: c.id.into_inner(),
            body: c.body.unwrap_or_default(),
        }).collect())
    }

    async fn update_comment(
        &self,
        owner: &str,
        repo: &str,
        comment_id: u64,
        body: &str,
    ) -> Result<(), octocrab::Error> {
        self.octo
            .issues(owner, repo)
            .update_comment(octocrab::models::CommentId(comment_id), body)
            .await?;
        Ok(())
    }

    async fn post_comment_raw(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
        body: &str,
    ) -> Result<(), octocrab::Error> {
        self.octo
            .issues(owner, repo)
            .create_comment(pr_number, body)
            .await?;
        Ok(())
    }

    async fn health_check(&self) -> Result<(), octocrab::Error> {
        self.octo.ratelimit().get().await?;
        Ok(())
    }
}

// ---- Test Support ----

#[cfg(test)]
pub(crate) mod test_support {
    use super::{CommentMode, GitHubApi, PrComment};
    use async_trait::async_trait;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Debug, Clone)]
    #[allow(dead_code)]
    pub struct PostedComment {
        pub owner: String,
        pub repo: String,
        pub pr_number: u64,
        pub body: String,
        pub mode: CommentMode,
    }

    #[allow(dead_code)]
    pub struct InMemoryGitHubApi {
        pub posts: Arc<Mutex<Vec<PostedComment>>>,
        pub comments: Arc<Mutex<Vec<PrComment>>>,
        pub updated_comments: Arc<Mutex<Vec<(u64, String)>>>,
        pub raw_posts: Arc<Mutex<Vec<PostedComment>>>,
        pub changed_files: Arc<Mutex<Vec<String>>>,
        pub head_sha: Arc<Mutex<String>>,
    }

    impl InMemoryGitHubApi {
        pub fn new() -> Self {
            Self {
                posts: Arc::new(Mutex::new(Vec::new())),
                comments: Arc::new(Mutex::new(Vec::new())),
                updated_comments: Arc::new(Mutex::new(Vec::new())),
                raw_posts: Arc::new(Mutex::new(Vec::new())),
                changed_files: Arc::new(Mutex::new(Vec::new())),
                head_sha: Arc::new(Mutex::new("test-sha-abc123".to_string())),
            }
        }
    }

    #[async_trait]
    impl GitHubApi for InMemoryGitHubApi {
        async fn post_comment(
            &self,
            owner: &str,
            repo: &str,
            pr_number: u64,
            output: &str,
            mode: CommentMode,
        ) -> Result<(), octocrab::Error> {
            self.posts.lock().await.push(PostedComment {
                owner: owner.to_string(),
                repo: repo.to_string(),
                pr_number,
                body: output.to_string(),
                mode,
            });
            Ok(())
        }

        async fn get_pr_changed_files(
            &self,
            _owner: &str,
            _repo: &str,
            _pr_number: u64,
        ) -> Result<Vec<String>, octocrab::Error> {
            Ok(self.changed_files.lock().await.clone())
        }

        async fn get_pr_head_sha(
            &self,
            _owner: &str,
            _repo: &str,
            _pr_number: u64,
        ) -> Result<String, octocrab::Error> {
            Ok(self.head_sha.lock().await.clone())
        }

        async fn list_comments(
            &self,
            _owner: &str,
            _repo: &str,
            _pr_number: u64,
        ) -> Result<Vec<PrComment>, octocrab::Error> {
            Ok(self.comments.lock().await.clone())
        }

        async fn update_comment(
            &self,
            _owner: &str,
            _repo: &str,
            comment_id: u64,
            body: &str,
        ) -> Result<(), octocrab::Error> {
            self.updated_comments.lock().await.push((comment_id, body.to_string()));
            Ok(())
        }

        async fn post_comment_raw(
            &self,
            owner: &str,
            repo: &str,
            pr_number: u64,
            body: &str,
        ) -> Result<(), octocrab::Error> {
            self.raw_posts.lock().await.push(PostedComment {
                owner: owner.to_string(),
                repo: repo.to_string(),
                pr_number,
                body: body.to_string(),
                mode: CommentMode::Plan,
            });
            Ok(())
        }

        async fn health_check(&self) -> Result<(), octocrab::Error> {
            Ok(())
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::test_support::InMemoryGitHubApi;
    use super::{CommentMode, GitHubApi, PrComment};

    #[tokio::test]
    async fn post_comment_plan_mode_records_correctly() {
        let api = InMemoryGitHubApi::new();
        api.post_comment("owner", "repo", 1, "plan output", CommentMode::Plan)
            .await
            .unwrap();
        let comments = api.posts.lock().await;
        assert_eq!(comments.len(), 1);
        assert!(matches!(comments[0].mode, CommentMode::Plan));
    }

    #[tokio::test]
    async fn post_comment_apply_mode_records_correctly() {
        let api = InMemoryGitHubApi::new();
        api.post_comment("owner", "repo", 1, "apply output", CommentMode::Apply)
            .await
            .unwrap();
        let comments = api.posts.lock().await;
        assert!(matches!(comments[0].mode, CommentMode::Apply));
    }

    #[tokio::test]
    async fn get_pr_head_sha_returns_test_sha() {
        let api = InMemoryGitHubApi::new();
        let sha = api.get_pr_head_sha("owner", "repo", 1).await.unwrap();
        assert!(!sha.is_empty());
        assert_eq!(sha, "test-sha-abc123");
    }

    #[tokio::test]
    async fn list_comments_returns_seeded_comments() {
        let api = InMemoryGitHubApi::new();
        {
            let mut comments = api.comments.lock().await;
            comments.push(PrComment { id: 1, body: "first".to_string() });
            comments.push(PrComment { id: 2, body: "second".to_string() });
        }
        let result = api.list_comments("o", "r", 1).await.unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id, 1);
        assert_eq!(result[1].body, "second");
    }

    #[tokio::test]
    async fn update_comment_records_call() {
        let api = InMemoryGitHubApi::new();
        api.update_comment("o", "r", 42, "new body").await.unwrap();
        let updated = api.updated_comments.lock().await;
        assert_eq!(updated.len(), 1);
        assert_eq!(updated[0], (42, "new body".to_string()));
    }

    #[tokio::test]
    async fn get_pr_changed_files_returns_seeded_files() {
        let api = InMemoryGitHubApi::new();
        {
            let mut files = api.changed_files.lock().await;
            files.push("production/us-east-1/jobs/foo.yaml".to_string());
        }
        let result = api.get_pr_changed_files("o", "r", 1).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], "production/us-east-1/jobs/foo.yaml");
    }
}
