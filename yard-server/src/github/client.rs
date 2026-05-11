use async_trait::async_trait;
use octocrab::Octocrab;

/// Determines the header/footer text injected around plan/apply output.
#[derive(Debug, Clone, Copy)]
pub enum CommentMode {
    Plan,
    Apply,
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
        // WR-03: prevent triple-backtick fence breakout
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
}

// ---- Test Support ----

#[cfg(test)]
pub(crate) mod test_support {
    use super::{CommentMode, GitHubApi};
    use async_trait::async_trait;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    /// Record of a single `post_comment` invocation captured by
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
        pub mode: CommentMode,
    }

    /// In-memory `GitHubApi` impl: captures `post_comment` calls into
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
            // Not exercised by the Plan path; returned default (D-15).
            Ok(Vec::new())
        }

        async fn get_pr_head_sha(
            &self,
            _owner: &str,
            _repo: &str,
            _pr_number: u64,
        ) -> Result<String, octocrab::Error> {
            // WR-05 fix: return a test SHA instead of empty string so
            // Apply-path tests can exercise get_pr_head_sha without
            // getting a misleading empty value.
            Ok("test-sha-abc123".to_string())
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::test_support::InMemoryGitHubApi;
    use super::CommentMode;
    use super::GitHubApi;

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
        assert!(!sha.is_empty(), "Should return a non-empty test SHA");
        assert_eq!(sha, "test-sha-abc123");
    }
}
