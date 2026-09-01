//! SDK-based test plugin binary (~75 lines vs ~170-line manual `test_plugin`).
//! Only depends on `yard-plugin-sdk` (SDK-03 single-dependency ergonomics).

use yard_plugin_sdk::{
    anyhow, CodegenResponse, DeployResponse, DestroyResponse, PluginHandler, PluginServer,
    Resource, ResourceStatus, SchemaField, SchemaResponse, ValidateResponse, Value, VerifyResponse,
};

struct TestSdkProvider;

impl PluginHandler for TestSdkProvider {
    fn name(&self) -> &str { "test-plugin-sdk" }
    fn version(&self) -> &str { "0.1.0" }

    fn validate(&self, _job_name: &str, _job_config: &Value) -> anyhow::Result<ValidateResponse> {
        Ok(ValidateResponse { errors: vec![] })
    }

    fn codegen(&self, _job_name: &str, _job_config: &Value) -> anyhow::Result<CodegenResponse> {
        Ok(CodegenResponse {
            script: Some("# generated-by-sdk-test\nprint('hello from sdk')".to_string()),
        })
    }

    fn deploy(
        &self, _job_name: &str, _job_config: &Value, _artifact: &str,
    ) -> anyhow::Result<DeployResponse> {
        // Deliberate println! to exercise SDK-02 stdout protection.
        println!("this should go to stderr not protocol");
        Ok(DeployResponse {
            resources: vec![Resource {
                r#type: "SdkTestResource".into(),
                id: "sdk-test-123".into(),
                provider: "test-plugin-sdk".into(),
            }],
        })
    }

    fn destroy(&self, _job_name: &str, _resources: &[Resource]) -> anyhow::Result<DestroyResponse> {
        Ok(DestroyResponse {})
    }

    fn verify(&self, _job_name: &str, _resources: &[Resource]) -> anyhow::Result<VerifyResponse> {
        Ok(VerifyResponse {
            statuses: vec![ResourceStatus {
                resource: Resource {
                    r#type: "SdkTestResource".into(),
                    id: "sdk-test-123".into(),
                    provider: "test-plugin-sdk".into(),
                },
                exists: true,
            }],
        })
    }

    fn schema(&self) -> anyhow::Result<SchemaResponse> {
        Ok(SchemaResponse {
            fields: vec![SchemaField {
                name: "region".into(),
                field_type: "string".into(),
                required: true,
                description: "AWS region".into(),
            }],
        })
    }
}

fn main() -> ! {
    PluginServer::run(TestSdkProvider)
}
