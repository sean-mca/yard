<!-- generated-by: gsd-doc-writer -->
# Glue provider

> **Moved.** As of yard v2.0, the Glue provider is a plugin distributed separately from the yard CLI.

The compiled-in Glue provider was removed in v2.0. A Glue job is now deployed by a Glue plugin declared on the job with `type: glue`, `plugin_version`, and `plugin_source`. The fields accepted under `providers.glue`, the generated script, and the resources the plugin needs (execution role, script bucket, permissions) are documented by the plugin, not here.

For upgrading from v1.x, see the [v2.0 migration guide](../migrations/v2.0.md).
