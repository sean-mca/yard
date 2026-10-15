<!-- generated-by: gsd-doc-writer -->
# EMR provider

> **Moved.** As of yard v2.0, the EMR provider is a plugin distributed separately from the yard CLI.

The compiled-in EMR provider was removed in v2.0. An EMR job is deployed by an EMR plugin declared on the job with `type: emr`, `plugin_version`, and `plugin_source`. The fields accepted under `providers.emr` and the resources the plugin needs (cluster, script bucket, permissions) are documented by the plugin, not here.

To build one, follow [build a plugin](../../how-to/build-a-plugin.md).

For upgrading from v1.x, see the [v2.0 migration guide](../migrations/v2.0.md).
