//! Plugin handler trait defining the business-logic interface for provider plugins.

/// Trait that plugin authors implement to handle provider operations.
///
/// Each method corresponds to a [`yard_structs::PluginOperation`] variant.
/// The SDK's [`crate::PluginServer`] dispatches incoming requests to the
/// appropriate method and serializes the return value back over the
/// protocol channel.
pub trait PluginHandler {}
