//! Centralizes version-dependent Python string literals for Airflow codegen
//! per D-01.
//!
//! Every emission site in [`triggers`](super::triggers) and
//! [`generation`](super::generation) calls methods from the
//! [`VersionCodegen`] extension trait on [`AirflowMajorVersion`] instead of
//! hardcoding V2 strings. The V2 path produces byte-identical output to
//! pre-Phase-56 code; the V3 path emits `Asset` / `providers-standard`
//! import strings.
//!
//! D-02: The extension trait lives here in yard-core (codegen concern), NOT
//! in yard-structs (pure data types). Rust's orphan rule prohibits inherent
//! `impl` blocks on foreign types; the sealed extension trait achieves the
//! same ergonomics (`version.class_name()`) while keeping codegen strings
//! out of the data-types crate.

use yard_structs::AirflowMajorVersion;

use super::triggers::VERSION_BANNER;

/// AF3 version contract banner for event-driven DAGs (D-04 / D-05 / D-06).
///
/// Three requirement lines: `apache-airflow >= 3.0`,
/// `apache-airflow-providers-amazon >= 9.0.0`, and
/// `apache-airflow-providers-standard` (unconditionally included per D-05).
/// No aiobotocore line (AF3 handles internally), no Triggerer line
/// (deferrable is default in AF3).
pub(super) const VERSION_BANNER_V3: &str = "# Airflow version contract:
#   - apache-airflow >= 3.0
#   - apache-airflow-providers-amazon >= 9.0.0
#   - apache-airflow-providers-standard
";

/// Extension trait providing version-dependent codegen string helpers on
/// [`AirflowMajorVersion`].
///
/// Sealed to prevent external implementations; all methods return
/// `&'static str` for zero-allocation codegen.
pub(crate) trait VersionCodegen {
    /// Class name for the event-driven scheduling primitive.
    ///
    /// V2 returns `"Dataset"`, V3 returns `"Asset"`.
    fn class_name(self) -> &'static str;

    /// Full Python import line for the Dataset/Asset class.
    ///
    /// V2 returns `"from airflow.datasets import Dataset"`,
    /// V3 returns `"from airflow.sdk import Asset"`.
    fn class_import(self) -> &'static str;

    /// BashOperator import line.
    ///
    /// V2 returns the core `airflow.operators.bash` path,
    /// V3 returns the `providers-standard` path.
    fn bash_op_import(self) -> &'static str;

    /// EmptyOperator import line.
    ///
    /// V2 returns the core `airflow.operators.empty` path,
    /// V3 returns the `providers-standard` path.
    fn empty_op_import(self) -> &'static str;

    /// Version contract banner for event-driven DAGs (D-06).
    ///
    /// V2 returns the existing [`VERSION_BANNER`](super::triggers::VERSION_BANNER),
    /// V3 returns [`VERSION_BANNER_V3`].
    fn version_banner(self) -> &'static str;
}

impl VersionCodegen for AirflowMajorVersion {
    #[inline]
    fn class_name(self) -> &'static str {
        match self {
            Self::V2 => "Dataset",
            Self::V3 => "Asset",
            _ => unreachable!("unsupported airflow version for codegen: {self}"),
        }
    }

    #[inline]
    fn class_import(self) -> &'static str {
        match self {
            Self::V2 => "from airflow.datasets import Dataset",
            Self::V3 => "from airflow.sdk import Asset",
            _ => unreachable!("unsupported airflow version for codegen: {self}"),
        }
    }

    #[inline]
    fn bash_op_import(self) -> &'static str {
        match self {
            Self::V2 => "from airflow.operators.bash import BashOperator",
            Self::V3 => "from airflow.providers.standard.operators.bash import BashOperator",
            _ => unreachable!("unsupported airflow version for codegen: {self}"),
        }
    }

    #[inline]
    fn empty_op_import(self) -> &'static str {
        match self {
            Self::V2 => "from airflow.operators.empty import EmptyOperator",
            Self::V3 => "from airflow.providers.standard.operators.empty import EmptyOperator",
            _ => unreachable!("unsupported airflow version for codegen: {self}"),
        }
    }

    #[inline]
    fn version_banner(self) -> &'static str {
        match self {
            Self::V2 => VERSION_BANNER,
            Self::V3 => VERSION_BANNER_V3,
            _ => unreachable!("unsupported airflow version for codegen: {self}"),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn class_name_v2_returns_dataset() {
        assert_eq!(AirflowMajorVersion::V2.class_name(), "Dataset");
    }

    #[test]
    fn class_name_v3_returns_asset() {
        assert_eq!(AirflowMajorVersion::V3.class_name(), "Asset");
    }

    #[test]
    fn class_import_v2_returns_dataset_import() {
        assert_eq!(
            AirflowMajorVersion::V2.class_import(),
            "from airflow.datasets import Dataset"
        );
    }

    #[test]
    fn class_import_v3_returns_asset_import() {
        assert_eq!(
            AirflowMajorVersion::V3.class_import(),
            "from airflow.sdk import Asset"
        );
    }

    #[test]
    fn bash_op_import_v2_returns_core_path() {
        assert_eq!(
            AirflowMajorVersion::V2.bash_op_import(),
            "from airflow.operators.bash import BashOperator"
        );
    }

    #[test]
    fn bash_op_import_v3_returns_providers_standard_path() {
        assert_eq!(
            AirflowMajorVersion::V3.bash_op_import(),
            "from airflow.providers.standard.operators.bash import BashOperator"
        );
    }

    #[test]
    fn empty_op_import_v2_returns_core_path() {
        assert_eq!(
            AirflowMajorVersion::V2.empty_op_import(),
            "from airflow.operators.empty import EmptyOperator"
        );
    }

    #[test]
    fn empty_op_import_v3_returns_providers_standard_path() {
        assert_eq!(
            AirflowMajorVersion::V3.empty_op_import(),
            "from airflow.providers.standard.operators.empty import EmptyOperator"
        );
    }

    #[test]
    fn version_banner_v2_contains_airflow_2_9() {
        let banner = AirflowMajorVersion::V2.version_banner();
        assert!(
            banner.contains("apache-airflow >= 2.9"),
            "V2 banner must reference airflow >= 2.9: {banner}"
        );
    }

    #[test]
    fn version_banner_v3_contains_airflow_3_0() {
        let banner = AirflowMajorVersion::V3.version_banner();
        assert!(
            banner.contains("apache-airflow >= 3.0"),
            "V3 banner must reference airflow >= 3.0: {banner}"
        );
        assert!(
            banner.contains("apache-airflow-providers-standard"),
            "V3 banner must include providers-standard: {banner}"
        );
        assert!(
            banner.contains("apache-airflow-providers-amazon >= 9.0.0"),
            "V3 banner must include providers-amazon >= 9.0.0: {banner}"
        );
    }
}
