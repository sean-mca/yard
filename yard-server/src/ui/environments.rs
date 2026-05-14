use dioxus::prelude::*;

/// Stub: EnvironmentList page. Full implementation in Task 2.
#[component]
pub fn EnvironmentList() -> Element {
    rsx! {
        div { class: "p-6",
            p { "Loading environments..." }
        }
    }
}

/// Stub: EnvironmentDetail page. Full implementation in Task 2.
#[component]
pub fn EnvironmentDetail(env: String) -> Element {
    rsx! {
        div { class: "p-6",
            p { "Environment: {env}" }
        }
    }
}

/// Stub: RegionDetail page. Full implementation in Task 2.
#[component]
pub fn RegionDetail(env: String, region: String) -> Element {
    rsx! {
        div { class: "p-6",
            p { "Region: {env}/{region}" }
        }
    }
}
