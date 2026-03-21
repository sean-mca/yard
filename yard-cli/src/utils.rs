// use yard_core::utils::get_current_context;

// pub fn interpolate_value(raw: &str) -> String {
//     let mut result = raw.to_string();

//     // Define the tokens we support
//     let tokens = ["${get_context_id()}"];

//     for token in tokens {
//         if result.contains(token) {
//             let replacement = match token {
//                 "${get_context_id()}" => get_current_context().unwrap_or_else(|_| "unknown".into()),
//                 _ => continue, // Should never happen with the array above
//             };

//             result = result.replace(token, &replacement);
//         }
//     }

//     result
// }
