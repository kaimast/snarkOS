// Copyright (c) 2019-2025 Provable Inc.
// This file is part of the snarkOS library.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::MAX_TIMESTAMP_DELTA_IN_SECS;

use time::OffsetDateTime;

/// Returns the current UTC epoch timestamp.
pub fn now() -> i64 {
    OffsetDateTime::now_utc().unix_timestamp()
}

/// Returns false if the timestamp is too far in the future.
pub fn check_timestamp_for_liveness(timestamp: i64) -> bool {
    // Ensure the timestamp is within range.
    timestamp <= (now() + MAX_TIMESTAMP_DELTA_IN_SECS)
}

#[cfg(test)]
mod prop_tests {
    use super::*;
    use crate::MAX_TIMESTAMP_DELTA_IN_SECS;

    use proptest::prelude::*;
    use test_strategy::proptest;

    fn any_valid_timestamp() -> BoxedStrategy<i64> {
        (Just(now()), 0..MAX_TIMESTAMP_DELTA_IN_SECS).prop_map(|(now, delta)| now + delta).boxed()
    }

    fn any_invalid_timestamp() -> BoxedStrategy<i64> {
        (Just(now()), MAX_TIMESTAMP_DELTA_IN_SECS..).prop_map(|(now, delta)| now + delta).boxed()
    }

    #[proptest]
    fn test_check_timestamp_for_liveness(#[strategy(any_valid_timestamp())] timestamp: i64) {
        assert!(check_timestamp_for_liveness(timestamp));
    }

    #[proptest]
    fn test_check_timestamp_for_liveness_too_far_in_future(#[strategy(any_invalid_timestamp())] timestamp: i64) {
        assert!(!check_timestamp_for_liveness(timestamp));
    }
}
