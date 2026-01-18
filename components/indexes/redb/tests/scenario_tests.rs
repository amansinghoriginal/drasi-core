// Copyright 2025 The Drasi Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{env, sync::Arc};

use async_trait::async_trait;

use drasi_core::{
    interface::{AccumulatorIndex, ElementIndex, FutureQueue},
    query::QueryBuilder,
};
use shared_tests::QueryTestConfig;
use uuid::Uuid;

use drasi_index_redb::{
    element_index::{RedbElementIndex, RedbIndexOptions},
    future_queue::RedbFutureQueue,
    result_index::RedbResultIndex,
};

struct RedbQueryConfig {
    pub path: String,
}

impl RedbQueryConfig {
    pub fn new() -> Self {
        let base_path = match env::var("REDB_PATH") {
            Ok(path) => path,
            Err(_) => "test-data".to_string(),
        };
        // Create unique directory per test instance
        let path = format!("{}/{}", base_path, Uuid::new_v4());

        // Ensure directory exists
        if let Err(e) = std::fs::create_dir_all(&path) {
            log::warn!("Failed to create test directory {}: {}", path, e);
        }

        RedbQueryConfig { path }
    }

    #[allow(clippy::unwrap_used)]
    pub fn build_future_queue(&self, query_id: &str) -> RedbFutureQueue {
        RedbFutureQueue::new(query_id, &self.path, 64 * 1024 * 1024).unwrap()
    }
}

impl Drop for RedbQueryConfig {
    fn drop(&mut self) {
        // Clean up the test-specific directory
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

#[allow(clippy::unwrap_used)]
#[async_trait]
impl QueryTestConfig for RedbQueryConfig {
    async fn config_query(&self, builder: QueryBuilder) -> QueryBuilder {
        log::info!("using Redb indexes");
        let query_id = format!("test-{}", Uuid::new_v4());

        let options = RedbIndexOptions {
            archive_enabled: true,
            cache_size_bytes: 64 * 1024 * 1024, // 64MB for tests
        };

        let element_index =
            RedbElementIndex::new(&query_id, &self.path, options).unwrap();
        let ari = RedbResultIndex::new(&query_id, &self.path, 64 * 1024 * 1024).unwrap();
        let fqi = RedbFutureQueue::new(&query_id, &self.path, 64 * 1024 * 1024).unwrap();

        element_index.clear().await.unwrap();
        ari.clear().await.unwrap();
        fqi.clear().await.unwrap();

        let element_index = Arc::new(element_index);

        builder
            .with_element_index(element_index.clone())
            .with_archive_index(element_index.clone())
            .with_result_index(Arc::new(ari))
            .with_future_queue(Arc::new(fqi))
    }
}

mod building_comfort {
    use super::RedbQueryConfig;
    use serial_test::serial;
    use shared_tests::use_cases::*;

    #[tokio::test]
    #[serial]
    async fn building_comfort_use_case() {
        let test_config = RedbQueryConfig::new();
        building_comfort::building_comfort_use_case(&test_config).await;
    }
}

mod curbside_pickup {
    use super::RedbQueryConfig;
    use serial_test::serial;
    use shared_tests::use_cases::*;

    #[tokio::test]
    #[serial]
    async fn order_ready_then_vehicle_arrives() {
        let test_config = RedbQueryConfig::new();
        curbside_pickup::order_ready_then_vehicle_arrives(&test_config).await;
    }

    #[tokio::test]
    #[serial]
    async fn vehicle_arrives_then_order_ready() {
        let test_config = RedbQueryConfig::new();
        curbside_pickup::vehicle_arrives_then_order_ready(&test_config).await;
    }

    #[tokio::test]
    #[serial]
    async fn vehicle_arrives_then_order_ready_duplicate() {
        let test_config = RedbQueryConfig::new();
        curbside_pickup::vehicle_arrives_then_order_ready_duplicate(&test_config).await;
    }
}

mod incident_alert {
    use super::RedbQueryConfig;
    use serial_test::serial;
    use shared_tests::use_cases::*;

    #[tokio::test]
    #[serial]
    pub async fn incident_alert() {
        let test_config = RedbQueryConfig::new();
        incident_alert::incident_alert(&test_config).await;
    }
}

mod min_value {
    use super::RedbQueryConfig;
    use serial_test::serial;
    use shared_tests::use_cases::*;

    #[tokio::test]
    #[serial]
    pub async fn min_value() {
        let test_config = RedbQueryConfig::new();
        min_value::min_value(&test_config).await;
    }
}

mod overdue_invoice {
    use super::RedbQueryConfig;
    use serial_test::serial;
    use shared_tests::use_cases::*;

    #[tokio::test]
    #[serial]
    pub async fn overdue_invoice() {
        let test_config = RedbQueryConfig::new();
        overdue_invoice::overdue_invoice(&test_config).await;
    }

    #[tokio::test]
    #[serial]
    pub async fn overdue_count_persistent() {
        let test_config = RedbQueryConfig::new();
        overdue_invoice::overdue_count_persistent(&test_config).await;
    }
}

mod sensor_heartbeat {
    use super::RedbQueryConfig;
    use serial_test::serial;
    use shared_tests::use_cases::*;

    #[tokio::test]
    #[serial]
    pub async fn not_reported() {
        let test_config = RedbQueryConfig::new();
        sensor_heartbeat::not_reported(&test_config).await;
    }

    #[tokio::test]
    #[serial]
    pub async fn percent_not_reported() {
        let test_config = RedbQueryConfig::new();
        sensor_heartbeat::percent_not_reported(&test_config).await;
    }
}

mod temporal_retrieval {
    use super::RedbQueryConfig;
    use serial_test::serial;
    use shared_tests::temporal_retrieval::get_version_by_timestamp;
    use shared_tests::temporal_retrieval::get_versions_by_timerange;

    #[tokio::test]
    #[serial]
    async fn get_version_by_timestamp() {
        let test_config = RedbQueryConfig::new();
        get_version_by_timestamp::get_version_by_timestamp(&test_config).await;
    }

    #[tokio::test]
    #[serial]
    async fn get_versions_by_range() {
        let test_config = RedbQueryConfig::new();
        get_versions_by_timerange::get_versions_by_timerange(&test_config).await;
    }

    #[tokio::test]
    #[serial]
    async fn get_versions_by_range_with_initial_value() {
        let test_config = RedbQueryConfig::new();
        get_versions_by_timerange::get_versions_by_timerange_with_initial_value_flag(&test_config)
            .await;
    }
}

mod greater_than_a_threshold {
    use super::RedbQueryConfig;
    use serial_test::serial;
    use shared_tests::use_cases::*;

    #[tokio::test]
    #[serial]
    pub async fn greater_than_a_threshold() {
        let test_config = RedbQueryConfig::new();
        greater_than_a_threshold::greater_than_a_threshold(&test_config).await;
    }

    #[tokio::test]
    #[serial]
    pub async fn greater_than_a_threshold_by_customer() {
        let test_config = RedbQueryConfig::new();
        greater_than_a_threshold::greater_than_a_threshold_by_customer(&test_config).await;
    }
}

mod linear_regression {
    use super::RedbQueryConfig;
    use serial_test::serial;
    use shared_tests::use_cases::*;

    #[tokio::test]
    #[serial]
    async fn linear_gradient() {
        let test_config = RedbQueryConfig::new();
        linear_regression::linear_gradient(&test_config).await;
    }
}

mod index {
    use super::RedbQueryConfig;
    use drasi_core::interface::FutureQueue;
    use serial_test::serial;
    use uuid::Uuid;

    #[tokio::test]
    #[serial]
    async fn future_queue_push_always() {
        let test_config = RedbQueryConfig::new();
        let fqi = test_config.build_future_queue(format!("test-{}", Uuid::new_v4()).as_str());
        fqi.clear().await.unwrap();
        shared_tests::index::future_queue::push_always(&fqi).await;
    }

    #[tokio::test]
    #[serial]
    async fn future_queue_push_not_exists() {
        let test_config = RedbQueryConfig::new();
        let fqi = test_config.build_future_queue(format!("test-{}", Uuid::new_v4()).as_str());
        fqi.clear().await.unwrap();
        shared_tests::index::future_queue::push_not_exists(&fqi).await;
    }

    #[tokio::test]
    #[serial]
    async fn future_queue_push_overwrite() {
        let test_config = RedbQueryConfig::new();
        let fqi = test_config.build_future_queue(format!("test-{}", Uuid::new_v4()).as_str());
        fqi.clear().await.unwrap();
        shared_tests::index::future_queue::push_overwrite(&fqi).await;
    }
}

mod before {
    use super::RedbQueryConfig;
    use serial_test::serial;
    use shared_tests::use_cases::*;

    #[tokio::test]
    #[serial]
    async fn before_value() {
        let test_config = RedbQueryConfig::new();
        before::before_value(&test_config).await;
    }

    #[tokio::test]
    #[serial]
    async fn before_sum() {
        let test_config = RedbQueryConfig::new();
        before::before_sum(&test_config).await;
    }
}

mod prev_unique {
    use super::RedbQueryConfig;
    use serial_test::serial;
    use shared_tests::use_cases::*;

    #[tokio::test]
    #[serial]
    async fn prev_unique() {
        let test_config = RedbQueryConfig::new();
        prev_distinct::prev_unique(&test_config).await;
    }

    #[tokio::test]
    #[serial]
    async fn prev_unique_with_match() {
        let test_config = RedbQueryConfig::new();
        prev_distinct::prev_unique_with_match(&test_config).await;
    }
}

mod collect_aggregation {
    use super::RedbQueryConfig;
    use shared_tests::use_cases::*;

    #[tokio::test]
    async fn collect_based_aggregation_test() {
        let test_config = RedbQueryConfig::new();
        collect_aggregation::collect_based_aggregation_test(&test_config).await;
    }

    #[tokio::test]
    async fn simple_aggregation_test() {
        let test_config = RedbQueryConfig::new();
        collect_aggregation::simple_aggregation_test(&test_config).await;
    }

    #[tokio::test]
    async fn collect_with_filter() {
        let test_config = RedbQueryConfig::new();
        collect_aggregation::collect_with_filter_test(&test_config).await;
    }

    #[tokio::test]
    async fn collect_objects() {
        let test_config = RedbQueryConfig::new();
        collect_aggregation::collect_objects_test(&test_config).await;
    }

    #[tokio::test]
    async fn collect_mixed_types() {
        let test_config = RedbQueryConfig::new();
        collect_aggregation::collect_mixed_types_test(&test_config).await;
    }

    #[tokio::test]
    async fn multiple_collects() {
        let test_config = RedbQueryConfig::new();
        collect_aggregation::multiple_collects_test(&test_config).await;
    }
}

mod source_update_upsert {
    use super::RedbQueryConfig;
    use shared_tests::use_cases::*;

    #[tokio::test]
    async fn test_upsert_semantics() {
        let test_config = RedbQueryConfig::new();
        source_update_upsert::test_upsert_semantics(&test_config).await;
    }

    #[tokio::test]
    async fn test_partial_updates() {
        let test_config = RedbQueryConfig::new();
        source_update_upsert::test_partial_updates(&test_config).await;
    }

    #[tokio::test]
    async fn test_stateless_processing() {
        let test_config = RedbQueryConfig::new();
        source_update_upsert::test_stateless_processing(&test_config).await;
    }

    #[tokio::test]
    async fn test_query_matching() {
        let test_config = RedbQueryConfig::new();
        source_update_upsert::test_query_matching(&test_config).await;
    }

    #[tokio::test]
    async fn test_multiple_entities() {
        let test_config = RedbQueryConfig::new();
        source_update_upsert::test_multiple_entities(&test_config).await;
    }

    #[tokio::test]
    async fn test_relationship_upsert() {
        let test_config = RedbQueryConfig::new();
        source_update_upsert::test_relationship_upsert(&test_config).await;
    }

    #[tokio::test]
    async fn test_aggregation_with_upserts() {
        let test_config = RedbQueryConfig::new();
        source_update_upsert::test_aggregation_with_upserts(&test_config).await;
    }
}
