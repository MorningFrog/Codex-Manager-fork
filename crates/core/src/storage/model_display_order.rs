//! Display order is independent of the model rank used by API key ceilings.
use std::collections::{HashMap, HashSet};

use rusqlite::{params, Result};
use serde::{Deserialize, Serialize};

use super::{ManagedModelV2, Storage};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedModelReorderV2Params {
    pub slugs: Vec<String>,
    pub expected_slugs: Vec<String>,
}

/// Return indexes into the canonical current catalog, after checking both the
/// caller's snapshot and its complete permutation. Shared by all backends.
pub fn validate_managed_model_reorder_v2(
    current_slugs: &[String],
    input: &ManagedModelReorderV2Params,
) -> std::result::Result<Vec<usize>, String> {
    let normalize = |slug: &str| slug.trim().to_ascii_lowercase();
    if current_slugs.len() != input.expected_slugs.len()
        || !current_slugs
            .iter()
            .zip(&input.expected_slugs)
            .all(|(current, expected)| normalize(current) == normalize(expected))
    {
        return Err("model_order_conflict: refresh the model catalog before sorting".into());
    }
    if input.slugs.len() != current_slugs.len() {
        return Err("model_order_invalid: provide the complete model catalog".into());
    }
    let indexes = current_slugs
        .iter()
        .enumerate()
        .map(|(index, slug)| (normalize(slug), index))
        .collect::<HashMap<_, _>>();
    let mut seen = HashSet::new();
    input
        .slugs
        .iter()
        .map(|slug| {
            let key = normalize(slug);
            if key.is_empty() || !seen.insert(key.clone()) {
                return Err("model_order_invalid: empty or duplicate model slug".into());
            }
            indexes
                .get(&key)
                .copied()
                .ok_or_else(|| format!("model_order_invalid: unknown model slug {slug}"))
        })
        .collect()
}

impl Storage {
    pub fn reorder_managed_models_v2(
        &self,
        input: &ManagedModelReorderV2Params,
    ) -> Result<Vec<ManagedModelV2>> {
        // The SQLite adapter starts BEGIN IMMEDIATE, serializing this snapshot
        // check with concurrent model creates, renames, deletes and reorders.
        let tx = self.conn.unchecked_transaction()?;
        let current = self.list_managed_models_v2(true)?;
        let slugs = current
            .iter()
            .map(|model| model.slug.clone())
            .collect::<Vec<_>>();
        let indexes = validate_managed_model_reorder_v2(&slugs, input)
            .map_err(rusqlite::Error::InvalidParameterName)?;
        tx.execute("DELETE FROM model_display_order", [])?;
        let mut reordered = Vec::with_capacity(current.len());
        for (position, index) in indexes.into_iter().enumerate() {
            let model = current[index].clone();
            tx.execute(
                "INSERT INTO model_display_order(model_id,position) VALUES(?1,?2)",
                params![model.id, position as i64],
            )?;
            reordered.push(model);
        }
        tx.commit()?;
        Ok(reordered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::ManagedModelV2Upsert;

    fn slugs(models: &[ManagedModelV2]) -> Vec<String> {
        models.iter().map(|model| model.slug.clone()).collect()
    }

    #[test]
    fn display_order_preserves_metadata_and_persists_across_reopen() {
        let path = std::env::temp_dir().join(format!(
            "codexmanager-model-order-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let storage = Storage::open(&path).unwrap();
        storage.init().unwrap();
        let mut custom = storage.list_managed_models_v2(true).unwrap()[0].clone();
        custom.id.clear();
        custom.slug = "order-custom-fixture".into();
        custom.origin = "custom".into();
        custom.builtin_revision = None;
        custom.routes.clear();
        custom.permission_group_ids.clear();
        let custom = storage
            .upsert_managed_model_v2(&ManagedModelV2Upsert {
                previous_slug: None,
                model: custom,
            })
            .unwrap();
        let original = storage.list_managed_models_v2(true).unwrap();
        assert!(original.len() > 2);
        let expected = slugs(&original);
        let mut reversed = expected.clone();
        reversed.reverse();
        let result = storage
            .reorder_managed_models_v2(&ManagedModelReorderV2Params {
                slugs: reversed
                    .iter()
                    .map(|slug| format!(" {} ", slug.to_ascii_uppercase()))
                    .collect(),
                expected_slugs: expected,
            })
            .unwrap();
        assert_eq!(slugs(&result), reversed);
        for model in &result {
            let before = original.iter().find(|entry| entry.id == model.id).unwrap();
            assert_eq!(
                serde_json::to_value(model).unwrap(),
                serde_json::to_value(before).unwrap()
            );
        }
        drop(storage);
        let storage = Storage::open(&path).unwrap();
        storage.init().unwrap();
        assert_eq!(
            slugs(&storage.list_managed_models_v2(true).unwrap()),
            reversed
        );
        let other_window = Storage::open(&path).unwrap();
        let stale_error = other_window
            .reorder_managed_models_v2(&ManagedModelReorderV2Params {
                slugs: slugs(&original),
                expected_slugs: slugs(&original),
            })
            .unwrap_err();
        assert!(stale_error.to_string().contains("model_order_conflict"));
        drop(other_window);
        let listed_api = slugs(&storage.list_api_models_v2().unwrap());
        assert_eq!(
            listed_api,
            result
                .iter()
                .filter(|m| m.enabled && m.supported_in_api && m.visibility == "list")
                .map(|m| m.slug.clone())
                .collect::<Vec<_>>()
        );

        // Renames retain the stable ID's slot; newly created models append even
        // when their legacy security rank would otherwise put them first.
        let custom_position = result
            .iter()
            .position(|model| model.id == custom.id)
            .unwrap();
        let mut renamed = result[custom_position].clone();
        let previous_slug = renamed.slug.clone();
        renamed.slug = "order-renamed-fixture".into();
        storage
            .upsert_managed_model_v2(&ManagedModelV2Upsert {
                previous_slug: Some(previous_slug),
                model: renamed.clone(),
            })
            .unwrap();
        let mut added = renamed.clone();
        added.id.clear();
        added.slug = "order-added-fixture".into();
        added.origin = "custom".into();
        added.builtin_revision = None;
        added.sort_order = i64::MIN;
        added.routes.clear();
        storage
            .upsert_managed_model_v2(&ManagedModelV2Upsert {
                previous_slug: None,
                model: added,
            })
            .unwrap();
        let after = storage.list_managed_models_v2(true).unwrap();
        assert_eq!(after[custom_position].id, custom.id);
        assert_eq!(after[custom_position].slug, renamed.slug);
        assert_eq!(after.last().unwrap().slug, "order-added-fixture");
        storage.delete_managed_model_v2(&renamed.slug).unwrap();
        let count: i64 = storage
            .conn
            .query_row(
                "SELECT COUNT(*) FROM model_display_order WHERE model_id=?1",
                [&renamed.id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
        drop(storage);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn invalid_or_stale_display_order_cannot_partially_change_the_catalog() {
        let storage = Storage::open_in_memory().unwrap();
        storage.init().unwrap();
        let expected = slugs(&storage.list_managed_models_v2(true).unwrap());
        let mut invalid_orders = vec![expected[..expected.len() - 1].to_vec()];
        let mut duplicate = expected.clone();
        duplicate[0] = duplicate[1].clone();
        invalid_orders.push(duplicate);
        let mut unknown = expected.clone();
        unknown[0] = "unknown-model".into();
        invalid_orders.push(unknown);
        let mut blank = expected.clone();
        blank[0] = " ".into();
        invalid_orders.push(blank);
        for invalid in invalid_orders {
            let error = storage
                .reorder_managed_models_v2(&ManagedModelReorderV2Params {
                    slugs: invalid,
                    expected_slugs: expected.clone(),
                })
                .unwrap_err();
            assert!(error.to_string().contains("model_order_invalid"));
            assert_eq!(
                slugs(&storage.list_managed_models_v2(true).unwrap()),
                expected
            );
        }
        let mut reversed = expected.clone();
        reversed.reverse();
        storage
            .reorder_managed_models_v2(&ManagedModelReorderV2Params {
                slugs: reversed.clone(),
                expected_slugs: expected.clone(),
            })
            .unwrap();
        let error = storage
            .reorder_managed_models_v2(&ManagedModelReorderV2Params {
                slugs: expected.clone(),
                expected_slugs: expected,
            })
            .unwrap_err();
        assert!(error.to_string().contains("model_order_conflict"));
        assert_eq!(
            slugs(&storage.list_managed_models_v2(true).unwrap()),
            reversed
        );
    }
}
