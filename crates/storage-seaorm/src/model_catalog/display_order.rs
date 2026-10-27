//! Presentation order never changes the security rank stored on models.
use std::collections::HashMap;

use codexmanager_core::storage::{
    validate_managed_model_reorder_v2, ManagedModelReorderV2Params, ManagedModelV2,
};
use sea_orm::entity::prelude::*;
use sea_orm::{DatabaseConnection, Set, TransactionTrait};

use super::{CatalogModelRecord, ManagedModelsRepository};

#[derive(Clone, Debug, DeriveEntityModel)]
#[sea_orm(table_name = "model_display_order")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub model_id: String,
    pub position: i64,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::models::Entity",
        from = "Column::ModelId",
        to = "super::models::Column::Id",
        on_delete = "Cascade"
    )]
    CatalogModel,
}
impl ActiveModelBehavior for ActiveModel {}

pub(super) async fn sort_records(
    db: &impl ConnectionTrait,
    records: &mut [CatalogModelRecord],
) -> Result<(), DbErr> {
    let positions = Entity::find()
        .all(db)
        .await?
        .into_iter()
        .map(|entry| (entry.model_id, entry.position))
        .collect::<HashMap<_, _>>();
    records.sort_by_key(|record| {
        let position = positions.get(&record.id).copied();
        (
            position.is_none(),
            position.unwrap_or_default(),
            record.sort_order,
            record.slug.to_ascii_lowercase(),
        )
    });
    Ok(())
}

impl ManagedModelsRepository {
    pub async fn reorder(
        db: &DatabaseConnection,
        input: &ManagedModelReorderV2Params,
    ) -> Result<Vec<ManagedModelV2>, DbErr> {
        let tx = db.begin().await?;
        // Model aggregate writes and seed reconciliation take this same lock.
        crate::UsersRepository::lock(&tx, "model_groups").await?;
        let current = Self::list(&tx, true).await?;
        let slugs = current
            .iter()
            .map(|model| model.slug.clone())
            .collect::<Vec<_>>();
        let indexes = validate_managed_model_reorder_v2(&slugs, input).map_err(DbErr::Custom)?;
        Entity::delete_many().exec(&tx).await?;
        let mut reordered = Vec::with_capacity(current.len());
        for (position, index) in indexes.into_iter().enumerate() {
            let model = current[index].clone();
            Entity::insert(ActiveModel {
                model_id: Set(model.id.clone()),
                position: Set(position as i64),
            })
            .exec(&tx)
            .await?;
            reordered.push(model);
        }
        tx.commit().await?;
        Ok(reordered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SeaOrmStorage;
    use codexmanager_core::storage::{ManagedModelV2Upsert, ModelPriceV2, StorageBackendKind};

    fn slugs(models: &[ManagedModelV2]) -> Vec<String> {
        models.iter().map(|model| model.slug.clone()).collect()
    }

    async fn exercise_display_order(db: &DatabaseConnection) {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let fixtures = (0..3)
            .map(|index| ManagedModelV2Upsert {
                previous_slug: None,
                model: ManagedModelV2 {
                    slug: format!("display-order-fixture-{stamp}-{index}"),
                    display_name: format!("Display order fixture {index}"),
                    origin: "custom".into(),
                    enabled: index != 2,
                    supported_in_api: true,
                    visibility: if index == 1 { "hide" } else { "list" }.into(),
                    sort_order: index * 10,
                    instructions_mode: "passthrough".into(),
                    price: ModelPriceV2 {
                        price_status: "missing".into(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            })
            .collect::<Vec<_>>();
        let custom = ManagedModelsRepository::upsert_many(db, &fixtures)
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let original = ManagedModelsRepository::list(db, true).await.unwrap();
        assert!(original.len() > 2);
        let expected = slugs(&original);
        let mut reversed = expected.clone();
        reversed.reverse();
        let result = ManagedModelsRepository::reorder(
            db,
            &ManagedModelReorderV2Params {
                slugs: reversed
                    .iter()
                    .map(|slug| format!(" {} ", slug.to_ascii_uppercase()))
                    .collect(),
                expected_slugs: expected.clone(),
            },
        )
        .await
        .unwrap();
        assert_eq!(slugs(&result), reversed);
        assert_eq!(
            slugs(&ManagedModelsRepository::list(db, true).await.unwrap()),
            reversed
        );
        for model in &result {
            let before = original.iter().find(|entry| entry.id == model.id).unwrap();
            assert_eq!(
                serde_json::to_value(model).unwrap(),
                serde_json::to_value(before).unwrap()
            );
        }
        let stale = ManagedModelsRepository::reorder(
            db,
            &ManagedModelReorderV2Params {
                slugs: expected.clone(),
                expected_slugs: expected,
            },
        )
        .await
        .unwrap_err();
        assert!(stale.to_string().contains("model_order_conflict"));
        for invalid in [
            reversed[..reversed.len() - 1].to_vec(),
            {
                let mut value = reversed.clone();
                value[0] = value[1].clone();
                value
            },
            {
                let mut value = reversed.clone();
                value[0] = "unknown-model".into();
                value
            },
        ] {
            let error = ManagedModelsRepository::reorder(
                db,
                &ManagedModelReorderV2Params {
                    slugs: invalid,
                    expected_slugs: reversed.clone(),
                },
            )
            .await
            .unwrap_err();
            assert!(error.to_string().contains("model_order_invalid"));
            assert_eq!(
                slugs(&ManagedModelsRepository::list(db, true).await.unwrap()),
                reversed
            );
        }
        let custom_position = result
            .iter()
            .position(|model| model.id == custom.id)
            .unwrap();
        let mut renamed = result[custom_position].clone();
        let old_slug = renamed.slug.clone();
        renamed.slug = format!("{}-display-renamed", renamed.slug);
        ManagedModelsRepository::upsert_many(
            db,
            &[ManagedModelV2Upsert {
                previous_slug: Some(old_slug),
                model: renamed.clone(),
            }],
        )
        .await
        .unwrap();
        let mut added = renamed.clone();
        added.id.clear();
        added.slug = format!("display-order-added-fixture-{stamp}");
        added.origin = "custom".into();
        added.builtin_revision = None;
        added.sort_order = i64::MIN;
        added.routes.clear();
        ManagedModelsRepository::upsert_many(
            db,
            &[ManagedModelV2Upsert {
                previous_slug: None,
                model: added.clone(),
            }],
        )
        .await
        .unwrap();
        let after = ManagedModelsRepository::list(db, true).await.unwrap();
        assert_eq!(after[custom_position].id, custom.id);
        assert_eq!(after[custom_position].slug, renamed.slug);
        assert_eq!(after.last().unwrap().slug, added.slug);
        ManagedModelsRepository::delete(db, &renamed.slug)
            .await
            .unwrap();
        assert!(Entity::find_by_id(renamed.id)
            .one(db)
            .await
            .unwrap()
            .is_none());
        ManagedModelsRepository::delete(db, &added.slug)
            .await
            .unwrap();
        for fixture in &fixtures[1..] {
            ManagedModelsRepository::delete(db, &fixture.model.slug)
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn sqlite_display_order_is_atomic_and_preserves_metadata() {
        let storage = SeaOrmStorage::connect(StorageBackendKind::Sqlite, "sqlite::memory:")
            .await
            .unwrap();
        storage.migrate().await.unwrap();
        exercise_display_order(storage.connection()).await;
    }

    #[cfg(feature = "mysql")]
    #[tokio::test]
    #[ignore = "requires isolated MySQL database"]
    async fn mysql_display_order_is_atomic_and_preserves_metadata() {
        let url = std::env::var("CODEXMANAGER_TEST_MYSQL_URL").unwrap();
        let storage = SeaOrmStorage::connect(StorageBackendKind::Mysql, &url)
            .await
            .unwrap();
        storage.migrate().await.unwrap();
        exercise_display_order(storage.connection()).await;
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    #[ignore = "requires isolated PostgreSQL database"]
    async fn postgres_display_order_is_atomic_and_preserves_metadata() {
        let url = std::env::var("CODEXMANAGER_TEST_POSTGRES_URL").unwrap();
        let storage = SeaOrmStorage::connect(StorageBackendKind::Postgres, &url)
            .await
            .unwrap();
        storage.migrate().await.unwrap();
        exercise_display_order(storage.connection()).await;
    }
}
