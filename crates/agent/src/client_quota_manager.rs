use anyhow::Result;
use rutomq_control::{ClientQuota, ClientQuotaEntity, MetadataStore};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;
use tokio::time::{Duration, Instant};

const CACHE_TTL: Duration = Duration::from_secs(1);
const BURST: Duration = Duration::from_secs(1);
const MAX_THROTTLE: Duration = Duration::from_millis(i32::MAX as u64);

#[derive(Clone)]
pub(crate) struct ClientQuotaManager {
    metadata: Arc<dyn MetadataStore>,
    cache: Arc<RwLock<QuotaCache>>,
    buckets: Arc<Mutex<HashMap<BucketKey, Bucket>>>,
}

#[derive(Default)]
struct QuotaCache {
    loaded_at: Option<Instant>,
    quotas: BTreeMap<ClientQuotaEntity, BTreeMap<String, f64>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct BucketKey {
    entity: ClientQuotaEntity,
    quota_key: String,
}

struct Bucket {
    limit: f64,
    next_free: Instant,
    retry_credit: Duration,
    retry_at: Instant,
}

#[derive(Clone, Debug)]
pub(crate) struct QuotaReservation {
    key: Option<BucketKey>,
    service_time: Duration,
    pub delay: Duration,
}

impl QuotaReservation {
    pub(crate) fn unlimited() -> Self {
        Self {
            key: None,
            service_time: Duration::ZERO,
            delay: Duration::ZERO,
        }
    }

    pub fn throttle_time_ms(&self) -> i32 {
        i32::try_from(self.delay.as_millis()).unwrap_or(i32::MAX)
    }
}

impl ClientQuotaManager {
    pub fn new(metadata: Arc<dyn MetadataStore>) -> Self {
        Self {
            metadata,
            cache: Arc::new(RwLock::new(QuotaCache::default())),
            buckets: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn invalidate(&self) {
        self.cache.write().await.loaded_at = None;
    }

    pub async fn reserve_user(
        &self,
        quota_key: &str,
        principal: &str,
        client_id: &str,
        amount: f64,
    ) -> Result<QuotaReservation> {
        let quotas = self.quotas().await?;
        let user = principal
            .strip_prefix("User:")
            .unwrap_or(principal)
            .to_owned();
        let candidates = user_candidates(&user, client_id);
        let matched = candidates.into_iter().find_map(|entity| {
            quotas
                .get(&entity)
                .and_then(|values| values.get(quota_key))
                .map(|limit| (entity, *limit))
        });
        Ok(
            matched.map_or_else(QuotaReservation::unlimited, |(entity, limit)| {
                self.reserve(entity, quota_key, limit, amount)
            }),
        )
    }

    pub async fn reserve_ip(
        &self,
        quota_key: &str,
        ip: &str,
        amount: f64,
    ) -> Result<QuotaReservation> {
        let quotas = self.quotas().await?;
        let named = ClientQuotaEntity {
            ip: Some(Some(ip.to_owned())),
            ..ClientQuotaEntity::default()
        };
        let default = ClientQuotaEntity {
            ip: Some(None),
            ..ClientQuotaEntity::default()
        };
        let matched = [named, default].into_iter().find_map(|entity| {
            quotas
                .get(&entity)
                .and_then(|values| values.get(quota_key))
                .map(|limit| (entity, *limit))
        });
        Ok(
            matched.map_or_else(QuotaReservation::unlimited, |(entity, limit)| {
                self.reserve(entity, quota_key, limit, amount)
            }),
        )
    }

    /// Kafka does not charge a throttled Fetch response because its records are
    /// removed. The cooldown still needs to grant the retry enough credit when
    /// one record batch is larger than one second of quota.
    pub fn cancel_for_fetch_retry(&self, reservation: &QuotaReservation, retry_delay: Duration) {
        let Some(key) = &reservation.key else {
            return;
        };
        let now = Instant::now();
        let mut buckets = self.buckets.lock().expect("client quota bucket lock");
        if let Some(bucket) = buckets.get_mut(key) {
            bucket.next_free = bucket
                .next_free
                .checked_sub(reservation.service_time)
                .unwrap_or(now)
                .max(now);
            bucket.retry_credit = bucket
                .retry_credit
                .saturating_add(reservation.service_time)
                .min(MAX_THROTTLE);
            bucket.retry_at = now + retry_delay;
        }
    }

    async fn quotas(&self) -> Result<BTreeMap<ClientQuotaEntity, BTreeMap<String, f64>>> {
        {
            let cache = self.cache.read().await;
            if cache
                .loaded_at
                .is_some_and(|loaded_at| loaded_at.elapsed() < CACHE_TTL)
            {
                return Ok(cache.quotas.clone());
            }
        }
        let loaded = self.metadata.client_quotas().await?;
        let quotas = loaded
            .into_iter()
            .map(|ClientQuota { entity, values }| (entity, values))
            .collect::<BTreeMap<_, _>>();
        let mut cache = self.cache.write().await;
        cache.loaded_at = Some(Instant::now());
        cache.quotas = quotas.clone();
        Ok(quotas)
    }

    fn reserve(
        &self,
        entity: ClientQuotaEntity,
        quota_key: &str,
        limit: f64,
        amount: f64,
    ) -> QuotaReservation {
        if amount <= 0.0 || !amount.is_finite() || limit <= 0.0 || !limit.is_finite() {
            return QuotaReservation::unlimited();
        }
        let service_seconds = amount / limit;
        let service_time = if service_seconds.is_finite() {
            Duration::from_secs_f64(service_seconds.min(MAX_THROTTLE.as_secs_f64()))
        } else {
            MAX_THROTTLE
        };
        let now = Instant::now();
        let key = BucketKey {
            entity,
            quota_key: quota_key.to_owned(),
        };
        let mut buckets = self.buckets.lock().expect("client quota bucket lock");
        if buckets.len() > 10_000 {
            buckets.retain(|_, bucket| bucket.next_free > now);
        }
        let bucket = buckets.entry(key.clone()).or_insert(Bucket {
            limit,
            next_free: now,
            retry_credit: Duration::ZERO,
            retry_at: now,
        });
        if bucket.limit != limit {
            bucket.limit = limit;
            bucket.next_free = now;
            bucket.retry_credit = Duration::ZERO;
            bucket.retry_at = now;
        }
        if now >= bucket.retry_at && bucket.retry_credit >= service_time {
            bucket.retry_credit = bucket.retry_credit.saturating_sub(service_time);
            return QuotaReservation {
                key: Some(key),
                service_time,
                delay: Duration::ZERO,
            };
        }
        bucket.next_free =
            (bucket.next_free.max(now) + service_time).min(now + BURST + MAX_THROTTLE);
        let permitted_at = bucket.next_free.checked_sub(BURST).unwrap_or(now);
        QuotaReservation {
            key: Some(key),
            service_time,
            delay: permitted_at.saturating_duration_since(now),
        }
    }
}

fn user_candidates(user: &str, client_id: &str) -> Vec<ClientQuotaEntity> {
    let named_user = Some(Some(user.to_owned()));
    let named_client = (!client_id.is_empty()).then(|| Some(client_id.to_owned()));
    let mut entities = Vec::with_capacity(8);
    if let Some(named_client) = &named_client {
        entities.push(entity(named_user.clone(), Some(named_client.clone())));
    }
    entities.push(entity(named_user.clone(), Some(None)));
    entities.push(entity(named_user, None));
    if let Some(named_client) = &named_client {
        entities.push(entity(Some(None), Some(named_client.clone())));
    }
    entities.push(entity(Some(None), Some(None)));
    entities.push(entity(Some(None), None));
    if let Some(named_client) = named_client {
        entities.push(entity(None, Some(named_client)));
    }
    entities.push(entity(None, Some(None)));
    entities
}

fn entity(user: Option<Option<String>>, client_id: Option<Option<String>>) -> ClientQuotaEntity {
    ClientQuotaEntity {
        user,
        client_id,
        ip: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rutomq_control::{
        CLIENT_ID_ENTITY, ClientQuotaAlteration, MemoryMetadataStore, MetadataStore,
        PRODUCER_BYTE_RATE, USER_ENTITY,
    };

    #[tokio::test]
    async fn quota_precedence_and_bucket_sharing_match_kafka_order() {
        let store = Arc::new(MemoryMetadataStore::new());
        store
            .alter_client_quotas(vec![
                alteration(entity(Some(None), None), 1_000.0),
                alteration(entity(Some(Some("alice".to_owned())), Some(None)), 100.0),
            ])
            .await
            .unwrap();
        let manager = ClientQuotaManager::new(store);
        let first = manager
            .reserve_user(PRODUCER_BYTE_RATE, "User:alice", "client-a", 100.0)
            .await
            .unwrap();
        assert_eq!(first.delay, Duration::ZERO);
        let second = manager
            .reserve_user(PRODUCER_BYTE_RATE, "User:alice", "client-b", 100.0)
            .await
            .unwrap();
        assert!(second.delay >= Duration::from_millis(900));
    }

    #[tokio::test]
    async fn fetch_cooldown_allows_a_batch_larger_than_one_second_of_quota() {
        let store = Arc::new(MemoryMetadataStore::new());
        store
            .alter_client_quotas(vec![alteration(
                entity(Some(Some("alice".to_owned())), None),
                100.0,
            )])
            .await
            .unwrap();
        let manager = ClientQuotaManager::new(store);
        let reservation = manager
            .reserve_user(PRODUCER_BYTE_RATE, "User:alice", "", 105.0)
            .await
            .unwrap();
        assert!(reservation.delay >= Duration::from_millis(49));
        manager.cancel_for_fetch_retry(&reservation, reservation.delay);
        tokio::time::sleep(reservation.delay).await;
        let retry = manager
            .reserve_user(PRODUCER_BYTE_RATE, "User:alice", "", 105.0)
            .await
            .unwrap();
        assert_eq!(retry.delay, Duration::ZERO);
    }

    fn alteration(entity: ClientQuotaEntity, value: f64) -> ClientQuotaAlteration {
        ClientQuotaAlteration {
            entity,
            ops: BTreeMap::from([(PRODUCER_BYTE_RATE.to_owned(), Some(value))]),
        }
    }

    #[test]
    fn user_candidate_order_has_no_empty_named_client() {
        let candidates = user_candidates("alice", "");
        assert_eq!(candidates.len(), 5);
        assert_eq!(candidates[0].user, Some(Some("alice".to_owned())));
        assert_eq!(candidates[0].client_id, Some(None));
        assert_eq!(
            candidates[4],
            ClientQuotaEntity {
                user: None,
                client_id: Some(None),
                ip: None,
            }
        );
        assert_eq!(USER_ENTITY, "user");
        assert_eq!(CLIENT_ID_ENTITY, "client-id");
    }
}
