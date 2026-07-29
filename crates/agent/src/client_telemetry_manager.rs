use super::authorization::AuthorizationContext;
use crate::health::Metrics;
use crate::kafka_error::{
    INVALID_REQUEST, NO_ERROR, TELEMETRY_TOO_LARGE, THROTTLING_QUOTA_EXCEEDED,
    UNKNOWN_SERVER_ERROR, UNKNOWN_SUBSCRIPTION_ID, UNSUPPORTED_COMPRESSION_TYPE,
};
use kafka_protocol::messages::{
    GetTelemetrySubscriptionsRequest, GetTelemetrySubscriptionsResponse, PushTelemetryRequest,
    PushTelemetryResponse,
};
use kafka_protocol::protocol::StrBytes;
use rutomq_control::{
    CLIENT_ID, CLIENT_INSTANCE_ID, CLIENT_METRICS_DEFAULT_INTERVAL_MS, CLIENT_SOFTWARE_NAME,
    CLIENT_SOFTWARE_VERSION, CLIENT_SOURCE_ADDRESS, CLIENT_SOURCE_PORT, MetadataStore,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use uuid::Uuid;

const ACCEPTED_COMPRESSION_TYPES: [i8; 4] = [4, 3, 1, 2];
const MAX_INSTANCES: usize = 16_384;
const MIN_INSTANCE_TTL: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub(super) struct ClientTelemetryManager {
    metadata: Arc<dyn MetadataStore>,
    metrics: Arc<Metrics>,
    max_bytes: usize,
    instances: Arc<Mutex<HashMap<Uuid, ClientInstance>>>,
}

#[derive(Clone)]
struct ClientInstance {
    subscription: EffectiveSubscription,
    last_get: Option<Instant>,
    last_push: Option<Instant>,
    last_seen: Instant,
    last_error: i16,
    terminating: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct EffectiveSubscription {
    id: i32,
    metrics: Vec<String>,
    push_interval_ms: i32,
}

impl ClientTelemetryManager {
    pub fn new(metadata: Arc<dyn MetadataStore>, metrics: Arc<Metrics>, max_bytes: usize) -> Self {
        Self {
            metadata,
            metrics,
            max_bytes,
            instances: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn get(
        &self,
        request: GetTelemetrySubscriptionsRequest,
        client_id: &str,
        context: &AuthorizationContext,
    ) -> GetTelemetrySubscriptionsResponse {
        let instance_id = if request.client_instance_id.is_nil() {
            Uuid::new_v4()
        } else {
            request.client_instance_id
        };
        let subscription = match self
            .effective_subscription(instance_id, client_id, context)
            .await
        {
            Ok(subscription) => subscription,
            Err(_) => return self.get_error(UNKNOWN_SERVER_ERROR),
        };
        let now = Instant::now();
        let mut instances = self.instances.lock().await;
        prune_instances(&mut instances, now);
        make_room(&mut instances, instance_id);
        let instance = instances
            .entry(instance_id)
            .or_insert_with(|| ClientInstance::new(subscription.clone(), now));
        let changed = instance.subscription != subscription;
        if changed {
            instance.subscription = subscription.clone();
            instance.terminating = false;
        }
        let retry_allowed = matches!(
            instance.last_error,
            UNKNOWN_SUBSCRIPTION_ID | UNSUPPORTED_COMPRESSION_TYPE
        );
        if !changed
            && !retry_allowed
            && too_soon(instance.last_get, subscription.push_interval_ms, now)
        {
            self.metrics.client_telemetry_errors.inc();
            instance.last_error = THROTTLING_QUOTA_EXCEEDED;
            instance.last_seen = now;
            self.update_instance_gauge(&instances);
            return self.get_error(THROTTLING_QUOTA_EXCEEDED);
        }
        instance.last_get = Some(now);
        instance.last_seen = now;
        instance.last_error = NO_ERROR;
        self.update_instance_gauge(&instances);

        GetTelemetrySubscriptionsResponse::default()
            .with_error_code(NO_ERROR)
            .with_client_instance_id(instance_id)
            .with_subscription_id(subscription.id)
            .with_accepted_compression_types(ACCEPTED_COMPRESSION_TYPES.to_vec())
            .with_push_interval_ms(subscription.push_interval_ms)
            .with_telemetry_max_bytes(self.max_bytes as i32)
            .with_delta_temporality(true)
            .with_requested_metrics(
                subscription
                    .metrics
                    .into_iter()
                    .map(StrBytes::from_string)
                    .collect(),
            )
    }

    pub async fn push(
        &self,
        request: PushTelemetryRequest,
        client_id: &str,
        context: &AuthorizationContext,
    ) -> PushTelemetryResponse {
        if request.client_instance_id.is_nil() {
            return self.push_error(INVALID_REQUEST);
        }
        let subscription = match self
            .effective_subscription(request.client_instance_id, client_id, context)
            .await
        {
            Ok(subscription) => subscription,
            Err(_) => return self.push_error(UNKNOWN_SERVER_ERROR),
        };
        let now = Instant::now();
        let mut instances = self.instances.lock().await;
        prune_instances(&mut instances, now);
        make_room(&mut instances, request.client_instance_id);
        let instance = instances
            .entry(request.client_instance_id)
            .or_insert_with(|| ClientInstance::new(subscription.clone(), now));
        if instance.subscription != subscription {
            instance.subscription = subscription.clone();
            instance.last_get = None;
            instance.last_push = None;
            instance.terminating = false;
        }

        let error_code = if instance.terminating {
            INVALID_REQUEST
        } else if !request.terminating
            && too_soon(instance.last_push, subscription.push_interval_ms, now)
        {
            THROTTLING_QUOTA_EXCEEDED
        } else {
            instance.last_push = Some(now);
            if request.subscription_id != subscription.id {
                UNKNOWN_SUBSCRIPTION_ID
            } else if !(0..=4).contains(&request.compression_type) {
                UNSUPPORTED_COMPRESSION_TYPE
            } else if request.metrics.len() > self.max_bytes {
                TELEMETRY_TOO_LARGE
            } else {
                NO_ERROR
            }
        };
        instance.last_seen = now;
        instance.last_error = error_code;
        if request.terminating {
            instance.terminating = true;
        }
        self.update_instance_gauge(&instances);
        drop(instances);

        if error_code == NO_ERROR {
            self.metrics.client_telemetry_pushes.inc();
            self.metrics
                .client_telemetry_bytes
                .inc_by(request.metrics.len() as u64);
        } else {
            self.metrics.client_telemetry_errors.inc();
        }
        PushTelemetryResponse::default().with_error_code(error_code)
    }

    async fn effective_subscription(
        &self,
        instance_id: Uuid,
        client_id: &str,
        context: &AuthorizationContext,
    ) -> Result<EffectiveSubscription, rutomq_control::ControlError> {
        let attributes = client_attributes(instance_id, client_id, context);
        let mut metrics = BTreeSet::new();
        let mut push_interval_ms = CLIENT_METRICS_DEFAULT_INTERVAL_MS;
        for subscription in self.metadata.client_metric_subscriptions().await? {
            if !subscription.matches(&attributes) {
                continue;
            }
            metrics.extend(subscription.metrics());
            push_interval_ms = push_interval_ms.min(subscription.push_interval_ms());
        }
        if metrics.contains("*") {
            metrics.clear();
            metrics.insert("*".to_owned());
        }
        let metrics = metrics.into_iter().collect::<Vec<_>>();
        Ok(EffectiveSubscription {
            id: subscription_id(instance_id, &metrics, push_interval_ms),
            metrics,
            push_interval_ms,
        })
    }

    fn get_error(&self, error_code: i16) -> GetTelemetrySubscriptionsResponse {
        self.metrics.client_telemetry_errors.inc();
        GetTelemetrySubscriptionsResponse::default().with_error_code(error_code)
    }

    fn push_error(&self, error_code: i16) -> PushTelemetryResponse {
        self.metrics.client_telemetry_errors.inc();
        PushTelemetryResponse::default().with_error_code(error_code)
    }

    fn update_instance_gauge(&self, instances: &HashMap<Uuid, ClientInstance>) {
        self.metrics
            .client_telemetry_instances
            .set(instances.len() as i64);
    }
}

impl ClientInstance {
    fn new(subscription: EffectiveSubscription, now: Instant) -> Self {
        Self {
            subscription,
            last_get: None,
            last_push: None,
            last_seen: now,
            last_error: NO_ERROR,
            terminating: false,
        }
    }
}

fn client_attributes(
    instance_id: Uuid,
    client_id: &str,
    context: &AuthorizationContext,
) -> BTreeMap<String, String> {
    let mut attributes = BTreeMap::from([
        (CLIENT_INSTANCE_ID.to_owned(), instance_id.to_string()),
        (CLIENT_ID.to_owned(), client_id.to_owned()),
        (CLIENT_SOURCE_ADDRESS.to_owned(), context.host.clone()),
    ]);
    if let Some(port) = context.source_port {
        attributes.insert(CLIENT_SOURCE_PORT.to_owned(), port.to_string());
    }
    if let Some(name) = &context.client_software_name {
        attributes.insert(CLIENT_SOFTWARE_NAME.to_owned(), name.clone());
    }
    if let Some(version) = &context.client_software_version {
        attributes.insert(CLIENT_SOFTWARE_VERSION.to_owned(), version.clone());
    }
    attributes
}

fn too_soon(last: Option<Instant>, interval_ms: i32, now: Instant) -> bool {
    last.is_some_and(|last| {
        now.saturating_duration_since(last) < Duration::from_millis(interval_ms as u64)
    })
}

fn prune_instances(instances: &mut HashMap<Uuid, ClientInstance>, now: Instant) {
    instances.retain(|_, instance| {
        let ttl = MIN_INSTANCE_TTL.max(Duration::from_millis(
            instance.subscription.push_interval_ms as u64 * 3,
        ));
        now.saturating_duration_since(instance.last_seen) <= ttl
    });
}

fn make_room(instances: &mut HashMap<Uuid, ClientInstance>, incoming: Uuid) {
    if instances.len() < MAX_INSTANCES || instances.contains_key(&incoming) {
        return;
    }
    if let Some(oldest) = instances
        .iter()
        .min_by_key(|(_, instance)| instance.last_seen)
        .map(|(id, _)| *id)
    {
        instances.remove(&oldest);
    }
}

fn subscription_id(instance_id: Uuid, metrics: &[String], push_interval_ms: i32) -> i32 {
    let mut bytes = Vec::new();
    for metric in metrics {
        bytes.extend_from_slice(metric.as_bytes());
        bytes.push(0);
    }
    bytes.extend_from_slice(&push_interval_ms.to_be_bytes());
    bytes.extend_from_slice(instance_id.as_bytes());
    crc32c(&bytes) as i32
}

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0x82f6_3b78 & (0u32.wrapping_sub(crc & 1)));
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_id_is_stable_and_changes_with_inputs() {
        let id = Uuid::parse_str("93b98f8d-cf14-4bc8-8894-8ce6d555df3c").unwrap();
        let first = subscription_id(id, &["*".to_owned()], 100);
        assert_eq!(first, subscription_id(id, &["*".to_owned()], 100));
        assert_ne!(first, subscription_id(id, &["producer.".to_owned()], 100));
        assert_ne!(first, subscription_id(id, &["*".to_owned()], 101));
    }
}
