use crate::kafka_error::{
    FETCH_SESSION_ID_NOT_FOUND, FETCH_SESSION_TOPIC_ID_ERROR, INVALID_FETCH_SESSION_EPOCH, NO_ERROR,
};
use indexmap::IndexMap;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic, ForgottenTopic};
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::{FetchRequest, FetchResponse};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use uuid::Uuid;

const DEFAULT_MAX_SESSIONS: usize = 1_024;
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);

#[derive(Clone)]
pub(super) struct FetchSessionManager {
    state: Arc<Mutex<FetchSessionState>>,
    max_sessions: usize,
    idle_timeout: Duration,
}

pub(super) struct PreparedFetch {
    pub(super) request: FetchRequest,
    pub(super) token: FetchSessionToken,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FetchSessionToken {
    Sessionless,
    Full {
        session_id: i32,
    },
    Incremental {
        session_id: i32,
        expected_epoch: i32,
    },
}

struct FetchSessionState {
    sessions: HashMap<i32, FetchSession>,
    next_id: i32,
}

struct FetchSession {
    uses_topic_ids: bool,
    epoch: i32,
    last_used: Instant,
    topics: IndexMap<TopicKey, CachedTopic>,
}

struct CachedTopic {
    template: FetchTopic,
    partitions: IndexMap<i32, CachedPartition>,
}

struct CachedPartition {
    request: FetchPartition,
    high_watermark: i64,
    log_start_offset: i64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum TopicKey {
    Id(Uuid),
    Name(String),
}

impl Default for FetchSessionManager {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(FetchSessionState {
                sessions: HashMap::new(),
                next_id: 1,
            })),
            max_sessions: DEFAULT_MAX_SESSIONS,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
        }
    }
}

impl FetchSessionManager {
    pub(super) fn prepare(
        &self,
        mut request: FetchRequest,
        version: i16,
    ) -> Result<PreparedFetch, i16> {
        if version < 7 {
            return Ok(PreparedFetch {
                request,
                token: FetchSessionToken::Sessionless,
            });
        }

        let now = Instant::now();
        let uses_topic_ids = version >= 13;
        let mut state = self.lock_state();
        state.expire(now, self.idle_timeout);

        if request.session_epoch == 0 || request.session_epoch == -1 {
            if request.session_id != 0 {
                state.sessions.remove(&request.session_id);
            }
            request.forgotten_topics_data.clear();
            if request.session_epoch == -1
                || self.max_sessions == 0
                || request
                    .topics
                    .iter()
                    .all(|topic| topic.partitions.is_empty())
            {
                return Ok(PreparedFetch {
                    request,
                    token: FetchSessionToken::Sessionless,
                });
            }

            state.make_room(self.max_sessions);
            let session_id = state.allocate_id();
            state.sessions.insert(
                session_id,
                FetchSession::new(&request.topics, uses_topic_ids, now),
            );
            return Ok(PreparedFetch {
                request,
                token: FetchSessionToken::Full { session_id },
            });
        }

        let session_id = request.session_id;
        let Some(session) = state.sessions.get_mut(&session_id) else {
            return Err(FETCH_SESSION_ID_NOT_FOUND);
        };
        if session.epoch != request.session_epoch {
            return Err(INVALID_FETCH_SESSION_EPOCH);
        }
        if session.uses_topic_ids != uses_topic_ids {
            return Err(FETCH_SESSION_TOPIC_ID_ERROR);
        }

        session.apply_updates(&request.topics);
        session.forget(&request.forgotten_topics_data);
        if session.topics.is_empty() {
            state.sessions.remove(&session_id);
            request.topics.clear();
            request.forgotten_topics_data.clear();
            return Ok(PreparedFetch {
                request,
                token: FetchSessionToken::Sessionless,
            });
        }

        session.epoch = next_epoch(session.epoch);
        session.last_used = now;
        request.topics = session.request_topics();
        request.forgotten_topics_data.clear();
        Ok(PreparedFetch {
            request,
            token: FetchSessionToken::Incremental {
                session_id,
                expected_epoch: session.epoch,
            },
        })
    }

    pub(super) fn shape_response(&self, token: FetchSessionToken, response: &mut FetchResponse) {
        match token {
            FetchSessionToken::Sessionless => response.session_id = 0,
            FetchSessionToken::Full { session_id } => {
                let state = self.lock_state();
                if let Some(session) = state.sessions.get(&session_id) {
                    session.retain_expected_partitions(response, false);
                    response.session_id = session_id;
                } else {
                    response.session_id = 0;
                }
            }
            FetchSessionToken::Incremental {
                session_id,
                expected_epoch,
            } => {
                let state = self.lock_state();
                let Some(session) = state.sessions.get(&session_id) else {
                    session_error(response, FETCH_SESSION_ID_NOT_FOUND, 0);
                    return;
                };
                if session.epoch != expected_epoch {
                    session_error(response, INVALID_FETCH_SESSION_EPOCH, session_id);
                    return;
                }
                session.retain_expected_partitions(response, true);
                response.session_id = session_id;
            }
        }
    }

    pub(super) fn commit_response(&self, token: FetchSessionToken, response: &FetchResponse) {
        if response.error_code != NO_ERROR {
            return;
        }
        let (session_id, expected_epoch) = match token {
            FetchSessionToken::Sessionless => return,
            FetchSessionToken::Full { session_id } => (session_id, Some(1)),
            FetchSessionToken::Incremental {
                session_id,
                expected_epoch,
            } => (session_id, Some(expected_epoch)),
        };
        let mut state = self.lock_state();
        if let Some(session) = state.sessions.get_mut(&session_id)
            && expected_epoch == Some(session.epoch)
        {
            session.record_response(response);
        }
    }

    pub(super) fn abort_preflight(&self, token: FetchSessionToken) {
        if let FetchSessionToken::Full { session_id } = token {
            self.lock_state().sessions.remove(&session_id);
        }
    }

    pub(super) fn throttle_response(&self, token: FetchSessionToken, response: &mut FetchResponse) {
        response.responses.clear();
        match token {
            FetchSessionToken::Sessionless => response.session_id = 0,
            FetchSessionToken::Full { session_id } => {
                self.lock_state().sessions.remove(&session_id);
                response.session_id = 0;
            }
            FetchSessionToken::Incremental { session_id, .. } => {
                response.session_id = session_id;
            }
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, FetchSessionState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl FetchSessionState {
    fn expire(&mut self, now: Instant, idle_timeout: Duration) {
        self.sessions
            .retain(|_, session| now.duration_since(session.last_used) <= idle_timeout);
    }

    fn make_room(&mut self, max_sessions: usize) {
        while self.sessions.len() >= max_sessions {
            let Some(session_id) = self
                .sessions
                .iter()
                .min_by_key(|(id, session)| (session.last_used, **id))
                .map(|(id, _)| *id)
            else {
                break;
            };
            self.sessions.remove(&session_id);
        }
    }

    fn allocate_id(&mut self) -> i32 {
        loop {
            let candidate = self.next_id;
            self.next_id = if candidate == i32::MAX {
                1
            } else {
                candidate + 1
            };
            if !self.sessions.contains_key(&candidate) {
                return candidate;
            }
        }
    }
}

impl FetchSession {
    fn new(topics: &[FetchTopic], uses_topic_ids: bool, now: Instant) -> Self {
        let mut session = Self {
            uses_topic_ids,
            epoch: 1,
            last_used: now,
            topics: IndexMap::new(),
        };
        session.apply_updates(topics);
        session
    }

    fn apply_updates(&mut self, updates: &[FetchTopic]) {
        for topic in updates {
            if topic.partitions.is_empty() {
                continue;
            }
            let key = request_topic_key(topic, self.uses_topic_ids);
            let cached = self
                .topics
                .entry(key)
                .or_insert_with(|| CachedTopic::new(topic));
            cached.update(topic);
        }
    }

    fn forget(&mut self, forgotten: &[ForgottenTopic]) {
        for topic in forgotten {
            let key = forgotten_topic_key(topic, self.uses_topic_ids);
            let remove_topic = if let Some(cached) = self.topics.get_mut(&key) {
                for partition in &topic.partitions {
                    cached.partitions.shift_remove(partition);
                }
                cached.partitions.is_empty()
            } else {
                false
            };
            if remove_topic {
                self.topics.shift_remove(&key);
            }
        }
    }

    fn request_topics(&self) -> Vec<FetchTopic> {
        self.topics
            .values()
            .map(CachedTopic::request_topic)
            .collect()
    }

    fn retain_expected_partitions(&self, response: &mut FetchResponse, incremental: bool) {
        response.responses.retain_mut(|topic| {
            let key = response_topic_key(topic, self.uses_topic_ids);
            let Some(cached) = self.topics.get(&key) else {
                return false;
            };
            topic.partitions.retain(|partition| {
                cached
                    .partitions
                    .get(&partition.partition_index)
                    .is_some_and(|partition_cache| {
                        !incremental || partition_cache.must_respond(partition)
                    })
            });
            !topic.partitions.is_empty()
        });
    }

    fn record_response(&mut self, response: &FetchResponse) {
        for topic in &response.responses {
            let key = response_topic_key(topic, self.uses_topic_ids);
            let Some(cached) = self.topics.get_mut(&key) else {
                continue;
            };
            for partition in &topic.partitions {
                if let Some(partition_cache) = cached.partitions.get_mut(&partition.partition_index)
                {
                    partition_cache.record_response(partition);
                }
            }
        }
    }
}

impl CachedTopic {
    fn new(topic: &FetchTopic) -> Self {
        let mut template = topic.clone();
        template.partitions.clear();
        Self {
            template,
            partitions: IndexMap::new(),
        }
    }

    fn update(&mut self, topic: &FetchTopic) {
        self.template.topic = topic.topic.clone();
        self.template.topic_id = topic.topic_id;
        self.template.unknown_tagged_fields = topic.unknown_tagged_fields.clone();
        for partition in &topic.partitions {
            self.partitions
                .entry(partition.partition)
                .and_modify(|cached| cached.request = partition.clone())
                .or_insert_with(|| CachedPartition::new(partition.clone()));
        }
    }

    fn request_topic(&self) -> FetchTopic {
        let mut topic = self.template.clone();
        topic.partitions = self
            .partitions
            .values()
            .map(|partition| partition.request.clone())
            .collect();
        topic
    }
}

impl CachedPartition {
    fn new(request: FetchPartition) -> Self {
        Self {
            request,
            high_watermark: -1,
            log_start_offset: -1,
        }
    }

    fn must_respond(&self, response: &PartitionData) -> bool {
        response
            .records
            .as_ref()
            .is_some_and(|records| !records.is_empty())
            || self.high_watermark != response.high_watermark
            || self.log_start_offset != response.log_start_offset
            || response.preferred_read_replica.0 != -1
            || response.error_code != NO_ERROR
            || response.diverging_epoch != Default::default()
    }

    fn record_response(&mut self, response: &PartitionData) {
        self.high_watermark = if response.error_code == NO_ERROR {
            response.high_watermark
        } else {
            -1
        };
        self.log_start_offset = response.log_start_offset;
    }
}

fn request_topic_key(topic: &FetchTopic, uses_topic_ids: bool) -> TopicKey {
    if uses_topic_ids {
        TopicKey::Id(topic.topic_id)
    } else {
        TopicKey::Name(topic.topic.as_str().to_owned())
    }
}

fn forgotten_topic_key(topic: &ForgottenTopic, uses_topic_ids: bool) -> TopicKey {
    if uses_topic_ids {
        TopicKey::Id(topic.topic_id)
    } else {
        TopicKey::Name(topic.topic.as_str().to_owned())
    }
}

fn response_topic_key(topic: &FetchableTopicResponse, uses_topic_ids: bool) -> TopicKey {
    if uses_topic_ids {
        TopicKey::Id(topic.topic_id)
    } else {
        TopicKey::Name(topic.topic.as_str().to_owned())
    }
}

fn next_epoch(epoch: i32) -> i32 {
    if epoch < 0 {
        -1
    } else if epoch == i32::MAX {
        1
    } else {
        epoch + 1
    }
}

fn session_error(response: &mut FetchResponse, error_code: i16, session_id: i32) {
    response.error_code = error_code;
    response.session_id = session_id;
    response.responses.clear();
}
