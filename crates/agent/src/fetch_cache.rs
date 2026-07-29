use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::ops::Range;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    object_key: String,
    start: u64,
    end: u64,
}

impl CacheKey {
    fn new(object_key: &str, range: &Range<u64>) -> Self {
        Self {
            object_key: object_key.to_owned(),
            start: range.start,
            end: range.end,
        }
    }
}

#[derive(Default)]
struct CacheState {
    entries: HashMap<CacheKey, Bytes>,
    order: VecDeque<CacheKey>,
    bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CacheUpdate {
    pub evictions: u64,
    pub bytes: usize,
}

#[derive(Clone)]
pub(crate) struct FetchCache {
    max_bytes: usize,
    state: Arc<Mutex<CacheState>>,
}

impl FetchCache {
    pub(crate) fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            state: Arc::new(Mutex::new(CacheState::default())),
        }
    }

    pub(crate) fn get(&self, object_key: &str, range: &Range<u64>) -> Option<Bytes> {
        if self.max_bytes == 0 {
            return None;
        }
        let key = CacheKey::new(object_key, range);
        let mut state = self.state.lock().expect("Fetch cache lock is not poisoned");
        let value = state.entries.get(&key)?.clone();
        state.order.retain(|candidate| candidate != &key);
        state.order.push_back(key);
        Some(value)
    }

    pub(crate) fn insert(&self, object_key: &str, range: &Range<u64>, value: Bytes) -> CacheUpdate {
        let mut state = self.state.lock().expect("Fetch cache lock is not poisoned");
        if self.max_bytes == 0 || value.len() > self.max_bytes {
            return CacheUpdate {
                evictions: 0,
                bytes: state.bytes,
            };
        }
        let key = CacheKey::new(object_key, range);
        if let Some(previous) = state.entries.remove(&key) {
            state.bytes = state.bytes.saturating_sub(previous.len());
            state.order.retain(|candidate| candidate != &key);
        }
        state.bytes = state.bytes.saturating_add(value.len());
        state.entries.insert(key.clone(), value);
        state.order.push_back(key);

        let mut evictions = 0u64;
        while state.bytes > self.max_bytes {
            let Some(oldest) = state.order.pop_front() else {
                break;
            };
            if let Some(removed) = state.entries.remove(&oldest) {
                state.bytes = state.bytes.saturating_sub(removed.len());
                evictions = evictions.saturating_add(1);
            }
        }
        CacheUpdate {
            evictions,
            bytes: state.bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(index: u64) -> Range<u64> {
        index * 4..index * 4 + 4
    }

    #[test]
    fn evicts_the_least_recently_used_range_within_the_byte_bound() {
        let cache = FetchCache::new(8);
        cache.insert("object", &range(0), Bytes::from_static(b"aaaa"));
        cache.insert("object", &range(1), Bytes::from_static(b"bbbb"));
        assert_eq!(cache.get("object", &range(0)).unwrap(), "aaaa");

        let update = cache.insert("object", &range(2), Bytes::from_static(b"cccc"));
        assert_eq!(
            update,
            CacheUpdate {
                evictions: 1,
                bytes: 8
            }
        );
        assert!(cache.get("object", &range(1)).is_none());
        assert_eq!(cache.get("object", &range(0)).unwrap(), "aaaa");
        assert_eq!(cache.get("object", &range(2)).unwrap(), "cccc");
    }

    #[test]
    fn disabled_and_oversized_entries_never_exceed_the_bound() {
        let disabled = FetchCache::new(0);
        assert_eq!(
            disabled.insert("object", &(0..1), Bytes::from_static(b"x")),
            CacheUpdate {
                evictions: 0,
                bytes: 0
            }
        );
        assert!(disabled.get("object", &(0..1)).is_none());

        let cache = FetchCache::new(4);
        cache.insert("small", &(0..4), Bytes::from_static(b"keep"));
        assert_eq!(
            cache.insert("large", &(0..5), Bytes::from_static(b"large")),
            CacheUpdate {
                evictions: 0,
                bytes: 4
            }
        );
        assert_eq!(cache.get("small", &(0..4)).unwrap(), "keep");
        assert!(cache.get("large", &(0..5)).is_none());
    }
}
