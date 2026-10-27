//! Routes events by path before inspecting subscriber state or the filesystem.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;

use crate::SubscriberId;
use crate::SubscriberWatchKey;

#[derive(Default)]
pub(crate) struct WatchIndex {
    paths: BTreeMap<PathBuf, HashSet<SubscriberWatchKey>>,
    subscribers: HashMap<SubscriberWatchKey, HashSet<SubscriberId>>,
}

impl WatchIndex {
    pub(crate) fn insert(&mut self, key: &SubscriberWatchKey, subscriber: SubscriberId) {
        let subscribers = self.subscribers.entry(key.clone()).or_default();
        if subscribers.is_empty() {
            for path in [&key.requested.path, &key.matched.path] {
                self.paths
                    .entry(path.clone())
                    .or_default()
                    .insert(key.clone());
            }
        }
        subscribers.insert(subscriber);
    }

    pub(crate) fn remove(&mut self, key: &SubscriberWatchKey, subscriber: SubscriberId) {
        let Some(subscribers) = self.subscribers.get_mut(key) else {
            return;
        };
        subscribers.remove(&subscriber);
        if !subscribers.is_empty() {
            return;
        }
        self.subscribers.remove(key);
        for path in [&key.requested.path, &key.matched.path] {
            if let Some(keys) = self.paths.get_mut(path) {
                keys.remove(key);
                if keys.is_empty() {
                    self.paths.remove(path);
                }
            }
        }
    }

    pub(crate) fn subscribers(
        &self,
        key: &SubscriberWatchKey,
    ) -> impl Iterator<Item = SubscriberId> {
        self.subscribers.get(key).into_iter().flatten().copied()
    }

    pub(crate) fn matching(&self, event_path: &Path) -> HashSet<SubscriberWatchKey> {
        let mut matching = HashSet::new();
        for ancestor in event_path.ancestors() {
            if let Some(keys) = self.paths.get(ancestor) {
                matching.extend(
                    keys.iter()
                        .filter(|key| {
                            key.requested.recursive
                                || ancestor == event_path
                                || event_path.parent() == Some(ancestor)
                        })
                        .cloned(),
                );
            }
        }
        // Parent create/delete events can change a missing target's fallback watch.
        // Path ordering compares components, so descendants form one contiguous range.
        for (_, keys) in self
            .paths
            .range(event_path.to_path_buf()..)
            .take_while(|(path, _)| path.starts_with(event_path))
        {
            matching.extend(keys.iter().cloned());
        }
        matching
    }
}
