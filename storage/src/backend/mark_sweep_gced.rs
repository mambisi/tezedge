use std::thread;
use std::collections::{HashSet};

use crate::merkle_storage::{Entry, EntryHash, ContextValue, hash_entry};
use crate::storage_backend::{StorageBackend as KVStore, StorageBackendError as KVStoreError, StorageBackendStats as KVStoreStats, size_of_vec, StorageBackendError};
use linked_hash_set::LinkedHashSet;
use std::collections::hash_map::RandomState;
use crypto::hash::HashType;

/// Garbage Collected Key Value Store
pub struct MarkSweepGCed<T: KVStore> {
    store: T,
    /// stores commit hashes
    commit_store: Vec<LinkedHashSet<EntryHash>>,
    /// number of cycles to retain
    cycle_threshold: usize,
    ///
    cycle_block_count : usize,
    ///
    last_commit_tree : Option<LinkedHashSet<EntryHash>>
}

impl<T: 'static + KVStore + Default> MarkSweepGCed<T> {
    pub fn new(cycle_threshold: usize,cycle_block_count : usize) -> Self {
        Self {
            store: Default::default(),
            cycle_threshold,
            commit_store: Vec::new(),
            cycle_block_count,
            last_commit_tree: None
        }
    }

    fn get_entry(&self, key: &EntryHash) -> Result<Option<Entry>, KVStoreError> {
        match self.store.get(key)? {
            None => Ok(None),
            Some(entry_bytes) => Ok(Some(bincode::deserialize(&entry_bytes)?)),
        }
    }

    pub fn gc(&mut self, _last_commit_hash: Option<EntryHash>) -> Result<(), KVStoreError> {
        let mut garbage: LinkedHashSet<EntryHash> = self.commit_store.drain(..self.cycle_block_count).into_iter().flatten().collect();
        if let Some(items) =  self.commit_store.last() {
            println!("Relative Commit ITEMS {}", items.len());
            for i in items.iter() {
                garbage.remove(i);
            }
        }
        if let Some(items) =  self.commit_store.first() {
            println!("Recent Commit ITEMS {}", items.len());
            for i in items.iter() {
                garbage.remove(i);
            }
        }
        self.sweep_entries(garbage);
        Ok(())
    }

    fn mark_entries(&self, garbage: &mut LinkedHashSet<EntryHash>, entry_hash: &EntryHash) {
        if let Ok(Some(Entry::Commit(entry))) = self.get_entry(entry_hash) {
            self.mark_entries_recursively(&Entry::Commit(entry), garbage);
        }else {
            panic!("Not commit")
        }
    }

    fn sweep_entries(&mut self, garbage: LinkedHashSet<EntryHash>) -> Result<(), KVStoreError> {
        println!("Garbage Collection {} items", garbage.len());

        for item in garbage {
            self.store.delete(&item);
        }
        Ok(())
    }

    fn mark_entries_recursively(&self, entry: &Entry, garbage: &mut LinkedHashSet<EntryHash>) {
        if let Ok(hash) = hash_entry(entry) {
            garbage.remove(&hash);
            match entry {
                Entry::Blob(_) => {}
                Entry::Tree(tree) => {
                    tree.iter().for_each(|(key, child_node)| {
                        match self.get_entry(&child_node.entry_hash) {
                            Ok(Some(entry)) => self.mark_entries_recursively(&entry, garbage),
                            _ => {}
                        };
                    });
                }
                Entry::Commit(commit) => {
                    match self.get_entry(&commit.root_hash) {
                        Ok(Some(entry)) => self.mark_entries_recursively(&entry, garbage),
                        _ => {}
                        Err(_) => {}
                    }
                }
            }
        }
    }

    /*
    fn collect_garbage_entries_recursively(&self, entry: &Entry, garbage: &mut HashSet<EntryHash>) {
        if let Ok(hash) = hash_entry(entry) {
            garbage.insert(hash);
            match entry {
                Entry::Blob(_) => {}
                Entry::Tree(tree) => {
                    tree.iter().for_each(|(key, child_node)| {
                        match self.get_entry(&child_node.entry_hash) {
                            Ok(Some(entry)) => self.mark_entries_recursively(&entry, garbage),
                            _ => {}
                        };
                    });
                }
                Entry::Commit(commit) => {
                    match self.get_entry(&commit.root_hash) {
                        Ok(Some(entry)) => self.mark_entries_recursively(&entry, garbage),
                        _ => {}
                        Err(_) => {}
                    }
                }
            }
        }
    }
     */
}


impl<T: 'static + KVStore + Default> KVStore for MarkSweepGCed<T> {
    fn is_persisted(&self) -> bool {
        self.store.is_persisted()
    }

    fn get(&self, key: &EntryHash) -> Result<Option<ContextValue>, KVStoreError> {
        self.store.get(key)
    }

    fn contains(&self, key: &EntryHash) -> Result<bool, KVStoreError> {
        self.store.contains(key)
    }

    fn put(
        &mut self,
        key: EntryHash,
        value: ContextValue,
    ) -> Result<bool, KVStoreError> {
        self.store.put(key, value)
    }

    fn merge(&mut self, key: EntryHash, value: ContextValue) -> Result<(), KVStoreError> {
        self.store.merge(key, value)
    }

    fn delete(&mut self, key: &EntryHash) -> Result<Option<ContextValue>, KVStoreError> {
        self.store.delete(key)
    }

    fn retain(&mut self, pred: HashSet<EntryHash>) -> Result<(), KVStoreError> {
        self.store.retain(pred)
    }

    fn mark_reused(&mut self, _key: EntryHash) {}

    fn start_new_cycle(&mut self, last_commit_hash: Option<EntryHash>) {
        if self.commit_store.len() >= ( self.cycle_threshold  + 1)* self.cycle_block_count {
            self.gc(last_commit_hash);
        }
    }

    fn wait_for_gc_finish(&self) {}

    fn get_stats(&self) -> Vec<KVStoreStats> {
        self.store.get_stats()
    }

    fn store_commit_tree(&mut self, commit_tree: LinkedHashSet<[u8; 32], RandomState>) {
        self.commit_store.push(commit_tree);
    }

    fn collect(&mut self, garbage: HashSet<[u8; 32], RandomState>) -> Result<(), StorageBackendError> {
        for item in garbage {
            self.store.delete(&item);
        }
        Ok(())
    }
}


// #[cfg(test)]
// mod tests {
//     use super::*;
//     use std::mem;
//     use crate::backend::BTreeMapBackend;

//     fn empty_kvstore_gced(cycle_count: usize) -> KVStoreGCed<BTreeMapBackend> {
//         KVStoreGCed::new(cycle_count)
//     }

//     fn entry_hash(key: &[u8]) -> EntryHash {
//         assert!(key.len() < 32);
//         let mut result = [0u8; 32];

//         for (index, value) in key.iter().enumerate() {
//             result[index] = *value;
//         }

//         result
//     }

//     fn blob(value: Vec<u8>) -> Entry {
//         Entry::Blob(value)
//     }

//     fn blob_serialized(value: Vec<u8>) -> Vec<u8> {
//         bincode::serialize(&blob(value)).unwrap()
//     }

//     fn get<T: 'static + KVStore + Default>(store: &KVStoreGCed<T>, key: &[u8]) -> Option<Entry> {
//         store.get(&entry_hash(key)).unwrap()
//             .map(|x| bincode::deserialize(&x[..]).unwrap())
//     }

//     fn put<T: 'static + KVStore + Default>(store: &mut KVStoreGCed<T>, key: &[u8], value: Entry) {
//         store.put(entry_hash(key), bincode::serialize(&value).unwrap()).unwrap();
//     }

//     fn mark_reused<T: 'static + KVStore + Default>(store: &mut KVStoreGCed<T>, key: &[u8]) {
//         store.mark_reused(entry_hash(key));
//     }

//     #[test]
//     fn test_stats() {
//         let store = &mut empty_kvstore_gced(3);

//         let kv1 = (entry_hash(&[1]), blob_serialized(vec![1]));
//         let kv2 = (entry_hash(&[2]), blob_serialized(vec![1, 2]));
//         let kv3 = (entry_hash(&[3]), blob_serialized(vec![1, 2, 3]));
//         let kv4 = (entry_hash(&[4]), blob_serialized(vec![1, 2, 3, 4]));

//         store.put(kv1.0.clone(), kv1.1.clone()).unwrap();
//         store.put(kv2.0.clone(), kv2.1.clone()).unwrap();
//         store.start_new_cycle();
//         store.put(kv3.0.clone(), kv3.1.clone()).unwrap();
//         store.start_new_cycle();
//         store.put(kv4.0.clone(), kv4.1.clone()).unwrap();
//         store.mark_reused(kv1.0.clone());

//         store.wait_for_gc_finish();

//         let stats: Vec<_> = store.get_stats().into_iter().rev().take(3).rev().collect();
//         assert_eq!(stats[0].key_bytes, 64);
//         assert_eq!(stats[0].value_bytes, size_of_vec(&kv1.1) + size_of_vec(&kv2.1));
//         assert_eq!(stats[0].reused_keys_bytes, 96);

//         assert_eq!(stats[1].key_bytes, 32);
//         assert_eq!(stats[1].value_bytes, size_of_vec(&kv3.1));
//         assert_eq!(stats[1].reused_keys_bytes, 0);

//         assert_eq!(stats[2].key_bytes, 32);
//         assert_eq!(stats[2].value_bytes, size_of_vec(&kv4.1));
//         assert_eq!(stats[2].reused_keys_bytes, 0);

//         assert_eq!(store.total_mem_usage_as_bytes(), vec![
//             4 * mem::size_of::<EntryHash>(),
//             96, // reused keys
//             size_of_vec(&kv1.1),
//             size_of_vec(&kv2.1),
//             size_of_vec(&kv3.1),
//             size_of_vec(&kv4.1),
//         ].iter().sum::<usize>());

//         store.start_new_cycle();
//         store.wait_for_gc_finish();

//         let stats = store.get_stats();
//         assert_eq!(stats[0].key_bytes, 32);
//         assert_eq!(stats[0].value_bytes, size_of_vec(&kv3.1));
//         assert_eq!(stats[0].reused_keys_bytes, 0);

//         assert_eq!(stats[1].key_bytes, 64);
//         assert_eq!(stats[1].value_bytes, size_of_vec(&kv1.1) + size_of_vec(&kv4.1));
//         assert_eq!(stats[1].reused_keys_bytes, 0);

//         assert_eq!(stats[2].key_bytes, 0);
//         assert_eq!(stats[2].value_bytes, 0);
//         assert_eq!(stats[2].reused_keys_bytes, 0);

//         assert_eq!(store.total_mem_usage_as_bytes(), vec![
//             3 * mem::size_of::<EntryHash>(),
//             size_of_vec(&kv1.1),
//             size_of_vec(&kv3.1),
//             size_of_vec(&kv4.1),
//         ].iter().sum::<usize>());
//     }
// }