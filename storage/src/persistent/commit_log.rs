// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::{fmt, io};

use failure::Fail;
use serde::{Deserialize, Serialize};

use crate::commit_log::error::TezedgeCommitLogError;
use crate::commit_log::CommitLog;
use crate::persistent::codec::{Decoder, Encoder, SchemaError};
use crate::persistent::schema::{CommitLogDescriptor, CommitLogSchema};
use crate::persistent::BincodeEncoded;

pub type CommitLogRef = Arc<RwLock<CommitLog>>;

/// Possible errors for commit log
#[derive(Debug, Fail)]
pub enum CommitLogError {
    #[fail(display = "Schema error: {}", error)]
    SchemaError { error: SchemaError },
    #[fail(display = "Commit log I/O error {}", error)]
    IOError { error: io::Error },
    #[fail(display = "Commit log {} is missing", name)]
    MissingCommitLog { name: &'static str },
    #[fail(display = "Failed to read record at {}", location)]
    ReadError { location: Location },
    #[fail(display = "Failed to read record data corrupted")]
    CorruptData,
    #[fail(display = "Tezedge CommitLog error: {:#?}", error)]
    TezedgeCommitLogError { error: TezedgeCommitLogError },
}

impl From<SchemaError> for CommitLogError {
    fn from(error: SchemaError) -> Self {
        CommitLogError::SchemaError { error }
    }
}

impl From<TezedgeCommitLogError> for CommitLogError {
    fn from(error: TezedgeCommitLogError) -> Self {
        CommitLogError::TezedgeCommitLogError { error }
    }
}

impl From<io::Error> for CommitLogError {
    fn from(error: io::Error) -> Self {
        CommitLogError::IOError { error }
    }
}

impl slog::Value for CommitLogError {
    fn serialize(
        &self,
        _record: &slog::Record,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        serializer.emit_arguments(key, &format_args!("{}", self))
    }
}

type ByteLimit = usize;
type ItemCount = u32;

/// Precisely identifies location of a record in a commit log.
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct Location(pub u64, pub ByteLimit);

impl Location {
    #[inline]
    pub fn is_consecutive(&self, prev: &Location) -> bool {
        (prev.0 < self.0) && (self.0 - prev.0 == 1)
    }
}

impl fmt::Display for Location {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_fmt(format_args!("Location({},{})", self.0, self.1))
    }
}

impl BincodeEncoded for Location {}

/// Range of values to get from a commit log
#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Range(pub u64, pub ByteLimit, pub ItemCount);

/// Implement this trait for a commit log engine.
pub trait CommitLogWithSchema<S: CommitLogSchema> {
    /// Append new record to a commit log.
    fn append(&self, value: &S::Value) -> Result<Location, CommitLogError>;

    /// Retrieve a stored record.
    fn get(&self, location: &Location) -> Result<S::Value, CommitLogError>;

    /// Retrieve stored records stored in a single range.
    fn get_range(&self, range: &Range) -> Result<Vec<S::Value>, CommitLogError>;
}

impl<S: CommitLogSchema> CommitLogWithSchema<S> for CommitLogs {
    fn append(&self, value: &S::Value) -> Result<Location, CommitLogError> {
        let cl = self
            .cl_handle(S::name())
            .ok_or(CommitLogError::MissingCommitLog { name: S::name() })?;
        let mut cl = cl.write().expect("Write lock failed");
        let bytes = value.encode()?;
        let offset = cl
            .append_msg(&bytes)
            .map_err(|error| CommitLogError::TezedgeCommitLogError { error })?;

        Ok(Location(offset, bytes.len()))
    }

    fn get(&self, location: &Location) -> Result<S::Value, CommitLogError> {
        let cl = self
            .cl_handle(S::name())
            .ok_or(CommitLogError::MissingCommitLog { name: S::name() })?;
        let cl = cl.read().expect("Read lock failed");
        let mut msg_buf =
            cl.read(location.0 as usize, 1)
                .map_err(|_| CommitLogError::ReadError {
                    location: location.clone(),
                })?;
        let bytes = msg_buf.next().ok_or(CommitLogError::CorruptData)?;
        let value = S::Value::decode(&bytes)?;

        Ok(value)
    }

    fn get_range(&self, range: &Range) -> Result<Vec<S::Value>, CommitLogError> {
        let cl = self
            .cl_handle(S::name())
            .ok_or(CommitLogError::MissingCommitLog { name: S::name() })?;
        let cl = cl.read().expect("Read lock failed");
        let msg_buf =
            cl.read(range.0 as usize, range.2 as usize)
                .map_err(|_| CommitLogError::ReadError {
                    location: Location(range.0, range.2 as usize),
                })?;
        msg_buf
            .take(range.2 as usize)
            .map(|message| S::Value::decode(&message).map_err(|_| CommitLogError::CorruptData))
            .collect()
    }
}

pub fn fold_consecutive_locations(locations: &[Location]) -> Vec<Range> {
    if locations.is_empty() {
        Vec::with_capacity(0)
    } else {
        let mut ranges = vec![];

        let mut prev = locations[0];
        let mut range = Range(prev.0, prev.1, 1);
        for curr in &locations[1..] {
            if curr.is_consecutive(&prev) {
                range.1 += curr.1;
                range.2 += 1;
            } else {
                ranges.push(range);
                range = Range(curr.0, curr.1, 1);
            }
            prev = *curr;
        }
        ranges.push(range);

        ranges
    }
}

/// Provides access to all registered commit logs via a log family reference.
pub struct CommitLogs {
    base_path: PathBuf,
    commit_log_map: RwLock<HashMap<String, CommitLogRef>>,
}

impl CommitLogs {
    pub(crate) fn new<P, I>(path: P, cfs: I) -> Result<Self, CommitLogError>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = CommitLogDescriptor>,
    {
        let myself = Self {
            base_path: path.as_ref().into(),
            commit_log_map: RwLock::new(HashMap::new()),
        };

        for descriptor in cfs.into_iter() {
            Self::register(&myself, descriptor.name())?;
        }

        Ok(myself)
    }

    /// Register a new commit log.
    fn register(&self, name: &str) -> Result<(), CommitLogError> {
        let path = self.base_path.join(name);
        if !Path::new(&path).exists() {
            std::fs::create_dir_all(&path)?;
        }
        let log = CommitLog::new(path)?;

        let mut commit_log_map = self.commit_log_map.write().unwrap();
        commit_log_map.insert(name.into(), Arc::new(RwLock::new(log)));

        Ok(())
    }

    /// Retrieve handle to a registered commit log.
    #[inline]
    fn cl_handle(&self, name: &str) -> Option<CommitLogRef> {
        let commit_log_map = self.commit_log_map.read().unwrap();
        commit_log_map.get(name).cloned()
    }

    /// Flush all registered commit logs.
    pub fn flush(&self) -> Result<(), CommitLogError> {
        let commit_log_map = self.commit_log_map.read().unwrap();
        for commit_log in commit_log_map.values() {
            let mut commit_log = commit_log.write().unwrap();
            commit_log.flush()?;
        }

        Ok(())
    }
}

impl Drop for CommitLogs {
    fn drop(&mut self) {
        let _ = self.flush().expect("Failed to flush commit logs");
    }
}

#[cfg(test)]
impl Location {
    pub fn new(offset: u64) -> Self {
        Self(offset, 0)
    }

    pub(crate) fn offset(&self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use crate::persistent::commit_log::fold_consecutive_locations;

    use super::*;

    #[test]
    fn test_fold_consecutive_locations_empty() {
        let locations = vec![];
        let ranges = fold_consecutive_locations(&locations);
        assert!(ranges.is_empty());
    }

    #[test]
    fn test_fold_consecutive_locations_single() {
        let locations = vec![Location(1, 10)];
        let ranges = fold_consecutive_locations(&locations);
        assert_eq!(vec![Range(1, 10, 1)], ranges);
    }

    #[test]
    fn test_fold_consecutive_locations_multi() {
        let locations = vec![
            Location(1, 10),
            Location(2, 10),
            Location(5, 10),
            Location(7, 10),
            Location(8, 10),
            Location(9, 10),
            Location(6, 10),
        ];
        let ranges = fold_consecutive_locations(&locations);
        assert_eq!(
            vec![
                Range(1, 20, 2),
                Range(5, 10, 1),
                Range(7, 30, 3),
                Range(6, 10, 1)
            ],
            ranges
        );
    }

    #[test]
    fn test_commit_log_storage() {
        let path = env!("LOG_DIR");
        let mut commitlog = CommitLog::new(path).unwrap();
        let msgs = vec![vec![1,2,3], vec![4,6,7], vec![21,23,45], vec![10,56,12], vec![51,3,4],vec![76,12,45]];
        for msg in msgs.iter() {
            println!("{:?}", commitlog.append_msg(msg))
        }
        let items = commitlog.read(2, 2).unwrap();
        items.for_each(|i| {
            println!("{:?}", i)
        })
    }
}
