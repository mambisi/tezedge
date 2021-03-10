use crate::commit_log::error::TezedgeCommitLogError;
use crate::commit_log::{Index, MessageSet, DATA_FILE_NAME, INDEX_FILE_NAME, TH_LENGTH};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

pub(crate) struct Reader {
    indexes : Vec<Index>,
    data_file: File,
}

impl Reader {
    pub(crate) fn new(indexes : Vec<Index>, data_file : File) -> Result<Self, TezedgeCommitLogError> {

        let reader = Self {
            indexes,
            data_file,
        };
        Ok(reader)
    }

    pub fn indexes(&self) -> Vec<Index> {
        self.indexes.clone()
    }

    pub(crate) fn range(
        &self,
        from: usize,
        limit: usize,
    ) -> Result<MessageSet, TezedgeCommitLogError> {
        let indexes = self.indexes();

        if from + limit > indexes.len() {
            return Err(TezedgeCommitLogError::OutOfRange);
        }
        let mut data_file_buf_reader = BufReader::new(&self.data_file);
        let from_index = indexes[from];
        let range: Vec<_> = indexes[from..].iter().copied().take(limit).collect();
        let total_data_size = range.iter().fold(0_u64, |acc, item| acc + item.data_length);
        let mut bytes = vec![0; total_data_size as usize];
        data_file_buf_reader.seek(SeekFrom::Start(from_index.position))?;
        data_file_buf_reader.read_exact(&mut bytes)?;

        Ok(MessageSet::new(range, bytes))
    }
}
