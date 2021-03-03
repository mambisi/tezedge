use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::fs::{File, OpenOptions};
use crate::persistent::commit_log::error::TezedgeCommitLogError;
use std::path::{Path, PathBuf};
use crate::persistent::commit_log::{Index, INDEX_FILE_NAME, DATA_FILE_NAME, TH_LENGTH};
use std::ops::Sub;

pub(crate) struct Writer {
    index_file: BufWriter<File>,
    data_file: BufWriter<File>,
    last_index: i64
}

impl Writer {
    pub(crate) fn new<P: AsRef<Path>>(dir: P) -> Result<Self, TezedgeCommitLogError> {
        if !dir.as_ref().exists() {
            std::fs::create_dir_all(dir.as_ref())?;
        }
        if dir.as_ref().exists() & !dir.as_ref().is_dir() {
           return Err(TezedgeCommitLogError::PathError);
        }

        let mut index_file_path = PathBuf::new();
        index_file_path.push(dir.as_ref());
        index_file_path.push(INDEX_FILE_NAME);

        let mut data_file_path = PathBuf::new();
        data_file_path.push(dir.as_ref());
        data_file_path.push(DATA_FILE_NAME);

        let index_file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(index_file_path.as_path())?;

        let data_file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(data_file_path.as_path())?;

        let last_index = Self::last_index(index_file.try_clone()?);


        Ok(Self {
            index_file : BufWriter::new(index_file),
            data_file: BufWriter::new(data_file),
            last_index
        })
    }

    pub(crate) fn write(&mut self, buf: &[u8]) -> Result<u64, TezedgeCommitLogError> {
        if buf.len() > u64::MAX as usize {
            return Err(TezedgeCommitLogError::MessageLengthError)
        }
        let message_length = buf.len() as u64;
        let message_pos = self.data_file.seek(SeekFrom::End(0))?;
        self.data_file.write_all(&buf)?;
        let th = Index::new(message_pos, message_length);
        self.index_file.seek(SeekFrom::End(0))?;
        self.index_file.write(&th.to_vec())?;
        self.last_index += 1;
        Ok(self.last_index as u64)
    }

    pub fn last_index(index_file : File) -> i64{
        let metadata = match index_file.metadata(){
            Ok(m) => {
                m
            }
            Err(_) => {
                return -1
            }
        };
        let items_count = metadata.len() / (TH_LENGTH as u64);
        (items_count as i64 ).sub(1)
    }

    pub(crate) fn flush(&mut self) -> Result<(), TezedgeCommitLogError> {
        self.data_file.flush()?;
        self.index_file.flush()?;
        Ok(())
    }
}