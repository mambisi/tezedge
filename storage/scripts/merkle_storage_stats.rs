// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::convert::TryInto;
use std::io::Read;
use std::fs::File;
use crypto::hash::{HashType, BlockHash};
use clap::{Arg, App};

use storage::*;
// use context_action_storage::ContextAction;
use merkle_storage::{MerkleStorage, Entry, EntryHash, check_commit_hashes};
use storage_backend::StorageBackend;
use backend::{RocksDBBackend, SledBackend, InMemoryBackend, BTreeMapBackend, MarkSweepGCed, KVStoreGCed};

mod actions_tool;
use actions_tool::{ActionsFileReader, ContextAction};

fn parse_mem_value(value: &str) -> usize {
    let mut pair = value.split_whitespace();
    let value: usize = pair.next().unwrap().parse().unwrap();

    match pair.next() {
        Some("kB") => value * 1024,
        _ => panic!("unknown memory usage unit."),
    }
}

#[cfg(target_os="linux")]
fn mem_usage() -> usize {
    let mut text_buf = Default::default();

    File::open("/proc/self/status")
        .and_then(|mut f| f.read_to_string(&mut text_buf))
        .unwrap();

    let mut res = 0;

    for line in text_buf.lines() {
        let mut pairs = line.split(':');
        match (pairs.next(), pairs.next()) {
            // (Some("VmPeak"), Some(text)) => virt_peak = parse_mem_value(text),
            // (Some("VmSize"), Some(text)) => virt = parse_mem_value(text),
            (Some("VmRSS"), Some(text)) => res = parse_mem_value(text),
            // (Some("VmSwap"), Some(text))
            _ => {}
        }
    }

    res
}

#[cfg(not(target_os="linux"))]
fn mem_usage() -> usize {
    0
}

struct Args {
    backend: String,
    preserved_cycles: usize,
    cycle_block_count: u32,
    actions_file: String,
}

impl Args {
    pub fn read_args() -> Self {
        let app = App::new("merkle_storage_stats")
            .about("generate merkle storage statistics data")
            .arg(Arg::with_name("preserved_cycles")
                .help("last number of cycles which should be preserved")
                .default_value("5"))
            .arg(Arg::with_name("cycle_block_count")
                .help("amount of blocks in each cycle")
                .default_value("2048"))
            .arg(Arg::with_name("actions_file")
                .required(true)
                .help("path to the actions.bin")
                .index(1))
            .arg(Arg::with_name("backend")
                     .short("b")
                     .long("backend")
                     .default_value("in-memory-gced")
                     .help("backend to use for storing merkle storage. Possible values: in-memory-gced, in-memory-mark-sweep-gced")
                 // + "rocksdb, sled, in-memory-gced, in-memory-mark-sweep-gced")
            );

        let matches = app.get_matches();


        Self {
            backend: matches.value_of("backend").unwrap_or("in-memory-gced").to_string(),
            preserved_cycles: matches.value_of("preserved_cycles")
                .unwrap_or("7")
                .parse()
                .unwrap(),
            cycle_block_count: matches.value_of("cycle_block_count")
                .unwrap_or("2048")
                .parse()
                .unwrap(),
            actions_file: matches.value_of("actions_file")
                .expect("actions_file is required argument")
                .to_string(),
        }
    }
}

fn main() {
    gen_stats(Args::read_args());
}

fn gen_stats(args: Args) {
    // let mut cycle_commit_hashes: Vec<Vec<EntryHash>> =
    //     vec![Default::default(); args.preserved_cycles - 1];

    let backend: Box<dyn StorageBackend + Send + Sync> = match args.backend.as_str() {
        "in-memory-gced" => Box::new(
            KVStoreGCed::<BTreeMapBackend>::new(args.preserved_cycles)
        ),
        "in-memory-mark-sweep-gced" => Box::new(
            MarkSweepGCed::<InMemoryBackend>::new(args.preserved_cycles, args.cycle_block_count as usize)
        ),
        _ => {
            eprintln!("unsupported backend supplied: {}", args.backend);
            return;
        },
    };

    let mut merkle = MerkleStorage::new(backend);

    println!("block level, key bytes, value bytes, reused keys bytes, total mem, process mem, total latency");

    for (block, actions) in ActionsFileReader::new(&args.actions_file).unwrap().into_iter() {
        let actions_len = actions.len();

        for action in actions.into_iter() {
            // if let ContextAction::Commit { new_context_hash, .. } = &action {
            //     cycle_commit_hashes.last_mut().unwrap().push(
            //         new_context_hash[..].try_into().unwrap()
            //     );
            // }

            match &action {
                ContextAction::Set { key, value, ignored, .. } => {
                    if !ignored {
                        merkle.set(&key, &value).unwrap();
                    }
                }
                ContextAction::Copy { to_key, from_key, ignored, .. } => {
                    if !ignored {
                        merkle.copy(&from_key, &to_key).unwrap();
                    }
                }
                ContextAction::Delete { key, ignored, .. } => {
                    if !ignored {
                        merkle.delete(&key).unwrap();
                    }
                }
                ContextAction::RemoveRecursively { key, ignored, .. } => {
                    if !ignored {
                        merkle.delete(&key).unwrap();
                    }
                }
                ContextAction::Commit { author, message, date, new_context_hash, .. } => {
                    let commit_hash = merkle.commit(*date as u64, author.to_string(), message.to_string()).unwrap();
                    assert_eq!(
                        &commit_hash,
                        &new_context_hash[..],
                    );
                }
                ContextAction::Checkout { context_hash, .. } => {
                    merkle.checkout(context_hash.as_slice().try_into().unwrap()).unwrap();
                }
                _ => {}
            };
            // merkle.apply_context_action(&action).unwrap();
        }

        let stats = merkle.get_merkle_stats().unwrap();

        println!("{}, {}, {}, {}, {}, {}, {}",
                 block.block_level,
                 stats.kv_store_stats.key_bytes,
                 stats.kv_store_stats.value_bytes,
                 stats.kv_store_stats.reused_keys_bytes,
                 stats.kv_store_stats.total_as_bytes(),
                 mem_usage(),
                 merkle.get_block_latency(0).unwrap(),
        );

        let level = block.block_level;

        if level % args.cycle_block_count == 0 && level > 0 {
            merkle.start_new_cycle().unwrap();

            // let commits_iter = cycle_commit_hashes.iter()
            //     .flatten()
            //     .cloned();
            // check_commit_hashes(&merkle, commits_iter).unwrap();

            // cycle_commit_hashes = cycle_commit_hashes.into_iter()
            //     .skip(1)
            //     .chain(vec![vec![]])
            //     .collect();
        }
    }
}