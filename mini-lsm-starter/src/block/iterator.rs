// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        if self.is_valid() {
            let (start, end) = self.value_range;
            let value_start = start + 2 + self.key.len() + 2;
            let value_end = end; // end is already the end of the value
            &self.block.data[value_start..value_end]
        } else {
            &[]
        }
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.idx = 0;
        if self.block.offsets.is_empty() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }

        // Update the value range
        let offset = self.block.offsets[self.idx] as usize;
        let next_offset = if self.idx + 1 < self.block.offsets.len() {
            self.block.offsets[self.idx + 1] as usize
        } else {
            self.block.data.len()
        };
        self.value_range = (offset, next_offset);

        // Update the key
        let key_len = u16::from_le_bytes([self.block.data[offset], self.block.data[offset + 1]]);
        let key_start = offset + 2;
        let key_end = key_start + key_len as usize;
        self.key
            .set_from_slice(KeySlice::from_slice(&self.block.data[key_start..key_end]));
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if !self.is_valid() {
            return;
        }
        // If the current index is the last one, we should invalidate the iterator
        self.idx += 1;
        if self.idx == self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }

        // Update the value range
        let offset = self.block.offsets[self.idx] as usize;
        let next_offset = if self.idx + 1 < self.block.offsets.len() {
            self.block.offsets[self.idx + 1] as usize
        } else {
            self.block.data.len()
        };
        self.value_range = (offset, next_offset);

        // Update the key
        let key_len = u16::from_le_bytes([self.block.data[offset], self.block.data[offset + 1]]);
        let key_start = offset + 2;
        let key_end = key_start + key_len as usize;
        self.key
            .set_from_slice(KeySlice::from_slice(&self.block.data[key_start..key_end]));
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        self.seek_to_first();
        while self.is_valid() && self.key.as_key_slice() < key {
            self.next();
        }
    }
}
