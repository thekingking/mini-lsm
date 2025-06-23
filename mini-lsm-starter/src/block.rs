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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Bytes;
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut encoded = Vec::with_capacity(
            2 + // Number of offsets
            self.offsets.len() * 2 + // Offsets size
            self.data.len(), // Data size
        );
        encoded.extend_from_slice(&self.data);

        // Write the offsets
        for &offset in &self.offsets {
            encoded.extend_from_slice(&offset.to_le_bytes());
        }

        // Write the data
        encoded.extend_from_slice(&(self.offsets.len() as u16).to_be_bytes());

        Bytes::from(encoded)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        // Read the number of offsets
        if data.len() < 2 {
            panic!("Data too short to contain offsets");
        }
        let num_of_elements =
            u16::from_be_bytes([data[data.len() - 2], data[data.len() - 1]]) as usize;

        let offset_start = data.len() - 2 - num_of_elements * 2;
        let offset_end = data.len() - 2;
        let offsets_data = &data[offset_start..offset_end];
        let mut offsets = Vec::new();
        for i in 0..num_of_elements {
            let offset_bytes = &offsets_data[i * 2..i * 2 + 2];
            let offset = u16::from_le_bytes([offset_bytes[0], offset_bytes[1]]);
            offsets.push(offset);
        }

        Block {
            data: data[..offset_start].to_vec(),
            offsets,
        }
    }
}
