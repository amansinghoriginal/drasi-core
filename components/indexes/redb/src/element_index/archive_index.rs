// Copyright 2025 The Drasi Authors.
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

//! Archive Index Implementation
//!
//! The archive index functionality is implemented directly in the parent
//! `element_index.rs` module as part of `RedbElementIndex`, which implements
//! both `ElementIndex` and `ElementArchiveIndex` traits.
//!
//! This module exists for organizational consistency with the RocksDB backend
//! structure and may contain archive-specific utilities in the future.
