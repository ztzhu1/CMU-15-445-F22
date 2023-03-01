//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

#define MASK(width) ((1 << (width)) - 1)

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size_));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetIndicesCorespondingTo(std::shared_ptr<Bucket> bucket) const -> std::vector<size_t> {
  std::vector<size_t> indices;
  int local_depth = bucket->GetDepth();
  size_t local_mask = MASK(local_depth);
  size_t local_index = bucket->CurrentLocalIndex();
  local_index |= (1 << (local_depth - 1));
  for (size_t index = 0; index < (1 << global_depth_); index++) {
    if ((index & local_mask) == local_index) {
      indices.push_back(index);
    }
  }
  return indices;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto bucket = FindBucket(key);
  return bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto bucket = FindBucket(key);
  return bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  auto bucket = FindBucket(key);
  if (bucket->IsFull()) {
    RedistributeBucket(bucket);
    bucket = FindBucket(key);
  }
  assert(bucket->Insert(key, value));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  int old_local_depth = bucket->GetDepth();
  assert(old_local_depth <= global_depth_);

  if (old_local_depth < global_depth_) {
    size_t old_local_index = bucket->CurrentLocalIndex();
    size_t new_local_index = old_local_index | (1 << old_local_depth);
    bucket->IncrementDepth();
    int new_local_depth = old_local_depth + 1;
    auto new_bucket = std::make_shared<Bucket>(bucket_size_, new_local_depth);
    num_buckets_++;

    /**
     * I made a huge mistake here. I used to write it as
     * ```
     * auto old_list = bucket->GetItems();
     * ```
     * This seems to construct a new std::list instead of
     * binding returning value reference.
     */
    std::list<std::pair<K, V>> &old_list = bucket->GetItems();
    std::list<std::pair<K, V>> &new_list = new_bucket->GetItems();
    size_t old_list_size = old_list.size();
    auto it = old_list.begin();
    for (size_t i = 0; i < old_list_size; i++) {
      if (bucket->LocalIndexOf(it->first) == new_local_index) {
        new_list.push_back(std::make_pair(it->first, it->second));
        it = old_list.erase(it);
      } else {
        it++;
      }
    }
    // update dir_
    std::vector<size_t> indices_to_be_updated = GetIndicesCorespondingTo(bucket);
    for (const auto index : indices_to_be_updated) {
      dir_[index] = new_bucket;
    }
  } else {  // local depth = global depth
    global_depth_++;
    size_t old_size = dir_.size();
    dir_.reserve(old_size * 2);
    std::copy_n(dir_.begin(), old_size, std::back_inserter(dir_));
    RedistributeBucket(bucket);
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &p : list_) {
    if (p.first == key) {
      value = p.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto it = list_.begin();
  while (it != list_.end()) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
    it++;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (IsFull()) {
    return false;
  }
  for (auto &pair : list_) {
    if (pair.first == key) {
      pair.second = value;
      return true;
    }
  }
  list_.push_back(std::make_pair(key, value));
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::CurrentLocalIndex() -> size_t {
  assert(!list_.empty());
  return LocalIndexOf(list_.front().first);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::LocalIndexOf(const K &key) -> size_t {
  int mask = (1 << depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
