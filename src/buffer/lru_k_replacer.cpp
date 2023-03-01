//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

#include <numeric>

#include "common/exception.h"

namespace bustub {

/* ----------- LRUKNode ----------- */
LRUKNode::LRUKNode(size_t k) : k_(k) {}

void LRUKNode::RecordAccess(time_stamp_t time_stamp) {
  size_t history_size = history_.size();
  assert(history_size <= k_);
  if (history_size == k_) {
    history_.pop_back();
  }
  history_.emplace_front(time_stamp);
}

auto LRUKNode::EarliestTimeStamp() -> time_stamp_t {
  assert(!history_.empty());
  return history_.back();
}

/* ----------- LRUKReplacer ----------- */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock lock(latch_);
  // less than k access
  std::vector<std::pair<frame_id_t, LRUKNode *>> lt_k_access;
  for (auto &[id, node] : node_table_) {
    if (node.Evictable() && !node.KAccess()) {
      lt_k_access.emplace_back(std::make_pair(id, &node));
    }
  }
  time_stamp_t earliest = std::numeric_limits<time_stamp_t>::max();
  if (!lt_k_access.empty()) {
    // The nodes whose access num is less than k but unevictable
    // are filtered out.
    // All nodes in `lt_k_access` are evictable.
    for (const auto &pair : lt_k_access) {
      if (pair.second->EarliestTimeStamp() < earliest) {
        *frame_id = pair.first;
        earliest = pair.second->EarliestTimeStamp();
      }
    }
  } else {
    for (auto &[id, node] : node_table_) {
      if (!node.Evictable()) {
        continue;
      }
      time_stamp_t curr_node_time_stamp = node.EarliestTimeStamp();
      if (curr_node_time_stamp < earliest) {
        *frame_id = id;
        earliest = curr_node_time_stamp;
      }
    }
  }
  if (earliest != std::numeric_limits<time_stamp_t>::max()) {
    node_table_.erase(*frame_id);
    curr_size_--;
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);
  ExamineFrameIdValid(frame_id);
  const auto [it, inserted] = node_table_.try_emplace(frame_id, k_);
  it->second.RecordAccess(current_timestamp_++);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool evictable) {
  std::scoped_lock lock(latch_);
  ExamineFrameIdValid(frame_id);
  auto it = node_table_.find(frame_id);
  if (it == node_table_.end()) {
    return;
  }
  if (it->second.Evictable() && !evictable) {
    it->second.SetEvictable(false);
    curr_size_--;
  } else if (!it->second.Evictable() && evictable) {
    it->second.SetEvictable(true);
    curr_size_++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);
  ExamineFrameIdValid(frame_id);
  auto it = node_table_.find(frame_id);
  if (it == node_table_.end()) {
    return;
  }
  if (it->second.Evictable()) {
    node_table_.erase(it);
    curr_size_--;
  } else {
    throw bustub::Exception("try to remove an unevictable node!");
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock lock(latch_);
  return curr_size_;
}

void LRUKReplacer::ExamineFrameIdValid(frame_id_t frame_id) {
  //   BUSTUB_ASSERT(frame_id <= replacer_size_, "Invalid frame id!");
  assert(frame_id <= static_cast<int32_t>(replacer_size_));
}
}  // namespace bustub
