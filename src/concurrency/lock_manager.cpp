//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <cassert>
#include <utility>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  auto txn_id = txn->GetTransactionId();
  auto state = txn->GetState();
  auto level = txn->GetIsolationLevel();

  CheckLockTable(txn, state, level, lock_mode);

  table_lock_map_latch_.lock();
  auto it = table_lock_map_.find(oid);
  if (it == table_lock_map_.end()) {
    table_lock_map_.insert(std::pair(oid, std::make_shared<LockRequestQueue>()));
    it = table_lock_map_.find(oid);
  }
  auto queue = it->second;
  table_lock_map_latch_.unlock();

  std::unique_lock lock(queue->latch_);
  LockRequest *request = nullptr;
  for (const auto r : queue->request_queue_) {
    if (r->txn_id_ == txn_id) {
      assert(r->oid_ == oid);
      request = r;
      break;
    }
  }
  if (request == nullptr) {
    request = new LockRequest(txn_id, lock_mode, oid);
    queue->request_queue_.push_back(request);
  }
  /* 1. request has been granted */
  if (request->granted_) {
    if (request->lock_mode_ == lock_mode) {
      /* trivial */
      return true;
    } else {
      /* needs to upgrade */
      auto old_mode = request->lock_mode_;
      CheckUpgradeTableLock(txn, old_mode, lock_mode);
      if (queue->upgrading_ != INVALID_TXN_ID && queue->upgrading_ != txn_id) {
        // another transaction has already upgraded
        Abort(txn, AbortReason::UPGRADE_CONFLICT);
      }
      // ready to upgrade
      if (CompatibleWithAll(queue, lock_mode, txn_id, true)) {
        auto lock_set = GetTableLockSet(txn, old_mode);
        assert(lock_set->find(oid) != lock_set->end());
        lock_set->erase(oid);
        queue->upgrading_ = txn_id;
        request->lock_mode_ = lock_mode;
        request->granted_ = true;
        RecordTableLock(txn, lock_mode, oid);
        return true;
      }
      // the request needs to wait
      queue->cv_.wait(lock, [&]() { return CompatibleWithAll(queue, lock_mode, txn_id, true); });

      auto lock_set = GetTableLockSet(txn, old_mode);
      assert(lock_set->find(oid) != lock_set->end());
      lock_set->erase(oid);
      queue->upgrading_ = txn_id;
      request->lock_mode_ = lock_mode;
      request->granted_ = true;
      RecordTableLock(txn, lock_mode, oid);
      return true;
    }
  }
  /* 2. request is not granted.
        If the request is compatible with all preceding requests, it can be granted.
        Noting that there is a corner case: there may be one request still can hold
      the lock due to upgrading, even if it's not compatible with the target request
      and behind it. */
  if (CompatibleWithAll(queue, lock_mode, txn_id, false)) {
    request->granted_ = true;
    RecordTableLock(txn, lock_mode, oid);
    return true;
  }
  // the request needs to wait
  queue->cv_.wait(lock, [&]() { return CompatibleWithAll(queue, lock_mode, txn_id, false); });
  request->granted_ = true;
  RecordTableLock(txn, lock_mode, oid);
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  auto txn_id = txn->GetTransactionId();
  auto level = txn->GetIsolationLevel();

  table_lock_map_latch_.lock();
  auto it = table_lock_map_.find(oid);
  if (it == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    Abort(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto queue = it->second;
  table_lock_map_latch_.unlock();

  std::lock_guard lock(queue->latch_);
  auto queue_it = CheckUnlockTable(txn, level, queue, oid);
  auto request = *queue_it;
  assert(request->oid_ == oid);
  assert(request->txn_id_ == txn_id);
  auto lock_mode = request->lock_mode_;
  auto lock_set = GetTableLockSet(txn, lock_mode);

  UpdateState(txn, level, lock_mode);
  if (queue->upgrading_ == txn_id) {
    queue->upgrading_ = INVALID_TXN_ID;
  }
  queue->request_queue_.erase(queue_it);
  lock_set->erase(oid);
  delete request;

  queue->cv_.notify_all();
  return true;  // when should I return false?
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool { return true; }

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

/* ------ private ------ */
void LockManager::CheckLockTable(Transaction *txn, TransactionState state, IsolationLevel level, LockMode mode) {
  assert(txn->GetState() != TransactionState::COMMITTED);
  assert(state == TransactionState::GROWING || state == TransactionState::SHRINKING);
  switch (level) {
    case IsolationLevel::REPEATABLE_READ:
      if (state == TransactionState::SHRINKING) {
        Abort(txn, AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (state == TransactionState::SHRINKING) {
        if (mode != LockMode::INTENTION_SHARED && mode != LockMode::SHARED) {
          Abort(txn, AbortReason::LOCK_ON_SHRINKING);
        }
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      if (mode != LockMode::EXCLUSIVE && mode != LockMode::INTENTION_EXCLUSIVE) {
        Abort(txn, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      if (state == TransactionState::SHRINKING) {
        Abort(txn, AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    default:
      assert(false);
  }
}

void LockManager::CheckUpgradeTableLock(Transaction *txn, LockMode old_mode, LockMode new_mode) {
  assert(txn->GetState() != TransactionState::COMMITTED);
  assert(old_mode != new_mode);
  switch (old_mode) {
    case LockMode::INTENTION_SHARED:
      /* all other modes is allowd */
      break;
    case LockMode::SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
      if (new_mode != LockMode::EXCLUSIVE && new_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
        Abort(txn, AbortReason::INCOMPATIBLE_UPGRADE);
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (new_mode != LockMode::EXCLUSIVE) {
        Abort(txn, AbortReason::INCOMPATIBLE_UPGRADE);
      }
      break;
    default:
      Abort(txn, AbortReason::INCOMPATIBLE_UPGRADE);
  }
}

auto LockManager::CheckUnlockTable(Transaction *txn, IsolationLevel level, std::shared_ptr<LockRequestQueue> queue,
                                   table_oid_t oid) -> std::list<LockRequest *>::iterator {
  /* check whether the txn holds the lock */
  auto it = queue->request_queue_.begin();
  while (it != queue->request_queue_.end()) {
    assert((*it)->oid_ == oid);
    if ((*it)->txn_id_ == txn->GetTransactionId()) {
      break;
    }
    ++it;
  }
  if (it == queue->request_queue_.end()) { /* not found */
    Abort(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto request = *it;
  // If the request is found in the queue, it must be granted, or
  // the transaction should have been blocked in function `LockTable`.
  assert(request->granted_);
  /* Extra check to ensure this granted lock has been recorded before. */
  auto table_lock_set = GetTableLockSet(txn, request->lock_mode_);
  assert(table_lock_set->find(oid) != table_lock_set->end());

  /* It should not hold any row lock */
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();
  if (s_row_lock_set->find(oid) != s_row_lock_set->end()) {
    Abort(txn, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  if (x_row_lock_set->find(oid) != x_row_lock_set->end()) {
    Abort(txn, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  return it;
}

void LockManager::RecordTableLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  auto lock_set = GetTableLockSet(txn, lock_mode);
  assert(lock_set->find(oid) == lock_set->end());
  lock_set->insert(oid);
}

auto LockManager::GetTableLockSet(Transaction *txn, LockMode lock_mode)
    -> std::shared_ptr<std::unordered_set<table_oid_t>> {
  std::shared_ptr<std::unordered_set<table_oid_t>> lock_set;
  switch (lock_mode) {
    case LockMode::SHARED:
      lock_set = txn->GetSharedTableLockSet();
      break;
    case LockMode::EXCLUSIVE:
      lock_set = txn->GetExclusiveTableLockSet();
      break;
    case LockMode::INTENTION_SHARED:
      lock_set = txn->GetIntentionSharedTableLockSet();
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      lock_set = txn->GetIntentionExclusiveTableLockSet();
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      lock_set = txn->GetSharedIntentionExclusiveTableLockSet();
      break;
    default:
      assert(false);
  }
  return lock_set;
}

/** only for table lock */
auto LockManager::Compatible(LockMode a, LockMode b) -> bool {
  switch (a) {
    case LockMode::SHARED:
      return b == LockMode::SHARED || b == LockMode::INTENTION_SHARED;
    case LockMode::EXCLUSIVE:
      return false;
    case LockMode::INTENTION_SHARED:
      return b != LockMode::EXCLUSIVE;
    case LockMode::INTENTION_EXCLUSIVE:
      return b == LockMode::INTENTION_EXCLUSIVE || b == LockMode::INTENTION_SHARED;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return b == LockMode::INTENTION_SHARED;
    default:
      assert(false);
  }
  return false;
}

auto LockManager::CompatibleWithAll(std::shared_ptr<LockRequestQueue> queue, LockMode lock_mode, txn_id_t txn_id,
                                    bool upgrade) -> bool {
  bool comp_with_all = true;
  auto it = queue->request_queue_.begin();
  while (it != queue->request_queue_.end()) {
    if ((*it)->txn_id_ == txn_id) {
      break;
    }
    if (!Compatible((*it)->lock_mode_, lock_mode)) {
      if (!upgrade || (*it)->granted_) {
        comp_with_all = false;
        break;
      }
    }
    ++it;
  }
  assert(it != queue->request_queue_.end());
  if (upgrade) {
    return comp_with_all;
  }

  if (comp_with_all && queue->upgrading_ != INVALID_TXN_ID && queue->upgrading_ != txn_id) {
    /* there is some other transaction which is upgraded */
    while (it != queue->request_queue_.end()) {
      if ((*it)->txn_id_ == queue->upgrading_) {
        assert((*it)->granted_);
        if (!Compatible((*it)->lock_mode_, lock_mode)) {
          comp_with_all = false;
          break;
        }
      }
      ++it;
    }
    assert(it != queue->request_queue_.end());
  }
  return comp_with_all;
}

void LockManager::Abort(Transaction *txn, AbortReason reason) {
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), reason);
}

void LockManager::UpdateState(Transaction *txn, IsolationLevel level, LockMode lock_mode) {
  if (txn->GetState() == TransactionState::COMMITTED) {
    return;
  }
  if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) {
    return;
  }
  switch (level) {
    case IsolationLevel::REPEATABLE_READ:
      txn->SetState(TransactionState::SHRINKING);
      break;
    case IsolationLevel::READ_COMMITTED:
      if (lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      assert(lock_mode == LockMode::EXCLUSIVE);
      txn->SetState(TransactionState::SHRINKING);
      break;
    default:
      assert(false);
  }
}

}  // namespace bustub
