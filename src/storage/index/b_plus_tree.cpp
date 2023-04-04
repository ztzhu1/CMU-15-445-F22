#include <algorithm>
#include <string>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

static constexpr int NEW_RECORD = 1;
static constexpr int UPDATE_RECORD = 0;

#define DEF_PARENT_PAGE_VAR(child_page_name)                                         \
  auto parent_page_id = child_page_name->GetParentPageId(); /* NOLINT */             \
  auto parent_page = buffer_pool_manager_->FetchPage(parent_page_id);                \
  auto parent_bplus_page = reinterpret_cast<InternalPage *>(parent_page->GetData()); \
  [[maybe_unused]] auto parent_pairs = parent_bplus_page->GetPairs();

#define DEF_SIBLING_PAGE_VAR(page_type, page_name, pos, need_wlatch)                                     \
  auto page_name##_page_id = parent_pairs[pos].second;                                                   \
  auto page_name##_page = buffer_pool_manager_->FetchPage(page_name##_page_id);                          \
  if (need_wlatch) {                                                                                     \
    page_name##_page->WLatch();                                                                          \
  }                                                                                                      \
  auto page_name##_bplus_page = reinterpret_cast<page_type *>(page_name##_page->GetData()); /* NOLINT */ \
  auto page_name##_pairs = page_name##_bplus_page->GetPairs();                                           \
  auto page_name##_size = page_name##_bplus_page->GetSize();

#define DEF_LEFT_PAGE_VAR(page_type, need_wlatch) DEF_SIBLING_PAGE_VAR(page_type, left, pointer_pos - 1, need_wlatch)
#define DEF_RIGHT_PAGE_VAR(page_type, need_wlatch) DEF_SIBLING_PAGE_VAR(page_type, right, pointer_pos + 1, need_wlatch)

#define UNLATCH_UNPIN(page, latch_type, dirty)               \
  do {                                                       \
    auto id = page->GetPageId();                /* NOLINT */ \
    page->latch_type##Unlatch();                /* NOLINT */ \
    buffer_pool_manager_->UnpinPage(id, dirty); /* NOLINT */ \
  } while (0)

#define CLEAR_LOCKED_PAGES(dirty, locked_pages)            \
  do {                                                     \
    for (auto locked_page : locked_pages) { /* NOLINT */   \
      if (locked_page) {                                   \
        UNLATCH_UNPIN(locked_page, W, dirty); /* NOLINT */ \
      } else {                                             \
        fake_parent_of_root_.WUnlock();                    \
      }                                                    \
    }                                                      \
    locked_pages.clear(); /* NOLINT */                     \
  } while (0)

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  std::lock_guard l(test_mu_);
  fake_parent_of_root_.RLock();
  auto page = FindLeafPage(key, false, transaction);
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  int pos = -1;
  bool found = leaf_page->FindKeyIndex(key, pos, comparator_);
  if (found) {
    result->push_back(leaf_page->GetPairs()[pos].second);
  }
  UNLATCH_UNPIN(page, R, false);
  return found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  std::lock_guard l(test_mu_);
  bool ret = false;
  fake_parent_of_root_.RLock();
  if (IsEmpty()) {
    fake_parent_of_root_.RUnlock();
    fake_parent_of_root_.WLock();
    if (IsEmpty()) {
      InitRootAndInsert(key, value, transaction);
      fake_parent_of_root_.WUnlock();
      return true;
    }
    fake_parent_of_root_.WUnlock();
    fake_parent_of_root_.RLock();
  }
  auto page = FindLeafPage(key, true, transaction);
  auto bplus_page = reinterpret_cast<LeafPage *>(page->GetData());
  if (bplus_page->SafeToUpdate(UpdateMode::INSERT)) {
    ret = InsertIntoLeaf(bplus_page, key, value, transaction);
    UNLATCH_UNPIN(page, W, true);
  } else {
    UNLATCH_UNPIN(page, W, false);
    std::vector<Page *> locked_pages;
    page = FindLeafPageSafely(key, locked_pages, UpdateMode::INSERT, transaction);
    bplus_page = reinterpret_cast<LeafPage *>(page->GetData());

    ret = InsertIntoLeaf(bplus_page, key, value, transaction);
    CLEAR_LOCKED_PAGES(true, locked_pages);
  }
  return ret;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  std::lock_guard l(test_mu_);

  fake_parent_of_root_.RLock();
  if (IsEmpty()) {
    fake_parent_of_root_.RUnlock();
    return;
  }

  auto page = FindLeafPage(key, true, transaction);
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  if (leaf_page->SafeToUpdate(UpdateMode::REMOVE)) {
    RemoveFromLeaf(leaf_page, key, transaction);
    UNLATCH_UNPIN(page, W, true);
  } else {
    UNLATCH_UNPIN(page, W, false);
    std::vector<Page *> locked_pages;
    page = FindLeafPageSafely(key, locked_pages, UpdateMode::REMOVE, transaction);
    auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
    RemoveFromLeaf(leaf_page, key, transaction);
    CLEAR_LOCKED_PAGES(true, locked_pages);
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  auto page = FindLeftMostPage();
  return INDEXITERATOR_TYPE(buffer_pool_manager_, &comparator_, page);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto page = FindLeafPage(key, false);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, &comparator_, page, key);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}

/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/*-------- private ---------*/
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InitRootAndInsert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  Page *page = buffer_pool_manager_->NewPage(&root_page_id_);
  page->WLatch();

  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  leaf_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);

  leaf_page->GetPairs()[0] = std::make_pair(key, value);
  leaf_page->IncreaseSize(1);

  UpdateRootPageId(NEW_RECORD);

  UNLATCH_UNPIN(page, W, true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(LeafPage *leaf_page, const KeyType &key, const ValueType &value,
                                    Transaction *transaction) -> bool {
  assert(!leaf_page->IsFull());

  auto data = leaf_page->GetPairs();
  int size = leaf_page->GetSize();
  // find upper bound
  int pos = FindInsertLeafPos(data, size, key);
  if (pos == -1) {
    return false;
  }
  // shift
  if (size > 0) {
    std::memmove(static_cast<void *>(data + pos + 1), static_cast<void *>(data + pos),
                 (size - pos) * sizeof(LeafMappingType));
  }
  // assign
  data[pos] = std::make_pair(key, value);
  leaf_page->IncreaseSize(1);

  if (leaf_page->IsFull()) {
    SplitLeaf(leaf_page, transaction);
  }

  return true;
}

/** `leaf_page` and its parent page (if it has) should acquire w-latch */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitLeaf(LeafPage *leaf_page, Transaction *transaction) {
  assert(leaf_page->IsFull());

  /* prepare pages */
  auto left_page = leaf_page;
  page_id_t parent_page_id;
  page_id_t right_page_id;
  auto right_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&right_page_id)->GetData());
  auto left_page_pairs = left_page->GetPairs();
  auto right_page_pairs = right_page->GetPairs();

  InternalPage *parent_page;
  if (left_page->IsRootPage()) {
    parent_page = NewRootPage(parent_page_id);
    parent_page->GetPairs()[0].second = left_page->GetPageId();
  } else {
    parent_page_id = left_page->GetParentPageId();
    parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());
  }
  /* set parent id */
  left_page->SetParentPageId(parent_page_id);
  right_page->Init(right_page_id, parent_page_id, leaf_max_size_);
  /* get size */
  int left_page_size;
  int right_page_size;
  leaf_page->SizeAfterSplit(left_page_size, right_page_size);
  /* move entries of left page to right page */
  std::memcpy(static_cast<void *>(right_page_pairs), static_cast<void *>(left_page_pairs + left_page_size),
              right_page_size * sizeof(MappingType));
  left_page->IncreaseSize(-right_page_size);
  right_page->IncreaseSize(right_page_size);
  /* link two pages */
  right_page->SetNextPageId(left_page->GetNextPageId());
  left_page->SetNextPageId(right_page_id);
  /* update parent */
  InsertIntoInternal(parent_page, right_page_pairs[0].first, right_page_id, transaction);
  /* unpin pages */
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
  buffer_pool_manager_->UnpinPage(right_page_id, true);

  assert(!left_page->IsFull());
  assert(!right_page->IsFull());
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindInsertLeafPos(LeafMappingType *data, int size, const KeyType &key) -> int {
  int pos = 0;
  int result;
  for (; pos < size; pos++) {
    result = comparator_(key, data[pos].first);
    // duplicate key is not allowed
    if (result == 0) {
      return -1;
    }

    if (result < 0) {
      // key < K[pos], insert at `pos`.
      break;
    }
  }
  return pos;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoInternal(InternalPage *internal_page, KeyType &key, page_id_t value,
                                        Transaction *transaction) -> bool {
  assert(!internal_page->IsFull());

  auto data = internal_page->GetPairs();
  int size = internal_page->GetSize();
  // find upper bound
  int pos = FindInsertInternalPos(data, size, key);
  // shift
  if (size > 1) {
    std::memmove(static_cast<void *>(data + pos + 1), static_cast<void *>(data + pos),
                 (size - pos) * sizeof(InternalMappingType));
  }
  // assign
  data[pos] = std::make_pair(key, value);
  internal_page->IncreaseSize(1);

  if (internal_page->IsFull()) {
    SplitInternal(internal_page, transaction);
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitInternal(InternalPage *internal_page, Transaction *transaction) {
  assert(internal_page->IsFull());

  /* prepare pages */
  auto left_page = internal_page;
  page_id_t parent_page_id;
  page_id_t right_page_id;
  auto right_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&right_page_id)->GetData());
  auto left_page_pairs = left_page->GetPairs();
  auto right_page_pairs = right_page->GetPairs();

  InternalPage *parent_page;
  if (left_page->IsRootPage()) {
    parent_page = NewRootPage(parent_page_id);
    parent_page->GetPairs()[0].second = left_page->GetPageId();
  } else {
    parent_page_id = left_page->GetParentPageId();
    parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());
  }
  /* set parent id */
  left_page->SetParentPageId(parent_page_id);
  right_page->Init(right_page_id, parent_page_id, internal_max_size_);
  /* get size */
  int left_page_size;
  int right_page_size;
  internal_page->SizeAfterSplit(left_page_size, right_page_size);
  /* move entries of left page to right page */
  int lifting_pos = left_page_size;
  right_page_pairs[0].second = left_page_pairs[lifting_pos].second;
  std::memcpy(static_cast<void *>(right_page_pairs + 1), static_cast<void *>(left_page_pairs + lifting_pos + 1),
              (right_page_size - 1) * sizeof(InternalMappingType));
  left_page->SetSize(left_page_size);
  right_page->SetSize(right_page_size);
  /* update parent */
  for (int i = 0; i < right_page_size + 1; i++) {
    auto child_page_id = right_page_pairs[i].second;
    auto child_page = buffer_pool_manager_->FetchPage(child_page_id);
    auto child_bplus_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_bplus_page->SetParentPageId(right_page_id);
    buffer_pool_manager_->UnpinPage(child_page_id, true);
  }
  InsertIntoInternal(parent_page, left_page_pairs[lifting_pos].first, right_page_id, transaction);
  /* unpin pages */
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
  buffer_pool_manager_->UnpinPage(right_page_id, true);

  assert(!left_page->IsFull());
  assert(!right_page->IsFull());
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindInsertInternalPos(InternalMappingType *data, int size, const KeyType &key) -> int {
  int pos = 1;
  int result;
  for (; pos < size; pos++) {
    result = comparator_(key, data[pos].first);
    // duplicate key is not allowed
    assert(result != 0);

    if (result < 0) {
      // key < K[pos], insert at `pos`.
      break;
    }
  }
  return pos;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromLeaf(LeafPage *leaf_page, const KeyType &key, Transaction *transaction) {
  auto size = leaf_page->GetSize();
  auto leaf_page_id = leaf_page->GetPageId();
  auto pairs = leaf_page->GetPairs();
  int pos = -1;
  bool found = leaf_page->FindKeyIndex(key, pos, comparator_);
  if (!found) {
    return;
  }
  assert(pos >= 0);
  assert(pos < size);

  // std::cout << key << ", "  << std::endl;
  // some trivial and non-recursive cases
  if (leaf_page->IsRootPage()) {
    if (size > 1) {
      std::memmove(static_cast<void *>(pairs + pos), static_cast<void *>(pairs + pos + 1),
                   (size - pos - 1) * sizeof(LeafMappingType));
      leaf_page->IncreaseSize(-1);
    } else {
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(UPDATE_RECORD);
    }
  } else if (leaf_page->MoreThanMin()) {
    if (pos == 0) {
      /* we need to update parent's key */
      // DEF_PARENT_PAGE_VAR(leaf_page);
      // TODO(z): lock parent
      // parent_bplus_page->UpdateKey(pairs[0].first, pairs[1].first, comparator_);
      std::memmove(static_cast<void *>(pairs + pos), static_cast<void *>(pairs + pos + 1),
                   (size - pos - 1) * sizeof(LeafMappingType));
      // buffer_pool_manager_->UnpinPage(parent_page_id, true);
    } else if (pos < size - 1) {
      std::memmove(static_cast<void *>(pairs + pos), static_cast<void *>(pairs + pos + 1),
                   (size - pos - 1) * sizeof(LeafMappingType));
    }
    // If pos == size - 1, we don't do anything but decreasing size.
    // (the other situation needs to decrease size, too)
    leaf_page->IncreaseSize(-1);
  } else {
    // difficult
    DEF_PARENT_PAGE_VAR(leaf_page);
    /* borrow or merge, depends on the policy */
    int pointer_pos = parent_bplus_page->FindPointerIndex(leaf_page_id);
    auto policy = GetPolicy(leaf_page, pointer_pos);

    if (policy == Policy::BorrowFromLeft) {
      DEF_LEFT_PAGE_VAR(LeafPage, false);

      std::memmove(static_cast<void *>(pairs + 1), static_cast<void *>(pairs), pos * sizeof(LeafMappingType));
      pairs[0] = left_pairs[left_size - 1];

      left_bplus_page->IncreaseSize(-1);
      parent_pairs[pointer_pos].first = pairs[0].first;

      left_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(left_page_id, true);
    } else if (policy == Policy::BorrowFromRight) {
      DEF_RIGHT_PAGE_VAR(LeafPage, false);

      std::memmove(static_cast<void *>(pairs + pos), static_cast<void *>(pairs + pos + 1),
                   (size - pos - 1) * sizeof(LeafMappingType));
      pairs[size - 1] = right_pairs[0];
      std::memmove(static_cast<void *>(right_pairs), static_cast<void *>(right_pairs + 1),
                   (right_size - 1) * sizeof(LeafMappingType));

      right_bplus_page->IncreaseSize(-1);
      parent_pairs[pointer_pos].first = pairs[0].first;
      parent_pairs[pointer_pos + 1].first = right_pairs[0].first;

      right_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(right_page_id, true);
    } else if (policy == Policy::MergeWithLeft) { /* can't borrow, have to merge */
      DEF_LEFT_PAGE_VAR(LeafPage, false);

      // move pairs of leaf_page to the left sibling.
      // leaf_page itself is useless, but we don't consider deallocing it now.
      std::memmove(static_cast<void *>(left_pairs + left_size), static_cast<void *>(pairs),
                   pos * sizeof(LeafMappingType));
      std::memmove(static_cast<void *>(left_pairs + left_size + pos), static_cast<void *>(pairs + pos + 1),
                   (size - pos - 1) * sizeof(LeafMappingType));

      left_bplus_page->IncreaseSize(size - 1);
      leaf_page->SetSize(0);
      if (transaction != nullptr) {
        // transaction->LockTxn();
        transaction->AddIntoDeletedPageSet(leaf_page_id);
        // transaction->UnlockTxn();
      }
      left_bplus_page->SetNextPageId(leaf_page->GetNextPageId());

      // ok, I did all my tasks, `RemoveFromInternal` should deal
      // with the remaining internal-page-relative tasks.
      left_page->WUnlatch();
      RemoveFromInternal(parent_bplus_page, pointer_pos, transaction);
      buffer_pool_manager_->UnpinPage(left_page_id, true);
    } else if (policy == Policy::MergeWithRight) {
      DEF_RIGHT_PAGE_VAR(LeafPage, false);

      // move pairs of right sibling to the leaf_page.
      // right sibling page is useless but we don't consider deallocing it now.
      std::memmove(static_cast<void *>(pairs + pos), static_cast<void *>(pairs + pos + 1),
                   (size - pos - 1) * sizeof(LeafMappingType));
      std::memmove(static_cast<void *>(pairs + size - 1), static_cast<void *>(right_pairs),
                   (right_size) * sizeof(LeafMappingType));

      leaf_page->IncreaseSize(right_size - 1);
      right_bplus_page->SetSize(0);
      if (transaction != nullptr) {
        // transaction->LockTxn();
        transaction->AddIntoDeletedPageSet(right_page_id);
        // transaction->UnlockTxn();
      }
      leaf_page->SetNextPageId(right_bplus_page->GetNextPageId());

      // ok, I did all my tasks, `RemoveFromInternal` should deal
      // // with the remaining internal-page-relative tasks.
      right_page->WUnlatch();
      RemoveFromInternal(parent_bplus_page, pointer_pos + 1, transaction);
      buffer_pool_manager_->UnpinPage(right_page_id, true);
    } else {
      UNREACHABLE("");
    }
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
  }
}

/**
 * Called when merging the children.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromInternal(InternalPage *internal_page, int pos, Transaction *transaction) {
  auto pairs = internal_page->GetPairs();
  auto size = internal_page->GetSize();
  auto id = internal_page->GetPageId();
  assert(pos < size);

  if (internal_page->MoreThanMin()) {
    std::memmove(static_cast<void *>(pairs + pos), static_cast<void *>(pairs + pos + 1),
                 (size - pos - 1) * sizeof(InternalMappingType));
    internal_page->IncreaseSize(-1);
    return;
  }

  // there is only one key in the root internal page.
  if (internal_page->IsRootPage()) {
    auto child_id = pairs[0].second;
    auto child_page = buffer_pool_manager_->FetchPage(child_id);
    auto child_bplus_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_bplus_page->SetParentPageId(INVALID_PAGE_ID);

    root_page_id_ = child_id;
    UpdateRootPageId(UPDATE_RECORD);

    buffer_pool_manager_->UnpinPage(child_id, true);
  } else {
    DEF_PARENT_PAGE_VAR(internal_page);
    int pointer_pos = parent_bplus_page->FindPointerIndex(id);
    auto policy = GetPolicy(internal_page, pointer_pos);

    if (policy == Policy::BorrowFromLeft) {
      DEF_LEFT_PAGE_VAR(InternalPage, false);

      auto child_of_left_id = left_pairs[left_size - 1].second;

      std::memmove(static_cast<void *>(pairs + 1), static_cast<void *>(pairs), pos * sizeof(InternalMappingType));

      pairs[1].first = parent_pairs[pointer_pos].first;
      pairs[0].second = child_of_left_id;
      parent_pairs[pointer_pos].first = left_pairs[left_size - 1].first;

      left_bplus_page->IncreaseSize(-1);

      auto child_of_left_page = buffer_pool_manager_->FetchPage(child_of_left_id);
      // child_of_left_page->WLatch();
      auto child_of_left_bplus_page = reinterpret_cast<InternalPage *>(child_of_left_page->GetData());
      child_of_left_bplus_page->SetParentPageId(id);

      // child_of_left_page->WUnlatch();
      left_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(child_of_left_id, true);
      buffer_pool_manager_->UnpinPage(left_page_id, true);
    } else if (policy == Policy::BorrowFromRight) {
      DEF_RIGHT_PAGE_VAR(InternalPage, false);

      std::memmove(static_cast<void *>(pairs + pos), static_cast<void *>(pairs + pos + 1),
                   (size - pos - 1) * sizeof(InternalMappingType));

      auto child_of_right_id = right_pairs[0].second;

      pairs[size - 1].first = parent_pairs[pointer_pos + 1].first;
      pairs[size - 1].second = child_of_right_id;
      parent_pairs[pointer_pos + 1].first = right_pairs[1].first;

      std::memmove(static_cast<void *>(right_pairs), static_cast<void *>(right_pairs + 1),
                   (right_size - 1) * sizeof(InternalMappingType));
      right_bplus_page->IncreaseSize(-1);

      auto child_of_right_page = buffer_pool_manager_->FetchPage(child_of_right_id);
      // child_of_right_page->WLatch();
      auto child_of_right_bplus_page = reinterpret_cast<InternalPage *>(child_of_right_page->GetData());
      child_of_right_bplus_page->SetParentPageId(id);

      // child_of_right_page->WUnlatch();
      right_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(child_of_right_id, true);
      buffer_pool_manager_->UnpinPage(right_page_id, true);
    } else if (policy == Policy::MergeWithLeft) {
      DEF_LEFT_PAGE_VAR(InternalPage, false);

      auto child_of_internal_id = pairs[0].second;
      // parent pair sinks.
      left_pairs[left_size].first = parent_pairs[pointer_pos].first;
      left_pairs[left_size].second = child_of_internal_id;
      // move pairs of internal_page to the left sibling.
      // internal_page itself is useless, but we don't consider deallocing it now.
      std::memmove(static_cast<void *>(left_pairs + left_size + 1), static_cast<void *>(pairs + 1),
                   (pos - 1) * sizeof(InternalMappingType));
      std::memmove(static_cast<void *>(left_pairs + left_size + pos), static_cast<void *>(pairs + pos + 1),
                   (size - pos - 1) * sizeof(InternalMappingType));

      left_bplus_page->IncreaseSize(size - 1);
      internal_page->SetSize(0);
      if (transaction != nullptr) {
        // transaction->LockTxn();
        transaction->AddIntoDeletedPageSet(id);
        // transaction->UnlockTxn();
      }
      int new_left_size = left_bplus_page->GetSize();
      for (int i = left_size; i < new_left_size; i++) {
        auto child_id = left_pairs[i].second;
        auto child_page = buffer_pool_manager_->FetchPage(child_id);
        auto child_bplus_page = reinterpret_cast<BPlusTreePage *>(child_page);
        child_bplus_page->SetParentPageId(left_page_id);
        buffer_pool_manager_->UnpinPage(child_id, true);
      }

      left_page->WUnlatch();
      RemoveFromInternal(parent_bplus_page, pointer_pos, transaction);
      buffer_pool_manager_->UnpinPage(left_page_id, true);
    } else if (policy == Policy::MergeWithRight) {
      DEF_RIGHT_PAGE_VAR(InternalPage, false);

      auto child_of_right_id = right_pairs[0].second;
      std::memmove(static_cast<void *>(pairs + pos), static_cast<void *>(pairs + pos + 1),
                   (size - pos - 1) * sizeof(InternalMappingType));
      // parent pair sinks.
      pairs[size - 1].first = parent_pairs[pointer_pos + 1].first;
      pairs[size - 1].second = child_of_right_id;
      // move pairs of right sibling to the internal page.
      // right sibling page is useless, but we don't consider deallocing it now.
      std::memmove(static_cast<void *>(pairs + size), static_cast<void *>(right_pairs + 1),
                   (right_size - 1) * sizeof(InternalMappingType));

      internal_page->IncreaseSize(right_size - 1);
      right_bplus_page->SetSize(0);
      if (transaction != nullptr) {
        // transaction->LockTxn();
        transaction->AddIntoDeletedPageSet(right_page_id);
        // transaction->UnlockTxn();
      }
      int new_size = internal_page->GetSize();
      for (int i = size - 1; i < new_size; i++) {
        auto child_id = pairs[i].second;
        auto child_page = buffer_pool_manager_->FetchPage(child_id);
        auto child_bplus_page = reinterpret_cast<BPlusTreePage *>(child_page);
        child_bplus_page->SetParentPageId(id);
        buffer_pool_manager_->UnpinPage(child_id, true);
      }

      right_page->WUnlatch();
      RemoveFromInternal(parent_bplus_page, pointer_pos + 1, transaction);
      buffer_pool_manager_->UnpinPage(right_page_id, true);
    } else {
      UNREACHABLE("");
    }
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetPolicy(LeafPage *leaf_page, int pointer_pos) -> Policy {
  return GetPolicy(reinterpret_cast<BPlusTreePage *>(leaf_page), pointer_pos);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetPolicy(InternalPage *internal_page, int pointer_pos) -> Policy {
  return GetPolicy(reinterpret_cast<BPlusTreePage *>(internal_page), pointer_pos);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetPolicy(BPlusTreePage *bplus_page, int pointer_pos) -> Policy {
  auto parent_page_id = bplus_page->GetParentPageId();
  auto parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  auto parent_bplus_page = reinterpret_cast<InternalPage *>(parent_page->GetData());
  auto parent_pairs = parent_bplus_page->GetPairs();
  auto policy = Policy::Unknown;

  if (pointer_pos > 0) {
    auto left_page_id = parent_pairs[pointer_pos - 1].second;
    auto *left_page = buffer_pool_manager_->FetchPage(left_page_id);
    left_page->WLatch();
    auto *left_bplus_page = reinterpret_cast<BPlusTreePage *>(left_page->GetData());
    if (left_bplus_page->MoreThanMin()) {
      policy = Policy::BorrowFromLeft;
    } else {
      left_page->WUnlatch();
    }
    buffer_pool_manager_->UnpinPage(left_page_id, false);
  }
  if (policy == Policy::Unknown && pointer_pos < parent_bplus_page->GetSize() - 1) {
    auto right_page_id = parent_pairs[pointer_pos + 1].second;
    auto *right_page = buffer_pool_manager_->FetchPage(right_page_id);
    right_page->WLatch();
    auto *right_bplus_page = reinterpret_cast<BPlusTreePage *>(right_page->GetData());
    if (right_bplus_page->MoreThanMin()) {
      policy = Policy::BorrowFromRight;
    } else {
      right_page->WUnlatch();
    }
    buffer_pool_manager_->UnpinPage(right_page_id, false);
  }
  if (policy == Policy::Unknown && pointer_pos > 0) {
    policy = Policy::MergeWithLeft;
    auto left_page_id = parent_pairs[pointer_pos - 1].second;
    auto *left_page = buffer_pool_manager_->FetchPage(left_page_id);
    left_page->WLatch();
    buffer_pool_manager_->UnpinPage(left_page_id, false);
  }
  if (policy == Policy::Unknown && pointer_pos < parent_bplus_page->GetSize() - 1) {
    policy = Policy::MergeWithRight;
    auto right_page_id = parent_pairs[pointer_pos + 1].second;
    auto *right_page = buffer_pool_manager_->FetchPage(right_page_id);
    right_page->WLatch();
    buffer_pool_manager_->UnpinPage(right_page_id, false);
  }

  buffer_pool_manager_->UnpinPage(parent_page_id, false);
  assert(policy != Policy::Unknown);
  return policy;
}

/** `root_page_id_` should be w-locked by the caller. */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::NewRootPage(page_id_t &root_page_id) -> InternalPage * {
  auto root_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&root_page_id)->GetData());
  root_page->Init(root_page_id, INVALID_PAGE_ID, internal_max_size_);
  root_page_id_ = root_page_id;
  UpdateRootPageId(UPDATE_RECORD);
  return root_page;
}

/** The function may not release `root_page_id_mu_`
 *  if the root page is a leaf page and it's unsafe
 *  to update it.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool need_wlatch_at_leaf, Transaction *transaction) -> Page * {
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto bplus_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (bplus_page->IsLeafPage() && need_wlatch_at_leaf) {
    page->WLatch();
  } else {
    page->RLatch();
  }
  fake_parent_of_root_.RUnlock();

  if (transaction != nullptr) {
    // transaction->LockTxn();
    transaction->AddIntoPageSet(page);
    // transaction->UnlockTxn();
  }

  while (!bplus_page->IsLeafPage()) {
    auto internal_page = reinterpret_cast<InternalPage *>(bplus_page);
    int size = internal_page->GetSize();
    auto pairs = internal_page->GetPairs();
    int pos = 1;
    for (; pos < size; pos++) {
      if (comparator_(key, pairs[pos].first) < 0) {
        break;
      }
    }
    auto next_page = buffer_pool_manager_->FetchPage(pairs[pos - 1].second);
    bplus_page = reinterpret_cast<BPlusTreePage *>(next_page->GetData());
    if (bplus_page->IsLeafPage() && need_wlatch_at_leaf) {
      next_page->WLatch();
    } else {
      next_page->RLatch();
    }
    UNLATCH_UNPIN(page, R, false);
    page = next_page;
  }
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPageSafely(const KeyType &key, std::vector<Page *> &locked_pages, UpdateMode mode,
                                        Transaction *transaction) -> Page * {
  fake_parent_of_root_.WLock();
  locked_pages.push_back(nullptr);
  // std::cout << "key:" << key << " thinks root id is " << root_page_id_ << std::endl;
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);

  auto bplus_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  page->WLatch();
  if (bplus_page->SafeToUpdate(mode)) {
    CLEAR_LOCKED_PAGES(false, locked_pages);
  }
  locked_pages.push_back(page);

  while (!bplus_page->IsLeafPage()) {
    auto internal_page = reinterpret_cast<InternalPage *>(bplus_page);
    int size = internal_page->GetSize();
    auto pairs = internal_page->GetPairs();
    int pos = 1;
    for (; pos < size; pos++) {
      if (comparator_(key, pairs[pos].first) < 0) {
        break;
      }
    }
    auto next_page = buffer_pool_manager_->FetchPage(pairs[pos - 1].second);
    next_page->WLatch();
    bplus_page = reinterpret_cast<BPlusTreePage *>(next_page->GetData());
    if (bplus_page->SafeToUpdate(mode)) {
      CLEAR_LOCKED_PAGES(false, locked_pages);
    }
    locked_pages.push_back(next_page);

    page = next_page;
  }
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeftMostPage() -> Page * {
  fake_parent_of_root_.RLock();
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto bplus_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  page->RLatch();
  fake_parent_of_root_.RUnlock();

  while (!bplus_page->IsLeafPage()) {
    auto internal_page = reinterpret_cast<InternalPage *>(bplus_page);
    auto pairs = internal_page->GetPairs();
    auto next_page = buffer_pool_manager_->FetchPage(pairs[0].second);
    bplus_page = reinterpret_cast<BPlusTreePage *>(next_page->GetData());
    next_page->RLatch();
    UNLATCH_UNPIN(page, R, false);
    page = next_page;
  }
  return page;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 *
 * `root_page_id_` should be w-locked by the caller(or `init_root_id_mu_`
 * is acquired during the initialization).
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  Page *page = buffer_pool_manager_->FetchPage(HEADER_PAGE_ID);
  page->WLatch();
  auto *header_page = static_cast<HeaderPage *>(page);
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
