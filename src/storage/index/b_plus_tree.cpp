#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

#define UNLATCH_ALL()                                                          \
  do {                                                                         \
    auto page_set = transaction->GetPageSet();                                 \
    while (!page_set->empty()) {                                               \
      if (page_set->front() == nullptr) {                                      \
        fake_root_latch_.WUnlock();                                            \
      } else {                                                                 \
        page_set->front()->WUnlatch();                                         \
        buffer_pool_manager_->UnpinPage(page_set->front()->GetPageId(), true); \
      }                                                                        \
      page_set->pop_front();                                                   \
    }                                                                          \
  } while (0)

#define UNLATCH_UNTIL(page_id)                   \
  do {                                           \
    auto page_set = transaction->GetPageSet();   \
    bool found = false;                          \
    while (!page_set->empty()) {                 \
      assert(page_set->back() != nullptr);       \
      auto id = page_set->back()->GetPageId();   \
      if (id == (page_id)) {                     \
        found = true;                            \
      }                                          \
      page_set->back()->WUnlatch();              \
      buffer_pool_manager_->UnpinPage(id, true); \
      page_set->pop_back();                      \
      if (found) {                               \
        break;                                   \
      }                                          \
    }                                            \
    assert(found);                               \
  } while (0)

namespace bustub {
template <typename BPlusPageType>
inline auto ToBPlusPage(Page *page) -> BPlusPageType * {
  return reinterpret_cast<BPlusPageType *>(page->GetData());
}

template <typename PairType>
inline void Memmove(PairType *dst, PairType *src, int len) {
  std::memmove(static_cast<void *>(dst), static_cast<void *>(src), len * sizeof(PairType));  // NOLINT
}

template <typename PairType>
inline void Memcpy(PairType *dst, PairType *src, int len) {
  std::memcpy(static_cast<void *>(dst), static_cast<void *>(src), len * sizeof(PairType));  // NOLINT
}

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
  // std::lock_guard g(test_mu_);
  auto page_opt = FindLeafReadOnly(key, transaction);
  if (!page_opt.has_value()) {
    // empty
    fake_root_latch_.RUnlock();
    return false;
  }
  auto *page = page_opt.value();
  auto *leaf = ToBPlusPage<LeafPage>(page);
  auto size = leaf->GetSize();
  auto *pairs = leaf->GetPairs();
  int i = 0;
  for (; i < size; ++i) {
    if (Equal(key, pairs[i].first)) {
      result->push_back(pairs[i].second);
      break;
    }
  }
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  return i < size;
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
  // std::lock_guard g(test_mu_);
  auto mode = UpdateMode::Insert;
  auto leaf_opt = FindLeaf(key, transaction);
  if (!leaf_opt.has_value()) {
    /* empty */
    fake_root_latch_.RUnlock();
    leaf_opt = FindLeaf(key, mode, transaction);
    if (!leaf_opt.has_value()) {
      /* still empty */
      Page *root_page = buffer_pool_manager_->NewPage(&root_page_id_);
      assert(root_page != nullptr);
      UpdateRootPageId(1);

      auto *leaf = ToBPlusPage<LeafPage>(root_page);
      leaf->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
      auto *pairs = leaf->GetPairs();
      pairs[0] = std::make_pair(key, value);
      leaf->IncreaseSize(1);
      // buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
      UNLATCH_ALL();
      return true;
    }
  } else if (!leaf_opt.value()->SafeTo(mode)) {
    UNLATCH_ALL();
    leaf_opt = FindLeaf(key, mode, transaction);
    assert(leaf_opt.has_value());
  }

  /* Invariant: we have locked all unsafe pages. */
  auto *leaf = leaf_opt.value();
  /* TODO(ztzhu): We will do the exactly same scanning in `InsertInLeaf`.
     This can be optimized. */
  for (int i = 0; i < leaf->GetSize(); ++i) {
    if (Equal(key, leaf->GetPairs()[i].first)) {
      // buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
      UNLATCH_ALL();
      return false;
    }
  }
  /* key not exists */
  if (leaf->SafeToInsert()) {
    /* not full after insertion */
    bool successs = InsertInLeaf(leaf, key, value, transaction);
    assert(successs);
    // buffer_pool_manager_->UnpinPage(leaf->GetPageId(), successs);
    UNLATCH_ALL();
    return successs;
  }
  /* needs to split */
  auto size = leaf->GetSize();
  auto max_size = leaf->GetMaxSize();
  assert(size == max_size - 1);
  // create new leaf
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  assert(new_page != nullptr);
  auto *new_leaf = ToBPlusPage<LeafPage>(new_page);
  new_leaf->Init(new_page_id, leaf->GetParentPageId(), leaf_max_size_);
  // find inserting position
  auto *old_pairs = leaf->GetPairs();
  auto *new_pairs = new_leaf->GetPairs();
  int i = 0;
  for (; i < size; ++i) {
    if (Equal(key, old_pairs[i].first)) {
      return false;
    }
    if (LT(key, old_pairs[i].first)) {
      break;
    }
  }
  // move entries
  std::vector<LeafPairType> temp_pairs(size + 1);
  std::copy(old_pairs, old_pairs + i, temp_pairs.begin());
  std::copy(old_pairs + i, old_pairs + size, temp_pairs.begin() + i + 1);
  temp_pairs[i] = std::make_pair(key, value);

  auto new_leaf_size = max_size / 2;
  auto old_leaf_size = max_size - new_leaf_size;
  std::copy(temp_pairs.begin(), temp_pairs.begin() + old_leaf_size, old_pairs);
  std::copy(temp_pairs.begin() + old_leaf_size, temp_pairs.end(), new_pairs);
  // update size
  leaf->SetSize(old_leaf_size);
  new_leaf->SetSize(new_leaf_size);
  // update link
  new_leaf->SetNextPageId(leaf->GetNextPageId());
  leaf->SetNextPageId(new_leaf->GetPageId());
  // update parent internal page
  InsertInParent(reinterpret_cast<BPlusTreePage *>(leaf), new_pairs[0].first,
                 reinterpret_cast<BPlusTreePage *>(new_leaf), transaction);
  UNLATCH_ALL();
  return true;
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
  // std::lock_guard g(test_mu_);
  auto mode = UpdateMode::Remove;
  auto leaf_opt = FindLeaf(key, transaction);
  if (!leaf_opt.has_value()) {
    /* empty */
    fake_root_latch_.RUnlock();
    return;
  }
  if (!leaf_opt.value()->SafeTo(mode)) {
    UNLATCH_ALL();
    leaf_opt = FindLeaf(key, mode, transaction);
  }
  /* Invariant: we have locked all unsafe pages. */
  RemoveEntry(reinterpret_cast<BPlusTreePage *>(leaf_opt.value()), key, transaction);
  UNLATCH_ALL();
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  auto *page = buffer_pool_manager_->FetchPage(root_page_id_);
  assert(page != nullptr);
  auto *bplus_page = ToBPlusPage<BPlusTreePage>(page);
  while (!bplus_page->IsLeafPage()) {
    auto *internal_page = reinterpret_cast<InternalPage *>(bplus_page);
    auto id = internal_page->GetPairs()[0].second;
    buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), false);
    page = buffer_pool_manager_->FetchPage(id);
    assert(page != nullptr);
    bplus_page = ToBPlusPage<BPlusTreePage>(page);
  }
  return INDEXITERATOR_TYPE(buffer_pool_manager_, reinterpret_cast<LeafPage *>(bplus_page), 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto page_opt = FindLeafReadOnly(key);
  if (!page_opt.has_value()) {
    fake_root_latch_.RUnlock();
    return INDEXITERATOR_TYPE();
  }
  auto *page = page_opt.value();
  page->RUnlatch();  // This function is for testing, latch is unnecessary.
  auto *leaf = ToBPlusPage<LeafPage>(page);
  auto size = leaf->GetSize();
  int i = 0;
  for (; i < size; ++i) {
    if (Equal(key, leaf->GetPairs()[i].first)) {
      break;
    }
  }
  assert(i < size);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf, i);
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

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  assert(header_page != nullptr);
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/** caller is responsible for unlatching and unpinning leaf page  */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafReadOnly(const KeyType &key, Transaction *transaction) -> std::optional<Page *> {
  fake_root_latch_.RLock();
  if (IsEmpty()) {
    return std::nullopt;
  }
  auto id = root_page_id_;
  Page *page = buffer_pool_manager_->FetchPage(id);
  assert(page != nullptr);
  page->RLatch();
  fake_root_latch_.RUnlock();
  auto *bplus_page = ToBPlusPage<BPlusTreePage>(page);
  while (!bplus_page->IsLeafPage()) {
    auto size = bplus_page->GetSize();
    auto *internal = reinterpret_cast<InternalPage *>(bplus_page);
    auto *pairs = internal->GetPairs();
    int i = 1;
    for (; i < size; ++i) {
      if (LT(key, pairs[i].first)) {
        break;
      }
    }
    auto next_id = pairs[i - 1].second;

    auto *next_page = buffer_pool_manager_->FetchPage(next_id);
    assert(next_page != nullptr);
    next_page->RLatch();
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(id, true);
    bplus_page = ToBPlusPage<BPlusTreePage>(next_page);
    id = next_id;
    page = next_page;
  }
  return page;
}

/** caller is responsible for unlatching and unpinning leaf page  */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key, Transaction *transaction) -> std::optional<LeafPage *> {
  assert(transaction != nullptr);
  fake_root_latch_.RLock();
  if (IsEmpty()) {
    return std::nullopt;
  }
  auto id = root_page_id_;
  Page *page = buffer_pool_manager_->FetchPage(id);
  assert(page != nullptr);
  auto *bplus_page = ToBPlusPage<BPlusTreePage>(page);
  if (!bplus_page->IsLeafPage()) {
    page->RLatch();
  } else {
    page->WLatch();
  }
  fake_root_latch_.RUnlock();
  while (!bplus_page->IsLeafPage()) {
    auto size = bplus_page->GetSize();
    auto *internal = reinterpret_cast<InternalPage *>(bplus_page);
    auto *pairs = internal->GetPairs();
    int i = 1;
    for (; i < size; ++i) {
      if (LT(key, pairs[i].first)) {
        break;
      }
    }
    auto next_id = pairs[i - 1].second;

    auto *next_page = buffer_pool_manager_->FetchPage(next_id);
    assert(next_page != nullptr);
    bplus_page = ToBPlusPage<BPlusTreePage>(next_page);
    if (!bplus_page->IsLeafPage()) {
      next_page->RLatch();
    } else {
      next_page->WLatch();
    }
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(id, true);
    id = next_id;
    page = next_page;
  }
  transaction->AddIntoPageSet(page);
  return reinterpret_cast<LeafPage *>(bplus_page);
}

/** caller is responsible for unlatching and unpinning leaf page  */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key, const UpdateMode mode, Transaction *transaction)
    -> std::optional<LeafPage *> {
  assert(transaction != nullptr);
  fake_root_latch_.WLock();
  transaction->AddIntoPageSet(nullptr);
  if (IsEmpty()) {
    return std::nullopt;
  }
  auto id = root_page_id_;
  Page *page = buffer_pool_manager_->FetchPage(id);
  assert(page != nullptr);
  page->WLatch();
  auto *bplus_page = ToBPlusPage<BPlusTreePage>(page);
  if (bplus_page->SafeTo(mode)) {
    UNLATCH_ALL();
  }
  transaction->AddIntoPageSet(page);

  while (!bplus_page->IsLeafPage()) {
    auto size = bplus_page->GetSize();
    auto *internal = reinterpret_cast<InternalPage *>(bplus_page);
    auto *pairs = internal->GetPairs();
    int i = 1;
    for (; i < size; ++i) {
      if (LT(key, pairs[i].first)) {
        break;
      }
    }
    auto next_id = pairs[i - 1].second;

    auto *next_page = buffer_pool_manager_->FetchPage(next_id);
    assert(next_page != nullptr);
    next_page->WLatch();
    bplus_page = ToBPlusPage<BPlusTreePage>(next_page);
    if (bplus_page->SafeTo(mode)) {
      UNLATCH_ALL();
    }
    transaction->AddIntoPageSet(next_page);
    id = next_id;
    page = next_page;
  }
  return reinterpret_cast<LeafPage *>(bplus_page);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInLeaf(LeafPage *leaf, const KeyType &key, const ValueType &value, Transaction *transaction)
    -> bool {
  assert(leaf->SafeToInsert());
  auto *pairs = leaf->GetPairs();
  auto size = leaf->GetSize();
  int i = 0;
  for (; i < size; ++i) {
    if (Equal(key, pairs[i].first)) {
      return false;
    }
    if (LT(key, pairs[i].first)) {
      break;
    }
  }
  Memmove(pairs + i + 1, pairs + i, size - i);
  pairs[i] = std::make_pair(key, value);
  leaf->IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(BPlusTreePage *left_child, const KeyType &key, BPlusTreePage *right_child,
                                    Transaction *transaction) {
  if (left_child->IsRootPage()) {
    Page *new_root_page = buffer_pool_manager_->NewPage(&root_page_id_);
    assert(new_root_page != nullptr);
    UpdateRootPageId(0);
    auto *new_root_internal = ToBPlusPage<InternalPage>(new_root_page);
    new_root_internal->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    auto *pairs = new_root_internal->GetPairs();
    pairs[0].second = left_child->GetPageId();
    pairs[1] = std::make_pair(key, right_child->GetPageId());
    new_root_internal->SetSize(2);
    left_child->SetParentPageId(root_page_id_);
    right_child->SetParentPageId(root_page_id_);
    // buffer_pool_manager_->UnpinPage(left_child->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(right_child->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return;
  }
  /* not root page */
  auto parent_id = left_child->GetParentPageId();
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  assert(parent_page != nullptr);
  auto *parent_internal = ToBPlusPage<InternalPage>(parent_page);
  auto *parent_pairs = parent_internal->GetPairs();
  auto parent_size = parent_internal->GetSize();
  if (parent_internal->SafeToInsert()) {
    /* insert into parent directly */
    // find inserting position
    int i = 0;
    for (; i < parent_size; ++i) {
      if (parent_pairs[i].second == left_child->GetPageId()) {
        break;
      }
    }
    assert(i < parent_size);
    ++i;
    Memmove(parent_pairs + i + 1, parent_pairs + i, parent_size - i);
    parent_pairs[i] = std::make_pair(key, right_child->GetPageId());
    parent_internal->IncreaseSize(1);
    assert(left_child->GetParentPageId() == parent_id);
    right_child->SetParentPageId(parent_id);
    // buffer_pool_manager_->UnpinPage(left_child->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(right_child->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_id, true);
  } else {
    /* needs to split */
    auto max_size = parent_internal->GetMaxSize();
    assert(parent_size == max_size);
    // create new internal page
    page_id_t new_internal_page_id;
    Page *new_internal_page = buffer_pool_manager_->NewPage(&new_internal_page_id);
    assert(new_internal_page != nullptr);
    auto *new_internal = ToBPlusPage<InternalPage>(new_internal_page);
    new_internal->Init(new_internal_page_id, INVALID_PAGE_ID, internal_max_size_);
    // find inserting position
    auto *old_pairs = parent_internal->GetPairs();
    auto *new_pairs = new_internal->GetPairs();
    int i = 0;
    for (; i < parent_size; ++i) {
      if (parent_pairs[i].second == left_child->GetPageId()) {
        break;
      }
    }
    assert(i < parent_size);
    ++i;
    // move entries
    std::vector<IntPairType> temp_pairs(parent_size + 1);
    std::copy(old_pairs, old_pairs + i, temp_pairs.begin());
    std::copy(old_pairs + i, old_pairs + parent_size, temp_pairs.begin() + i + 1);
    temp_pairs[i] = std::make_pair(key, right_child->GetPageId());
    auto old_internal_size = (max_size + 1) / 2;
    auto new_internal_size = max_size + 1 - old_internal_size;
    std::copy(temp_pairs.begin(), temp_pairs.begin() + old_internal_size, old_pairs);
    std::copy(temp_pairs.begin() + old_internal_size, temp_pairs.end(), new_pairs);
    // update size
    parent_internal->SetSize(old_internal_size);
    new_internal->SetSize(new_internal_size);
    // update children's parents
    for (i = 0; i < old_internal_size; ++i) {
      auto *child_page = buffer_pool_manager_->FetchPage(old_pairs[i].second);
      assert(child_page != nullptr);
      auto *child_bplus_page = ToBPlusPage<BPlusTreePage>(child_page);
      child_bplus_page->SetParentPageId(parent_internal->GetPageId());
      buffer_pool_manager_->UnpinPage(child_bplus_page->GetPageId(), true);
    }
    for (i = 0; i < new_internal_size; ++i) {
      auto *child_page = buffer_pool_manager_->FetchPage(new_pairs[i].second);
      assert(child_page != nullptr);
      auto *child_bplus_page = ToBPlusPage<BPlusTreePage>(child_page);
      child_bplus_page->SetParentPageId(new_internal->GetPageId());
      buffer_pool_manager_->UnpinPage(child_bplus_page->GetPageId(), true);
    }

    // buffer_pool_manager_->UnpinPage(left_child->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(right_child->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_id, true);
    UNLATCH_UNTIL(left_child->GetPageId());
    InsertInParent(reinterpret_cast<BPlusTreePage *>(parent_internal), new_pairs[0].first,
                   reinterpret_cast<BPlusTreePage *>(new_internal), transaction);
  }
}

/** callee(i.e. this function) is responsible for unpinning bplus page  */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveEntry(BPlusTreePage *bplus_page, const KeyType &key, Transaction *transaction) {
  assert(bplus_page != nullptr);
  int size = bplus_page->GetSize();
  int i = 0;
  LeafPairType *leaf_pairs = nullptr;
  IntPairType *int_pairs = nullptr;
  if (bplus_page->IsLeafPage()) {
    i = 0;
    leaf_pairs = reinterpret_cast<LeafPage *>(bplus_page)->GetPairs();
  } else {
    i = 1;
    int_pairs = reinterpret_cast<InternalPage *>(bplus_page)->GetPairs();
  }
  for (; i < size; ++i) {                                                                   // NOLINT
    if (Equal(key, bplus_page->IsLeafPage() ? leaf_pairs[i].first : int_pairs[i].first)) {  // NOLINT
      break;
    }
  }
  if (i >= size) {
    // key doesn't exist
    // buffer_pool_manager_->UnpinPage(bplus_page->GetPageId(), true);
    return;
  }
  if (bplus_page->SafeToRemove()) {
    /* easy */
    if (bplus_page->IsLeafPage()) {
      Memmove(leaf_pairs + i, leaf_pairs + i + 1, size - i - 1);
    } else {
      Memmove(int_pairs + i, int_pairs + i + 1, size - i - 1);
    }
    bplus_page->DecreaseSize(1);
    // buffer_pool_manager_->UnpinPage(bplus_page->GetPageId(), true);
    return;
  }
  /* needs to borrow or merge */
  if (bplus_page->IsRootPage()) {
    if (bplus_page->IsLeafPage()) {
      /* the tree will be empty after deletion */
      assert(size == 1);
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(0);
    } else {
      assert(size == 2);
      auto child_id = int_pairs[0].second;  // NOLINT
      auto *child_page = buffer_pool_manager_->FetchPage(child_id);
      assert(child_page != nullptr);
      auto *child_bplus = ToBPlusPage<BPlusTreePage>(child_page);
      child_bplus->SetParentPageId(INVALID_PAGE_ID);
      root_page_id_ = child_id;
      UpdateRootPageId(0);
      buffer_pool_manager_->UnpinPage(child_id, true);
    }
    // buffer_pool_manager_->UnpinPage(bplus_page->GetPageId(), true);
    return;
  }
  /* not root page */
  auto bplus_id = bplus_page->GetPageId();
  auto parent_id = bplus_page->GetParentPageId();
  auto *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  assert(parent_page != nullptr);
  auto *parent_int = ToBPlusPage<InternalPage>(parent_page);
  auto *parent_pairs = parent_int->GetPairs();
  auto parent_size = parent_int->GetSize();
  int j = 0;
  for (; j < parent_size; ++j) {
    if (bplus_id == parent_pairs[j].second) {
      break;
    }
  }
  assert(j < parent_size);
  if (j > 0) {
    auto sibling_id = parent_pairs[j - 1].second;
    auto *sibling_page = buffer_pool_manager_->FetchPage(sibling_id);
    assert(sibling_page != nullptr);
    sibling_page->WLatch();
    auto *sibling_bplus = ToBPlusPage<BPlusTreePage>(sibling_page);
    if (sibling_bplus->SafeToRemove()) {
      /* borrow from left */
      // printf("%d borrows from %d\n", bplus_page->GetPageId(), sibling_bplus->GetPageId());
      if (sibling_bplus->IsInternalPage()) {
        auto *sibling_int = reinterpret_cast<InternalPage *>(sibling_bplus);
        auto *sibling_pairs = sibling_int->GetPairs();
        int m = sibling_int->GetSize() - 1;
        Memmove(int_pairs + 1, int_pairs, i);
        int_pairs[0].second = sibling_pairs[m].second;
        int_pairs[1].first = parent_pairs[j].first;
        parent_pairs[j].first = sibling_pairs[m].first;
        sibling_int->DecreaseSize(1);
        auto *child_page = buffer_pool_manager_->FetchPage(sibling_pairs[m].second);
        assert(child_page != nullptr);
        auto *child_bplus = ToBPlusPage<BPlusTreePage>(child_page);
        child_bplus->SetParentPageId(bplus_page->GetPageId());
        buffer_pool_manager_->UnpinPage(child_bplus->GetPageId(), true);
      } else { /* leaf page */
        auto *sibling_leaf = reinterpret_cast<LeafPage *>(sibling_bplus);
        auto *sibling_pairs = sibling_leaf->GetPairs();
        int m = sibling_leaf->GetSize() - 1;
        Memmove(leaf_pairs + 1, leaf_pairs, i);
        leaf_pairs[0].first = sibling_pairs[m].first;
        leaf_pairs[0].second = sibling_pairs[m].second;
        parent_pairs[j].first = sibling_pairs[m].first;
        sibling_leaf->DecreaseSize(1);
      }
      // need to release sibling here. If we just add it
      // into page set of transaction, dead lock should occur
      // because we may want to borrow from this sibling
      // the second time.
      sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(sibling_id, true);
      // buffer_pool_manager_->UnpinPage(bplus_id, true);
      buffer_pool_manager_->UnpinPage(parent_id, true);
    } else {
      /* merge with left */
      // printf("%d merges into %d\n", bplus_page->GetPageId(), sibling_bplus->GetPageId());
      if (sibling_bplus->IsInternalPage()) {
        auto *sibling_int = reinterpret_cast<InternalPage *>(sibling_bplus);
        auto *sibling_pairs = sibling_int->GetPairs();
        int m = sibling_int->GetSize();
        Memcpy(sibling_pairs + m, int_pairs, i);
        Memcpy(sibling_pairs + m + i, int_pairs + i + 1, size - i - 1);
        sibling_pairs[m].first = parent_pairs[j].first;
        bplus_page->DecreaseSize(size);
        sibling_int->IncreaseSize(size - 1);
        for (int k = m; k < m + size - 1; ++k) {
          auto *child_page = buffer_pool_manager_->FetchPage(sibling_pairs[k].second);
          assert(child_page != nullptr);
          auto *child_bplus = ToBPlusPage<BPlusTreePage>(child_page);
          child_bplus->SetParentPageId(sibling_int->GetPageId());
          buffer_pool_manager_->UnpinPage(child_bplus->GetPageId(), true);
        }
      } else { /* leaf page */
        auto *sibling_leaf = reinterpret_cast<LeafPage *>(sibling_bplus);
        auto *sibling_pairs = sibling_leaf->GetPairs();
        int m = sibling_leaf->GetSize();
        Memcpy(sibling_pairs + m, leaf_pairs, i);
        Memcpy(sibling_pairs + m + i, leaf_pairs + i + 1, size - i - 1);
        bplus_page->DecreaseSize(size);
        sibling_leaf->IncreaseSize(size - 1);
        sibling_leaf->SetNextPageId(reinterpret_cast<LeafPage *>(bplus_page)->GetNextPageId());
      }
      sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(sibling_id, true);
      buffer_pool_manager_->UnpinPage(parent_id, true);
      // buffer_pool_manager_->UnpinPage(bplus_id, true);
      UNLATCH_UNTIL(bplus_id);
      transaction->AddIntoDeletedPageSet(bplus_id);
      RemoveEntry(ToBPlusPage<BPlusTreePage>(parent_page), parent_pairs[j].first, transaction);
    }
  } else {
    auto sibling_id = parent_pairs[j + 1].second;
    auto *sibling_page = buffer_pool_manager_->FetchPage(sibling_id);
    assert(sibling_page != nullptr);
    sibling_page->WLatch();
    auto *sibling_bplus = ToBPlusPage<BPlusTreePage>(sibling_page);
    if (sibling_bplus->SafeToRemove()) {
      /* borrow from right */
      // printf("%d borrows from %d\n", bplus_page->GetPageId(), sibling_bplus->GetPageId());
      if (sibling_bplus->IsInternalPage()) {
        auto *sibling_int = reinterpret_cast<InternalPage *>(sibling_bplus);
        auto *sibling_pairs = sibling_int->GetPairs();
        Memmove(int_pairs + i, int_pairs + i + 1, size - i - 1);
        int_pairs[size - 1].first = parent_pairs[j + 1].first;
        int_pairs[size - 1].second = sibling_pairs[0].second;
        parent_pairs[j + 1].first = sibling_pairs[1].first;
        Memmove(sibling_pairs, sibling_pairs + 1, sibling_int->GetSize() - 1);
        sibling_int->DecreaseSize(1);
        auto *child_page = buffer_pool_manager_->FetchPage(int_pairs[size - 1].second);
        assert(child_page != nullptr);
        auto *child_bplus = ToBPlusPage<BPlusTreePage>(child_page);
        child_bplus->SetParentPageId(bplus_page->GetPageId());
        buffer_pool_manager_->UnpinPage(child_bplus->GetPageId(), true);
      } else { /* leaf page */
        auto *sibling_leaf = reinterpret_cast<LeafPage *>(sibling_bplus);
        auto *sibling_pairs = sibling_leaf->GetPairs();
        Memmove(leaf_pairs + i, leaf_pairs + i + 1, size - i - 1);
        leaf_pairs[size - 1].first = sibling_pairs[0].first;
        leaf_pairs[size - 1].second = sibling_pairs[0].second;
        parent_pairs[j + 1].first = sibling_pairs[1].first;
        Memmove(sibling_pairs, sibling_pairs + 1, sibling_leaf->GetSize() - 1);
        sibling_leaf->DecreaseSize(1);
      }
      buffer_pool_manager_->UnpinPage(parent_id, true);
      // buffer_pool_manager_->UnpinPage(bplus_id, true);
      sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(sibling_id, true);
    } else {
      /* merge with right */
      // printf("%d merges with %d\n", bplus_page->GetPageId(), sibling_bplus->GetPageId());
      if (sibling_bplus->IsInternalPage()) {
        auto *sibling_int = reinterpret_cast<InternalPage *>(sibling_bplus);
        auto *sibling_pairs = sibling_int->GetPairs();
        auto sibling_size = sibling_int->GetSize();
        Memmove(int_pairs + i, int_pairs + i + 1, size - i - 1);
        Memcpy(int_pairs + size - 1, sibling_pairs, sibling_size);
        int_pairs[size - 1].first = parent_pairs[j + 1].first;
        bplus_page->IncreaseSize(sibling_size - 1);
        sibling_int->DecreaseSize(sibling_size);
        for (int k = size - 1; k < size - 1 + sibling_size; ++k) {
          auto *child_page = buffer_pool_manager_->FetchPage(int_pairs[k].second);
          assert(child_page != nullptr);
          auto *child_bplus = ToBPlusPage<BPlusTreePage>(child_page);
          child_bplus->SetParentPageId(bplus_page->GetPageId());
          buffer_pool_manager_->UnpinPage(child_bplus->GetPageId(), true);
        }
      } else { /* leaf page */
        auto *sibling_leaf = reinterpret_cast<LeafPage *>(sibling_bplus);
        auto *sibling_pairs = sibling_leaf->GetPairs();
        auto sibling_size = sibling_leaf->GetSize();
        Memmove(leaf_pairs + i, leaf_pairs + i + 1, size - i - 1);
        Memcpy(leaf_pairs + size - 1, sibling_pairs, sibling_size);
        bplus_page->IncreaseSize(sibling_size - 1);
        sibling_leaf->DecreaseSize(sibling_size);
        reinterpret_cast<LeafPage *>(bplus_page)
            ->SetNextPageId(reinterpret_cast<LeafPage *>(sibling_leaf)->GetNextPageId());
      }
      // buffer_pool_manager_->UnpinPage(bplus_id, true);
      sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(sibling_id, true);
      buffer_pool_manager_->UnpinPage(parent_id, true);
      UNLATCH_UNTIL(bplus_id);
      transaction->AddIntoDeletedPageSet(bplus_id);
      RemoveEntry(ToBPlusPage<BPlusTreePage>(parent_page), parent_pairs[j + 1].first, transaction);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
inline auto BPLUSTREE_TYPE::LT(const KeyType &a, const KeyType &b) const -> bool { return comparator_(a, b) < 0; }

INDEX_TEMPLATE_ARGUMENTS
inline auto BPLUSTREE_TYPE::LE(const KeyType &a, const KeyType &b) const -> bool { return comparator_(a, b) <= 0; }

INDEX_TEMPLATE_ARGUMENTS
inline auto BPLUSTREE_TYPE::Equal(const KeyType &a, const KeyType &b) const -> bool { return comparator_(a, b) == 0; }

INDEX_TEMPLATE_ARGUMENTS
inline auto BPLUSTREE_TYPE::GT(const KeyType &a, const KeyType &b) const -> bool { return comparator_(a, b) > 0; }

INDEX_TEMPLATE_ARGUMENTS
inline auto BPLUSTREE_TYPE::GE(const KeyType &a, const KeyType &b) const -> bool { return comparator_(a, b) >= 0; }

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
  while (input) {  // NOLINT
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);  // NOLINT
  }
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
