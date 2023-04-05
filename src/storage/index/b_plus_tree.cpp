#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {

template <typename BPlusPageType>
inline auto ToBPlusPage(Page *page) -> BPlusPageType * {
  return reinterpret_cast<BPlusPageType *>(page->GetData());
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
  std::lock_guard g(test_mu_);
  auto leaf_opt = FindLeaf(key, transaction);
  if (!leaf_opt.has_value()) {
    // empty
    return false;
  }
  auto *leaf = leaf_opt.value();
  auto size = leaf->GetSize();
  auto *pairs = leaf->GetPairs();
  int i = 0;
  for (; i < size; ++i) {
    if (Equal(key, pairs[i].first)) {
      result->push_back(pairs[i].second);
      break;
    }
  }
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
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
  std::lock_guard g(test_mu_);
  auto leaf_opt = FindLeaf(key, transaction);
  if (!leaf_opt.has_value()) {
    /* empty */
    Page *root_page = buffer_pool_manager_->NewPage(&root_page_id_);
    assert(root_page != nullptr);
    UpdateRootPageId(1);

    auto *leaf = ToBPlusPage<LeafPage>(root_page);
    leaf->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    auto *pairs = leaf->GetPairs();
    pairs[0] = std::make_pair(key, value);
    leaf->IncreaseSize(1);
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    return true;
  }

  auto *leaf = leaf_opt.value();
  /* TODO(ztzhu): We will do the exactly same scanning in `InsertInLeaf`,
     This can be optimized. */
  for (int i = 0; i < leaf->GetSize(); ++i) {
    if (Equal(key, leaf->GetPairs()[i].first)) {
      buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
      return false;
    }
  }
  /* key not exists */
  if (leaf->SafeToInsert()) {
    /* not full after insertion */
    bool successs = InsertInLeaf(leaf, key, value, transaction);
    assert(successs);
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), successs);
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
  std::vector<MappingType> temp_pairs(size + 1);
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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

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
  auto leaf_opt = FindLeaf(key);
  if (!leaf_opt.has_value()) {
    return INDEXITERATOR_TYPE();
  }
  auto *leaf = leaf_opt.value();
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

/** caller is responsible for unpinning leaf page  */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key, Transaction *transaction) -> std::optional<LeafPage *> {
  if (IsEmpty()) {
    return std::nullopt;
  }
  auto id = root_page_id_;
  Page *page = buffer_pool_manager_->FetchPage(id);
  assert(page != nullptr);
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
    buffer_pool_manager_->UnpinPage(id, false);

    id = next_id;
    page = buffer_pool_manager_->FetchPage(id);
    assert(page != nullptr);
    bplus_page = ToBPlusPage<BPlusTreePage>(page);
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
  std::memmove(static_cast<void *>(pairs + i + 1), static_cast<void *>(pairs + i), (size - i) * sizeof(MappingType));
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
    buffer_pool_manager_->UnpinPage(left_child->GetPageId(), true);
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
    std::memmove(static_cast<void *>(parent_pairs + i + 1), static_cast<void *>(parent_pairs + i),
                 (parent_size - i) * sizeof(InternalMappingType));
    parent_pairs[i] = std::make_pair(key, right_child->GetPageId());
    parent_internal->IncreaseSize(1);
    assert(left_child->GetParentPageId() == parent_id);
    right_child->SetParentPageId(parent_id);
    buffer_pool_manager_->UnpinPage(left_child->GetPageId(), true);
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
    std::vector<InternalMappingType> temp_pairs(parent_size + 1);
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

    buffer_pool_manager_->UnpinPage(left_child->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(right_child->GetPageId(), true);
    InsertInParent(reinterpret_cast<BPlusTreePage *>(parent_internal), new_pairs[0].first,
                   reinterpret_cast<BPlusTreePage *>(new_internal));
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
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
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
