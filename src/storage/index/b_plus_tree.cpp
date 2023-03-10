#include <algorithm>
#include <string>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

#pragma clang diagnostic push
static constexpr int NEW_RECORD = 1;
static constexpr int UPDATE_RECORD = 0;

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
  auto page = FindLeafPage(key, transaction);
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  auto pairs = leaf_page->GetPairs();
  // binary search
  int left = 0;
  int right = leaf_page->GetSize() - 1;
  int mid = (left + right) / 2;
  while (left <= right) {
    int cmp_result = comparator_(key, pairs[mid].first);
    if (cmp_result < 0) {
      right = mid - 1;
    } else if (cmp_result > 0) {
      left = mid + 1;
    } else {
      result->emplace_back();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      return true;
    }
    mid = (left + right) / 2;
  }

  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return false;
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
  bool ret = false;
  if (IsEmpty()) {
    InitRootAndInsert(key, value, transaction);
    ret = true;
  } else {
    auto page = FindLeafPage(key, transaction);
    ret = InsertIntoLeaf(reinterpret_cast<LeafPage *>(page->GetData()), key, value, transaction);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

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
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

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
void BPLUSTREE_TYPE::InitRootAndInsert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  assert(IsEmpty());

  Page *page = buffer_pool_manager_->NewPage(&root_page_id_);

  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  leaf_page->Init(root_page_id_);

  leaf_page->GetPairs()[0] = std::make_pair(key, value);
  leaf_page->IncreaseSize(1);

  UpdateRootPageId(NEW_RECORD);

  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
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
    std::memmove(data + pos + 1, data + pos, (size - pos) * sizeof(LeafMappingType));
  }
  // assign
  data[pos] = std::make_pair(key, value);
  leaf_page->IncreaseSize(1);

  if (leaf_page->IsFull()) {
    SplitLeaf(leaf_page, transaction);
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoInternal(InternalPage *internal_page, const KeyType &key, const page_id_t value,
                                        Transaction *transaction) -> bool {
  assert(!internal_page->IsFull());

  auto data = internal_page->GetPairs();
  int size = internal_page->GetSize();
  // find upper bound
  int pos = FindInsertInternalPos(data, size, key);
  // shift
  if (size > 1) {
    std::memmove(data + pos + 1, data + pos, (size - pos) * sizeof(InternalMappingType));
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
void BPLUSTREE_TYPE::SplitLeaf(LeafPage *leaf_page, Transaction *transaction) {
  assert(leaf_page->IsFull());

  size_t left_page_size = leaf_page->GetMinSize();
  size_t right_page_size = leaf_page->GetMaxSize() - left_page_size;

  /* prepare pages */
  auto left_page = leaf_page;
  page_id_t parent_page_id, right_page_id;
  auto right_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&right_page_id)->GetData());
  auto left_page_pairs = left_page->GetPairs();
  auto right_page_pairs = right_page->GetPairs();

  InternalPage *parent_page;
  if (left_page->IsRootPage()) {
    parent_page = NewRootPage(parent_page_id);
  } else {
    parent_page_id = left_page->GetParentPageId();
    parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());
  }
  right_page->Init(right_page_id, parent_page_id);
  /* move entries of left page to right page */
  std::memcpy(right_page_pairs, left_page_pairs + left_page_size, right_page_size * sizeof(MappingType));
  left_page->IncreaseSize(-right_page_size);
  right_page->IncreaseSize(right_page_size);
  /* link two pages */
  left_page->SetNextPageId(right_page_id);
  /* set parent id */
  left_page->SetParentPageId(parent_page_id);
  right_page->SetParentPageId(parent_page_id);
  /* update parent */
  InsertIntoInternal(parent_page, right_page_pairs[0].first, right_page_id, transaction);
  /* unpin pages */
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
  buffer_pool_manager_->UnpinPage(right_page_id, true);

  assert(!left_page->IsFull());
  assert(!right_page->IsFull());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitInternal(InternalPage *internal_page, Transaction *transaction) {
  assert(internal_page->IsFull());

  // different from `SplitLeaf`, here we count the size without the first pair
  size_t left_page_size = internal_page->GetMinSize() - 1;
  size_t right_page_size = internal_page->GetMaxSize() - 1 - left_page_size - 1;

  /* prepare pages */
  auto left_page = internal_page;
  page_id_t parent_page_id, right_page_id;
  auto right_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&right_page_id)->GetData());
  auto left_page_pairs = left_page->GetPairs();
  auto right_page_pairs = right_page->GetPairs();

  InternalPage *parent_page;
  if (left_page->IsRootPage()) {
    parent_page = NewRootPage(parent_page_id);
  } else {
    parent_page_id = left_page->GetParentPageId();
    parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());
  }
  right_page->Init(right_page_id, parent_page_id);
  /* move entries of left page to right page */
  int lifting_pos = left_page_size + 1;
  right_page_pairs[0].second = left_page_pairs[lifting_pos].second;
  std::memcpy(right_page_pairs + 1, left_page_pairs + lifting_pos + 1, right_page_size * sizeof(InternalMappingType));
  left_page->SetSize(left_page_size + 1);
  right_page->SetSize(right_page_size + 1);
  /* set parent id */
  left_page->SetParentPageId(parent_page_id);
  right_page->SetParentPageId(parent_page_id);
  /* update parent */
  InsertIntoInternal(parent_page, left_page_pairs[lifting_pos].first, right_page_id, transaction);
  /* unpin pages */
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
  buffer_pool_manager_->UnpinPage(right_page_id, true);

  assert(!left_page->IsFull());
  assert(!right_page->IsFull());
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::NewRootPage(page_id_t &root_page_id) -> InternalPage * {
  InternalPage *root_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&root_page_id)->GetData());
  root_page->Init(root_page_id);
  root_page_id_ = root_page_id;
  UpdateRootPageId(UPDATE_RECORD);
  return root_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, Transaction *transaction) -> Page * {
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto bplus_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
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
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = next_page;
    bplus_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  return page;
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

#pragma clang diagnostic pop