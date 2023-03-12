//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(1);
  SetMaxSize(max_size);
  SetPageId(page_id);
  SetParentPageId(parent_id);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  assert(index < GetMaxSize());
  assert(index != 0);
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index < GetMaxSize());
  assert(index != 0);
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  assert(index < GetMaxSize());
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::UpdateKey(const KeyType &old_key, const KeyType &new_key,
                                               const KeyComparator &cmp) {
  int pos = -1;
  bool found = FindKeyIndex(old_key, pos, cmp);
  assert(found);
  if (pos == 0) {
    return;
  }
  (array_ + pos)->first = new_key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindKeyIndex(const KeyType &key, int &pos, const KeyComparator &cmp) const
    -> bool {
  if (cmp(key, (array_ + 1)->first) < 0) {
    pos = 0;
    return true;
  }
  // binary search
  int left = 1;
  int right = GetSize() - 1;
  int mid = (right + left) / 2;
  while (left <= right) {
    if (cmp(key, (array_ + mid)->first) < 0) {
      right = mid - 1;
    } else if (cmp(key, (array_ + mid)->first) > 0) {
      left = mid + 1;
    } else {
      pos = mid;
      return true;
    }
    mid = (right + left) / 2;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindPointerIndex(const page_id_t page_id) const -> int {
  int pos = 0;
  int size = GetSize();
  for (; pos < size; pos++) {
    if ((array_ + pos)->second == page_id) {
      break;
    }
  }
  assert(pos < size);
  return pos;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetPairs() -> MappingType * { return array_; }

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
