/**
 * index_iterator.cpp
 */
#include <cassert>
#include <iostream>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, KeyComparator *comparator, Page *leaf_page,
                                  const KeyType &key)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), curr_leaf_page_(leaf_page) {
  curr_leaf_page_id_ = curr_leaf_page_->GetPageId();
  curr_leaf_bplus_page_ = reinterpret_cast<LeafPage *>(curr_leaf_page_->GetData());
  int pos = -1;
  assert(curr_leaf_bplus_page_->FindKeyIndex(key, pos, *comparator_));
  curr_pos_in_leaf_page_ = pos;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (IsEnd()) {
    return;
  }
  buffer_pool_manager_->UnpinPage(curr_leaf_page_id_, false);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() const -> bool { return curr_leaf_page_ == nullptr; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  assert(!IsEnd());
  return curr_leaf_bplus_page_->GetPairs()[curr_pos_in_leaf_page_];
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  assert(!IsEnd());
  auto size = curr_leaf_bplus_page_->GetSize();
  assert(curr_pos_in_leaf_page_ < size);
  if (curr_pos_in_leaf_page_ == size - 1) {
    curr_pos_in_leaf_page_ = 0;
    auto next_page_id = curr_leaf_bplus_page_->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
      buffer_pool_manager_ = nullptr;
      comparator_ = nullptr;
      curr_leaf_page_id_ = -1;
      curr_leaf_page_ = nullptr;
      curr_leaf_bplus_page_ = nullptr;
      return *this;
    }

    auto next_page = buffer_pool_manager_->FetchPage(next_page_id);
    buffer_pool_manager_->UnpinPage(curr_leaf_page_id_, false);
    curr_leaf_page_id_ = next_page_id;
    curr_leaf_page_ = next_page;
    curr_leaf_bplus_page_ = reinterpret_cast<LeafPage *>(curr_leaf_page_->GetData());
  } else {
    ++curr_pos_in_leaf_page_;
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  if (itr.IsEnd()) {
    return IsEnd();
  }
  if (IsEnd()) {
    return itr.IsEnd();
  }
  return curr_leaf_page_id_ == itr.curr_leaf_page_id_ && curr_pos_in_leaf_page_ == itr.curr_pos_in_leaf_page_;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
