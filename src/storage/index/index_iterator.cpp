/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, LeafPage *init_page, int init_index)
    : curr_page_(init_page), curr_index_(init_index), buffer_pool_manager_(buffer_pool_manager) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(const IndexIterator &that) {
  curr_page_ = that.curr_page_;
  curr_index_ = that.curr_index_;
  buffer_pool_manager_ = that.buffer_pool_manager_;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (IsEnd()) {
    return;
  }
  buffer_pool_manager_->UnpinPage(curr_page_->GetPageId(), false);
  curr_page_ = nullptr;
  curr_index_ = 0;
  buffer_pool_manager_ = nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return curr_page_ == nullptr; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return curr_page_->GetPairs()[curr_index_]; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  assert(curr_index_ < curr_page_->GetSize());
  if (curr_index_ == curr_page_->GetSize() - 1) {
    auto next_id = curr_page_->GetNextPageId();
    buffer_pool_manager_->UnpinPage(curr_page_->GetPageId(), false);
    curr_index_ = 0;
    if (next_id == INVALID_PAGE_ID) {
      curr_page_ = nullptr;
      buffer_pool_manager_ = nullptr;
    } else {
      curr_page_ = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(next_id)->GetData());
    }
  } else {
    ++curr_index_;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
