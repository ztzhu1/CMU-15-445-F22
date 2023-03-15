//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  IndexIterator() = default;
  IndexIterator(const IndexIterator &) = delete;
  IndexIterator(const IndexIterator &&that) noexcept;
  auto operator=(const IndexIterator &) -> IndexIterator & = delete;

  explicit IndexIterator(BufferPoolManager *buffer_pool_manager, KeyComparator *comparator_, Page *leaf_page);

  explicit IndexIterator(BufferPoolManager *buffer_pool_manager, KeyComparator *comparator_, Page *leaf_page,
                         const KeyType &key);

  ~IndexIterator();  // NOLINT

  auto IsEnd() const -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool;

  auto operator!=(const IndexIterator &itr) const -> bool { return !(*this == itr); }

 private:
  // add your own private member variables here
  BufferPoolManager *buffer_pool_manager_{nullptr};
  KeyComparator *comparator_{nullptr};
  page_id_t curr_leaf_page_id_{-1};
  Page *curr_leaf_page_{nullptr};
  LeafPage *curr_leaf_bplus_page_{nullptr};
  int curr_pos_in_leaf_page_{0};
};

}  // namespace bustub
