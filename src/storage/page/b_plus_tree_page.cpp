//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
auto BPlusTreePage::IsLeafPage() const -> bool { return page_type_ == IndexPageType::LEAF_PAGE; }
auto BPlusTreePage::IsInternalPage() const -> bool { return page_type_ == IndexPageType::INTERNAL_PAGE; }
auto BPlusTreePage::IsRootPage() const -> bool { return parent_page_id_ == INVALID_PAGE_ID; }
void BPlusTreePage::SetPageType(IndexPageType page_type) { page_type_ = page_type; }

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
auto BPlusTreePage::GetSize() const -> int { return size_; }
void BPlusTreePage::SetSize(int size) { size_ = size; }
void BPlusTreePage::IncreaseSize(int amount) { size_ += amount; }
auto BPlusTreePage::IsFull() const -> bool { return size_ == max_size_; }
auto BPlusTreePage::MoreThanMin() const -> bool { return size_ > GetMinSize(); }
auto BPlusTreePage::SafeToUpdate(UpdateMode mode) const -> bool {
  if (mode == UpdateMode::INSERT) {
    return size_ + 1 < max_size_;
  }
  if (mode == UpdateMode::REMOVE) {
    return MoreThanMin();
  }
  if (mode == UpdateMode::NOT_UPDATE) {
    return true;
  }
  UNREACHABLE("");
}

/*
 * Helper methods to get/set max size (capacity) of the page
 */
auto BPlusTreePage::GetMaxSize() const -> int { return max_size_; }
void BPlusTreePage::SetMaxSize(int size) { max_size_ = size; }

static inline auto DivCeil(int a, int b) -> int { return (a + (b - 1)) / b; }

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 */
auto BPlusTreePage::GetMinSize() const -> int {
  if (IsRootPage()) {
    return IsLeafPage() ? 1 : 2;
  }
  if (IsLeafPage()) {
    return DivCeil(max_size_, 2) - 1;
  }
  // internal page
  return max_size_ / 2;
}

void BPlusTreePage::SizeAfterSplit(int &left, int &right) const {
  if (IsLeafPage()) {
    left = max_size_ / 2;
    right = max_size_ - left;
  } else {
    // internal page
    left = DivCeil(max_size_, 2);
    right = max_size_ / 2;
  }
}

/*
 * Helper methods to get/set parent page id
 */
auto BPlusTreePage::GetParentPageId() const -> page_id_t { return parent_page_id_; }
void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) { parent_page_id_ = parent_page_id; }

/*
 * Helper methods to get/set self page id
 */
auto BPlusTreePage::GetPageId() const -> page_id_t { return page_id_; }
void BPlusTreePage::SetPageId(page_id_t page_id) { page_id_ = page_id; }

/*
 * Helper methods to set lsn
 */
void BPlusTreePage::SetLSN(lsn_t lsn) { lsn_ = lsn; }

}  // namespace bustub
