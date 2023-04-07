//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_concurrent_test.cpp
//
// Identification: test/storage/b_plus_tree_concurrent_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>  // NOLINT
#include <chrono>     // NOLINT
#include <cstdio>
#include <functional>
#include <iterator>
#include <numeric>  // NOLINT
#include <random>   // NOLINT
#include <thread>   // NOLINT

#include "buffer/buffer_pool_manager_instance.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

static std::random_device rd;

namespace bustub {
// helper function to launch multiple threads
template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, Args &&...args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(args..., thread_itr));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

// helper function to insert
void InsertHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree->Insert(index_key, rid, transaction);
  }
  delete transaction;
}

// helper function to seperate insert
void InsertHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                       int total_threads, __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key.SetFromInteger(key);
      tree->Insert(index_key, rid, transaction);
    }
  }
  delete transaction;
}

void InsertHelperRepeat(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                        int total_threads, __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : keys) {
    if ((static_cast<uint64_t>(key) % total_threads == thread_itr) ||
        (static_cast<uint64_t>(key) % (total_threads - 1) == thread_itr)) {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key.SetFromInteger(key);
      tree->Insert(index_key, rid, transaction);
    }
  }
  delete transaction;
}

// helper function to delete
void DeleteHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &remove_keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree->Remove(index_key, transaction);
  }
  delete transaction;
}

// helper function to seperate delete
void DeleteHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree,
                       const std::vector<int64_t> &remove_keys, int total_threads,
                       __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : remove_keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      index_key.SetFromInteger(key);
      tree->Remove(index_key, transaction);
    }
  }
  delete transaction;
}

TEST(BPlusTreeConcurrentTest, InsertTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(2, InsertHelper, &tree, keys);

  std::vector<RID> rids;
  GenericKey<8> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  // tree.Draw(bpm, "tree.dot");

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, InsertTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(2, InsertHelperSplit, &tree, keys, 2);

  std::vector<RID> rids;
  GenericKey<8> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, InsertTest3) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 4, 5);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 3000;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  int num_thread = 8;
  LaunchParallelTest(num_thread, InsertHelperSplit, &tree, keys, num_thread);
  // tree.Draw(bpm, "tree.dot");

  std::vector<RID> rids;
  GenericKey<8> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, InsertTest4) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 3000;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  int num_thread = 8;
  LaunchParallelTest(num_thread, InsertHelperRepeat, &tree, keys, num_thread);
  // tree.Draw(bpm, "tree.dot");

  std::vector<RID> rids;
  GenericKey<8> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DeleteTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // sequential insert
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  LaunchParallelTest(2, DeleteHelper, &tree, remove_keys);

  int64_t start_key = 2;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DeleteTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // sequential insert
  std::vector<int64_t> keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {1, 4, 3, 2, 5, 6};
  LaunchParallelTest(2, DeleteHelperSplit, &tree, remove_keys, 2);

  int64_t start_key = 7;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 4);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DeleteTest3) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(500, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 3, 4);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // sequential insert
  int num_key = 3000;
  std::vector<int64_t> keys(num_key);
  std::iota(keys.begin(), keys.end(), 1);
  std::shuffle(keys.begin(), keys.end(), std::mt19937{rd()});
  // std::copy(keys.begin(), keys.end(), std::ostream_iterator<int64_t>(std::cout, " "));
  // std::cout << std::endl;
  InsertHelper(&tree, keys);
  // tree.Draw(bpm, "tree_before_delete.dot");

  int num_remove = num_key / 2;
  std::vector<int64_t> remove_keys(num_remove);
  std::iota(remove_keys.begin(), remove_keys.end(), 1);
  std::shuffle(remove_keys.begin(), remove_keys.end(), std::mt19937{rd()});

  // std::copy(remove_keys.begin(), remove_keys.end(), std::ostream_iterator<int64_t>(std::cout, " "));
  // std::cout << std::endl;

  int num_thread = 8;
  LaunchParallelTest(num_thread, DeleteHelper, &tree, remove_keys);
  // tree.Draw(bpm, "tree_after_delete.dot");

  int64_t start_key = num_remove + 1;
  int64_t current_key = start_key;
  int64_t size = 0;
  for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, num_key - num_remove);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DeleteTest4) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // sequential insert
  int num_key = 5000;
  std::vector<int64_t> keys(num_key);
  std::iota(keys.begin(), keys.end(), 1);
  std::shuffle(keys.begin(), keys.end(), std::mt19937{rd()});

  // std::copy(keys.begin(), keys.end(), std::ostream_iterator<int64_t>(std::cout, " "));
  // std::cout << std::endl;

  InsertHelper(&tree, keys);
  // tree.Draw(bpm, "tree.dot");

  int num_remove = num_key / 2;
  std::vector<int64_t> remove_keys(num_remove);
  std::iota(remove_keys.begin(), remove_keys.end(), 1);
  std::shuffle(remove_keys.begin(), remove_keys.end(), std::mt19937{rd()});
  // tree.Draw(bpm, "tree.dot");

  // std::copy(remove_keys.begin(), remove_keys.end(), std::ostream_iterator<int64_t>(std::cout, " "));
  // std::cout << std::endl;

  int num_thread = 8;
  LaunchParallelTest(num_thread, DeleteHelperSplit, &tree, remove_keys, num_thread);
  tree.Draw(bpm, "tree.dot");

  int64_t start_key = num_remove + 1;
  int64_t current_key = start_key;
  int64_t size = 0;
  for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, num_key - num_remove);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, MixTest) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // first, populate index
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  InsertHelper(&tree, keys);

  // concurrent insert
  keys.clear();
  for (int i = 6; i <= 10; i++) {
    keys.push_back(i);
  }
  LaunchParallelTest(1, InsertHelper, &tree, keys);
  // concurrent delete
  std::vector<int64_t> remove_keys = {1, 4, 3, 5, 6};
  LaunchParallelTest(1, DeleteHelper, &tree, remove_keys);

  int64_t start_key = 2;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    size = size + 1;
  }

  EXPECT_EQ(size, 5);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

}  // namespace bustub
