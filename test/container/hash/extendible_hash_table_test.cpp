/**
 * extendible_hash_test.cpp
 */

#include <memory>
#include <thread>  // NOLINT

#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"

namespace bustub {

// void DebugTable(std::shared_ptr<ExtendibleHashTable<int, int>> table) {
//   printf("global depth: %d\nbucket num: %d\n", table->GetGlobalDepth(), table->GetNumBuckets());
//   for (int i = 0; i < table->GetNumBuckets(); i++) {
//     printf("bucket %d depth: %d, size: %lu | ", i, table->GetLocalDepth(i), table->dir_[i]->list_.size());
//     for (auto j : table->dir_[i]->list_) {
//       printf("%d ", j.first);
//     }
//     printf("\n");
//   }
//   printf("\n");
// }

// TEST(ExtendibleHashTableTest, DISABLED_MyTest) {
//   auto table = std::make_shared<ExtendibleHashTable<int, int>>(2);

//   DebugTable(table);
//   table->Insert(0, 0);
//   DebugTable(table);
//   table->Insert(2, 2);
//   DebugTable(table);
//   table->Insert(1, 1);
//   DebugTable(table);

//   for (int i = 0; i < 100; i++) {
//     std::thread t1([&table]() { table->Insert(1, 1); });
//     std::thread t0([&table]() { table->Insert(0, 0); });
//     std::thread t2([&table]() { table->Insert(2, 2); });
//     t0.join();
//     t1.join();
//     t2.join();
//     EXPECT_EQ(table->GetGlobalDepth(), 1);
//   }
// }

TEST(ExtendibleHashTableTest, SampleTest) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);

  table->Insert(1, "a");
  table->Insert(2, "b");
  table->Insert(3, "c");
  table->Insert(4, "d");
  table->Insert(5, "e");
  table->Insert(6, "f");
  table->Insert(7, "g");
  table->Insert(8, "h");
  table->Insert(9, "i");
  EXPECT_EQ(2, table->GetLocalDepth(0));
  EXPECT_EQ(3, table->GetLocalDepth(1));
  EXPECT_EQ(2, table->GetLocalDepth(2));
  EXPECT_EQ(2, table->GetLocalDepth(3));

  std::string result;
  table->Find(9, result);
  EXPECT_EQ("i", result);
  table->Find(8, result);
  EXPECT_EQ("h", result);
  table->Find(2, result);
  EXPECT_EQ("b", result);
  EXPECT_FALSE(table->Find(10, result));

  EXPECT_TRUE(table->Remove(8));
  EXPECT_TRUE(table->Remove(4));
  EXPECT_TRUE(table->Remove(1));
  EXPECT_FALSE(table->Remove(20));
}

TEST(ExtendibleHashTableTest, ConcurrentInsertTest) {
  const int num_runs = 50;
  const int num_threads = 3;

  // Run concurrent test multiple times to guarantee correctness.
  for (int run = 0; run < num_runs; run++) {
    auto table = std::make_unique<ExtendibleHashTable<int, int>>(2);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int tid = 0; tid < num_threads; tid++) {
      threads.emplace_back([tid, &table]() { table->Insert(tid, tid); });
    }
    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    EXPECT_EQ(table->GetGlobalDepth(), 1);
    for (int i = 0; i < num_threads; i++) {
      int val;
      EXPECT_TRUE(table->Find(i, val));
      EXPECT_EQ(i, val);
    }
  }
}

TEST(ExtendibleHashTableTest, ConcurrentInsertHardTest) {
  const int num_runs = 50;
  const int num_threads = 31;

  // Run concurrent test multiple times to guarantee correctness.
  for (int run = 0; run < num_runs; run++) {
    auto table = std::make_shared<ExtendibleHashTable<int, int>>(2);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int tid = 0; tid < num_threads; tid++) {
      threads.emplace_back([tid, &table]() { table->Insert(tid * tid, tid); });
    }
    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    EXPECT_EQ(table->GetGlobalDepth(), 8);
  }
}
}  // namespace bustub
