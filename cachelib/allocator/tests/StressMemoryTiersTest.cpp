/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <numeric>

#include "cachelib/allocator/tests/MemoryTiersTest.h"
#include "cachelib/allocator/tests/TestBase.h"

namespace facebook {
namespace cachelib {
namespace tests {

template <typename Allocator>
class MemoryTiersTest : public AllocatorTest<Allocator> {
 public:
  LruAllocatorConfig createTieredCacheConfig(size_t totalCacheSize,
                                             size_t numTiers = 2) {
    return configTieredCache<LruAllocator>(totalCacheSize, numTiers);
  }

  LruAllocatorConfig createDramCacheConfig(size_t totalCacheSize) {
    return configTieredCache<LruAllocator>(totalCacheSize, 1);
  }
};

using LruMemoryTiersTest = MemoryTiersTest<LruAllocator>;

std::map<std::string, size_t> generateAndInsert(
    LruAllocator::Config& cfg,
    Range& keyRange,
    const std::pair<size_t, size_t>& dataSize) {
  size_t itemsInserted = 0, itemsFound = 0, itemsEvicted = 0, itemsMoved = 0;
  std::map<std::string, size_t> stats;

  std::unique_ptr<LruAllocator> alloc;

  size_t bytesAllocatedBeforeMove = 0;
  size_t numItemsBeforeMove = 0;

  cfg.setRemoveCallback(removeCb);
  cfg.enableMovingOnSlabRelease(moveCb);

  alloc = std::unique_ptr<LruAllocator>(
      new LruAllocator(LruAllocator::SharedMemNew, cfg));

  const size_t numBytes = alloc->getCacheMemoryStats().cacheSize;
  auto poolId = alloc->addPool("my pool", numBytes);

  Context context(*alloc, poolId, dataSize);
  ParallelFunction insertParallel = InsertGeneratedDataFunction(context);
  itemsInserted = insertParallel(keyRange);
  itemsEvicted = insertParallel.getRemovedKeysSet().size();
  itemsMoved = insertParallel.getMovedKeysSet().size();

  ParallelFunction lookupParallel = LookupGeneratedKeysFunction(context);
  itemsFound = lookupParallel(keyRange);

  EXPECT_EQ(lookUpKeys(removedKeys, *alloc), 0);

  EXPECT_EQ(keyRange.second - keyRange.first, itemsInserted);
  EXPECT_EQ(itemsFound + itemsEvicted, itemsInserted);
  EXPECT_LE(itemsFound, itemsInserted);

  return stats;
}

TEST_F(LruMemoryTiersTest, TestStressInserts) {
  const size_t N = /* 8 * */ 2 * minSlabsNumber(2) * Slab::kSize,
               M = /* 8 * */ 20000, nKey = 2 * 12, nVal = 1 * KB - nKey;
  std::vector<std::pair<std::string, size_t>> sortedStats = {};
  std::vector<std::tuple<size_t, size_t, size_t, size_t, std::string>>
      stress_params = {
          // params: total cache size, number of items to insert, size of keys,
          // size of values, description
          /* std::make_tuple(N * MB, M * 1000, 0, nVal,
                          "Cache is undersaturated, just a few data items"),
          std::make_tuple(N * MB, M * 108298, 0, nVal,
                          "Data fills the entire 50Mb of DRAM-only cache"),
          std::make_tuple(N * MB, M * 180000, 0, nVal,
                          "Too much data for the cache size"), */
          std::make_tuple(N, M * 1, nKey, nVal,
                          "Cache is undersaturated, just a few data items")};

  auto printStats = [&](std::map<std::string, size_t>& data) {
    for (const auto& [key, value] : data) {
      std::cout << value << "; ";
    }
    std::cout << std::endl;
  };

  for (auto params : stress_params) {
    Range keyRange = std::make_pair(0, std::get<1>(params));
    std::pair dataSize =
        std::make_pair(std::get<2>(params), std::get<3>(params));
    LruAllocatorConfig dramCacheConfig =
        createDramCacheConfig(std::get<0>(params));
    std::map<std::string, size_t> dramOnlyCacheStats;
    dramOnlyCacheStats = generateAndInsert(dramCacheConfig, keyRange, dataSize);

    if (sortedStats.empty()) {
      sortedStats = std::vector<std::pair<std::string, size_t>>(
          dramOnlyCacheStats.begin(), dramOnlyCacheStats.end());
      std::sort(sortedStats.begin(), sortedStats.end(),
                [](const auto& lhs, const auto& rhs) {
                  return lhs.first < rhs.first;
                });
      for (auto& header : sortedStats) {
        std::cout << header.first.substr(header.first.find(" ") + 1) << ";";
      }
      std::cout << std::endl;
    }

    printStats(dramOnlyCacheStats);

    for (auto numTiers : {2}) {
      LruAllocatorConfig tieredCacheConfig =
          createTieredCacheConfig(std::get<0>(params), numTiers);
      auto tieredCacheStats =
          generateAndInsert(tieredCacheConfig, keyRange, dataSize);

      printStats(tieredCacheStats);
    }
    break;
  }
}
} // namespace tests
} // namespace cachelib
} // namespace facebook
