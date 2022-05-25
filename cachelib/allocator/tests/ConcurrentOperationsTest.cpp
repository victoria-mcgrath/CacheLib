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
class CuncurrentOperationsTest : public AllocatorTest<Allocator> {
 public:
  void SetUp() {
    itemsInserted = 0;
    itemsFound = 0;
    itemsEvicted = 0;
    itemsMoved = 0;
  }

  void TearDown() {
    EXPECT_EQ(itemsFound + itemsEvicted, itemsInserted);
    EXPECT_LE(itemsFound, itemsInserted);
  }

  void concurrentTest(size_t numTiers,
                      size_t numPools,
                      size_t totalCacheSize,
                      size_t keySize,
                      size_t valSize,
                      size_t numInserts) {
    Range keyRange = std::make_pair(0, numInserts);
    std::pair dataSize = std::make_pair(keySize, valSize);
    std::vector<std::thread> threads;
    std::vector<std::pair<size_t, size_t>> params;
    auto cfg = configTieredCache<LruAllocator>(totalCacheSize, numTiers);

    std::unique_ptr<LruAllocator> alloc = std::unique_ptr<LruAllocator>(
        new LruAllocator(LruAllocator::SharedMemNew, cfg));

    size_t numBytes = alloc->getCacheMemoryStats().cacheSize;
    size_t bytesChunk = numBytes / numPools,
           insertsChunk = numInserts / numPools;

    for (size_t i = 0; i < numPools - 1; ++i) {
      params.push_back(std::make_pair(bytesChunk, insertsChunk));
      numBytes -= bytesChunk;
      numInserts -= insertsChunk;
    }
    params.push_back(std::make_pair(numBytes, numInserts));

    for (size_t i = 0; i < numPools; ++i) {
      Context context(
          *alloc,
          alloc->addPool(folly::sformat("my pool {}", i), std::get<0>(params[i])),
          dataSize);
      ParallelFunction insertParallel = InsertGeneratedDataFunction(context);
      itemsInserted = insertParallel(std::make_pair(
          i * insertsChunk, i * insertsChunk + std::get<1>(params[i])));
    }
  }

  size_t itemsInserted, itemsFound, itemsEvicted, itemsMoved;
};

using LruCuncurrentOperationsTest = CuncurrentOperationsTest<LruAllocator>;

TEST_F(LruCuncurrentOperationsTest, Tier1Pool1KeySize1) {
  const size_t totalCacheSize = 2 * minSlabsNumber(2) * Slab::kSize;
  const size_t keySize = 24, valSize = 1 * KB - keySize, numInserts = 20000;
  Range keyRange = std::make_pair(0, numInserts);
  std::pair dataSize = std::make_pair(keySize, valSize);
  auto cfg = configTieredCache<LruAllocator>(totalCacheSize, 1);

  std::unique_ptr<LruAllocator> alloc = std::unique_ptr<LruAllocator>(
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
}

} // namespace tests
} // namespace cachelib
} // namespace facebook
