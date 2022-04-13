/*
 * Copyright (c) Intel Corporation.
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

#include "cachelib/allocator/tests/MemoryTiersTest.h"//"cachelib/allocator/tests/AllocatorMemoryTiersTest.h"

namespace facebook {
namespace cachelib {
namespace tests {

template <typename AllocatorT>
class AllocatorMemoryTiersTest : public AllocatorTest<AllocatorT> {
};

using LruAllocatorMemoryTiersTest = AllocatorMemoryTiersTest<LruAllocator>;

// TODO(MEMORY_TIER): add more tests with different eviction policies

TEST_F(LruAllocatorMemoryTiersTest, Allocate2TiersTest) {
  auto config = configTieredCache<LruAllocator>(100 * Slab::kSize, 2);
  auto allocator = std::make_unique<LruAllocator>(LruAllocator::SharedMemNew, config);
}

TEST_F(LruAllocatorMemoryTiersTest, ValidateTieredCacheSizeEqualTiers) {
  const size_t maxTiers = 5;
  size_t numSlabs = minSlabsNumber(maxTiers);
  size_t cacheSize = numSlabs * Slab::kSize;

  for(auto numTiers = 1; numTiers <= maxTiers; ++numTiers) {
    auto config = configTieredCache<LruAllocator>(cacheSize, numTiers);
    validateCacheSize(LruAllocator(LruAllocator::SharedMemNew, config), cacheSize);
  }
}

TEST_F(LruAllocatorMemoryTiersTest, ValidateTieredCacheSizeUnevenTiers) {
  const size_t maxTiers = 5;
  size_t numSlabs = minSlabsNumber(maxTiers);
  size_t cacheSize = numSlabs * Slab::kSize;

  for(auto numTiers = 1; numTiers <= maxTiers; ++numTiers) {
    std::vector<size_t> ratios{};
    for (size_t i = numTiers; i > 0; --i) {
      ratios.push_back(i);
    }
    auto config = configTieredCache<LruAllocator>(cacheSize, numTiers, ratios);
    validateCacheSize(LruAllocator(LruAllocator::SharedMemNew, config), cacheSize);
  }
}

} // end of namespace tests
} // end of namespace cachelib
} // end of namespace facebook
