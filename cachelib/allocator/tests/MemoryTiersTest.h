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

#pragma once

#include <chrono>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/allocator/CacheAllocatorConfig.h"
#include "cachelib/allocator/MemoryTierCacheConfig.h"
#include "cachelib/allocator/tests/TestBase.h"

namespace facebook {
namespace cachelib {
namespace tests {

const size_t metadataSize = Slab::kSize;
const size_t minTierSlabsNumber = 2;

const size_t KB = 1024ULL;
constexpr size_t MB = KB * KB;
constexpr size_t GB = MB * KB;

const char endOfPattern[] = "x";

using LruAllocatorConfig = CacheAllocatorConfig<LruAllocator>;
using LruMemoryTierConfigs = LruAllocatorConfig::MemoryTierConfigs;
using StringDataKeyValue = std::pair<std::string, std::string>;
using StringDataKeyValues = std::vector<StringDataKeyValue>;
using Range = std::pair<size_t, size_t>;
using Item = typename LruAllocator::Item;
using RemoveCbData = typename LruAllocator::RemoveCbData;

thread_local std::set<std::string> movedKeys;
thread_local std::set<std::string> removedKeys;

auto moveCb = [&](const Item& oldItem, Item& newItem, Item*) {
  std::memcpy(newItem.getWritableMemory(), oldItem.getMemory(),
              oldItem.getSize());
  movedKeys.insert(oldItem.getKey().str());
};

auto removeCb = [&](const RemoveCbData& data) {
  removedKeys.insert(data.item.getKey().str());
};

template <typename AllocatorT = LruAllocator>
typename AllocatorT::Config configTieredCache(size_t cacheSize,
                                              size_t numTiers,
                                              std::vector<size_t> ratios = {}) {
  typename AllocatorT::Config config;
  std::vector<MemoryTierCacheConfig> tierConfig;

  if (ratios.size() && (numTiers != ratios.size())) {
    throw std::invalid_argument(
        "Number of new ratios doesn't match number of memory tiers.");
  }

  config.setCacheSize(cacheSize);
  for (auto i = 0; i < numTiers; ++i) {
    size_t ratio = 1;
    if (i < ratios.size()) {
      ratio = ratios[i];
    }
    tierConfig.push_back(MemoryTierCacheConfig::fromFile(
                             folly::sformat("/tmp/tier{}-{}", i, ::getpid()))
                             .setRatio(ratio));
  }
  config.configureMemoryTiers(tierConfig)
      .enableCachePersistence(
          folly::sformat("/tmp/multi-tier-test/{}", ::getpid()))
      .usePosixForShm();
  return config;
}

template <typename AllocatorT>
void validateCacheSize(const AllocatorT& allocator, size_t totalCacheSize) {
  auto numBytes = allocator.getCacheMemoryStats().cacheSize;
  EXPECT_EQ(totalCacheSize, numBytes + metadataSize * allocator.getNumTiers());
}

size_t minSlabsNumber(size_t numTiers) {
  // Calculate the number of slabs to allocate which
  // is divisible by every number of tiers under test
  // and guarantees that there's at least the minimum
  // required number of slabs for each tier.
  size_t numSlabs = 1;
  for (auto i = 1; i <= numTiers; ++i) {
    numSlabs *= i;
  }
  return minTierSlabsNumber * numSlabs;
}

std::string generateString(const size_t id, size_t length) {
  std::string result = std::to_string(id);
  if (length) {
    result += endOfPattern;
    auto pattern = result;
    length -= (result.length() - 1);
    if (length < pattern.length()) {
      throw std::invalid_argument(
          "Given data pattern is too large for the requested length.");
    }
    for (size_t j = 0; j < length / pattern.length(); ++j) {
      result += pattern;
    }
    result += pattern.substr(0, length % pattern.length());
  }
  return result;
}

template <typename AllocatorT>
size_t insertGeneratedData(const std::pair<size_t, size_t>& keyRange,
                           const std::pair<size_t, size_t>& dataSize,
                           AllocatorT& alloc,
                           PoolId poolId) {
  size_t itemsInserted = 0;
  size_t minItemSize = std::to_string(keyRange.second - 1).length();

  if ((dataSize.first && (minItemSize > dataSize.first)) ||
      (dataSize.second && (minItemSize > dataSize.second))) {
    throw std::invalid_argument(
        "The requested sizes for keys and values are too small.");
  }

  for (auto i = keyRange.first; i < keyRange.second; ++i) {
    auto key = generateString(i, dataSize.first);
    auto value = generateString(i, dataSize.second);

    auto h = alloc.allocate(poolId, key, value.length() + 1);
    EXPECT_TRUE(h);

    std::memcpy(h->getWritableMemory(), value.c_str(), value.length() + 1);

    if (alloc.insert(h)) {
      ++itemsInserted;
    }
  }

  return itemsInserted;
}

template <typename AllocatorT>
size_t lookUpGeneratedKeys(const std::pair<size_t, size_t>& keyRange,
                           const std::pair<size_t, size_t>& dataSize,
                           AllocatorT& alloc) {
  size_t itemsFound = 0;
  for (auto i = keyRange.first; i < keyRange.second; ++i) {
    auto key = generateString(i, dataSize.first);
    auto x = alloc.find(key);
    if (x) {
      ++itemsFound;
      auto pos = key.find(endOfPattern);
      size_t id = 0;
      if (pos > -1) {
        id = std::stoi(key.substr(0, pos));
      } else {
        id = std::stoi(key);
      }
      auto expectedValue = generateString(id, dataSize.second);
      EXPECT_TRUE(expectedValue ==
                  std::string(reinterpret_cast<const char*>(x->getMemory())));
    }
  }
  return itemsFound;
}

template <typename AllocatorT>
size_t lookUpKeys(std::set<std::string>& keys, AllocatorT& alloc) {
  size_t itemsFound = 0;
  for (auto& key : keys) {
    auto x = alloc.find(key);
    if (x) {
      ++itemsFound;
    }
  }
  return itemsFound;
}

template <typename Allocator>
class FunctionContext {
  Allocator& allocator_;

  const PoolId poolId_;
  const Range dataSize_;

 public:
  FunctionContext(Allocator& allocator,
                  PoolId poolId = 0,
                  const Range& dataSize = std::make_pair(0, 0))
      : allocator_(allocator), poolId_(poolId), dataSize_(dataSize) {}

  const PoolId getPoolId() const { return poolId_; }

  Allocator& getAllocator() const { return allocator_; }

  const Range& getDataSize() const { return dataSize_; }
};

template <typename Context>
class Function {
  Context& context_;
  std::set<std::string> taskLocalRemovedKeys;
  std::set<std::string> taskLocalMovedKeys;

 public:
  Function(Context& context) : context_(context) {}

  Context& getContext() { return context_; }

  void captureStats() {
    taskLocalRemovedKeys = removedKeys;
    taskLocalMovedKeys = movedKeys;
  }

  std::set<std::string>& getRemovedKeysSet() { return taskLocalRemovedKeys; }

  std::set<std::string>& getMovedKeysSet() { return taskLocalMovedKeys; }
};

template <typename F>
class ParallelFunction {
  F function_;
  size_t stats_{};
  size_t numWorkers_;
  double ms_{};

  std::set<std::string> totalRemovedKeys;
  std::set<std::string> totalMovedKeys;

 public:
  ParallelFunction(F function,
                   size_t numWorkers = std::thread::hardware_concurrency())
      : function_(function), numWorkers_(numWorkers) {}

  size_t operator()(const Range& r) {
    std::vector<std::unique_ptr<F>> workers =
        function_.generateTasks(numWorkers_);
    size_t result = 0;

    auto t1 = std::chrono::high_resolution_clock::now();
    run(r, workers);

    auto t2 = std::chrono::high_resolution_clock::now();
    recordTime(t1, t2);

    for (size_t i = 0; i < numWorkers_; ++i) {
      result += workers[i]->getStats();
    }
    function_.destroyTasks(workers);
    return result;
  }

  size_t getStats() const { return stats_; }

  double getTime() const { return ms_; }

  std::set<std::string>& getRemovedKeysSet() { return totalRemovedKeys; }

  std::set<std::string>& getMovedKeysSet() { return totalMovedKeys; }

 protected:
  void run(const Range& r, std::vector<std::unique_ptr<F>>& workers) {
    reset();
    std::vector<std::thread> threads;
    size_t stride = (r.second - r.first) / numWorkers_,
           remainder = (r.second - r.first) % numWorkers_, begin = r.first,
           end = begin + stride;

    for (size_t i = 0; (end <= r.second) && (remainder || stride);
         end += stride, ++i) {
      if (remainder > 0) {
        --remainder;
        ++end;
      }
      threads.push_back(std::thread([&workers, i, begin, end]() {
        workers[i]->operator()(std::make_pair(begin, end));
        workers[i]->captureStats();
      }));
      begin = end;
    }

    for (auto& thread : threads) {
      thread.join();
    }
    gatherStats(workers);
    std::cout << "Threads Done" << std::endl;
  }

  double recordTime(
      std::chrono::time_point<std::chrono::high_resolution_clock>& t1,
      std::chrono::time_point<std::chrono::high_resolution_clock>& t2) {
    std::chrono::duration<double, std::milli> ms = t2 - t1;
    ms_ = ms.count();
    std::cout << "Time: " << ms_ << std::endl;
    return ms_;
  }

  void gatherStats(std::vector<std::unique_ptr<F>>& workers) {
    for (auto& worker : workers) {
      totalRemovedKeys.insert(worker->getRemovedKeysSet().begin(),
                              worker->getRemovedKeysSet().end());
      totalMovedKeys.insert(worker->getMovedKeysSet().begin(),
                            worker->getMovedKeysSet().end());
    }
  }

  void reset() { ms_ = stats_ = 0; }
};

template <typename Allocator, typename Function>
class InsertGeneratedData : public Function {
  using InsertTask = InsertGeneratedData<Allocator, Function>;

 private:
  size_t itemsInserted_{0};

 public:
  InsertGeneratedData(FunctionContext<Allocator>& context)
      : Function(context) {}

  void operator()(const Range& r) {
    itemsInserted_ =
        insertGeneratedData<Allocator>(r,
                                       this->getContext().getDataSize(),
                                       this->getContext().getAllocator(),
                                       this->getContext().getPoolId());
  }

  size_t getStats() { return itemsInserted_; }

  std::vector<std::unique_ptr<InsertTask>> generateTasks(size_t numWorkers) {
    std::vector<std::unique_ptr<InsertTask>> tasks;
    for (auto i = 0; i < numWorkers; ++i) {
      tasks.push_back(
          std::unique_ptr<InsertTask>(new InsertTask(this->getContext())));
    }
    return tasks;
  }
  void destroyTasks(std::vector<std::unique_ptr<InsertTask>>& tasks) {
    tasks.erase(tasks.begin(), tasks.end());
  }
};

template <typename Allocator, typename Function>
class LookupGeneratedKeys : public Function {
  using LookupTask = LookupGeneratedKeys<Allocator, Function>;

 private:
  size_t itemsFound_{0};

 public:
  LookupGeneratedKeys(FunctionContext<Allocator>& context)
      : Function(context) {}

  void operator()(const Range& r) {
    itemsFound_ = lookUpGeneratedKeys<Allocator>(
        r, this->getContext().getDataSize(), this->getContext().getAllocator());
  }

  size_t getStats() { return itemsFound_; }

  std::vector<std::unique_ptr<LookupTask>> generateTasks(size_t numWorkers) {
    std::vector<std::unique_ptr<LookupTask>> tasks;
    for (auto i = 0; i < numWorkers; ++i) {
      tasks.push_back(
          std::unique_ptr<LookupTask>(new LookupTask(this->getContext())));
    }
    return tasks;
  }
  void destroyTasks(std::vector<std::unique_ptr<LookupTask>>& tasks) {
    tasks.erase(tasks.begin(), tasks.end());
  }
};

using Context = FunctionContext<LruAllocator>;
using InsertGeneratedDataFunction =
    InsertGeneratedData<LruAllocator, Function<Context>>;
using LookupGeneratedKeysFunction =
    LookupGeneratedKeys<LruAllocator, Function<Context>>;

} // namespace tests
} // namespace cachelib
} // namespace facebook
