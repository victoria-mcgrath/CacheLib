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
#include <sys/stat.h>

#include <system_error>
#include <variant>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include <folly/Format.h>
#include <folly/Range.h>
#pragma GCC diagnostic pop

namespace facebook {
namespace cachelib {

enum ShmAttachT { ShmAttach };
enum ShmNewT { ShmNew };

enum PageSizeT {
  NORMAL = 0,
  TWO_MB,
  ONE_GB,
};

constexpr int kInvalidFD = -1;

struct FileShmSegmentOpts {
  FileShmSegmentOpts(std::string path = ""): path(path) {}
  std::string path;
};

struct PosixSysVSegmentOpts {
  PosixSysVSegmentOpts(bool usePosix = false): usePosix(usePosix) {}
  bool usePosix;
};

using ShmTypeOpts = std::variant<FileShmSegmentOpts, PosixSysVSegmentOpts>;

struct ShmSegmentOpts {
  PageSizeT pageSize{PageSizeT::NORMAL};
  bool readOnly{false};
  size_t alignment{1}; // alignment for mapping.
  // opts specific to segment type
  ShmTypeOpts typeOpts{PosixSysVSegmentOpts(false)};

  explicit ShmSegmentOpts(PageSizeT p) : pageSize(p) {}
  explicit ShmSegmentOpts(PageSizeT p, bool ro) : pageSize(p), readOnly(ro) {}
  explicit ShmSegmentOpts(PageSizeT p, bool ro, const std::string& path) :
                                       pageSize(p), readOnly(ro),
                                       typeOpts(path) {}
  explicit ShmSegmentOpts(PageSizeT p, bool ro, bool posix) :
                                       pageSize(p), readOnly(ro),
                                       typeOpts(posix) {}
  ShmSegmentOpts() : pageSize(PageSizeT::NORMAL) {}
};

// Represents a mapping on shm with and address and size
struct ShmAddr {
  ShmAddr(void* a, size_t s) : addr(a), size(s) {}
  ShmAddr() {}

  bool isMapped() const noexcept { return addr != nullptr; }

  void* addr{nullptr}; // start of the memory
  size_t size{0};      // length from start that actually has a backing shm
};

/* common interface for both posix and sysv shared memory segments */
class ShmBase {
 public:
  ShmBase(ShmSegmentOpts opts, std::string name)
      : opts_(std::move(opts)), name_(std::move(name)) {}
  ShmBase(const ShmBase&) = delete;
  ShmBase& operator=(const ShmBase&) = delete;

  virtual ~ShmBase() {}

  bool isActive() const noexcept { return state_ == State::NORMAL; }
  bool isMarkedForRemoval() const noexcept {
    return state_ == State::MARKED_FOR_REMOVAL;
  }

  virtual size_t getSize() const = 0;
  virtual std::string getKeyStr() const = 0;
  virtual void* mapAddress(void* addr) const = 0;
  virtual void unMap(void* addr) const = 0;
  virtual void markForRemoval() = 0;

  const std::string& getName() const { return name_; }

 protected:
  void markActive() noexcept { state_ = State::NORMAL; }
  void markForRemove() noexcept { state_ = State::MARKED_FOR_REMOVAL; }

  // options for this segment
  ShmSegmentOpts opts_{};

  // address mapping that ensures that we own this segment for the lifeteime
  // of the object.
  void* referenceMapping_{nullptr};

 private:
  enum class State { NORMAL, MARKED_FOR_REMOVAL };
  State state_{State::NORMAL}; // current state of the segment.
  std::string name_{};         // name of the segment
};

namespace detail {

/* current page size of the system by the type */
size_t getPageSize(PageSizeT p = PageSizeT::NORMAL);

/* round up to the closest page size */
size_t getPageAlignedSize(size_t size, PageSizeT p = PageSizeT::NORMAL);

/* returns page aligned size for the input that is atleast as big as the input
 * size */
size_t pageAligned(size_t size, PageSizeT p = PageSizeT::NORMAL);

/* true if the length is page aligned  */
bool isPageAlignedSize(size_t length, PageSizeT p = PageSizeT::NORMAL);

/* true if the address is page aligned */
bool isPageAlignedAddr(void* addr, PageSizeT p = PageSizeT::NORMAL);

// return the page size of the address mapping in this process.
//
// @throw  std::invalid_argument if the address mapping is not found.
PageSizeT getPageSizeInSMap(void* addr);
} // namespace detail
} // namespace cachelib
} // namespace facebook
