//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  //  throw NotImplementedException(
  //      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //      "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  frame_id_t fid;
  std::unique_lock<std::mutex> lock(latch_);

  if (free_list_.empty()) {
    if (replacer_->Evict(&fid)) {
      BUSTUB_ENSURE(page_table_.find(f2p_[fid]) != page_table_.end(), "N: evit page not in page table")
      BUSTUB_ENSURE(this->DeletePageUnlocked(f2p_[fid]), "N: delete page failed")
      BUSTUB_ENSURE(!free_list_.empty(), "N: free list size is 0")
    } else {
      return nullptr;
    }
    BUSTUB_ENSURE(!free_list_.empty(), "N1: free list size is 0")
  }
  BUSTUB_ENSURE(!free_list_.empty(), "N2: free list size is 0")

  fid = free_list_.front();
  free_list_.pop_front();
  while (page_used_[next_page_id_]) {
    next_page_id_++;
  }
  *page_id = AllocatePage();

  page_table_[*page_id] = fid;
  f2p_[fid] = *page_id;
  page_used_[*page_id] = true;
  //  lock.unlock();

  pages_[fid].page_id_ = *page_id;
  pages_[fid].ResetMemory();
  disk_manager_->ReadPage(*page_id, pages_[fid].data_);
  pages_[fid].pin_count_ = 1;

  replacer_->RecordAccess(fid);
  return pages_ + fid;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  frame_id_t fid;
  std::unique_lock<std::mutex> lock(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    if (free_list_.empty()) {
      if (replacer_->Evict(&fid)) {
        BUSTUB_ENSURE(page_table_.find(f2p_[fid]) != page_table_.end(), "F: evit page not in page table")
        BUSTUB_ENSURE(this->DeletePageUnlocked(f2p_[fid]), "F: delete page failed")
        BUSTUB_ENSURE(!free_list_.empty(), "F: free list size is 0")
      } else {
        return nullptr;
      }
      BUSTUB_ENSURE(!free_list_.empty(), "F1: free list size is 0")
    }
    BUSTUB_ENSURE(!free_list_.empty(), "F2: free list size is 0")
    fid = free_list_.front();
    free_list_.pop_front();

    page_table_[page_id] = fid;
    f2p_[fid] = page_id;
    page_used_[page_id] = true;

    pages_[fid].page_id_ = page_id;
    pages_[fid].pin_count_ = 0;
    pages_[fid].ResetMemory();
    disk_manager_->ReadPage(page_id, pages_[fid].data_);
  }
  fid = page_table_[page_id];
  //  lock.unlock();

  pages_[fid].pin_count_++;
  replacer_->RecordAccess(fid);

  // 我草我终于知道了为啥了, unpin后如果这里fetch不增加pin后更改evictable, 就会一直可以被删除
  replacer_->SetEvictable(fid, pages_[fid].pin_count_ == 0);
  return pages_ + fid;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  frame_id_t fid = page_table_[page_id];
  //  lock.unlock();

  if (pages_[fid].pin_count_ == 0) {
    return false;
  }
  pages_[fid].pin_count_ -= 1;
  if (is_dirty) {
    pages_[fid].is_dirty_ = true;
  }
  replacer_->SetEvictable(fid, pages_[fid].pin_count_ == 0);
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  return FlushPageUnlocked(page_id);
}

void BufferPoolManager::FlushAllPages() {
  std::unique_lock<std::mutex> lock(latch_);
  for (auto &it : page_table_) {
    FlushPageUnlocked(it.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  return DeletePageUnlocked(page_id);
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, this->FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = this->FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = this->FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, this->NewPage(page_id)}; }

auto BufferPoolManager::FlushPageUnlocked(page_id_t page_id) -> bool {
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  frame_id_t fid = page_table_[page_id];
  if (pages_[fid].is_dirty_) {
    disk_manager_->WritePage(page_id, pages_[fid].data_);
    pages_[fid].is_dirty_ = false;
    return true;
  }
  return true;
}

auto BufferPoolManager::DeletePageUnlocked(page_id_t page_id) -> bool {
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  frame_id_t fid = page_table_[page_id];
  if (pages_[fid].pin_count_ > 0) {
    return false;
  }

  FlushPageUnlocked(page_id);

  pages_[fid].ResetMemory();
  pages_[fid].page_id_ = INVALID_PAGE_ID;
  pages_[fid].pin_count_ = 0;
  pages_[fid].is_dirty_ = false;

  replacer_->Remove(fid);
  page_table_.erase(page_id);
  f2p_.erase(fid);
  free_list_.emplace_back(fid);
  return true;
}
}  // namespace bustub
