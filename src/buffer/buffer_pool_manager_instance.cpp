//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
//  throw NotImplementedException(
//      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
//      "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  frame_id_t target_frame_id;
  if (!free_list_.empty()) {
    target_frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&target_frame_id)) {
      page_id = nullptr;
      return nullptr;
    }
  }

  if (pages_[target_frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[target_frame_id].GetPageId(), pages_[target_frame_id].GetData());
  }

  page_table_->Remove(pages_[target_frame_id].GetPageId());

  pages_[target_frame_id].ResetMemory();
  pages_[target_frame_id].page_id_ = AllocatePage();
  pages_[target_frame_id].pin_count_ = 1;

  replacer_->RecordAccess(target_frame_id);
  replacer_->SetEvictable(target_frame_id, false);

  page_table_->Insert(pages_[target_frame_id].page_id_, target_frame_id);

  *page_id = pages_[target_frame_id].GetPageId();
  return &pages_[target_frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  frame_id_t target_frame_id;

  if (page_table_->Find(page_id, target_frame_id)) {
    replacer_->RecordAccess(target_frame_id);
    return &pages_[target_frame_id];
  }

  if (!free_list_.empty()) {
    target_frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&target_frame_id)) {
      return nullptr;
    }
  }

  if (pages_[target_frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[target_frame_id].GetPageId(), pages_[target_frame_id].GetData());
  }

  page_table_->Remove(pages_[target_frame_id].GetPageId());

  pages_[target_frame_id].ResetMemory();
  pages_[target_frame_id].page_id_ = page_id;
  pages_[target_frame_id].pin_count_ = 1;

  page_table_->Insert(pages_[target_frame_id].page_id_, target_frame_id);

  disk_manager_->ReadPage(pages_[target_frame_id].page_id_, pages_[target_frame_id].GetData());
  replacer_->RecordAccess(target_frame_id);
  replacer_->SetEvictable(target_frame_id, false);

  return &pages_[target_frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  frame_id_t target_frame_id;

  if (!page_table_->Find(page_id, target_frame_id)) {
    return false;
  }

  if (pages_[target_frame_id].GetPinCount() == 0) {
    return false;
  }

  pages_[target_frame_id].pin_count_ -= 1;
  if (pages_[target_frame_id].GetPinCount() == 0) {
    replacer_->SetEvictable(target_frame_id, true);
  }

  pages_[target_frame_id].is_dirty_ = is_dirty;

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  frame_id_t target_frame_id;

  if (!page_table_->Find(page_id, target_frame_id)) {
    return false;
  }

  disk_manager_->WritePage(page_id, pages_[target_frame_id].GetData());
  pages_[target_frame_id].is_dirty_ = false;
  replacer_->RecordAccess(target_frame_id);

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; ++i) {
    FlushPgImp(pages_[i].GetPageId());
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  frame_id_t target_frame_id;

  if (!page_table_->Find(page_id, target_frame_id)) {
    return true;
  }

  if (pages_[target_frame_id].GetPinCount() > 0) {
    return false;
  }

  page_table_->Remove(page_id);
  replacer_->Remove(target_frame_id);
  pages_[target_frame_id].ResetMemory();
  pages_[target_frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[target_frame_id].pin_count_ = 0;
  pages_[target_frame_id].is_dirty_ = false;

  free_list_.push_back(target_frame_id);

  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
