//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t target_frame_id = -1;
  for (auto &iter : map_) {
    if (iter.second.IsEvictable()) {
      target_frame_id = iter.first;
      break;
    }
  }

  if (target_frame_id == -1) {
    return false;
  }

  access_info_t target_frame_access_info = map_.at(target_frame_id).GetAccessInfo();

  for (auto &iter : map_) {
    if (!iter.second.IsEvictable()) {
      continue;
    }

    access_info_t current_access_info = iter.second.GetAccessInfo();
    if ((current_access_info.k_distance_ < target_frame_access_info.k_distance_) ||
        (current_access_info.k_distance_ == target_frame_access_info.k_distance_ &&
         current_access_info.earliest_access_timestamp_ >= target_frame_access_info.earliest_access_timestamp_)) {
      continue;
    }

    target_frame_id = iter.first;
    target_frame_access_info = current_access_info;
  }

  map_.erase(target_frame_id);
  curr_size_--;
  *frame_id = target_frame_id;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  CheckFrameId(frame_id);

  std::scoped_lock<std::mutex> lock(latch_);

  if (map_.count(frame_id) == 0) {
    map_.insert({frame_id, FrameInfo(k_)});
  }

  map_.at(frame_id).RecordAccess(current_timestamp_);

  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  CheckFrameId(frame_id);
  std::scoped_lock<std::mutex> lock(latch_);
  if (map_.count(frame_id) == 0) {
    throw std::invalid_argument("Frame is not found.");
  }
  if (set_evictable) {
    if (!map_.at(frame_id).IsEvictable()) {
      curr_size_++;
      map_.at(frame_id).SetEvictable(true);
    }
  } else {
    if (map_.at(frame_id).IsEvictable()) {
      curr_size_--;
      map_.at(frame_id).SetEvictable(false);
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  CheckFrameId(frame_id);
  std::scoped_lock<std::mutex> lock(latch_);
  if (map_.count(frame_id) == 0) {
    return;
  }
  if (!map_.at(frame_id).IsEvictable()) {
    throw std::invalid_argument("Frame is not evictable.");
  }
  map_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

void LRUKReplacer::CheckFrameId(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id >= 0 && (size_t)frame_id <= replacer_size_,
                "frame id is invalid (ie. larger than replacer_size_)");
}

}  // namespace bustub
