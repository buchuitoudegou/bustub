//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  max_size = num_pages;
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  if (Size() == 0) {
    return false;
  }
  int result = -1;
  while (result < 0) {
    // find out the page that
    // 1. in the ClockPlacer
    // 2. NOT currently unpinned
    if (this->page_ref[this->pages[cursor]]) {
      this->page_ref[this->pages[cursor]] = false;
      // clock hand increment
      cursor += 1;
      cursor %= this->pages.size();
    } else {
      result = cursor;
      *frame_id = this->pages[result];
      this->page_ref.erase(this->page_ref.find(this->pages[result]));
      auto it = this->pages.begin();
      for (int i = 0; i < result; ++i) {
        it ++;
      }
      this->pages.erase(it);
    }
  }
  return true; 
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  // a page is pinned
  // remove it (should not be moved out)
  auto target = this->page_ref.find(frame_id);
  if (target != this->page_ref.end()) {
    this->page_ref.erase(target);
    auto it = this->pages.begin();
    for (;it != this->pages.end(); it += 1) {
      if (*it == frame_id) {
        break;
      }
    }
    this->pages.erase(it);
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  // a page unpinned
  // ref flag set to be true
  // add to the ClockPlacer
  // page has NOT been in placer
  if (this->max_size == this->pages.size()) {
    return;
  }
  if (this->page_ref.find(frame_id) == this->page_ref.end()) {
    this->page_ref.insert(std::make_pair(frame_id, true));
    this->pages.push_back(frame_id);
  } else {
    this->page_ref[frame_id] = true;
  }
}

size_t ClockReplacer::Size() { 
  return this->pages.size();
}

}  // namespace bustub
