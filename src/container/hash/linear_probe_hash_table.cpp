//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/linear_probe_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
      page_id_t header_page_id = INVALID_PAGE_ID;
      auto header_page = 
          reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager->NewPage(&header_page_id, nullptr)->GetData());
      header_page_id_ = header_page_id;
      header_page->SetPageId(header_page_id);
      header_page->SetSize(num_buckets);
      page_id_t block_page_id = INVALID_PAGE_ID;
      for (size_t i = 0; i < num_buckets; ++i) {
        buffer_pool_manager->NewPage(&block_page_id, nullptr);
        header_page->AddBlockPageId(block_page_id);
        buffer_pool_manager->UnpinPage(block_page_id, false);
      }
      buffer_pool_manager->UnpinPage(header_page_id, true);
    }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  Page* raw_header_page = buffer_pool_manager_->FetchPage(header_page_id_, nullptr);
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(raw_header_page->GetData());
  raw_header_page->RLatch();
  uint64_t hash_val = hash_fn_.GetHash(key);
  size_t block_idx = hash_val % header_page->GetSize();
  size_t slot_idx = hash_val % BLOCK_ARRAY_SIZE;

  page_id_t block_page_id = header_page->GetBlockPageId(block_idx);
  Page* raw_block_page = buffer_pool_manager_->FetchPage(block_page_id, nullptr);
  HashTableBlockPage<KeyType, ValueType, KeyComparator>* block_page = 
      reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>* >(raw_block_page->GetData());
  
  size_t search_slot_idx = slot_idx;
  size_t search_block_idx = block_idx;
  raw_block_page->RLatch();
  while (true) {
    if (!block_page->IsOccupied(search_slot_idx)) {
      break;
    }
    if (block_page->IsReadable(search_slot_idx)) {
      if (!comparator_(key, block_page->KeyAt(search_slot_idx))) {
        result->push_back(block_page->ValueAt(search_slot_idx));
      }
    }
    search_slot_idx = (search_slot_idx + 1) % BLOCK_ARRAY_SIZE;
    if (search_slot_idx == slot_idx) {
      search_block_idx = (search_block_idx + 1) % header_page->GetSize();
      if (search_block_idx == block_idx) {
        break;
      }
      buffer_pool_manager_->UnpinPage(block_page_id, false);
      block_page_id = header_page->GetBlockPageId(search_block_idx);
      raw_block_page->RUnlatch();
      raw_block_page = buffer_pool_manager_->FetchPage(block_page_id, nullptr);
      raw_block_page->RLatch();
      block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>* >(raw_block_page->GetData());
    }
  }
  raw_block_page->RUnlatch();
  raw_header_page->RUnlatch();
  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(block_page_id, false);
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  return result->size();
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  bool insert_success = true;
  Page* raw_header_page = buffer_pool_manager_->FetchPage(header_page_id_, nullptr);
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(raw_header_page->GetData());
  raw_header_page->RLatch();
  uint64_t hash_val = hash_fn_.GetHash(key);
  size_t block_idx = hash_val % header_page->GetSize();
  size_t slot_idx = hash_val % BLOCK_ARRAY_SIZE;
  page_id_t block_page_id = header_page->GetBlockPageId(block_idx);
  Page* raw_block_page = buffer_pool_manager_->FetchPage(block_page_id, nullptr);
  HashTableBlockPage<KeyType, ValueType, KeyComparator>* block_page = 
      reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>* >(raw_block_page->GetData());
  
  size_t search_slot_idx = slot_idx;
  size_t search_block_idx = block_idx;
  raw_block_page->WLatch();
  while (true) {
    if (!block_page->IsReadable(search_slot_idx)) {
      block_page->Insert(search_slot_idx, key, value);
      break;
    }
    if (block_page->IsReadable(search_slot_idx)) {
      if (!comparator_(key, block_page->KeyAt(search_slot_idx))) {
        if (value == block_page->ValueAt(search_slot_idx)) {
          insert_success = false;
          break;
        }
      }
    }
    search_slot_idx = (search_slot_idx + 1) % BLOCK_ARRAY_SIZE;
    if (search_slot_idx == slot_idx) {
      search_block_idx = (search_block_idx + 1) % header_page->GetSize();
      if (search_block_idx == block_idx) {
        size_t size = header_page->GetSize();
        if (size > 1020 / 2) {
          return false;
        } else {
          // resize
          buffer_pool_manager_->UnpinPage(block_page_id, true);
          buffer_pool_manager_->UnpinPage(header_page_id_, false);
          raw_block_page->WUnlatch();
          raw_header_page->RUnlatch();
          table_latch_.RUnlock();
          Resize(size);
          bool result = Insert(transaction, key, value);
          return result;
        }
      }
      buffer_pool_manager_->UnpinPage(block_page_id, false);
      block_page_id = header_page->GetBlockPageId(search_block_idx);
      raw_block_page->WUnlatch();
      raw_block_page = buffer_pool_manager_->FetchPage(block_page_id, nullptr);
      raw_block_page->WLatch();
      block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>* >(raw_block_page->GetData());
    }
  }
  raw_block_page->WUnlatch();
  raw_header_page->RUnlatch();
  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(block_page_id, true);
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  return insert_success;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  bool remove_success = true;
  Page* raw_header_page = buffer_pool_manager_->FetchPage(header_page_id_, nullptr);
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(raw_header_page->GetData());
  raw_header_page->RLatch();
  uint64_t hash_val = hash_fn_.GetHash(key);
  size_t block_idx = hash_val % header_page->GetSize();
  size_t slot_idx = hash_val % BLOCK_ARRAY_SIZE;

  page_id_t block_page_id = header_page->GetBlockPageId(block_idx);
  Page* raw_block_page = buffer_pool_manager_->FetchPage(block_page_id, nullptr);
  HashTableBlockPage<KeyType, ValueType, KeyComparator>* block_page = 
      reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>* >(raw_block_page->GetData());
  
  size_t search_slot_idx = slot_idx;
  size_t search_block_idx = block_idx;
  raw_block_page->WLatch();
  while (true) {
    if (block_page->IsReadable(search_slot_idx)) {
      if (!comparator_(key, block_page->KeyAt(search_slot_idx))) {
        if (value == block_page->ValueAt(search_slot_idx)) {
          block_page->Remove(search_slot_idx);
          break;
        }
      }
    }
    if (!block_page->IsOccupied(search_slot_idx)) {
      remove_success = false;
      break;
    }
    search_slot_idx = (search_slot_idx + 1) % BLOCK_ARRAY_SIZE;
    if (search_slot_idx == slot_idx) {
      search_block_idx = (search_block_idx + 1) % header_page->GetSize();
      buffer_pool_manager_->UnpinPage(block_page_id, false);
      block_page_id = header_page->GetBlockPageId(search_block_idx);
      raw_block_page->WUnlatch();
      raw_block_page = buffer_pool_manager_->FetchPage(block_page_id, nullptr);
      raw_block_page->WLatch();
      block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>* >(raw_block_page->GetData());
    }
  }
  buffer_pool_manager_->UnpinPage(block_page_id, true);
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  raw_block_page->WUnlatch();
  raw_header_page->RUnlatch();
  table_latch_.RUnlock();
  return remove_success;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  table_latch_.WLock();
  Page* raw_header_page = buffer_pool_manager_->FetchPage(header_page_id_, nullptr);
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(raw_header_page->GetData());
  raw_header_page->WLatch();
  size_t cur_size = initial_size;
  if (cur_size * 2 < 1020) {
    header_page->SetSize(cur_size * 2);
    page_id_t block_page_id = INVALID_PAGE_ID;
    for (size_t i = cur_size; i < cur_size * 2; ++i) {
      buffer_pool_manager_->NewPage(&block_page_id, nullptr);
      header_page->AddBlockPageId(block_page_id);
      buffer_pool_manager_->UnpinPage(block_page_id, false);
    }
  }
  raw_header_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, true);
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  table_latch_.RLock();
  Page* raw_header_page = buffer_pool_manager_->FetchPage(header_page_id_, nullptr);
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(raw_header_page->GetData());
  raw_header_page->RLatch();
  size_t size = header_page->GetSize();
  raw_header_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  table_latch_.RUnlock();
  return size;
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
