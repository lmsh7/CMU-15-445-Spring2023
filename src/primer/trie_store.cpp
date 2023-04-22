#include "primer/trie_store.h"
#include "common/exception.h"

namespace bustub {

template <class T>
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {
  // Pseudo-code:
  // (1) Take the root lock, get the root, and release the root lock. Don't lookup the value in the
  //     trie while holding the root lock.
  // (2) Lookup the value in the trie.
  // (3) If the value is found, return a ValueGuard object that holds a reference to the value and the
  //     root. Otherwise, return std::nullopt.

  std::unique_lock<std::mutex> root_lock(root_lock_);
  auto now_root = root_;
  root_lock.unlock();
  auto value = now_root.Get<T>(key);
  if (value != nullptr) {
    return ValueGuard<T>(now_root, *value);
  }
  return std::nullopt;
}

template <class T>
void TrieStore::Put(std::string_view key, T value) {
  std::unique_lock<std::mutex> write_lock(write_lock_);
  auto now_root = root_.Put<T>(key, std::move(value));
  std::unique_lock<std::mutex> root_lock(root_lock_);
  root_ = now_root;
  root_lock.unlock();
  write_lock.unlock();
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
}

void TrieStore::Remove(std::string_view key) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  std::unique_lock<std::mutex> write_lock(write_lock_);
  auto now_root = root_.Remove(key);
  std::unique_lock<std::mutex> root_lock(root_lock_);
  root_ = now_root;
  root_lock.unlock();
  write_lock.unlock();

  //  throw NotImplementedException("TrieStore::Remove is not implemented.");
}

// Below are explicit instantiation of template functions.

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<uint32_t>>;
template void TrieStore::Put(std::string_view key, uint32_t value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<std::string>>;
template void TrieStore::Put(std::string_view key, std::string value);

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<Integer>>;
template void TrieStore::Put(std::string_view key, Integer value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<MoveBlocked>>;
template void TrieStore::Put(std::string_view key, MoveBlocked value);

}  // namespace bustub
