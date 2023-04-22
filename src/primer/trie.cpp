#include "primer/trie.h"
#include <memory>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  auto now = std::const_pointer_cast<TrieNode>(root_);
  uint64_t ind = 0;
  while (now != nullptr) {
    if (ind == key.size()) {
      break;
    }
    now = std::const_pointer_cast<TrieNode>(now->children_[key[ind]]);
    ind++;
  }
  auto ans = std::dynamic_pointer_cast<TrieNodeWithValue<T>>(now);
  if (ans) {
    return ans->value_.get();
  }
  return nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  std::shared_ptr<TrieNode> new_root;
  auto value_ptr = std::make_shared<T>(std::move(value));

  if (!key.empty()) {
    new_root = root_ == nullptr ? std::make_shared<TrieNode>() : std::shared_ptr<TrieNode>(root_->Clone());
  } else {
    if (root_ == nullptr) {
      new_root = std::shared_ptr<TrieNodeWithValue<T>>(new TrieNodeWithValue<T>(value_ptr));
    } else {
      new_root = std::shared_ptr<TrieNodeWithValue<T>>(new TrieNodeWithValue<T>(root_->children_, value_ptr));
    }
  }
  std::shared_ptr<Trie> new_trie(new Trie(new_root));
  auto now = std::const_pointer_cast<TrieNode>(new_trie->root_);

  for (size_t i = 0; i < key.size(); i++) {
    if (now->children_.find(key[i]) == now->children_.end()) {  // 找不到已有的孩子
      if (i != key.size() - 1) {
        now->children_[key[i]] = std::make_shared<TrieNode>();
      } else {  // 最后一个点去构造with value
        now->children_[key[i]] = std::shared_ptr<TrieNodeWithValue<T>>(new TrieNodeWithValue<T>(value_ptr));
      }
      now = std::const_pointer_cast<TrieNode>(now->children_[key[i]]);
    } else {                      // 找到了已有的孩子
      if (i == key.size() - 1) {  // 可能是叶子节点, 也可能是之前的中间节点, 需要替换value
        now->children_[key[i]] = std::shared_ptr<TrieNodeWithValue<T>>(
            new TrieNodeWithValue<T>(now->children_[key[i]]->children_, value_ptr));
      } else {
        auto son = std::shared_ptr<TrieNode>(now->children_[key[i]]->Clone());
        now->children_[key[i]] = son;
        now = son;
      }
    }
  }
  //  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key
  //  already
  //  // exists, you should create a new `TrieNodeWithValue`.
  //  auto fuck = std::const_pointer_cast<TrieNode>(new_trie->root_);
  //  std::cout << (fuck->children_.find('t') == fuck->children_.end()) << std::endl;
  return *new_trie;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  std::shared_ptr<TrieNode> new_root;
  if (!key.empty()) {
    new_root = root_ == nullptr ? std::make_shared<TrieNode>() : std::shared_ptr<TrieNode>(root_->Clone());
  } else {
    new_root = std::make_shared<TrieNode>(root_->children_);
    //    std::cout << new_root->is_value_node_ << "fuck: \n";
  }
  std::shared_ptr<Trie> new_trie(new Trie(new_root));
  auto now = std::const_pointer_cast<TrieNode>(new_trie->root_);

  for (size_t i = 0; i < key.size(); i++) {
    if (now->children_.find(key[i]) == now->children_.end()) {
      return *new_trie;
    }
    // 找到了已有的孩子
    if (i == key.size() -
                 1) {  // 可能是叶子节点, 也可能是之前的中间节点, 判断是否有孩子, 如果有就构造TrieNode, 如果没有就删除
      if (now->children_[key[i]]->children_.empty()) {
        now->children_[key[i]].reset();
        now->children_.erase(key[i]);
      } else {
        now->children_[key[i]] = std::make_shared<TrieNode>(now->children_[key[i]]->children_);
      }
    } else {
      auto son = std::shared_ptr<TrieNode>(now->children_[key[i]]->Clone());
      now->children_[key[i]] = son;
      now = son;
    }
  }
  //  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key
  //  already
  //  // exists, you should create a new `TrieNodeWithValue`.
  //  auto fuck = std::const_pointer_cast<TrieNode>(new_trie->root_);
  //  std::cout << (fuck->children_.find('t') == fuck->children_.end()) << std::endl;
  return *new_trie;

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
