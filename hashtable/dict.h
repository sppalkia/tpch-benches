#ifndef __NVL_DICT_H__
#define __NVL_DICT_H__

#include <stdlib.h>

size_t hash(int v) { return v; }

namespace {
const size_t INITIAL_SIZE = 16;
const double LOAD_FACTOR = 0.7;
}

/**
 * Append-only dictionary using open hashing. This dictionary keeps both keys and values in dense
 * structs, i.e., it copies them instead of tracking pointers. We can add another one that doesn't
 * do this if needed. Note that for vector keys (e.g. strings), we are just storing a pointer.
 *
 * This class is used for both immutable dictionaries and DictBuilder. Also, like in Vec, the
 * copy constructor and assignment operators just create a reference to the same underlying data.
 *
 * The probing algorithm used is quadratic probing, with a power-of-2 hash table size.
 */
template<typename K, typename V>
class Dict {
  struct Entry { bool filled; K key; V value; };

  Entry *_entries;
  size_t _size;       // Number of elements we've really filled
  size_t _capacity;   // Total number of slots we have in entries

public:
  Dict(): _entries(0), _capacity(0), _size(0) {}

  Dict(const Dict<K, V>& other):
    _entries(other._entries), _size(other._size), _capacity(other._capacity) {}

  ~Dict() {
    if (_entries != 0) free(_entries);
  }

  Dict<K, V>& operator = (const Dict<K, V>& other) {
    _entries = other._entries;
    _size = other._size;
    _capacity = other._capacity;
    return *this;
  }

  /** Get a pointer to the value for a given key, or null if it is missing */
  V* get(const K& key) {
    if (!_entries) {
      return 0;
    }
    size_t mask = _capacity - 1;
    size_t pos = hash(key) & mask;
    size_t step = 1;
    while (_entries[pos].filled) {
      if (_entries[pos].key == key) {
        return &_entries[pos].value;
      }
      pos = (pos + step) & mask;
      step += 1;
    }
    return 0;
  }

  /** Insert a value with the given key, overriding any previous one */
  void put(const K& key, const V& value) {
    if (_size >= LOAD_FACTOR * _capacity) {
      growAndRehash();
    }
    bool newKey = putInto(_entries, _capacity, key, value);
    if (newKey) {
      _size += 1;
    }
  }

  size_t size() const {
    return _size;
  }

  /** Iterate over other dict and insert its elements into current dict **/
  void combine(const Dict<K,V>& other) {
    for (int i=0; i<other._capacity; i++) {
      if (other._entries[i].filled) {
        put(other._entries[i].key, other._entries[i].value);
      }
    }
  }

private:
  /**
   * Double the size of the table and re-hash values into new positions.
   */
  void growAndRehash() {
    size_t newCapacity = (_capacity == 0) ? INITIAL_SIZE : _capacity * 2;
    Entry *newEntries = (Entry*) malloc(newCapacity * sizeof(Entry));
    for (size_t i = 0; i < newCapacity; i++) {
      newEntries[i].filled = false;
    }
    for (size_t i = 0; i < _capacity; i++) {
      if (_entries[i].filled) {
        putInto(newEntries, newCapacity, _entries[i].key, _entries[i].value);
      }
    }
    if (_entries) {
      free(_entries);
    }
    _entries = newEntries;
    _capacity = newCapacity;
  }

  /**
   * Add an entry into a hash table without resising it (it's guaranteed that the entry will fit).
   * Returns true if we added a new key or false if the key already existed.
   */
  bool putInto(Entry *entries, size_t capacity, const K& key, const V& value) {
    size_t mask = capacity - 1;
    size_t pos = hash(key) & mask;
    size_t step = 1;
    while (entries[pos].filled && !(entries[pos].key == key)) {
      pos = (pos + step) & mask;
      step += 1;
    }
    bool wasFilled = entries[pos].filled;

    // Insert or update.
    if (entries[pos].filled) {
      entries[pos].value += value;
    } else {
      entries[pos].filled = true;
      entries[pos].key = key;
      entries[pos].value = value;
    }

    return !wasFilled;
  }
};

// TODO: implement equality, hash and comparisons if we want to allow those.

#endif // __NVL_DICT_H__
