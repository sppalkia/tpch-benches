#ifndef __NVL_SYNC_DICT_H__
#define __NVL_SYNC_DICT_H__

#include <stdlib.h>
#include <mutex>
#include "dict.h"

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
class SynchronizedDict {
  struct Entry { bool filled; K key; V value; std::mutex lock; };

  Entry *_entries;
  size_t _size;       // Number of elements we've really filled
  size_t _capacity;   // Total number of slots we have in entries

  public:
  SynchronizedDict(int capacity):_size(0),_capacity(capacity) {
    _entries = new Entry[capacity];
  }

  ~SynchronizedDict() { delete[] _entries; }

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

  /** Insert a value with the given key, update any previous one */
  void put(const K& key, const V& value) {
    size_t mask = _capacity - 1;
    size_t pos = hash(key) & mask;
    size_t step = 1;
    bool done = false;
    while (!done) {
      while (_entries[pos].filled && !(_entries[pos].key == key)) {
        pos = (pos + step) & mask;
        step += 1;
      }

      // Whenever we encounter an unfilled entry,
      // lock it and check if it can be filled.
      // TODO: Verify correctness.
      _entries[pos].lock.lock();
      if (!_entries[pos].filled) {
        _entries[pos].key = key;
        _entries[pos].value = value;
        _entries[pos].filled = true;
        // TODO: Wrong. Maybe use atomic for this.
        _size += 1;
        done = true;
      } else if (_entries[pos].filled && _entries[pos].key == key) {
        _entries[pos].value += value;
        done = true;
      }
      _entries[pos].lock.unlock();
    }
  }

  size_t size() const {
    return _size;
  }
};

#endif // __NVL_DICT_H__
