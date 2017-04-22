#ifndef __NVL_CAPPED_DICT_H__
#define __NVL_CAPPED_DICT_H__

#include <stdlib.h>

/**
 * Append-only dictionary using open hashing. This dictionary keeps both keys and values in dense
 * structs, i.e., it copies them instead of tracking pointers. We can add another one that doesn't
 * do this if needed. Note that for vector keys (e.g. strings), we are just storing a pointer.
 *
 * The probing algorithm used is quadratic probing, with a power-of-2 hash table size.
 */
template<typename K, typename V>
class CappedDict {
  struct Entry { bool filled; K key; V value; };

  size_t _size;       // Number of elements we've really filled
  size_t _max_capacity; // Maximum capacity

public:
  Entry *_entries;
  size_t _capacity;   // Total number of slots we have in entries

  CappedDict(int max_capacity = 2<<12): _capacity(16), _size(0),
    _max_capacity(max_capacity) {
    _entries = (Entry*) malloc(_capacity * sizeof(Entry));
    for (size_t i = 0; i < _capacity; i++) {
      _entries[i].filled = false;
    }
  }

  ~CappedDict() { free(_entries); }

  void set_max_capacity(int cap) { _max_capacity = cap; }

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
  bool put(const K& key, const V& value) {
    bool success = putInto(key, value);
    while (!success && _capacity < _max_capacity) {
      growAndRehash();
      success = putInto(key, value);
    }
    return success;
  }

  size_t size() const {
    return _size;
  }

private:
  /**
   * Double the size of the table and re-hash values into new positions.
   */
  void growAndRehash() {
    size_t newCapacity = (_capacity == 0) ? 16 : _capacity * 2;
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
   * Add an entry into a hash table without resising it.
   * Allow at max 4 steps in quad probing.
   * Return true if we added an element, else false.
   */
  bool putInto(const K& key, const V& value) {
    size_t mask = _capacity - 1;
    // TODO: Hack - works for int/long only now.
    size_t pos = key & mask;
    size_t step = 1;
    while (step < 5 && _entries[pos].filled && !(_entries[pos].key == key)) {
      pos = (pos + step) & mask;
      step += 1;
    }

    if (step == 5) return false;

    bool wasFilled = _entries[pos].filled;

    // Insert or update.
    if (_entries[pos].filled) {
      _entries[pos].value += value;
    } else {
      _entries[pos].filled = true;
      _entries[pos].key = key;
      _entries[pos].value = value;
      _size += 1;
    }

    return true;
  }

  bool putInto(Entry* entries, size_t capacity, const K& key, const V& value) {
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
