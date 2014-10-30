//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
/*
Copyright 2013 Henrik Mühe and Florian Funke

This file is part of CampersCoreBurner.

CampersCoreBurner is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

CampersCoreBurner is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Asffero General Public License
along with CampersCoreBurner. If not, see <http://www.gnu.org/licenses/>.
*/

/*
COMMENT SUMMARY FOR THIS FILE:

This file contains an implementation of an optimized chaining hashset and hashmap.
Depending on the use case, these hashes perform up to 4x better than std::unordered_map.
*/

#pragma once

#include "hash.hpp"
#include "pool.hpp"
#include <cstdint>
#include <iostream>
#include <numeric>
#include <vector>

namespace campers {

/// A fast, chaining HashMap
template<class T,class Value,class HashFunc=awfy::AWFYHash>
class HashMap {
    /// An entry inside the hash map
public:
    struct Entry {
        /// The key
        T word;
        /// The value for this key
        Value value;
        /// The hash value
        uint32_t hashValue;
        /// Pointer to chain of entries for collisions
        Entry* next;

        /// Constructor
        Entry () : hashValue(0),next(nullptr) {}
        /// Constructor
        Entry (const T& word,uint32_t hashValue) : word(word),hashValue(hashValue),next(nullptr) {}
    };

    /// The hashmap of pointers to actual entries
    std::vector<Entry*> entries;

private:
    /// An instance of the hash funtion functor
    HashFunc hashFunc;
    /// The pool allocator.
    Pool<Entry> entryAllocator;
    /// The exponent, see HashSet and summary for details
    uint64_t exp;
    /// The mask, see HashSet and summary for details
    uint64_t mask;
    /// The number of elements inside the hash,
    uint32_t size_;
    /// The size this hash map was allocated with
    uint64_t allocSize;

public:
    /// Constructor
    HashMap() : HashMap(64) { }
    HashMap(uint32_t size) { hintSize(size); }
    /// Inserts an element into the hashtable. Returns the value for that key, regardless if an insertion happened or not. Hash value supplied externally
    Value* tryInsert(const T& word,uint32_t hashValue);
    /// Inserts an element into the hashtable. Returns the value for that key, regardless if an insertion happened or not.
    Value* tryInsert(const T& word) { return tryInsert(word,hashFunc(word)); }
    /// Finds an element inside the hashmap, returns the value if found or nullptr otherwise. Hash value supplied externally
    Value* find(const T& word,uint32_t hashValue);
    /// Finds an element inside the hashmap, returns the value if found or nullptr otherwise
    Value* find(const T& word) { return find(word,hashFunc(word)); }
    /// Alternative find: uses a type that can differ from T, e.g. T=string, T2=string_ref
    template <class T2> Value* altFind(const T2& word);
    /// Pre allocates the hash to be able to contain size elements efficiently. Also, see summary and HashSet
    void hintSize(uint32_t size);
    /// Returns the current number of elements inside the hash
    uint32_t size() const { return size_; }
    /// Grows the hash to twice its capacity
    // void grow() { grow(2*capacity()); };
    // /// Grows the hash to be able to efficiently contain 2* capacity elements
    // void grow(uint32_t newSize);
    /// Clears all entries in the HashMap
    // void clear();
    /// Returns the current capacity of the hash
    size_t capacity() const { return entries.size(); };

    /// Returns 1 if the value is contained
    size_t count(const T& word) { return find(word)!=nullptr?1:0; }
    /// Clears all entries in the map
    void clear();
};

/// Inserts an element into the hashtable. Returns the value for that key, regardless if an insertion happened or not.
template<class T,class Value,class HashFunc>
Value* HashMap<T,Value,HashFunc>::tryInsert(const T& word,uint32_t hashValue) {
    hashValue>>=8;
    Entry*& entry=entries[hashValue&mask];
    // Unroll empty slot branch
    if (entry==nullptr) {
        ++size_;
        entry=entryAllocator.allocate();
        new (entry) Entry(word,hashValue);
        return &entry->value;
    } else {
        // Unroll immediate hit branch
        if (entry->hashValue==hashValue&&entry->word==word) return &entry->value;

        // Iterate to the end of the chain
        Entry* entryIter;
        for (entryIter=entry;entryIter->next!=nullptr;entryIter=entryIter->next) if (entryIter->next->hashValue==hashValue&&entryIter->next->word==word) return &entryIter->next->value;

        // Word is not present, insert and return reference for backpatching
        ++size_;
        entryIter->next=entryAllocator.allocate();
        new (entryIter->next) Entry(word,hashValue);
        return &entryIter->next->value;
    }
}

/// Finds an element inside the hashmap, returns the value if found or nullptr otherwise. Hash value supplied externally
template<class T,class Value,class HashFunc>
Value* HashMap<T,Value,HashFunc>::find(const T& word,uint32_t hashValue) {
    hashValue>>=8;
    Entry* entry=entries[hashValue&mask];
    // Unroll empty slot branch
    if (entry==nullptr) {
        return nullptr;
    } else {
        // Unroll immediate hit branch
        if (entry->hashValue==hashValue&&entry->word==word) return &entry->value;

        // Iterate to the end of the chain
        for (;entry->next!=nullptr;entry=entry->next) if (entry->next->hashValue==hashValue&&entry->next->word==word) return &entry->next->value;

        return nullptr;
    }
}

/// Alternative find: uses a type that can differ from T, e.g. T=string, T2=string_ref
template<class T,class Value,class HashFunc>
template<class T2>
Value* HashMap<T,Value,HashFunc>::altFind(const T2& word) {
    uint32_t hashValue=hashFunc(word);
    hashValue>>=8;
    Entry* entry=entries[hashValue&mask];
    // Unroll empty slot branch
    if (entry==nullptr) {
        return nullptr;
    } else {
        // Unroll immediate hit branch
        if (entry->hashValue==hashValue&&entry->word==word) return &entry->value;

        // Iterate to the end of the chain
        for (;entry->next!=nullptr;entry=entry->next) if (entry->next->hashValue==hashValue&&entry->next->word==word) return &entry->next->value;

        return nullptr;
    }
}

/// Pre allocates the hash to be able to contain size elements efficiently. Also, see summary and HashSet
template<class T,class Value,class HashFunc>
void HashMap<T,Value,HashFunc>::hintSize(uint32_t size) {
    this->allocSize=size;
    for (exp=1;(uint32_t)(1<<exp)<size;++exp);
    entries.resize(1<<exp);
    mask=(1ull<<exp)-1ull;
    size_=0;
}

/// Clears all entries in the map
template<class T,class Value,class HashFunc>
void HashMap<T,Value,HashFunc>::clear() {
    memset(entries.data(),0,entries.size()*sizeof(Entry*));
    entryAllocator.reset();
    size_=0;
}

}
