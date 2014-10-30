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

You should have received a copy of the GNU Affero General Public License
along with CampersCoreBurner. If not, see <http://www.gnu.org/licenses/>.
*/

/*
COMMENT SUMMARY FOR THIS FILE:

A same size pool allocator which allows for extremely fast allocation of memory for same sized items.
*/

#pragma once

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <iostream>

/// A fixed size pool allocator
template<class T>
class Pool {
   /// Size of the first chunk used for allocations
   static const uint64_t initChunkSize=4096;
   /// Static assertions for correctness
   static_assert(sizeof(T)<=initChunkSize,"T too big for Pool allocator");
   static_assert(sizeof(T)>=sizeof(void*),"T must be bigger than pointer size for my free list");

   /// Free element type used to manage the free list inside the free elements
   struct FreeElement { FreeElement* next; };

   /// A chunk of memory which is later used to fulfill allocations
   struct Chunk {
      /// Size of this chunk
      uint64_t size;
      /// Number of bytes inside this chunk which are already used
      uint64_t used;
      /// Next chunk (inline linked list between chunks)
      Chunk* next;
      /// Used such that memory is always 16 aligned.
      uint64_t alignmentSpacer;

      /// Pointer to the memory, we overallocate the memory for the Chunk such that Chunk is just a header for a stretch of memory
      char* getMem() const { return reinterpret_cast<char*>(((char*)this)+sizeof(Chunk)); }

      /// Allocate a new chunk
      static Chunk* allocate(uint64_t size) {
         assert("Chunk allocation size must be bigger than chunk metadata size"&&size>sizeof(Chunk));
         Chunk* chunk=reinterpret_cast<Chunk*>(malloc(size));
         chunk->used=0;
         chunk->size=size-sizeof(Chunk);
         chunk->next=0;
         return chunk;
      }

      /// Swap chunks
      void swap(Chunk& other) {
         swap(size,other.size);
         swap(used,other.used);
         swap(next,other.next);
      }
   };

   /// First chunk (for deallocation)
   Chunk* firstChunk;
   /// Last chunk
   Chunk* lastChunk;
   /// List of free elements
   FreeElement* freeElements;

public:
   /// Iterator for this memory pool. Iterates over all elements, NOT all allocated elements.
   struct PoolIterator {
      /// Current chunk for iteration
      Chunk* currentChunk;
      /// Position inside this chunk
      uint64_t pos;

      /// Constructor
      PoolIterator() : currentChunk(0),pos(0) {}
      /// Constructor
      PoolIterator(Chunk* c,uint64_t p) : currentChunk(c),pos(p) {}
      /// Moves to the next element
      PoolIterator& operator++() {
         // Can not increment further
         if (!currentChunk) return *this;

         if (pos+sizeof(T) < currentChunk->used)
            // next in chunk
            pos+=sizeof(T);
         else if (currentChunk->next && currentChunk->next->used>0) {
            // next in next chunk
            currentChunk=currentChunk->next; pos=0;
         } else {
            // end
            currentChunk=0;
            pos=0;
         }
         return *this;
      }
      /// Equal for the iterator
      bool operator==(const PoolIterator& other) const { return other.currentChunk==currentChunk && (!other.currentChunk || other.pos==pos); }
      /// Not equal for the iterator
      bool operator!=(const PoolIterator& other) const { return other.currentChunk!=currentChunk || other.pos!=pos; }
      /// Dereferenciation
      T& operator*() { return *(T*)&currentChunk->getMem()[pos]; }
   };

   /// Create an iterator for the first element inside the pool
   PoolIterator begin() { return (firstChunk&&firstChunk->used) ? PoolIterator{firstChunk,0} : end(); }
   /// Create an iterator for the element past the last element inside this pool
   PoolIterator end() { return PoolIterator{0,0}; }

   /// Constructor, creates first chunk
   Pool() : firstChunk(Chunk::allocate(initChunkSize)),lastChunk(firstChunk),freeElements(0) {}
   /// Destructor, deallocates all memory
   ~Pool() {
      for (Chunk* c=firstChunk;c!=0;) {
         Chunk* toFree=c;
         c=c->next;
         free(toFree);
      }
   }
   /// Allocate a fixed-size stretch of memory using the pool allocator
   T* allocate() {
      assert(lastChunk!=nullptr);
      if (freeElements!=nullptr) {
         T* ptr=reinterpret_cast<T*>(freeElements);
         freeElements=freeElements->next;
         return ptr;
      } else if (lastChunk->used+sizeof(T)>=lastChunk->size) {
         // If there was a reset before we can avoid the allocation
         if(lastChunk->next!=nullptr) {
            lastChunk=lastChunk->next;
         } else {
            // Add a new chunk if space isn't sufficient
            lastChunk->next=Chunk::allocate(lastChunk->size<<1);
            lastChunk=lastChunk->next;
            lastChunk->next=nullptr;
         }
      }

      T* ptr=(T*)&lastChunk->getMem()[lastChunk->used];
      lastChunk->used+=sizeof(T);
      return ptr;
   }
   /// Return a piece of memory from the allocator to this allocator
   void deallocate(T* ptr) {
      FreeElement* f = reinterpret_cast<FreeElement*>(ptr);
      f->next = freeElements;
      freeElements=f;
   }
   /// Swap two pools
   void swap(Pool& other) {
      std::swap(firstChunk,other.firstChunk);
      std::swap(lastChunk,other.lastChunk);
      std::swap(freeElements,other.freeElements);
   }

   void reset() {
      for (Chunk* c=firstChunk;c!=0;) {
         c->used=0;
         c=c->next;
      }
      assert(lastChunk->size>0);
      lastChunk = firstChunk;
      assert(lastChunk->size>0);
      freeElements = nullptr;
   }
};

/// Swap two pools
template<class T> void swap(Pool<T>& a,Pool<T>& b) {
   a.swap(b);
}
