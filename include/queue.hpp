//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#pragma once

#include <cstddef>
#include <cassert>
#include <utility>
#include "macros.hpp"

namespace awfy {

   template<class T>
   class FixedSizeQueue {
   private:
      T* elems;
      T* startPtr;
      T* endPtr;
      size_t size_;

   public:
      FixedSizeQueue(size_t size) : elems(new T[size]),startPtr(elems),endPtr(elems),size_(size)
      {  }

      ~FixedSizeQueue() {
         if(elems!=nullptr) {
            delete[] elems;
         }
      }

      inline bool empty() const {
         return startPtr==endPtr;
      }

      inline size_t size() const {
         return endPtr-startPtr;
      }

      inline const T& front() const {
         assert(!empty());
         return *startPtr;
      }

      inline void pop_front() {
         assert(!empty());
         startPtr++;
      }

      inline T& push_back_pos() {
         assert(endPtr<elems+size_);
         return *endPtr++;
      }

      void reset(size_t newSize) {
         if(newSize>size_) {
            delete[] elems;
            elems = new T[newSize];
            size_ = newSize;
         }
         startPtr=elems;
         endPtr=elems;
      }

      inline std::pair<T*,T*> bounds() {
         return make_pair(startPtr,endPtr);
      }
   };

}
