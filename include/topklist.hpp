//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#pragma once

#include <cstddef>
#include <vector>
#include <utility>
#include <cassert>

namespace awfy {

   template<class T>
   class TopKComparer;

   template<class T>
   bool compare(const T& a, const T& b) { return TopKComparer<T>::compare(a,b); }

   template <class T>
   class TopKComparer {
      // Returns true if first param is larger or equal
      static bool compare(const T& a, const T& b);
   };


   /// Class managing a list of top k values
   template<class Key, class Value>
   class TopKList {
   public:
      typedef std::pair<Key, Value> EntryPair;
      typedef typename std::vector<EntryPair>::iterator EntryIter;

   private:
      const EntryPair initialBound;
      std::vector<EntryPair> topMatches;
      size_t k;

      // Find insert position of element
      EntryIter getInsertPos(const EntryPair& pair) {
         //TODO: Search from end
         EntryIter iter=topMatches.begin();
         while(iter!=topMatches.end() && compare(*iter, pair)) {
            iter++;
         }
         return iter;
      }

   public:
      TopKList(EntryPair initialBound) : initialBound(initialBound) {
      }

      TopKList(const TopKList&) = delete;
      TopKList& operator=(const TopKList&) = delete;
      TopKList(TopKList&&) = delete;
      TopKList& operator=(TopKList&&) = delete;

      /// Resets the top k list and initializes it with the k value
      void init(const size_t k) {
         this->k=k;
         topMatches.clear();
         topMatches.reserve(k);
         topMatches.push_back(initialBound);
      }

      /// Try to insert a value into top k list, will not update if value is too small
      /// Returns the new bound
      void insert(const Key& key, const Value& value) {
         EntryPair pair = std::make_pair(key, value);
         assert(compare(pair, initialBound)); //Everything we insert must be smaller than the initial bound
         // Insert entries in order until list is full
         if (topMatches.size() < k){
            auto insertPos=getInsertPos(pair);
            topMatches.insert(insertPos, pair);
         }
         // Check if new entry is larger than existing entry
         else if(!compare(topMatches.back(), pair)) {
            topMatches.pop_back(); // Remove last entry
            auto insertPos=getInsertPos(pair); // Insert new entry
            topMatches.insert(insertPos, pair); 
         }
      }

      inline const EntryPair& getBound() {
         return topMatches.back();
      }

      const std::vector<EntryPair>& getEntries() {
         //Remove dummy element if necesary
         if(topMatches.size()>0 && topMatches.back() == initialBound) {
            topMatches.pop_back();
         }
         //Return matches
         return topMatches;
      }
   };
}