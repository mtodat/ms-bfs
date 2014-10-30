//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#pragma once

#include "../queue.hpp"
#include "../graph.hpp"
#include <cstdint>

namespace Query4 {
   typedef uint64_t Distances; // Type for the sum of distances
   typedef uint8_t Distance; // Type for a distance between two points
   typedef uint32_t PersonId; // Type for person ids
   typedef PersonId Persons; // Type for counting persons

   typedef Graph<PersonId> PersonSubgraph;

   inline awfy::FixedSizeQueue<PersonId>& getThreadLocalPersonVisitQueue(size_t queueSize) {
      static __thread awfy::FixedSizeQueue<PersonId>* toVisitPtr=nullptr;
      if(toVisitPtr != nullptr) {
         awfy::FixedSizeQueue<PersonId>& q = *toVisitPtr;
         q.reset(queueSize);
         return q;
      } else {
         toVisitPtr = new awfy::FixedSizeQueue<PersonId>(queueSize);
         return *toVisitPtr;
    
      }
   }

   struct BatchBFSdata {
      const PersonId person;
      const Persons componentSize;

      Distances totalDistances;
      Persons totalReachable;

      BatchBFSdata(PersonId person, Persons componentSize)
         : person(person), componentSize(componentSize),
           totalDistances(0),totalReachable(0)
      { }

      BatchBFSdata(const BatchBFSdata&) = delete;
      BatchBFSdata& operator=(const BatchBFSdata&) = delete;

      BatchBFSdata(BatchBFSdata&& other)
         : person(other.person), componentSize(other.componentSize),
           totalDistances(other.totalDistances), totalReachable(other.totalReachable)
      { }
   };
}