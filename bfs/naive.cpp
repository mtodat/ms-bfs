//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#include "../include/bfs/naive.hpp"
#include "../include/TraceStats.hpp"

namespace Query4 {

void __attribute__ ((noinline)) BFSRunner::run(const PersonId start, const PersonSubgraph& subgraph, BatchBFSdata& bfsData
   #ifdef STATISTICS
   , BatchStatistics&/* statistics */
   #endif
   ) {
   BFSQueue& toVisit = getThreadLocalPersonVisitQueue(subgraph.size());
   assert(toVisit.empty()); //Data structures are in a sane state

   // Initialize BFS
   Distance* seen = new Distance[subgraph.size()]();
   seen[start] = 1; // Level = distance + 1, Level 0 = not seen
   toVisit.push_back_pos() = start;

   // Run rounds until we can either early exit or have reached all nodes
   Distance distance=0;
   do {
      size_t startTime = tschrono::now();

      const Persons personsRemaining=(bfsData.componentSize-1)-bfsData.totalReachable;
      Persons numDiscovered = runRound(subgraph, seen, toVisit, toVisit.size(), personsRemaining);
      assert(distance<std::numeric_limits<Distance>::max());
      distance++;

      // Adjust result
      bfsData.totalReachable+=numDiscovered;
      bfsData.totalDistances+=numDiscovered*distance;

      TraceStats<TYPE_BITS*WIDTH>& stats = TraceStats<TYPE_BITS*WIDTH>::getStats();
      stats.traceRound(distance);
      stats.addRoundDuration(distance, (tschrono::now()-startTime));

      // Exit criteria for full BFS
      if((bfsData.componentSize-1)==bfsData.totalReachable) {
         break;
      }
   } while(true);

   delete[] seen;
}

// XXX: __attribute__((optimize("align-loops")))  this crashed the compiler
Persons __attribute__((hot)) BFSRunner::runRound(const PersonSubgraph& subgraph, Distance* __restrict__ seen, BFSQueue& toVisit, const Persons numToVisit, const Persons numUnseen) {
   Persons numRemainingToVisit=numToVisit;
   Persons numRemainingUnseen=numUnseen;

   do {
      assert(!toVisit.empty());

      const PersonId person = toVisit.front();
      toVisit.pop_front();

      // Iterate over friends
      auto friendsBounds = subgraph.retrieve(person)->bounds();
      while(friendsBounds.first != friendsBounds.second) {
         if (seen[*friendsBounds.first]) {
            ++friendsBounds.first;
            continue;
         }
         toVisit.push_back_pos() = *friendsBounds.first;

         seen[*friendsBounds.first] = true;
         ++friendsBounds.first;
         numRemainingUnseen--;
      }

      numRemainingToVisit--;
   } while(numRemainingToVisit>0 && numRemainingUnseen>0);

   return numUnseen-numRemainingUnseen;
}

}