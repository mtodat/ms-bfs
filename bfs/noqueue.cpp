//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#include "../include/bfs/noqueue.hpp"
#include "../include/TraceStats.hpp"

#include <cstring>

namespace Query4 {

void __attribute__ ((noinline)) NoQueueBFSRunner::run(const PersonId start, const PersonSubgraph& subgraph, BatchBFSdata& bfsData) {
   // Initialize BFS
   Distance* seen = new Distance[subgraph.size()]();
   seen[start] = true; // Level = distance + 1, Level 0 = not seen

   Distance* currentVisit = new Distance[subgraph.size()]();
   Distance* nextVisit = new Distance[subgraph.size()]();
   currentVisit[start] = true;

   // Run rounds until we can either early exit or have reached all nodes
   Distance distance=0;
   do {
      size_t startTime = tschrono::now();

      // const Persons personsRemaining=(bfsData.componentSize-1)-bfsData.totalReachable;
      Persons numDiscovered = runRound(subgraph, seen, currentVisit, nextVisit);
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

      // Swap queues
      Distance* tmpVisit = currentVisit;
      currentVisit = nextVisit;
      nextVisit = tmpVisit;
      memset(nextVisit, 0, sizeof(Distance)*subgraph.size());
   } while(true);

   delete[] seen;
   delete[] currentVisit;
   delete[] nextVisit;
}

Persons __attribute__((hot)) NoQueueBFSRunner::runRound(const PersonSubgraph& subgraph, Distance* __restrict__ seen, const Distance* __restrict__ currentVisit, Distance* __restrict__ nextVisit) {
   // XXX: Search set bytes with memchr?
   // XXX: Prefetch
   // XXX: Track remaining unseen
   const auto numPersons = subgraph.size();
   Query4::Persons numDiscovered = 0;
   for(Persons person=0; person<numPersons; person++) {
      // Skip persons we don't want to visit
      if(currentVisit[person]==0) { continue; }

      // Iterate over friends
      auto friendsBounds = subgraph.retrieve(person)->bounds();
      while(friendsBounds.first != friendsBounds.second) {
         if (seen[*friendsBounds.first]) {
            ++friendsBounds.first;
            continue;
         }

         seen[*friendsBounds.first] = true;
         nextVisit[*friendsBounds.first] = true;
         ++friendsBounds.first;
         numDiscovered++;
      }      
   }

   return numDiscovered;
}

}