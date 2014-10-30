//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#include "../include/bfs/batch64.hpp"
#include "../include/bfs/batchdistance.hpp"

#include <cstring>

using namespace std;

namespace Query4 {

   void BatchBFSRunner::runBatch(vector<BatchBFSdata>& bfsData, const PersonSubgraph& subgraph
      #ifdef STATISTICS
      , BatchStatistics&/* statistics */
      #endif
      ) {
      const auto subgraphSize = subgraph.size();

      array<uint64_t*,2> toVisitLists;
      toVisitLists[0] = new uint64_t[subgraphSize];
      memset(toVisitLists[0],0,sizeof(uint64_t)*subgraphSize);
      toVisitLists[1] = new uint64_t[subgraphSize];
      memset(toVisitLists[1],0,sizeof(uint64_t)*subgraphSize);

      const uint32_t numQueries = bfsData.size();
      assert(numQueries>0 && numQueries<=64);

      PersonId minPerson = numeric_limits<PersonId>::max();
      uint64_t* seen = new uint64_t[subgraphSize];
      memset(seen, 0, sizeof(uint64_t)*subgraphSize);
      for(size_t a=0; a<numQueries; a++) {
         const uint64_t personMask = 1UL<<a;
         assert(seen[bfsData[a].person]==0);
         seen[bfsData[a].person] = personMask;
         (toVisitLists[0])[bfsData[a].person] = personMask;
         minPerson = min(minPerson, bfsData[a].person);
      }

      runBatchRound(bfsData, subgraph, minPerson, toVisitLists, seen);

      delete[] seen;
      delete[] toVisitLists[0];
      delete[] toVisitLists[1];
   }

   void BatchBFSRunner::runBatchRound(vector<BatchBFSdata>& bfsData, const PersonSubgraph& subgraph, PersonId minPerson, array<uint64_t*,2>& toVisitLists, uint64_t* __restrict__ seen) {
      const auto subgraphSize = subgraph.size();
      const uint32_t numQueries = bfsData.size();

      uint64_t processQuery = (~0UL);
      uint32_t queriesToProcess=numQueries;

      uint32_t numDistDiscovered[64] __attribute__((aligned(16)));  
      memset(numDistDiscovered,0,sizeof(uint32_t)*numQueries);

      uint8_t curToVisitQueue = 0;
      uint32_t nextDistance = 1;

      PersonId curPerson=minPerson;
      bool nextToVisitEmpty = true;
      BatchDistance<uint64_t, 1> batchDist(numDistDiscovered);
      do {
         const uint64_t* const toVisit = toVisitLists[curToVisitQueue];
         uint64_t* const nextToVisit = toVisitLists[1-curToVisitQueue];

         for(; curPerson<subgraphSize && toVisit[curPerson]==0; curPerson++) { }
         if(curPerson<subgraphSize) {
            const uint64_t toVisitEntry = toVisit[curPerson];

            const auto& curFriends=*subgraph.retrieve(curPerson);

            const auto toVisitProcess = toVisitEntry & processQuery;
            //const auto firstQueryId = __builtin_ctzl(toVisitEntry);
            // if((toVisitEntry>>(firstQueryId+1)) == 0) {
            //    //Only single person in this entry

            //    auto friendsBounds = curFriends.bounds();
            //    while(friendsBounds.first != friendsBounds.second) {
            //       if(toVisitProcess & (~seen[*friendsBounds.first])) {
            //          seen[*friendsBounds.first] |= toVisitEntry;
            //          nextToVisit[*friendsBounds.first] |= toVisitEntry;
            //          nextToVisitEmpty = false;
            //          numDistDiscovered[firstQueryId]++;
            //       }
            //       ++friendsBounds.first;
            //    }
            // } else {
               //More than one person
               auto friendsBounds = curFriends.bounds();
               while(friendsBounds.first != friendsBounds.second) {

                  //XXX: TODO processQuery needed here?
                  uint64_t newToVisit = toVisitProcess & (~seen[*friendsBounds.first]); //!seen & toVisit
                  if(newToVisit == 0) {
                     ++friendsBounds.first;
                     continue;
                  }

                  seen[*friendsBounds.first] |= toVisitEntry;
                  nextToVisit[*friendsBounds.first] |= newToVisit;

                  //TODO: Profile-based approach, use shift-based counting once many bits are set
                  
                  batchDist.updateDiscovered(newToVisit, 0);
                  ++friendsBounds.first;
               }
            // }

            //Go to next person
            curPerson++;
         } else {
            batchDist.finalize();
            //Swap queues
            for(uint32_t a=0; a<numQueries; a++) {
               if(processQuery & (1UL<<a)) {
                  bfsData[a].totalReachable += numDistDiscovered[a];
                  bfsData[a].totalDistances += numDistDiscovered[a]*nextDistance;

                  if((bfsData[a].componentSize-1)==bfsData[a].totalReachable) {
                     if(queriesToProcess==1) {
                        return;
                     }
                     processQuery &= ~(1UL<<a);
                     queriesToProcess--;
                     continue;
                  }
               }
            }
            if(nextToVisitEmpty) {
               return;
            }

            memset(toVisitLists[curToVisitQueue],0,sizeof(uint64_t)*subgraphSize);
            memset(numDistDiscovered,0,sizeof(uint32_t)*numQueries);
            nextToVisitEmpty = true;

            curPerson = 0;
            nextDistance++;
            curToVisitQueue = 1-curToVisitQueue;
         }
      } while(true);
   }
}