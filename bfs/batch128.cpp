//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#include "../include/bfs/batch128.hpp"
#include "../include/bfs/batchdistance.hpp"

#include <bitset>
#include <cstring>
#include <map>

using namespace std;

namespace Query4 {

   void BatchBFSRunner128::runBatch(vector<BatchBFSdata>& bfsData, const PersonSubgraph& subgraph
      #ifdef STATISTICS
      , BatchStatistics& statistics
      #endif
      ) {
      const auto subgraphSize = subgraph.size();

      array<__m128i*,2> toVisitLists;
      toVisitLists[0] = new __m128i[subgraphSize];
      memset(toVisitLists[0],0,sizeof(__m128i)*subgraphSize);
      toVisitLists[1] = new __m128i[subgraphSize];
      memset(toVisitLists[1],0,sizeof(__m128i)*subgraphSize);

      // const uint32_t numQueries = bfsData.size();
      // assert(numQueries>0 && numQueries<=128);

      __m128i* seen = new __m128i[subgraphSize];
      memset(seen, 0, sizeof(__m128i)*subgraphSize);

      runBatchRound(bfsData, subgraph, 9999999, toVisitLists, seen
         #ifdef STATISTICS
         , statistics
         #endif
      );

      delete[] seen;
      delete[] toVisitLists[0];
      delete[] toVisitLists[1];

      #ifdef STATISTICS
      statistics.finishBatch();
      #endif
   }

   void BatchBFSRunner128::runBatchRound(vector<BatchBFSdata>& bfsData, const PersonSubgraph& subgraph, PersonId minPerson, array<__m128i*,2>& toVisitLists, __m128i* __restrict__ seen
      #ifdef STATISTICS
      , BatchStatistics& statistics
      #endif
      ) {
      const auto subgraphSize = subgraph.size();
      const uint32_t numQueries = bfsData.size();

      const __m128i allOnes = _mm_set_epi32(0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF);

      __m128i processQuery = allOnes;
      uint32_t queriesToProcess=numQueries;

      uint32_t numDistDiscovered[128] __attribute__((aligned(16)));
      memset(numDistDiscovered,0,sizeof(uint32_t)*numQueries);

      uint8_t curToVisitQueue = 0;

      bool nextToVisitEmpty = true;
      BatchDistance<__m128i, 1> batchDist(numDistDiscovered);


      map<PersonId, __m128i> firstToVisit;
      for(size_t a=0; a<numQueries; a++) {
         const auto p = bfsData[a].person;
         seen[p] = sseMasks[a];
         minPerson = min(minPerson, p);

         const auto& curFriends=*subgraph.retrieve(p);
         auto friendsBounds = curFriends.bounds();
         while(friendsBounds.first != friendsBounds.second) {
            auto vITer = firstToVisit.find(*friendsBounds.first);
            if(vITer == firstToVisit.end()) {
               firstToVisit.insert(make_pair(*friendsBounds.first, sseMasks[a]));
            } else {
               vITer->second = _mm_or_si128(vITer->second, sseMasks[a]);
            }

            minPerson = min(minPerson, *friendsBounds.first);
            ++friendsBounds.first;
         }
      }
      for(const auto& t : firstToVisit) {
         (toVisitLists[0])[t.first] = t.second;
         seen[t.first] = _mm_or_si128(seen[t.first], t.second);
         batchDist.updateDiscovered(t.second, 0);
      }
      for(uint32_t a=0; a<numQueries; a++) {
         bfsData[a].totalReachable += numDistDiscovered[a];
         bfsData[a].totalDistances += numDistDiscovered[a];
      }
      PersonId curPerson=minPerson;

      uint32_t nextDistance = 2;

      // uint32_t set=0;
      // uint32_t run=0;
      do {
         const __m128i*__restrict const toVisit = toVisitLists[curToVisitQueue];
         __m128i*__restrict const nextToVisit = toVisitLists[1-curToVisitQueue];
         

         for(; curPerson<subgraphSize && _mm_testz_si128(toVisit[curPerson],allOnes)==1; curPerson++) { }
            if(curPerson<subgraphSize) {
               // set++;
            #ifdef STATISTICS
            const uint32_t numSetBits = __builtin_popcountl(_mm_extract_epi64(toVisit[curPerson],0))
                                      + __builtin_popcountl(_mm_extract_epi64(toVisit[curPerson],1));
            statistics.log(curPerson, nextDistance, numSetBits);
            #endif

               const auto& curFriends=*subgraph.retrieve(curPerson);

            // const uint64_t toVisit0 = _mm_extract_epi64(toVisit[curPerson], 0);
            // const uint64_t toVisit1 = _mm_extract_epi64(toVisit[curPerson], 1);
            // if((toVisit1==0 && __builtin_popcountl(toVisit0)==1) || (toVisit0==0 && __builtin_popcountl(toVisit1)==1)) {
            //    //Only one query
            //    const auto queryId = toVisit0 == 0 ? __builtin_ctzl(toVisit1)+64 : __builtin_ctzl(toVisit0);
            //    auto friendsBounds = curFriends.bounds();
            //    while(friendsBounds.first != friendsBounds.second) {
            //       if(_mm_testc_si128(seen[*friendsBounds.first], toVisit[curPerson])==0) {
            //          seen[*friendsBounds.first] = _mm_or_si128(seen[*friendsBounds.first], toVisit[curPerson]);
            //          nextToVisit[*friendsBounds.first] = _mm_or_si128(nextToVisit[*friendsBounds.first], toVisit[curPerson]);
            //          nextToVisitEmpty = false;
            //          numDistDiscovered[queryId]++;
            //       }
            //       ++friendsBounds.first;
            //    }
            // } else {
               //More than one query
               const auto toVisitAndProcess = _mm_and_si128(toVisit[curPerson], processQuery);
               auto friendsBounds = curFriends.bounds();
               while(friendsBounds.first != friendsBounds.second) {
                  if(friendsBounds.first+1 != friendsBounds.second) {
                     __builtin_prefetch(seen + *(friendsBounds.first+1),0);
                  }

                  const __m128i newToVisit = _mm_andnot_si128(seen[*friendsBounds.first], toVisitAndProcess);
                  if(_mm_testc_si128(seen[*friendsBounds.first], toVisitAndProcess)==1) { //newToVisit==0
                     ++friendsBounds.first;
                     continue;
                  }
                  __builtin_prefetch(nextToVisit + *friendsBounds.first,1);
                  seen[*friendsBounds.first] = _mm_or_si128(seen[*friendsBounds.first], toVisit[curPerson]);

                  //TODO: Profile-based approach, use shift-based counting once many bits are set
                  
                  nextToVisitEmpty = false;
                  batchDist.updateDiscovered(newToVisit, 0);

                  nextToVisit[*friendsBounds.first] = _mm_or_si128(nextToVisit[*friendsBounds.first], newToVisit);

                  ++friendsBounds.first;
               }
            // }

            //Go to next person
               curPerson++;
            } else {
               // cout<<run<<" "<<set/(double)subgraphSize<<"\n";
               // set=0;
               // run++;

               batchDist.finalize();

            //Swap queues
               for(uint32_t a=0; a<numQueries; a++) {
                  if(_mm_testz_si128(processQuery, sseMasks[a])==0) {
                     bfsData[a].totalReachable += numDistDiscovered[a];
                     bfsData[a].totalDistances += numDistDiscovered[a]*nextDistance;

                     if((bfsData[a].componentSize-1)==bfsData[a].totalReachable) {
                        if(queriesToProcess==1) {
                           return;
                        }
                        processQuery = _mm_andnot_si128(sseMasks[a], processQuery);
                        queriesToProcess--;
                        continue;
                     }
                  }
               }
               if(nextToVisitEmpty) {
                  return;
               }

               memset(toVisitLists[curToVisitQueue],0,sizeof(__m128i)*subgraphSize);
               memset(numDistDiscovered,0,sizeof(uint32_t)*numQueries);
               nextToVisitEmpty = true;

               curPerson = 0;
               nextDistance++;
               curToVisitQueue = 1-curToVisitQueue;
            }
         } while(true);
      }
   }