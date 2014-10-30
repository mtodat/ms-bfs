//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#include "../include/bfs/batch256.hpp"
#include "../include/bfs/statistics.hpp"

#include <cstring>

using namespace std;

namespace Query4 {

#ifdef AVX2
   void BatchBFSRunner256::runBatch(vector<BatchBFSdata>& bfsData, const PersonSubgraph& subgraph) {
      const auto subgraphSize = subgraph.size();

      __m256i** toVisitLists = new __m256i*[2];
      auto ret=posix_memalign(reinterpret_cast<void**>(toVisitLists),64,sizeof(__m256i)*subgraphSize);
      if(unlikely(ret!=0)) {
         throw -1;
      }
      memset(toVisitLists[0],0,sizeof(__m256i)*subgraphSize);
      ret=posix_memalign(reinterpret_cast<void**>(toVisitLists+1),64,sizeof(__m256i)*subgraphSize);
      if(unlikely(ret!=0)) {
         throw -1;
      }
      memset(toVisitLists[1],0,sizeof(__m256i)*subgraphSize);

      const uint32_t numQueries = bfsData.size();
      assert(numQueries>0 && numQueries<=256);

      PersonId minPerson = numeric_limits<PersonId>::max();
      __m256i* seen;
      ret=posix_memalign(reinterpret_cast<void**>(&seen),64,sizeof(__m256i)*subgraphSize);
      if(unlikely(ret!=0)) {
         throw -1;
      }
      memset(seen, 0, sizeof(__m256i)*subgraphSize);
      for(size_t a=0; a<numQueries; a++) {
         // assert(seen[bfsData[a].person].i==0);
         seen[bfsData[a].person] = sseMasks256[a];
         (toVisitLists[0])[bfsData[a].person] = sseMasks256[a];
         minPerson = min(minPerson, bfsData[a].person);
      }

      runBatchRound(bfsData, subgraph, minPerson, toVisitLists, seen);

      free(seen);
      free(toVisitLists[0]);
      free(toVisitLists[1]);
      delete[] toVisitLists;
   }

   void BatchBFSRunner256::runBatchRound(vector<BatchBFSdata>& bfsData, const PersonSubgraph& subgraph, PersonId minPerson, __m256i** toVisitLists, __m256i* __restrict__ seen) {
      const auto subgraphSize = subgraph.size();
      const uint32_t numQueries = bfsData.size();

      const __m256i allZeros = _mm256_set_epi32(0,0,0,0,0,0,0,0);
      const __m256i allOnes = _mm256_set_epi32(0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF);
      const __m256i byteOnes = _mm256_set_epi8(1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1);

      __m256i processQuery = allOnes;
      uint32_t queriesToProcess=numQueries;

      __m256i numDistDiscovered[8];
      for(int a=0; a<8; a++) {
         numDistDiscovered[a] = allZeros;
      }
      uint32_t numDistDiscoveredIteration=0;

      uint8_t curToVisitQueue = 0;
      uint32_t nextDistance = 1;

      auto updateDiscoveredNodes = [&bfsData, &numDistDiscovered, &nextDistance, &allZeros]() {
         for(int a=0; a<8; a++) {
            const auto e0 = _mm256_extract_epi8(numDistDiscovered[a], 0); bfsData[a+8*0].totalReachable += e0; bfsData[a+8*0].totalDistances += e0*nextDistance;
            const auto e1 = _mm256_extract_epi8(numDistDiscovered[a], 1); bfsData[a+8*1].totalReachable += e1; bfsData[a+8*1].totalDistances += e1*nextDistance;
            const auto e2 = _mm256_extract_epi8(numDistDiscovered[a], 2); bfsData[a+8*2].totalReachable += e2; bfsData[a+8*2].totalDistances += e2*nextDistance;
            const auto e3 = _mm256_extract_epi8(numDistDiscovered[a], 3); bfsData[a+8*3].totalReachable += e3; bfsData[a+8*3].totalDistances += e3*nextDistance;
            const auto e4 = _mm256_extract_epi8(numDistDiscovered[a], 4); bfsData[a+8*4].totalReachable += e4; bfsData[a+8*4].totalDistances += e4*nextDistance;
            const auto e5 = _mm256_extract_epi8(numDistDiscovered[a], 5); bfsData[a+8*5].totalReachable += e5; bfsData[a+8*5].totalDistances += e5*nextDistance;
            const auto e6 = _mm256_extract_epi8(numDistDiscovered[a], 6); bfsData[a+8*6].totalReachable += e6; bfsData[a+8*6].totalDistances += e6*nextDistance;
            const auto e7 = _mm256_extract_epi8(numDistDiscovered[a], 7); bfsData[a+8*7].totalReachable += e7; bfsData[a+8*7].totalDistances += e7*nextDistance;
            const auto e8 = _mm256_extract_epi8(numDistDiscovered[a], 8); bfsData[a+8*8].totalReachable += e8; bfsData[a+8*8].totalDistances += e8*nextDistance;
            const auto e9 = _mm256_extract_epi8(numDistDiscovered[a], 9); bfsData[a+8*9].totalReachable += e9; bfsData[a+8*9].totalDistances += e9*nextDistance;
            const auto e10 = _mm256_extract_epi8(numDistDiscovered[a], 10); bfsData[a+8*10].totalReachable += e10; bfsData[a+8*10].totalDistances += e10*nextDistance;
            const auto e11 = _mm256_extract_epi8(numDistDiscovered[a], 11); bfsData[a+8*11].totalReachable += e11; bfsData[a+8*11].totalDistances += e11*nextDistance;
            const auto e12 = _mm256_extract_epi8(numDistDiscovered[a], 12); bfsData[a+8*12].totalReachable += e12; bfsData[a+8*12].totalDistances += e12*nextDistance;
            const auto e13 = _mm256_extract_epi8(numDistDiscovered[a], 13); bfsData[a+8*13].totalReachable += e13; bfsData[a+8*13].totalDistances += e13*nextDistance;
            const auto e14 = _mm256_extract_epi8(numDistDiscovered[a], 14); bfsData[a+8*14].totalReachable += e14; bfsData[a+8*14].totalDistances += e14*nextDistance;
            const auto e15 = _mm256_extract_epi8(numDistDiscovered[a], 15); bfsData[a+8*15].totalReachable += e15; bfsData[a+8*15].totalDistances += e15*nextDistance;
            const auto e16 = _mm256_extract_epi8(numDistDiscovered[a], 16); bfsData[a+8*16].totalReachable += e16; bfsData[a+8*16].totalDistances += e16*nextDistance;
            const auto e17 = _mm256_extract_epi8(numDistDiscovered[a], 17); bfsData[a+8*17].totalReachable += e17; bfsData[a+8*17].totalDistances += e17*nextDistance;
            const auto e18 = _mm256_extract_epi8(numDistDiscovered[a], 18); bfsData[a+8*18].totalReachable += e18; bfsData[a+8*18].totalDistances += e18*nextDistance;
            const auto e19 = _mm256_extract_epi8(numDistDiscovered[a], 19); bfsData[a+8*19].totalReachable += e19; bfsData[a+8*19].totalDistances += e19*nextDistance;
            const auto e20 = _mm256_extract_epi8(numDistDiscovered[a], 20); bfsData[a+8*20].totalReachable += e20; bfsData[a+8*20].totalDistances += e20*nextDistance;
            const auto e21 = _mm256_extract_epi8(numDistDiscovered[a], 21); bfsData[a+8*21].totalReachable += e21; bfsData[a+8*21].totalDistances += e21*nextDistance;
            const auto e22 = _mm256_extract_epi8(numDistDiscovered[a], 22); bfsData[a+8*22].totalReachable += e22; bfsData[a+8*22].totalDistances += e22*nextDistance;
            const auto e23 = _mm256_extract_epi8(numDistDiscovered[a], 23); bfsData[a+8*23].totalReachable += e23; bfsData[a+8*23].totalDistances += e23*nextDistance;
            const auto e24 = _mm256_extract_epi8(numDistDiscovered[a], 24); bfsData[a+8*24].totalReachable += e24; bfsData[a+8*24].totalDistances += e24*nextDistance;
            const auto e25 = _mm256_extract_epi8(numDistDiscovered[a], 25); bfsData[a+8*25].totalReachable += e25; bfsData[a+8*25].totalDistances += e25*nextDistance;
            const auto e26 = _mm256_extract_epi8(numDistDiscovered[a], 26); bfsData[a+8*26].totalReachable += e26; bfsData[a+8*26].totalDistances += e26*nextDistance;
            const auto e27 = _mm256_extract_epi8(numDistDiscovered[a], 27); bfsData[a+8*27].totalReachable += e27; bfsData[a+8*27].totalDistances += e27*nextDistance;
            const auto e28 = _mm256_extract_epi8(numDistDiscovered[a], 28); bfsData[a+8*28].totalReachable += e28; bfsData[a+8*28].totalDistances += e28*nextDistance;
            const auto e29 = _mm256_extract_epi8(numDistDiscovered[a], 29); bfsData[a+8*29].totalReachable += e29; bfsData[a+8*29].totalDistances += e29*nextDistance;
            const auto e30 = _mm256_extract_epi8(numDistDiscovered[a], 30); bfsData[a+8*30].totalReachable += e30; bfsData[a+8*30].totalDistances += e30*nextDistance;
            const auto e31 = _mm256_extract_epi8(numDistDiscovered[a], 31); bfsData[a+8*31].totalReachable += e31; bfsData[a+8*31].totalDistances += e31*nextDistance;
         }
         for(int a=0; a<8; a++) {
            numDistDiscovered[a] = allZeros;
         }
      };

      #ifdef STATISTICS
      BatchStatistics statistics;
      #endif

      PersonId curPerson=minPerson;
      bool nextToVisitEmpty = true;
      do {
         const __m256i* const toVisit = toVisitLists[curToVisitQueue];
         __m256i* const nextToVisit = toVisitLists[1-curToVisitQueue];

         for(; curPerson<subgraphSize && _mm256_testz_si256(toVisit[curPerson],allOnes)==1; curPerson++) { }
         if(curPerson<subgraphSize) {
            #ifdef STATISTICS
            const uint32_t numSetBits = __builtin_popcountl(_mm256_extract_epi64(toVisit[curPerson],0))
                                     && __builtin_popcountl(_mm256_extract_epi64(toVisit[curPerson],1))
                                     && __builtin_popcountl(_mm256_extract_epi64(toVisit[curPerson],2))
                                     && __builtin_popcountl(_mm256_extract_epi64(toVisit[curPerson],3));
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
               const auto toVisitAndProcess = _mm256_and_si256(toVisit[curPerson], processQuery);
               auto friendsBounds = curFriends.bounds();
               while(friendsBounds.first != friendsBounds.second) {

                  const __m256i newToVisit = _mm256_andnot_si256(seen[*friendsBounds.first], toVisitAndProcess);
                  if(_mm256_testc_si256(seen[*friendsBounds.first], toVisitAndProcess)==1) { //newToVisit==0
                     ++friendsBounds.first;
                     continue;
                  }
                  seen[*friendsBounds.first] = _mm256_or_si256(seen[*friendsBounds.first], toVisit[curPerson]);
                  nextToVisit[*friendsBounds.first] = _mm256_or_si256(nextToVisit[*friendsBounds.first], newToVisit);

                  //TODO: Profile-based approach, use shift-based counting once many bits are set
                  
                  nextToVisitEmpty = false;

                  numDistDiscovered[0] = _mm256_add_epi8(numDistDiscovered[0], _mm256_and_si256(newToVisit, byteOnes));
                  numDistDiscovered[1] = _mm256_add_epi8(numDistDiscovered[1], _mm256_and_si256(_mm256_srli_epi64(newToVisit, 1), byteOnes));
                  numDistDiscovered[2] = _mm256_add_epi8(numDistDiscovered[2], _mm256_and_si256(_mm256_srli_epi64(newToVisit, 2), byteOnes));
                  numDistDiscovered[3] = _mm256_add_epi8(numDistDiscovered[3], _mm256_and_si256(_mm256_srli_epi64(newToVisit, 3), byteOnes));
                  numDistDiscovered[4] = _mm256_add_epi8(numDistDiscovered[4], _mm256_and_si256(_mm256_srli_epi64(newToVisit, 4), byteOnes));
                  numDistDiscovered[5] = _mm256_add_epi8(numDistDiscovered[5], _mm256_and_si256(_mm256_srli_epi64(newToVisit, 5), byteOnes));
                  numDistDiscovered[6] = _mm256_add_epi8(numDistDiscovered[6], _mm256_and_si256(_mm256_srli_epi64(newToVisit, 6), byteOnes));
                  numDistDiscovered[7] = _mm256_add_epi8(numDistDiscovered[7], _mm256_and_si256(_mm256_srli_epi64(newToVisit, 7), byteOnes));

                  if(numDistDiscoveredIteration==254) {
                     updateDiscoveredNodes();
                     numDistDiscoveredIteration=0;
                  } else {
                     numDistDiscoveredIteration++;
                  }

                  
                  ++friendsBounds.first;
               }
            // }

            //Go to next person
            curPerson++;
         } else {
            updateDiscoveredNodes();
            numDistDiscoveredIteration = 0;

            //Swap queues
            for(uint32_t a=0; a<numQueries; a++) {
               if(_mm256_testz_si256(processQuery, sseMasks256[a])==0) {
                  // bfsData[a].totalReachable += numDistDiscovered[a];
                  // bfsData[a].totalDistances += numDistDiscovered[a]*nextDistance;

                  if((bfsData[a].componentSize-1)==bfsData[a].totalReachable) {
                     if(queriesToProcess==1) {
                        return;
                     }
                     processQuery = _mm256_andnot_si256(sseMasks256[a], processQuery);
                     queriesToProcess--;
                     continue;
                  }
               }
            }
            if(nextToVisitEmpty) {
               return;
            }

            memset(toVisitLists[curToVisitQueue],0,sizeof(__m256i)*subgraphSize);
            // memset(numDistDiscovered,0,sizeof(uint32_t)*numQueries);
            nextToVisitEmpty = true;

            curPerson = 0;
            nextDistance++;
            curToVisitQueue = 1-curToVisitQueue;
         }
      } while(true);
   }
#endif

}