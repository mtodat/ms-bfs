//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#pragma once

#include "../TraceStats.hpp"
#include "statistics.hpp"
#include "base.hpp"
#include "batchdistance.hpp"
#include "bitops.hpp"
#include <array>
#include <cstring>

#define SORTED_NEIGHBOR_PROCESSING
#define DO_PREFETCH
#define BI_DIRECTIONAl

namespace Query4 {

// Batch part
template<typename bit_t, uint64_t width>
struct BatchBits {
   static const size_t TYPE_BITS_COUNT = sizeof(bit_t)*8;

   bit_t data[width];

   BatchBits() : data() {
   }

   #ifdef DEBUG
   bool isAllZero() const {
      for (unsigned i = 0; i < width; ++i) {
         if(BitBaseOp<bit_t>::notZero(data[i])) {
            return false;
         }
      }

      return true;
   }
   #endif

   #ifdef STATISTICS
   size_t count() const {
      size_t sum=0;
      for (unsigned i = 0; i < width; ++i) {
         sum+=BitBaseOp<bit_t>::popCount(data[i]);
      }
      return sum;
   }
   #endif

   void negate() {
      for (unsigned i = 0; i < width; ++i) {
         data[i] = ~data[i];
      }
   }

   void setBit(const size_t ix) {
      auto field = ix/TYPE_BITS_COUNT;
      auto field_bit = ix-(field*TYPE_BITS_COUNT);
      data[field] |= BitBaseOp<bit_t>::getSetMask(field_bit);
   }
};
static const unsigned int PREFETCH=38;
// General HugeBatchBFS loop
template<typename bit_t=uint64_t, uint64_t width=1, bool detectSingle=false, uint32_t alpha=14, uint32_t beta=24>
struct HugeBatchBfs {
   static const size_t TYPE=3;
   static const size_t WIDTH=width;
   static const size_t TYPE_BITS=sizeof(bit_t)*8;
   #ifdef DO_PREFETCH
   #endif
   static const size_t BATCH_BITS_COUNT = sizeof(bit_t)*width*8;
   typedef BatchBits<bit_t, width> Bitset;

   static constexpr uint64_t batchSize() {
      return BATCH_BITS_COUNT;
   }

   static void runBatch(std::vector<BatchBFSdata>& bfsData, const PersonSubgraph& subgraph
      #ifdef STATISTICS
      , BatchStatistics& statistics
      #endif
      ) {

      const auto subgraphSize = subgraph.size();

      // Initialize visit lists
      std::array<Bitset*,2> visitLists;
      for(int a=0; a<2; a++) {
         const auto ret=posix_memalign(reinterpret_cast<void**>(&(visitLists[a])),64,sizeof(Bitset)*subgraphSize);
         if(unlikely(ret!=0)) {
            throw -1;
         }
         new(visitLists[a]) Bitset[subgraphSize]();
      }


      const uint32_t numQueries = bfsData.size();
      assert(numQueries>0 && numQueries<=BATCH_BITS_COUNT);

      // Initialize seen vector
      PersonId minPerson = std::numeric_limits<PersonId>::max();
      Bitset* seen;
      const auto ret=posix_memalign(reinterpret_cast<void**>(&seen),64,sizeof(Bitset)*subgraphSize);
      if(unlikely(ret!=0)) {
         throw -1;
      }
      new(seen) Bitset[subgraphSize]();

      #ifdef BI_DIRECTIONAl
      bool topDown = true;
      uint64_t unexploredEdges = subgraph.numEdges;
      uint64_t visitNeighbors = 0;
      uint32_t frontierSize = numQueries;
      #endif

      // Initialize active queries
      for(size_t pos=0; pos<numQueries; pos++) {
         assert(seen[bfsData[pos].person].isAllZero());
         seen[bfsData[pos].person].setBit(pos);
         assert(!seen[bfsData[pos].person].isAllZero());
         (visitLists[0])[bfsData[pos].person].setBit(pos);
         minPerson = std::min(minPerson, bfsData[pos].person);

         #ifdef BI_DIRECTIONAl
         visitNeighbors += subgraph.retrieve(bfsData[pos].person)->size();
         #endif
      }

      // Initialize iteration workstate
      Bitset processQuery;
      processQuery.negate();

      uint32_t queriesToProcess=numQueries;
      alignas(64) uint32_t numDistDiscovered[BATCH_BITS_COUNT];
      memset(numDistDiscovered,0,BATCH_BITS_COUNT*sizeof(uint32_t));

      BatchDistance<bit_t, width> batchDist(numDistDiscovered);

      size_t curToVisitQueue = 0;
      uint32_t nextDistance = 1;

      PersonId startPerson=minPerson;

      // Run iterations
      do {
         size_t startTime = tschrono::now();
         Bitset* const toVisit = visitLists[curToVisitQueue];
         Bitset* const nextToVisit = visitLists[1-curToVisitQueue];

         assert(toVisit!=nullptr);
         assert(nextToVisit!=nullptr);

         #ifdef BI_DIRECTIONAl
         unexploredEdges -= visitNeighbors;
         std::pair<uint32_t, uint64_t> frontierInfo;
         if(topDown) {
            if(visitNeighbors <= unexploredEdges / alpha) {
               frontierInfo = runBatchRound(subgraph, startPerson, subgraphSize, toVisit, nextToVisit, seen, batchDist, processQuery
                  #if defined(STATISTICS)
                  , statistics, nextDistance
                  #elif defined(TRACE)
                  , nextDistance
                  #endif
                  );
               topDown = true;
            } else {
               frontierInfo = runBatchRoundRev(subgraph, startPerson, subgraphSize, toVisit, nextToVisit, seen, batchDist, processQuery
                  #if defined(STATISTICS)
                  , statistics, nextDistance
                  #elif defined(TRACE)
                  , nextDistance
                  #endif
                  );
               topDown = false;
            }
         } else {
            if(frontierSize >= subgraphSize / beta) {
               frontierInfo = runBatchRoundRev(subgraph, startPerson, subgraphSize, toVisit, nextToVisit, seen, batchDist, processQuery
                  #if defined(STATISTICS)
                  , statistics, nextDistance
                  #elif defined(TRACE)
                  , nextDistance
                  #endif
                  );
               topDown = false;
            } else {
               frontierInfo = runBatchRound(subgraph, startPerson, subgraphSize, toVisit, nextToVisit, seen, batchDist, processQuery
                  #if defined(STATISTICS)
                  , statistics, nextDistance
                  #elif defined(TRACE)
                  , nextDistance
                  #endif
                  );
               topDown = true;
            } 
         }
         frontierSize = frontierInfo.first;
         visitNeighbors = frontierInfo.second;
         #else
         runBatchRound(subgraph, startPerson, subgraphSize, toVisit, nextToVisit, seen, batchDist, processQuery
                  #if defined(STATISTICS)
                  , statistics, nextDistance
                  #elif defined(TRACE)
                  , nextDistance
                  #endif
                  );
         #endif

         // Update stats for all processed queries and check if the query is finished
         #ifdef DEBUG
         uint64_t newReached = 0;
         uint64_t numNotZero = 0;
         #endif
         for(uint32_t pos=0; pos<numQueries; pos++) {
            updateProcessQuery(processQuery, pos, numDistDiscovered[pos], bfsData[pos], nextDistance, queriesToProcess);
            #ifdef DEBUG
            if(numDistDiscovered[pos]>0) {
               newReached += numDistDiscovered[pos];
               numNotZero++;
            }
            #endif
         }

         TraceStats<BATCH_BITS_COUNT>& stats = TraceStats<BATCH_BITS_COUNT>::getStats();
         stats.traceRound(nextDistance);
         stats.addRoundDuration(nextDistance, (tschrono::now()-startTime));

         if(queriesToProcess==0) {
            break;
         }
         nextDistance++;

         // As long as not all queries are finished we have to find s.th. every round
         assert(newReached!=0);

         // Reset iteration state
         //memset(visitLists[curToVisitQueue],0,sizeof(Bitset)*subgraphSize);
         memset(numDistDiscovered,0,BATCH_BITS_COUNT*sizeof(uint32_t));

         // Swap queues
         startPerson = 0;
         curToVisitQueue = 1-curToVisitQueue;
      } while(true);

      free(seen);
      for(int a=0; a<2; a++) {
         free(visitLists[a]);
      }

      #ifdef STATISTICS
      statistics.finishBatch();
      #endif
   }


   #ifdef SORTED_NEIGHBOR_PROCESSING

   static std::pair<uint32_t,uint64_t> __attribute__((hot)) runBatchRound(const PersonSubgraph& subgraph, const PersonId startPerson, const PersonId limit, Bitset* visitList, Bitset* nextVisitList, Bitset* __restrict__ seen, BatchDistance<bit_t, width>& batchDist, const Bitset processQuery
      #if defined(STATISTICS)
      , BatchStatistics& statistics, uint32_t nextDistance
      #elif defined(TRACE)
      , uint32_t nextDistance
      #endif
   ) {
         // volatile bit_t pref;
      #ifdef DO_PREFETCH
      const int p2=min(PREFETCH, (unsigned int)(limit-startPerson));
      for(int a=1; a<p2; a++) {
         __builtin_prefetch(visitList + a,0);
         // pref=(visitList + a)->data[0];
      }
      #endif

      #ifdef TRACE
      size_t numBitsSet[BATCH_BITS_COUNT];
      size_t numFriendsAnalyzed = 0;
      size_t numNewSeen[BATCH_BITS_COUNT];
      memset(numBitsSet, 0, sizeof(size_t)*BATCH_BITS_COUNT);
      memset(numNewSeen, 0, sizeof(size_t)*BATCH_BITS_COUNT);
      std::vector<uint64_t> xx(513);
      #endif


      for (PersonId curPerson = startPerson; curPerson<limit; ++curPerson) {
         auto curVisit = visitList[curPerson];

         #ifdef DO_PREFETCH
         if(curPerson+PREFETCH < limit) {
            __builtin_prefetch(visitList + curPerson + PREFETCH,0);
            // pref=(visitList + curPerson + PREFETCH)->data[0];
         }
         #endif

         #ifdef TRACE
         {
            size_t bitsSet = 0;
            for(int i=0; i<width; i++) {
               bitsSet += BitBaseOp<bit_t>::popCount(curVisit.data[i]);
            }
            numBitsSet[bitsSet]++;
         }
         #endif

         bool zero=true;
         for(int i=0; i<width; i++) {
            if(BitBaseOp<bit_t>::notZero(curVisit.data[i])) {
               zero=false;
               break;
            }
         }
         if(zero) {
            continue;
         }

         const auto& curFriends=*subgraph.retrieve(curPerson);
         auto friendsBounds = curFriends.bounds();
         #ifdef DO_PREFETCH
         const int p=min(PREFETCH, (unsigned int)(friendsBounds.second-friendsBounds.first));
         for(int a=1; a<p; a++) {
            __builtin_prefetch(nextVisitList + *(friendsBounds.first+a),1);
            // pref=(nextVisitList + *(friendsBounds.first+a))->data[0];
         }
         #endif
         for(int i=0; i<width; i++) {
            curVisit.data[i] &= processQuery.data[i];
         }
         while(friendsBounds.first != friendsBounds.second) {
            #ifdef DO_PREFETCH
            if(friendsBounds.first+PREFETCH < friendsBounds.second) {
               __builtin_prefetch(nextVisitList + *(friendsBounds.first+PREFETCH),1);
               // pref=(nextVisitList + *(friendsBounds.first+PREFETCH))->data[0];
            }
            #endif

            #ifdef TRACE
            {
               numFriendsAnalyzed++;
               size_t newBits = 0;
               for(int i=0; i<width; i++) {
                  newBits += BitBaseOp<bit_t>::popCount(nextVisitList[*friendsBounds.first].data[i] | curVisit.data[i])
                     - BitBaseOp<bit_t>::popCount(nextVisitList[*friendsBounds.first].data[i] );
               }
               numNewSeen[newBits]++;
            }
            #endif

            for(int i=0; i<width; i++) {
               nextVisitList[*friendsBounds.first].data[i] |= curVisit.data[i];
            }
            ++friendsBounds.first;
      }
         for(int i=0; i<width; i++) {
            visitList[curPerson].data[i] = BitBaseOp<bit_t>::zero();
         }
      }

      #ifdef BI_DIRECTIONAl
      uint32_t frontierSize = 0;
      uint64_t nextVisitNeighbors = 0;
      #endif
      for (PersonId curPerson = 0; curPerson<limit; ++curPerson) {
         #ifdef BI_DIRECTIONAl
         bool nextVisitNonzero=false;
         #endif
         for(unsigned i=0; i<width; i++) {
            const bit_t nextVisit = nextVisitList[curPerson].data[i];
            if(BitBaseOp<bit_t>::notZero(nextVisit)) {
               const bit_t newVisits = BitBaseOp<bit_t>::andNot(nextVisit, seen[curPerson].data[i]);
               nextVisitList[curPerson].data[i] = newVisits;
               if(BitBaseOp<bit_t>::notZero(newVisits)) {
                  seen[curPerson].data[i] |= newVisits;
                  batchDist.updateDiscovered(newVisits, i);

                  #ifdef BI_DIRECTIONAl
                  nextVisitNonzero = true;
                  #endif
               }
            }
         }
         #ifdef BI_DIRECTIONAl
         if(nextVisitNonzero) {
            frontierSize++;
            nextVisitNeighbors += subgraph.retrieve(curPerson)->size();
         }
         #endif
      }

      #ifdef TRACE
      {
         TraceStats<BATCH_BITS_COUNT>& stats = TraceStats<BATCH_BITS_COUNT>::getStats();
         for (int i = 0; i < BATCH_BITS_COUNT; ++i) {
            stats.addRoundVisitBits(nextDistance, i, numBitsSet[i]);
            stats.addRoundFriendBits(nextDistance, i, numNewSeen[i]);
         }
      }
      #endif

      batchDist.finalize();
      #ifdef BI_DIRECTIONAl
      return std::make_pair(frontierSize, nextVisitNeighbors);
      #else
      return std::make_pair(0,0);
      #endif
   }

   static std::pair<uint32_t,uint64_t> __attribute__((hot)) runBatchRoundRev(const PersonSubgraph& subgraph, const PersonId startPerson, const PersonId limit, Bitset* visitList, Bitset* nextVisitList, Bitset* __restrict__ seen, BatchDistance<bit_t, width>& batchDist, const Bitset/* processQuery*/
      #if defined(STATISTICS)
      , BatchStatistics& statistics, uint32_t nextDistance
      #elif defined(TRACE)
      , uint32_t nextDistance
      #endif
   ) {
      uint32_t frontierSize = 0;
      uint64_t nextVisitNeighbors = 0;

         // volatile bit_t pref;
      #ifdef DO_PREFETCH
      const int p2=min(PREFETCH, (unsigned int)(limit-startPerson));
      for(int a=1; a<p2; a++) {
         __builtin_prefetch(seen + a,0);
      }
      #endif

      for (PersonId curPerson = startPerson; curPerson<limit; ++curPerson) {
         auto curSeen = seen[curPerson];

         #ifdef DO_PREFETCH
         if(curPerson+PREFETCH < limit) {
            __builtin_prefetch(seen + curPerson + PREFETCH,0);
         }
         #endif

         bool zero=true;
         for(int i=0; i<width; i++) {
            if(BitBaseOp<bit_t>::notAllOnes(curSeen.data[i])) {
               zero=false;
               break;
            }
         }
         if(zero) {
            continue;
         }

         const auto& curFriends=*subgraph.retrieve(curPerson);
         auto friendsBounds = curFriends.bounds();
         #ifdef DO_PREFETCH
         const int p=min(PREFETCH, (unsigned int)(friendsBounds.second-friendsBounds.first));
         for(int a=1; a<p; a++) {
            __builtin_prefetch(visitList + *(friendsBounds.first+a),1);
         }
         #endif
         Bitset nextVisit;
         while(friendsBounds.first != friendsBounds.second) {
            #ifdef DO_PREFETCH
            if(friendsBounds.first+PREFETCH < friendsBounds.second) {
               __builtin_prefetch(visitList + *(friendsBounds.first+PREFETCH),1);
            }
            #endif

            for(int i=0; i<width; i++) {
               nextVisit.data[i] |= visitList[*friendsBounds.first].data[i];
            }
            ++friendsBounds.first;
         }
         for(int i=0; i<width; i++) {
            nextVisit.data[i] = BitBaseOp<bit_t>::andNot(nextVisit.data[i], curSeen.data[i]);
         }

         nextVisitList[curPerson] = nextVisit;
         bool nextVisitNonzero=false;
         for(int i=0; i<width; i++) {
            if(BitBaseOp<bit_t>::notZero(nextVisit.data[i])) {
               seen[curPerson].data[i] = curSeen.data[i] | nextVisit.data[i];
               batchDist.updateDiscovered(nextVisit.data[i], i);

               nextVisitNonzero=true;
            }
         }
         if(nextVisitNonzero) {
            frontierSize++;
            nextVisitNeighbors += subgraph.retrieve(curPerson)->size();
         }
      }

      memset(visitList, 0, sizeof(Bitset) * limit);

      // for (PersonId curPerson = 0; curPerson<limit; ++curPerson) {
      //    for(unsigned i=0; i<width; i++) {
      //       const bit_t nextVisit = nextVisitList[curPerson].data[i];
      //       if(BitBaseOp<bit_t>::notZero(nextVisit)) {
      //          const bit_t newVisits = BitBaseOp<bit_t>::andNot(nextVisit, seen[curPerson].data[i]);
      //          nextVisitList[curPerson].data[i] = newVisits;
      //          if(BitBaseOp<bit_t>::notZero(newVisits)) {
      //             seen[curPerson].data[i] |= newVisits;
      //             batchDist.updateDiscovered(newVisits, i);
      //          }
      //       }
      //    }
      // }

      batchDist.finalize();
      return std::make_pair(frontierSize, nextVisitNeighbors);
   }

   #else

   enum VisitAction : uint32_t { EMPTY, SINGLE, MULTI};

   struct VisitResult {
      Bitset validVisit;
      uint32_t queryId;
      VisitAction action;
   };

   static void createVisitList(const Bitset& visitList, const Bitset& processQuery, VisitResult& result) {
      if(detectSingle) {
         uint32_t numFields = 0;
         uint32_t nonEmptyField = 0;
         for(unsigned i=0; i<width; i++) {
            result.validVisit.data[i] = visitList.data[i] & processQuery.data[i];
            if(BitBaseOp<bit_t>::notZero(visitList.data[i])) {
               if(BitBaseOp<bit_t>::notZero(result.validVisit.data[i])) {
                  numFields++;
                  nonEmptyField = i;
               }
            }
         }

         // Check if only a single bit is set
         if(numFields == 0) {
            result.action = EMPTY;
         } else if(numFields == 1) {
            auto bitPos = CtzlOp<bit_t>::ctzl(result.validVisit.data[nonEmptyField]);
            auto masked_field = BitBaseOp<bit_t>::andNot(result.validVisit.data[nonEmptyField], BitBaseOp<bit_t>::getSetMask(bitPos));
            if(BitBaseOp<bit_t>::isZero(masked_field)) {
               result.action = SINGLE;
               result.queryId = nonEmptyField*Bitset::TYPE_BITS_COUNT+bitPos;
            } else {
               result.action = MULTI;
            }
         } else {
            result.action = MULTI;
         }
      } else {
         //Need not detect single
         bool nonZero=false;
         for(unsigned i=0; i<width; i++) {
            if(BitBaseOp<bit_t>::notZero(visitList.data[i])) {
               nonZero=true;
               break;
            }
            // } else {
            //    result.validVisit.data[i] = BitBaseOp<bit_t>::zero();
            // }
         }
         if(nonZero) {
            for(unsigned i=0; i<width; i++) {
               result.validVisit.data[i] = visitList.data[i] & processQuery.data[i];
            }
            result.action = MULTI;
         } else {
            result.action = EMPTY;
         }
      }
   }

   static void __attribute__((hot)) runBatchRound(const PersonSubgraph& subgraph, const PersonId startPerson, const PersonId limit, Bitset* visitList, Bitset* nextVisitList, Bitset* __restrict__ seen, BatchDistance<bit_t, width>& batchDist, const Bitset processQuery
      #if defined(STATISTICS)
      , BatchStatistics& statistics, uint32_t nextDistance
      #elif defined(TRACE)
      , uint32_t nextDistance
      #endif
   ) {
      VisitResult visitResult;

      for (PersonId curPerson = startPerson; curPerson<limit; ++curPerson) {
         createVisitList(visitList[curPerson], processQuery, visitResult);
         // Skip persons with empty visit list
         if(visitResult.action == EMPTY) { continue; }

         const auto& curFriends=*subgraph.retrieve(curPerson);

         //Only single person in this entry
         if(!detectSingle || visitResult.action == MULTI) {
            //More than one person
            auto friendsBounds = curFriends.bounds();
            #ifdef DO_PREFETCH
            __builtin_prefetch(seen + *(friendsBounds.first+1),0);
            __builtin_prefetch(nextVisitList + *(friendsBounds.first+1),1);
            __builtin_prefetch(seen + *(friendsBounds.first+2),0);
            __builtin_prefetch(nextVisitList + *(friendsBounds.first+2),1);
            #endif
            while(friendsBounds.first != friendsBounds.second) {
               #ifdef DO_PREFETCH
               if(friendsBounds.first+3 < friendsBounds.second) {
                  __builtin_prefetch(seen + *(friendsBounds.first+3),0);
                  __builtin_prefetch(nextVisitList + *(friendsBounds.first+3),1);
               }
               #endif

               for(unsigned i=0; i<width; i++) {
                  bit_t newVisits = BitBaseOp<bit_t>::andNot(visitResult.validVisit.data[i], seen[*friendsBounds.first].data[i]);
                  if(BitBaseOp<bit_t>::notZero(newVisits)) {
                     seen[*friendsBounds.first].data[i] |= visitResult.validVisit.data[i];
                     nextVisitList[*friendsBounds.first].data[i] |= newVisits;

                     // Normal case until uint64_t
                     batchDist.updateDiscovered(newVisits, i);
                  }
               }
               ++friendsBounds.first;
            }
         } else {
            auto friendsBounds = curFriends.bounds();
            while(friendsBounds.first != friendsBounds.second) {
               #ifdef DO_PREFETCH
               if(friendsBounds.first+3 < friendsBounds.second) {
                  __builtin_prefetch(seen + *(friendsBounds.first+3),0);
                  __builtin_prefetch(nextVisitList + *(friendsBounds.first+3),1);
               }
               #endif
               auto field = visitResult.queryId/Bitset::TYPE_BITS_COUNT;
               bit_t newVisits = BitBaseOp<bit_t>::andNot(visitResult.validVisit.data[field], seen[*friendsBounds.first].data[field]);
               if(BitBaseOp<bit_t>::notZero(newVisits)) {
                  seen[*friendsBounds.first].data[field] |= visitResult.validVisit.data[field];
                  nextVisitList[*friendsBounds.first].data[field] |= newVisits;
                  batchDist.numDistDiscovered[visitResult.queryId]++;
               }
               ++friendsBounds.first;
            }
         }
      }

      batchDist.finalize();
   }

   #endif

   static void updateProcessQuery(Bitset& processQuery, const uint32_t pos, const uint32_t numDiscovered,
       BatchBFSdata& bfsData, const uint32_t distance, uint32_t& queriesToProcess) {
      auto field = pos/Bitset::TYPE_BITS_COUNT;
      auto field_bit = pos-(field*Bitset::TYPE_BITS_COUNT);

      if(BitBaseOp<bit_t>::notZero(processQuery.data[field] & BitBaseOp<bit_t>::getSetMask(field_bit))) {
         bfsData.totalReachable += numDiscovered;
         bfsData.totalDistances += numDiscovered*distance;

         if((bfsData.componentSize-1)==bfsData.totalReachable|| numDiscovered==0) {
            processQuery.data[field] = BitBaseOp<bit_t>::andNot(processQuery.data[field], BitBaseOp<bit_t>::getSetMask(field_bit));
            queriesToProcess--;
         }
      } else {
         assert(numDiscovered == 0);
      }
   }
};


}
