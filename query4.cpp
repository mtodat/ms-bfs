//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#include <iostream>
#include <sstream>
#include <random>
#include <bitset>
#include <cstring>
#include "query4.hpp"
#include <cstdlib>

using namespace std;

std::vector<pair<Query4::PersonId,Query4::PersonId>> generateTasks(const uint64_t maxBfs, const Query4::PersonId graphSize, const size_t batchSize) {
   // Determine number of persons
   Query4::PersonId numBfs = graphSize<maxBfs?graphSize:maxBfs;

   // Initialize task size as max of minMorselSize and batchSize;
   uint32_t taskSize=batchSize<Query4::minMorselSize?Query4::minMorselSize:batchSize;

   // Increase task size until max morsel tasks limit is met
   while(numBfs/taskSize>Query4::maxMorselTasks) {
      taskSize+=batchSize;
   }

   LOG_PRINT("[TaskGen] Task size "<< taskSize);

   // Create tasks
   std::vector<pair<Query4::PersonId,Query4::PersonId>> ranges;
   for (unsigned i = 0; i < numBfs; i+=taskSize) {
      Query4::PersonId start = i;
      Query4::PersonId limit = (start+taskSize)>numBfs?numBfs:(start+taskSize);
      ranges.push_back(make_pair(start,limit));
   }

   return ranges;
}
// Comperator to create correct ordering of results
namespace Query4 {

size_t getMaxMorselBatchSize() {
   char* userBatchSizeStr;
   userBatchSizeStr = getenv("MAX_BATCH_SIZE");
   if(userBatchSizeStr!=nullptr) {
      return atoi(userBatchSizeStr);
   } else {
      return std::numeric_limits<size_t>::max();
   }
}

double getCloseness(uint32_t totalPersons,uint64_t totalDistances,uint32_t totalReachable) {
   return (totalDistances>0 && totalReachable>0 && totalPersons>0)
            ? static_cast<double>((totalReachable-1)*(totalReachable-1)) / (static_cast<double>((totalPersons-1))*totalDistances)
            : 0.0;
}


ResultConcatenator::ResultConcatenator(QueryState* state, const char*& resultOut
   #ifdef STATISTICS
   , BatchStatistics& statistics
   #endif
   )
   : state(state), resultOut(resultOut)
   #ifdef STATISTICS
   , statistics(statistics)
   #endif
{ 
}

void ResultConcatenator::operator()() {
   #ifdef STATISTICS
   statistics.print();
   #endif

   ostringstream output;
   auto& topEntries=state->topResults.getEntries();
   assert(topEntries.size()<=state->k);
   const uint32_t resNum = min(state->k, (uint32_t)topEntries.size());
   for (uint32_t i=0; i<resNum; i++){
      if(i>0) {
         output<<"|";
      }

      output<<state->subgraph.mapInternalNodeId(topEntries[i].first);
   }
   const auto& outStr = output.str();
   auto resultBuffer = new char[outStr.size()+1];
   outStr.copy(resultBuffer, outStr.size());
   resultBuffer[outStr.size()]=0;
   resultOut = resultBuffer;
   delete state;
}
}