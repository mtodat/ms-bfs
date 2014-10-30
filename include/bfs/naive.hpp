//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#pragma once

#include "base.hpp"
#include "statistics.hpp"

namespace Query4 {

struct BFSRunner {
   static const size_t TYPE=1;
   static const size_t WIDTH=1;
   static const size_t TYPE_BITS=8;

   typedef awfy::FixedSizeQueue<PersonId> BFSQueue;

   static constexpr size_t batchSize() {
      return 1;
   }

   static inline void runBatch(std::vector<BatchBFSdata>& bfsData, const PersonSubgraph& subgraph
      #ifdef STATISTICS
      , BatchStatistics& statistics
      #endif
      ) {
      for(size_t i=0; i<bfsData.size(); i++) {
         run(bfsData[i].person, subgraph, bfsData[i]
            #ifdef STATISTICS
            , statistics
            #endif
            );
      }
   }

private:
   static void run(const PersonId start, const PersonSubgraph& subgraph, BatchBFSdata& bfsData
      #ifdef STATISTICS
      , BatchStatistics& statistics
      #endif
      );

   static Persons runRound(const PersonSubgraph& subgraph, Distance* __restrict__ seen, BFSQueue& toVisit, 
      const Persons numToVisit, const Persons numUnseen);
};

}