//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#pragma once

#include "base.hpp"
#include "statistics.hpp"


namespace Query4 {

struct NoQueueBFSRunner {
   static const size_t TYPE=2;
   static const size_t WIDTH=1;
   static const size_t TYPE_BITS=8;

   static constexpr size_t batchSize() {
      return 1;
   }

   static inline void runBatch(std::vector<BatchBFSdata>& bfsData, const PersonSubgraph& subgraph
      #ifdef STATISTICS
      , BatchStatistics&/* statistics*/
      #endif
      ) {
      for(size_t i=0; i<bfsData.size(); i++) {
         run(bfsData[i].person, subgraph, bfsData[i]);
      }
   }

private:
   static void run(const PersonId start, const PersonSubgraph& subgraph, BatchBFSdata& bfsData);

   static Persons runRound(const PersonSubgraph& subgraph, Distance* __restrict__ seen, const Distance* __restrict__ currentVisit, Distance* __restrict__ nextVisit);
};

}