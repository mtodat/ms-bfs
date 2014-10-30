//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#pragma once

#include "base.hpp"
#include "statistics.hpp"
#include <array>

namespace Query4 {

struct BatchBFSRunner {

   static constexpr uint64_t batchSize() {
      return sizeof(uint64_t)*8;
   }

   static void runBatch(std::vector<BatchBFSdata>& bfsData, const PersonSubgraph& subgraph
      #ifdef STATISTICS
      , BatchStatistics& statistics
      #endif
      );

private:
   static void runBatchRound(std::vector<BatchBFSdata>& bfsData, const PersonSubgraph& subgraph, PersonId minPerson, std::array<uint64_t*,2>& toVisitLists, 
      uint64_t* __restrict__ seen);
};

}