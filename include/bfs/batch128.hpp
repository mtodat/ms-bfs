//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#pragma once

#include "base.hpp"
#include "sse.hpp"
#include "statistics.hpp"
#include <array>

namespace Query4 {

struct BatchBFSRunner128 {

   static constexpr uint64_t batchSize() {
      return 128;
   }

   static void runBatch(std::vector<BatchBFSdata>& bfsData, const PersonSubgraph& subgraph
      #ifdef STATISTICS
      , BatchStatistics& statistics
      #endif
      );

private:
   static void runBatchRound(std::vector<BatchBFSdata>& bfsData, const PersonSubgraph& subgraph, PersonId minPerson, 
      std::array<__m128i*,2>& toVisitLists, __m128i* __restrict__ seen
      #ifdef STATISTICS
      , BatchStatistics& statistics
      #endif
      );
};

}