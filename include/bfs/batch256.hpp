//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#pragma once

#include "base.hpp"
#include "sse.hpp"

namespace Query4 {

#ifdef AVX2

struct BatchBFSRunner256 {
   static constexpr uint64_t batchSize() {
      return 256;
   }

   static void runBatch(std::vector<BatchBFSdata>& bfsData, const PersonSubgraph& subgraph);

private:
   static void runBatchRound(std::vector<BatchBFSdata>& bfsData, const PersonSubgraph& subgraph, PersonId minPerson, __m256i** toVisitLists, __m256i* __restrict__ seen);
};
#endif

}