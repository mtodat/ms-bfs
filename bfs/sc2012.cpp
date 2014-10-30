//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#include "../include/bfs/sc2012.hpp"
#include "../include/TraceStats.hpp"


namespace Query4 {

void __attribute__ ((noinline)) SCBFSRunner::run(const PersonId start, const PersonSubgraph& subgraph, BatchBFSdata& bfsData
   #ifdef STATISTICS
   , BatchStatistics&/* statistics */
   #endif
   ) {
   const auto subgraphSize = subgraph.size();

   // Initialize BFS
   std::array<SCBFSRunner::CustomBFSData, 2> visitLists;
   int curVisitList=0;
   CustomBFSData seen(subgraphSize);
   visitLists[0].assign(subgraphSize, 0);
   visitLists[1].assign(subgraphSize, 0);
   seen[start] = 1;
   (visitLists[0])[start] = 1;

   // Initialize switching condition
   uint32_t m_frontier, last_m_frontier, m_unexplored, n_frontier, top_down, bottom_up;
   bool is_top_down = true;
   m_frontier = 0;
   last_m_frontier = 0;
   m_unexplored = subgraph.numEdges;

   // Terminating condition
   size_t frontierSize = 1;

   //fprintf(stderr, "Starting new BFS\n");

   Distance distance=0;
   while(frontierSize > 0){
      size_t startTime = tschrono::now();

      SCBFSRunner::CustomBFSData& frontier = visitLists[curVisitList];
      SCBFSRunner::CustomBFSData& next = visitLists[1-curVisitList];


      // Switch between top-down and bottom-up
      last_m_frontier = m_frontier;
      m_frontier = 0;
      for (size_t i = 0; i < subgraphSize; ++i){
         if (frontier[i] == 0) continue;
         const auto friends = subgraph.retrieve(i);
         m_frontier += friends->size();
      }
      n_frontier = frontierSize;
      m_unexplored -= last_m_frontier;

      top_down = m_unexplored / SCBFSRunner::alpha;
      bottom_up = subgraph.size() / SCBFSRunner::beta;

      //fprintf(stderr, "m_frontier = %d, n_frontier = %d, m_unexplored = %d\n", m_frontier, n_frontier, m_unexplored);
      //fprintf(stderr, "top_down = %d, bottom_up = %d\n", top_down, bottom_up);

      if (is_top_down){
         if (m_frontier <= top_down){
            frontierSize = runRoundTopDown(subgraph, frontier, next, seen);
            is_top_down = true;
         } else {
            frontierSize = runRoundBottomUp(subgraph, frontier, next, seen);
            is_top_down = false;
         }
      } else {
         if (n_frontier >= bottom_up){
            frontierSize = runRoundBottomUp(subgraph, frontier, next, seen);
            is_top_down = false;
         } else {
            frontierSize = runRoundTopDown(subgraph, frontier, next, seen);
            is_top_down = true;
         }
      }

      frontier.assign(subgraphSize, 0);
      curVisitList = 1-curVisitList;

      //Closeness centrality
      Persons numDiscovered = frontierSize;
      assert(distance<std::numeric_limits<Distance>::max());
      distance++;

      // Adjust result
      bfsData.totalReachable+=numDiscovered;
      bfsData.totalDistances+=numDiscovered*distance;

      TraceStats<TYPE_BITS*WIDTH>& stats = TraceStats<TYPE_BITS*WIDTH>::getStats();
      stats.traceRound(distance);
      stats.addRoundDuration(distance, (tschrono::now()-startTime));
   }
}

size_t __attribute__((hot))SCBFSRunner::runRoundTopDown(const PersonSubgraph& subgraph,SCBFSRunner::CustomBFSData& frontier,SCBFSRunner::CustomBFSData& next,SCBFSRunner::CustomBFSData& seen){
   //fprintf(stderr, "Top-Down\n");
   size_t frontierSize = 0;
   const size_t subgraphSize = frontier.size();
   for (size_t i = 0; i < subgraphSize; ++i){
      if (frontier[i] == 0) continue;
      auto friendsBounds = subgraph.retrieve(i)->bounds();
      while(friendsBounds.first != friendsBounds.second) {
         if (seen[*friendsBounds.first] == 0){
            seen[*friendsBounds.first] = 1;
            next[*friendsBounds.first] = 1;
            frontierSize++;
         }
         ++friendsBounds.first;
      }
   }
   return frontierSize;
}

size_t __attribute__((hot))SCBFSRunner::runRoundBottomUp(const PersonSubgraph& subgraph,SCBFSRunner::CustomBFSData& frontier,SCBFSRunner::CustomBFSData& next,SCBFSRunner::CustomBFSData& seen){
   //fprintf(stderr, "Bottom-Up\n");
   size_t frontierSize = 0;
   const size_t subgraphSize = frontier.size();
   for (size_t i = 0; i < subgraphSize; ++i){
      if (seen[i] == 0){
         auto friendsBounds = subgraph.retrieve(i)->bounds();
         while(friendsBounds.first != friendsBounds.second) {
            PersonId n = *friendsBounds.first;
            if (frontier[n] != 0){
               seen[i] = 1;
               next[i] = 1;
               frontierSize++;
               break;
            }
            ++friendsBounds.first;
         }
      }
   }
   return frontierSize;
}

}