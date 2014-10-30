//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#pragma once

// #define STATISTICS
// #define FULL_STATISTICS

#include "include/idschedulers.hpp"

#include "include/topklist.hpp"
#include "include/log.hpp"
#include "include/bfs/base.hpp"

#include "include/bfs/batchhuge.hpp"
#include "include/scheduler.hpp"
#include "include/worker.hpp"
#include "include/bfs/naive.hpp"
#include "include/bfs/batch64.hpp"
#include "include/bfs/batch128.hpp"
#include "include/bfs/batch256.hpp"
#include "include/bfs/statistics.hpp"

#include <mutex>
#include <cmath>
#include <algorithm>
#include <random>


// #define OUTPUT_PROGRESS

using namespace std;

namespace Query4 {

static const uint32_t maxMorselTasks = 256000;
static const uint32_t minMorselSize = 1;

struct CentralityResult {
   PersonId person;
   uint64_t distances;
   uint32_t numReachable;
   double centrality;

   CentralityResult(PersonId person, uint64_t distances, uint32_t numReachable, double centrality)
      : person(person), distances(distances), numReachable(numReachable), centrality(centrality) {
   }

   bool operator==(const CentralityResult& other) const {
      return person==other.person&&centrality==other.centrality;
   }

   friend std::ostream& operator<<(std::ostream &out, const CentralityResult& v) {
      out<<v.person<<"::"<<v.distances;
      return out;
   }
};

typedef std::pair<PersonId, CentralityResult> CentralityEntry;
}

namespace awfy {
static const double EPSILON = 0.000000000001;

template<>
class TopKComparer<Query4::CentralityEntry> {
public:
   // Returns true if first param is larger or equal
   static bool compare(const Query4::CentralityEntry& a, const Query4::CentralityEntry& b)
   {
      auto delta = a.second.centrality - b.second.centrality;
      return ((delta >0) || (fabs(delta)< EPSILON && a.second.person < b.second.person));
   }
};
}

namespace Query4 {
typedef awfy::TopKComparer<CentralityEntry> CentralityCmp;

class QueryState {
public:
   const uint32_t k;
   const PersonSubgraph& subgraph;
   const uint64_t startTime;

   vector<uint8_t> personChecked;

   mutex topResultsMutex;
   awfy::TopKList<PersonId, CentralityResult> topResults;

   QueryState(const uint32_t k, const PersonSubgraph& subgraph)
      : k(k), subgraph(move(subgraph)), startTime(tschrono::now()), personChecked(subgraph.size()), topResultsMutex(),
         topResults(make_pair(std::numeric_limits<PersonId>::max(),CentralityResult(std::numeric_limits<PersonId>::max(), 0, 0, 0.0))) {
      topResults.init(k);
   }
};

double getCloseness(uint32_t totalPersons,uint64_t totalDistances,uint32_t totalReachable);

struct ResultConcatenator {
   QueryState* state;
   const char*& resultOut;
   #ifdef STATISTICS
   BatchStatistics& statistics;
   #endif

   ResultConcatenator(QueryState* state, const char*& resultOut
      #ifdef STATISTICS
      , BatchStatistics& statistics
      #endif
      );
   void operator()();

   // ResultConcatenator(ResultConcatenator&&) = default;
   // ResultConcatenator& operator=(ResultConcatenator&&) = default;
};

size_t getMaxMorselBatchSize();

template<typename BFSRunnerT>
struct MorselTask {
private:
   const uint32_t rangeStart;
   const uint32_t rangeEnd;

   size_t batchSize;

   QueryState& state;
   const PersonSubgraph& subgraph;
   vector<PersonId>& ids;

   const uint64_t startTime;


   #ifdef STATISTICS
   BatchStatistics& statistics;
   #endif

public:
   MorselTask(QueryState& state, PersonId rangeStart, PersonId rangeEnd, const PersonSubgraph& subgraph, vector<PersonId>& ids, uint64_t startTime
      #ifdef STATISTICS
      , BatchStatistics& statistics
      #endif
      )
      : rangeStart(rangeStart), rangeEnd(rangeEnd), state(state), subgraph(subgraph), ids(ids), startTime(startTime)
      #ifdef STATISTICS
      , statistics(statistics)
      #endif
   {
      batchSize = BFSRunnerT::batchSize();
      if(batchSize>getMaxMorselBatchSize()) {
         batchSize=getMaxMorselBatchSize();
      }
   }

   //Returns pair of processed persons and whether the bound was updated
   pair<uint32_t,bool> processPersonBatch(PersonId begin, PersonId end) {
      // Build batch with the desired size
      vector<BatchBFSdata> batchData;
      batchData.reserve(batchSize);

      PersonId index=begin;
      for(; batchData.size()<batchSize && index<end; index++) {
         PersonId subgraphPersonId = ids[index];
         assert(!state.personChecked[subgraphPersonId]);

         const uint32_t componentSize = subgraph.componentSizes[subgraph.personComponents[subgraphPersonId]];

         BatchBFSdata personData(subgraphPersonId, componentSize);

         state.personChecked[subgraphPersonId] = true;
         batchData.push_back(move(personData));
      }
      const uint32_t last = index-1;
      // #endif


      bool boundUpdated=false;
      if(batchData.size()>0) {
         //Run BFS
         BFSRunnerT::runBatch(batchData, state.subgraph
            #ifdef STATISTICS
            , statistics
            #endif
            );

         for(auto bIter=batchData.begin(); bIter!=batchData.end(); bIter++) {
            const auto closeness = getCloseness(bIter->componentSize, bIter->totalDistances, bIter->totalReachable);
            const PersonId externalPersonId = bIter->person;
            CentralityResult resultCentrality(externalPersonId, bIter->totalDistances, bIter->totalReachable, closeness);

            // Check if person qualifies as new top k value
            if(CentralityCmp::compare(make_pair(resultCentrality.person, resultCentrality), state.topResults.getBound())) {
               // Add improved value to top k list
               lock_guard<mutex> lock(state.topResultsMutex);
               state.topResults.insert(resultCentrality.person, resultCentrality);
            }
         }
      }

      return make_pair(last-begin+1, boundUpdated);
   }

   void operator()() {
      if(rangeEnd<rangeStart) {
         FATAL_ERROR("[MorselTask] Fail! Invalid task range: "<<rangeStart<<"-"<<rangeEnd);
      }

      #ifdef OUTPUT_PROGRESS
      static std::mutex m;
      {
         std::lock_guard<std::mutex> lock(m);
         std::cout<<"#"<<rangeStart<<" @ "<<tschrono::now() - startTime<<std::endl;
      }
      #endif

      PersonId person=rangeStart;
      while(person<rangeEnd) {
         const auto batchResult = processPersonBatch(person, rangeEnd);
         person += batchResult.first;
      }
   }
};
}

std::vector<pair<Query4::PersonId,Query4::PersonId>> generateTasks(const uint64_t maxBfs, const Query4::PersonId graphSize, const size_t batchSize);

template<typename BFSRunnerT>
std::string runBFS(const uint32_t k, const Query4::PersonSubgraph& subgraph, Workers& workers, const uint64_t maxBfs, uint64_t& runtimeOut
   #ifdef STATISTICS
   , Query4::BatchStatistics& statistics
   #endif
   ) {
   // #ifdef STATISTICS
   // numTouchedPersonInDistance.clear();
   // numTouchedPersonInDistance.resize(subgraph.size());
   // for(uint32_t a=1; a<subgraph.size(); a++) {
   //    numTouchedPersonInDistance[a].resize(Query4::maxStatisticsDistance);
   // }
   // #endif

   // Initialize query state
   Query4::QueryState* queryState = new Query4::QueryState(k, subgraph);

   // Determine bfs order
   std::vector<Query4::PersonId> ids(subgraph.size());
   for (unsigned i = 0; i < subgraph.size(); ++i) {
      ids[i] = i;
   }

   // Deterministic random shuffeling
   RandomNodeOrdering::order(ids, BFSRunnerT::batchSize(), subgraph);
   // Do final ordering on specified subset
   LOG_PRINT("[Query4] Starting sort "<<tschrono::now());
   // ComponentOrdering::order(ids, maxBfs, BFSRunnerT::batchSize(), subgraph);
   DegreeOrdering::order(ids, maxBfs, BFSRunnerT::batchSize(), subgraph);
   // TwoHopDegreeOrdering::order(ids, maxBfs, BFSRunnerT::batchSize(), subgraph);
   //AdvancedNeighborOrdering::order(ids, BFSRunnerT::batchSize(), subgraph);
   //NeighourDegreeOrdering::order(ids, BFSRunnerT::batchSize(), subgraph);
   LOG_PRINT("[Query4] Finished sort "<<tschrono::now());


   const auto start = tschrono::now();

   // Create bfs tasks from specified subset
   TaskGroup tasks;
   uint64_t numTraversedEdges = 0;
   auto ranges = generateTasks(maxBfs, subgraph.size(), BFSRunnerT::batchSize());
   for(auto& range : ranges) {
      Query4::MorselTask<BFSRunnerT> bfsTask(*queryState, range.first, range.second, subgraph, ids, start
         #ifdef STATISTICS
         , statistics
         #endif
         );
      tasks.schedule(LambdaRunner::createLambdaTask(bfsTask));

      for(Query4::PersonId i=range.first; i<range.second; i++) {
         numTraversedEdges += subgraph.componentEdgeCount[subgraph.personComponents[ids[i]]];
      }
   }
   TraceStats<BFSRunnerT::batchSize()>& stats = TraceStats<BFSRunnerT::batchSize()>::getStats();
   stats.setNumTraversedEdges(numTraversedEdges);

   //std::cout << "# TaskStats "<<maxBfs<<", "<<ranges.size()<< std::endl;

   const char* resultChar;
   tasks.join(LambdaRunner::createLambdaTask(move(Query4::ResultConcatenator(queryState, resultChar
       #ifdef STATISTICS
         , statistics
         #endif
         ))));

   LOG_PRINT("[Query4] Scheduling "<< ranges.size() << " tasks.");
   Scheduler scheduler;
   scheduler.schedule(tasks.close());
   scheduler.setCloseOnEmpty();

   workers.assist(scheduler);

   // Always run one executor on the main thread
   Executor executor(scheduler,0, false);
   executor.run();

   runtimeOut = tschrono::now() - start;

   scheduler.waitAllFinished();
   LOG_PRINT("[Query4] All tasks finished");

   std::string resultStr = std::string(resultChar);
   delete[] resultChar;

   return std::string(resultStr);
}
