//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#pragma once

#include "base.hpp"
#include "statistics.hpp"
#include <boost/thread/thread.hpp>

#define USE_ATOMIC 1
#define TOPK       7
namespace Query4 {

struct PARABFSRunner {  
  static const size_t TYPE=1;
  static const size_t WIDTH=1;
  static const size_t TYPE_BITS=8;

  struct TraverseParams {
    TraverseParams(uint32_t *cQ=0, uint32_t *nQ=0, uint32_t cqs=0, uint32_t st=0): cqueue(cQ), nqueue(nQ), cq_start(cqs), cq_end(0), stride(st), nq(0), sp(0), rp(0), level(0) {}

    void set(uint32_t q, uint64_t s, uint64_t r) {
      nq = q;
      sp = s;
      rp = r;
    }
    uint32_t *cqueue;
    uint32_t *nqueue;
    uint32_t  cq_start;
    uint32_t  cq_end;
    uint32_t  stride;
    uint32_t  nq;
    uint64_t  sp;
    uint64_t  rp;
    uint32_t  level;
  };

  typedef std::pair<uint32_t, double> CC;
  struct CompareCC{ 
    bool operator()(const CC &lhs, const CC &rhs) const{ 
      if (lhs.second == rhs.second) 
        return (lhs.first < rhs.first); 
      return (lhs.second > rhs.second); 
    } 
  };

  struct KData{
    uint32_t       personSize;
    uint32_t*      queue;

    bool * visited;
#if !USE_ATOMIC
    bool * visited2;
#endif

    boost::mutex                mutex;
    boost::condition_variable   condDone, condWork;
    boost::thread_group         workers;
    int                         workerBusy;
    bool                        shutdown;
    CC* cc;//closeness centrality
    std::vector<TraverseParams> params;
    int                         numThreads;
  };

  static KData *kdata;
  static bool isInitialized;

  static void init(uint32_t personSize){
    kdata->personSize = personSize;

    kdata->queue = (uint32_t*)malloc(sizeof(uint32_t)*kdata->personSize*(kdata->numThreads + 1));//Allocate n+1 queues
    kdata->visited = (bool*)malloc(sizeof(bool)*kdata->personSize*2);
#if !USE_ATOMIC
    kdata->visited2 = kdata->visited + kdata->personSize;
#endif
    kdata->cc = (CC*)malloc(sizeof(CC)*kdata->personSize);
    kdata->shutdown = true;
    isInitialized = true;
  }
 
  static void setThreadNum(int nThreads)
  {
    kdata->numThreads = nThreads;
  }

  static inline void runBatch(std::vector<BatchBFSdata>& bfsData, const PersonSubgraph& subgraph
     #ifdef STATISTICS
     , BatchStatistics& statistics
     #endif
     ) {
    if (!isInitialized)
    {
      init(subgraph.size());
      uint32_t* cqueue = &kdata->queue[kdata->numThreads*(kdata->personSize)];
      for (int i=0; i<kdata->numThreads; ++i)
        kdata->params.push_back(TraverseParams(cqueue, kdata->queue+i*(kdata->personSize), i, kdata->numThreads));
      start(kdata->params, subgraph);
    }
    for(size_t i=0; i<bfsData.size(); i++) {
       run(bfsData[i].person, subgraph, bfsData[i]
          #ifdef STATISTICS
          , statistics
          #endif
          );
     }
     }

  static constexpr size_t batchSize() {
     return 1;
  }

  static void finish()
  {
    stop();
    pfree();
    std::partial_sort(kdata->cc, kdata->cc+TOPK, kdata->cc+kdata->personSize, CompareCC());
  //  for(int i=0; i<TOPK; i++)
  //    printf("%d|", kdata->cc[i].first);
  }

  static void waitForWorkers();
  static void worker(TraverseParams *param, const PersonSubgraph& subgraph); 
  static void stop();
  static void pfree();
  static void dispatchToWorkers();
  static void start(std::vector<TraverseParams> &params, const PersonSubgraph& subgraph);
  static void traverse(TraverseParams *tp, const PersonSubgraph& subgraph);
  static void run(const PersonId start, const PersonSubgraph& subgraph, BatchBFSdata& bfsData
     #ifdef STATISTICS
     , BatchStatistics& statistics
     #endif
     );

};
}
