//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#include "../include/bfs/parabfs.hpp"
#include "../include/TraceStats.hpp"

namespace Query4 {

  PARABFSRunner::KData* PARABFSRunner::kdata = new PARABFSRunner::KData();
  bool PARABFSRunner::isInitialized = false;

  void PARABFSRunner::run(const PersonId s, const PersonSubgraph& subgraph, BatchBFSdata& bfsData
  #ifdef STATISTICS
  , BatchStatistics&/* statistics */
  #endif
  ) {
        memset(PARABFSRunner::kdata->visited, 0, PARABFSRunner::kdata->personSize*sizeof(bool));
    PARABFSRunner::kdata->visited[s] = 1;
    uint32_t * cqueue = &PARABFSRunner::kdata->queue[kdata->numThreads*(PARABFSRunner::kdata->personSize)];
    cqueue[0] = s; //Initialize current queue
    uint32_t cq = 1; //size of the current-queue
    int level = 0;
    for(int i=0; i<kdata->numThreads; i++) {
      PARABFSRunner::kdata->params[i].sp = 0;
      PARABFSRunner::kdata->params[i].rp = 0;
      PARABFSRunner::kdata->params[i].level = 0;
    }
     Distance distance=0;
    while(cq > 0) {
      for(int i=0; i<kdata->numThreads; i++)
        PARABFSRunner::kdata->params[i].cq_end = cq;
      size_t startTime = tschrono::now();
      dispatchToWorkers();
      waitForWorkers();
      assert(distance<std::numeric_limits<Distance>::max());
      distance++;
      TraceStats<TYPE_BITS*WIDTH>& stats = TraceStats<TYPE_BITS*WIDTH>::getStats();
      stats.traceRound(distance);
      stats.addRoundDuration(distance, (tschrono::now()-startTime));

      cq = 0;
      for(int i=0; i<kdata->numThreads; ++i)  {
#if USE_ATOMIC
        memcpy(cqueue+cq, PARABFSRunner::kdata->params[i].nqueue, sizeof(uint32_t)*PARABFSRunner::kdata->params[i].nq);
        cq += PARABFSRunner::kdata->params[i].nq;
#else
        uint32_t *nQ = PARABFSRunner::kdata->params[i].nqueue;
        for (int j=0; j<PARABFSRunner::kdata->params[i].nq; ++j) {
          if (PARABFSRunner::kdata->visited2[nQ[j]]) {
            PARABFSRunner::kdata->visited2[nQ[j]] = false;
            cqueue[cq++] = nQ[j];
            PARABFSRunner::kdata->params[i].sp += PARABFSRunner::kdata->params[i].level;
            ++PARABFSRunner::kdata->params[i].rp;
          }
        }
#endif
        PARABFSRunner::kdata->params[i].nq = 0;
      }

      //bfsData.totalReachable+=numDiscovered;
      bfsData.totalReachable+=cq;
      //bfsData.totalDistances+=numDiscovered*distance;
      bfsData.totalDistances+=cq*distance;
      if((bfsData.componentSize-1)==bfsData.totalReachable) {
          break;
      }
    }


  }

  void PARABFSRunner::traverse(TraverseParams *tp, const PersonSubgraph& subgraph){
    uint32_t *cqueue = tp->cqueue;
    uint32_t *nqueue = tp->nqueue;
    uint32_t  cq = tp->cq_start;
    uint32_t  cq_end = tp->cq_end;
    uint32_t  st = tp->stride;
    uint32_t  nq = tp->nq;
    uint64_t  sp = tp->sp;
    uint64_t  rp = tp->rp;
    uint32_t  level = ++tp->level;
    for(; cq<cq_end; cq+=st)
    {
      uint32_t u = cqueue[cq];//Other thread can write and read to queue at this time.
      auto friendsBounds = subgraph.retrieve(u)->bounds();
      while(friendsBounds.first != friendsBounds.second) {
        uint32_t v = *friendsBounds.first;
        {
            if (!PARABFSRunner::kdata->visited[v]) //This command would reduces executing atomic operations 
            {
#if USE_ATOMIC
              bool b = __sync_fetch_and_or(PARABFSRunner::kdata->visited+v, true);
              // for GCC >= 4.7, use the below instead
              // bool b = __atomic_fetch_or(visited+v, true, __ATOMIC_RELAXED);
              if (!b)
              {
                nqueue[nq++] = v;
                sp += level;
                rp += 1;
              }
#else
              PARABFSRunner::kdata->visited[v] = true;
              PARABFSRunner::kdata->visited2[v] = true;
              nqueue[nq++] = v;
#endif
            }
        }
        ++friendsBounds.first;
      }
    }
    tp->set(nq, sp, rp);
  }

  void PARABFSRunner::start(std::vector<TraverseParams> &params, const PersonSubgraph& subgraph) {
    if (!PARABFSRunner::kdata->shutdown)
      return;
    PARABFSRunner::kdata->shutdown = false;
    PARABFSRunner::kdata->workerBusy = kdata->numThreads;
    for (int i=0; i<kdata->numThreads; i++) {
      PARABFSRunner::kdata->workers.create_thread(boost::bind(&PARABFSRunner::worker, &params[i], boost::cref(subgraph)));
    }
    waitForWorkers();
  }

  void PARABFSRunner::stop() {
    PARABFSRunner::kdata->shutdown = true;
    PARABFSRunner::kdata->condWork.notify_all();
  }

  void PARABFSRunner::dispatchToWorkers()
  {
    PARABFSRunner::kdata->workerBusy = kdata->numThreads;
    PARABFSRunner::kdata->condWork.notify_all();
  }
  
  void PARABFSRunner::pfree(){
    free(PARABFSRunner::kdata->queue);
    free(PARABFSRunner::kdata->visited);
    free(PARABFSRunner::kdata->cc);
  }
  
  void PARABFSRunner::waitForWorkers() {
    boost::mutex::scoped_lock lock(PARABFSRunner::kdata->mutex);
    if (PARABFSRunner::kdata->workerBusy>0)
      PARABFSRunner::kdata->condDone.wait(lock);
  }

  void PARABFSRunner::worker(TraverseParams *param, const PersonSubgraph& subgraph) {
    while (1) {
      {
        boost::mutex::scoped_lock lock(PARABFSRunner::kdata->mutex);
        if (--PARABFSRunner::kdata->workerBusy==0)
          PARABFSRunner::kdata->condDone.notify_all();
        PARABFSRunner::kdata->condWork.wait(lock);
      }
      if (PARABFSRunner::kdata->shutdown) break;
      traverse(param, subgraph);
    }
  }
}
