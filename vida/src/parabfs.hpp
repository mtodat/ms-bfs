//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#include "person_graph.hpp"
#include <cstring>
#include <numeric>
#include <boost/thread/thread.hpp>
#include <boost/atomic.hpp>

#define USE_ATOMIC 1

struct ParaBFS
{
  typedef std::pair<uint32_t, double> CC;
  PersonGraph    *pg;
  uint32_t       personSize;
  int            nThread;
  int            k;
  uint32_t* queue;
  
  bool * visited;
#if !USE_ATOMIC
  bool * visited2;
#endif
  CC* cc;//closeness centrality
  uint32_t nVertex;

  boost::mutex                mutex;  
  boost::condition_variable   condDone, condWork;
  boost::thread_group         workers;
  int                         workerBusy;
  bool                        shutdown;

  struct TraverseParams {
    TraverseParams(uint32_t *cQ=0, uint32_t *nQ=0, uint32_t cqs=0, uint32_t st=0): cqueue(cQ), nqueue(nQ), cq_start(cqs), cq_end(0), stride(st), nq(0), sp(0), rp(0), level(0) {}
    
    void set(uint32_t q, uint64_t s, uint64_t r) {
      this->nq = q;
      this->sp = s;
      this->rp = r;
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

  std::vector<TraverseParams> params;

  ParaBFS(PersonGraph *g, int topk, int numThread, uint32_t numVertex): shutdown(true)
  {
    pg = g;
    k = topk;
    this->nThread = numThread;
    //nThread = 4;//Test
    this->personSize = g->personSize;
    if (numVertex != 0 && numVertex < this->personSize)
      nVertex = numVertex;
    else nVertex = this->personSize;

    this->queue = (uint32_t*)malloc(sizeof(uint32_t)*this->personSize*(this->nThread + 1));//Allocate n+1 queues
    this->visited = (bool*)malloc(sizeof(bool)*this->personSize*2);
#if !USE_ATOMIC
    this->visited2 = this->visited + this->personSize;
#endif
    this->cc = (CC*)malloc(sizeof(CC)*this->personSize);
  }

  ~ParaBFS()
  {
    free(this->queue);
    free(this->visited);
    free(this->cc);
  }

  void traverse(TraverseParams *tp)
  {
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
      for (int i=this->pg->edgeIndex[u]; i<this->pg->edgeIndex[u+1]; i++)
      {
        uint32_t v = this->pg->adjacencyList[i];
        {
            if (!this->visited[v]) //This command would reduces executing atomic operations 
            {
#if USE_ATOMIC
              bool b = __sync_fetch_and_or(visited+v, true);
              // for GCC >= 4.7, use the below instead
              // bool b = __atomic_fetch_or(visited+v, true, __ATOMIC_RELAXED);
              if (!b)
              {
                nqueue[nq++] = v;
                sp += level;
                rp += 1;
              }
#else
              this->visited[v] = true;
              this->visited2[v] = true;
              nqueue[nq++] = v;
#endif
            }
        }
      }
    }
    tp->set(nq, sp, rp);
  }

  void waitForWorkers() {
    boost::mutex::scoped_lock lock(this->mutex);
    if (this->workerBusy>0)
      this->condDone.wait(lock);
  }

  void worker(TraverseParams *param) {
    while (1) {
      {
        boost::mutex::scoped_lock lock(this->mutex);
        if (--this->workerBusy==0)
          this->condDone.notify_all();
        this->condWork.wait(lock);
      }
      if (this->shutdown) break;
      this->traverse(param);
    }
  }
  
  void stop() {
    this->shutdown = true;
    this->condWork.notify_all();
  }
  
  void dispatchToWorkers()
  {
    this->workerBusy = this->nThread;
    this->condWork.notify_all();
  }

  void start(std::vector<TraverseParams> &params) {
    if (!this->shutdown)
      return;
    this->shutdown = false;
    this->workerBusy = this->nThread;
    for (int i=0; i<this->nThread; i++) {
      this->workers.create_thread(boost::bind(&ParaBFS::worker, this, &params[i]));
    }
    this->waitForWorkers();
  }

  void run(uint32_t s)
  {
    memset(this->visited, 0, this->personSize*sizeof(bool));
    this->visited[s] = 1;
    uint32_t * cqueue = &queue[this->nThread*(this->personSize)];
    cqueue[0] = s; //Initialize current queue
    uint32_t cq = 1; //size of the current-queue
    int level = 0;
    for(int i=0; i<this->nThread; i++) {
      params[i].sp = 0;
      params[i].rp = 0;
      params[i].level = 0;
    }
    while(cq > 0) {
      for(int i=0; i<nThread; i++)
        params[i].cq_end = cq;
      this->dispatchToWorkers();
      this->waitForWorkers();
      cq = 0;
      for(int i=0; i<this->nThread; ++i)  {
#if USE_ATOMIC
        memcpy(cqueue+cq, params[i].nqueue, sizeof(uint32_t)*params[i].nq);
        cq += params[i].nq;
#else
        uint32_t *nQ = params[i].nqueue;
        for (int j=0; j<params[i].nq; ++j) {
          if (this->visited2[nQ[j]]) {
            this->visited2[nQ[j]] = false;
            cqueue[cq++] = nQ[j];
            params[i].sp += params[i].level;
            ++params[i].rp;
          }
        }
#endif
        params[i].nq = 0;
      }
    }
    uint64_t accum_sp=0, accum_rp=0;
    for(int i=0; i<this->nThread; i++)  {
      accum_sp += params[i].sp;
      accum_rp += params[i].rp;
    }
    if (accum_sp)
      cc[s] = std::make_pair(s, (accum_rp*accum_rp)/(double)(accum_sp*(this->personSize-1)));
  }

  struct CompareCC
  {
    bool operator()(const CC &lhs, const CC &rhs) const 
    {
      if (lhs.second == rhs.second)
        return (lhs.first < rhs.first);
      return (lhs.second > rhs.second);
    }
  };

  void perform()
  {
    uint32_t* cqueue = &queue[this->nThread*(this->personSize)];
    for (int i=0; i<nThread; ++i)
      params.push_back(TraverseParams(cqueue, queue+i*(this->personSize), i, this->nThread));
    this->start(params);
    for(int n=0; n<this->nVertex; n++)
      run(n);
    this->stop();

    std::partial_sort(cc, cc+k, cc+this->personSize, CompareCC());
    for(int i=0; i<k; i++)
      printf("%d|", cc[i].first);
    printf("\n");
  }
};
