//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#include "person_graph.hpp"
#include <cstring>

struct NaiveBFS
{
  typedef std::pair<uint32_t, double> CC;
  PersonGraph    *pg;
  uint32_t       personSize;
  int            k;
  uint32_t* queue;
  bool* visited;
  CC* cc;//closeness centrality
  int nThread;
  uint32_t nVertex;
  int* diameter;

  NaiveBFS(PersonGraph *g, int topk, int numThread, uint32_t numVertex)
  {
    pg = g;
    k = topk;
    nThread = numThread;
    this->personSize = g->personSize;
    if (numVertex != 0 && numVertex < this->personSize)
      nVertex = numVertex;
    else nVertex = this->personSize;
      
    queue = (uint32_t*)malloc(sizeof(uint32_t)*this->personSize*this->nThread);
    visited = (bool*)malloc(sizeof(bool)*this->personSize*this->nThread);
    cc = (CC*)malloc(sizeof(CC)*this->personSize);
    diameter = (int*)malloc(sizeof(int)*this->personSize);
    memset(diameter, 0, this->personSize);
  }
  
  void run(uint32_t s, bool *t_visited, uint32_t *t_queue)
  {
    memset(t_visited, 0, this->personSize);
    t_visited[s] = 1;
    uint64_t sp = 0;
    uint64_t rp = 0;

    uint32_t cq = 0; //index to the current queue
    t_queue[cq] = s;
    uint32_t nq = 1; //index to the next queue
    uint32_t nq_base = 1;
    int level = 0;
    while(cq < nq)
    {
      level++;
      while(cq < nq_base)//Parallize here for each level
      {//Enqueue all nodes in this level
        uint32_t u = t_queue[cq++];
        for (int i=this->pg->edgeIndex[u]; i<this->pg->edgeIndex[u+1]; i++)
        {//For each v adjacent to u
          uint32_t v = this->pg->adjacencyList[i];
          if (t_visited[v] == 0)
          {
            t_queue[nq++] = v;
            t_visited[v] = 1;
            sp += level;
            rp += 1;
          }
        }
      }
      nq_base = nq;
    }
    cc[s] = std::make_pair(s, (rp*rp)/(double)(sp*(this->personSize-1)));
    //if (diameter < level)
    //  diameter = level;
    diameter[s] = level;
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

  void call(uint32_t start, uint32_t source, int stride)
  {
   bool *t_visited = visited + start*this->personSize;
   uint32_t *t_queue = queue + start*this->personSize;
   for(int s=start; s<source; s+=stride)
      run(s, t_visited, t_queue);
  }

  void perform()
  {
    boost::thread_group workers;
    for(int i=0; i<this->nThread; i++)
      workers.create_thread(boost::bind(&NaiveBFS::call, this, i, this->nVertex, this->nThread));
//    for(int n=0; n<this->personSize; n++)
//      run(n);
    workers.join_all();
    std::partial_sort(cc, cc+k, cc+this->personSize, CompareCC());
    for(int i=0; i<k; i++)
      printf("%d|", cc[i].first);
    printf("\n");
    int max_diameter = 0;
    for(int i=0; i<this->personSize; i++)
      if (max_diameter < diameter[i])
        max_diameter = diameter[i];
    printf ("Diameter = %d\n", max_diameter);
  }
};
