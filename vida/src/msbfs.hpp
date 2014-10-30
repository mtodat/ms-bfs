//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#include "person_graph.hpp"
#include <deque>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>

typedef uint64_t BitMask;

struct Query4
{
  typedef std::pair<uint32_t, uint32_t> IdPair;
  typedef std::vector<uint32_t>         U32Vector;
  typedef std::vector<IdPair> IdPairVector;
  typedef boost::unordered_map<IdPair, uint32_t> IdPairMap;
  typedef boost::unordered_map<uint64_t, uint32_t> U64Map;
  typedef std::pair<double, uint64_t> TopK;
  typedef std::vector<TopK> TopKVector;

  PersonGraph     *pg;
  uint32_t         personSize;
  U32Vector        QR;
  uint32_t        *R;
  uint32_t        *D;
  uint32_t        *E;
  
  boost::mutex     pMutex;
  uint32_t         pCurrent;
  uint32_t         pMax;

  const char    * dataPath;
  void          * data;
  int k;//TOP K

  void perform()
  {
    std::string result = this->compute(k);
    printf("Result: %s\n", result.c_str());
  }

  Query4(PersonGraph *G, int topk): data(0)
  {
    k = topk;
    this->pg = G;
    this->personSize = this->pg->personSize;
    this->QR.resize(3*this->personSize+1 + this->pg->adjacencyList.size() + 8*this->personSize);
    this->R = &this->QR[0];
    this->D = this->R + personSize;
    this->E = this->D + personSize;
  }

  struct topk_comparator {
    bool operator()(const TopK &lhs, const TopK &rhs) const {
      if (lhs.first == rhs.first)
        return (lhs.second > rhs.second);
      return (lhs.first < rhs.first);
    }
  };
  typedef std::set<TopK, Query4::topk_comparator> TopKSet;

  template<typename IdType>
  void createAdjacencyList(uint32_t vertexCount, uint32_t *vMap, BitSet &personMap, uint32_t *E, uint32_t *A)
  {
    uint32_t edgeCount = 0;
    IdType *edges = (IdType*)(A);
    for (int nu=0; nu<vertexCount; nu++) {
      uint32_t u = this->D[nu];
      E[nu] = edgeCount;
      for (int k=this->pg->edgeIndex[u]; k<this->pg->edgeIndex[u+1]; k++) {
        uint32_t v = this->pg->adjacencyList[k];
        if (personMap[v]) {
          edges[edgeCount++] = vMap[v];
        }
      }
    }
    E[vertexCount] = edgeCount;
  }

  std::string compute(int k)
  {
    BitSet personMap(this->personSize);
    uint32_t n = this->personSize;
    if (n==0) return "";
    //for (int i=0; i<n; i++)
    //  personMap[i] = 1;
    personMap.set();

    this->D     = this->R + n;
    this->E     = this->D + n;
    uint32_t *A = this->E + n+1;
    uint32_t *Q = A + this->pg->adjacencyList.size();
    
    bool use16bit = n<65536;
    {
      for (int u=0,cnt=0; u<this->personSize; u++) {
        if (personMap[u]) {
          this->D[cnt] = u;
          Q[u] = cnt++;
        }
      }
    }
    if (use16bit)
      this->createAdjacencyList<uint16_t>(n, Q, personMap, E, A);
    else 
      this->createAdjacencyList<uint32_t>(n, Q, personMap, E, A);
    Q = A + E[n];
 
    IdPairVector rank(n);
    {
      personMap.resize(n);
      personMap.reset();
      for (int i=0; i<n; i++) {
        if (!personMap[i]) {
          if (use16bit)
            this->compute_r_p(i, personMap, n, E, (uint16_t*)A, Q);
          else
            this->compute_r_p(i, personMap, n, E, A, Q);
        }
        uint32_t deg = E[i+1]-E[i];
        rank[i] = IdPair(~deg, i);
      }
      hsort::sort(rank.begin(), rank.end());
    }
    uint32_t topPos = k + (n-k) % NBITS;
    double top_min = -1;
    const int nthread = 8;
    TopKSet pc[nthread];
    boost::thread_group workers;
    for (int i=0; i<nthread; i++)
      if (use16bit)
        workers.create_thread(boost::bind(&Query4::call<uint16_t>, this, IdPair(i, topPos), nthread, top_min,
                                          IdPair(n, k), boost::cref(rank), (uint16_t*)A, Q + i*n, boost::ref(pc[i])));
      else
        workers.create_thread(boost::bind(&Query4::call<uint32_t>, this, IdPair(i, topPos), nthread, top_min,
                                          IdPair(n, k), boost::cref(rank), A, Q + i*n, boost::ref(pc[i])));
    workers.join_all();
 
    for (int i=0; i<nthread; i++)
      if (pc[i].size()>0 && (top_min<0 || top_min>pc[i].begin()->first))
        top_min = pc[i].begin()->first;
    
    {
      if (use16bit)
        this->closeness_centrality(rank[topPos+8].second, top_min, n, this->E, (uint16_t*)A, Q);
      else
        this->closeness_centrality(rank[topPos+8].second, top_min, n, this->E, A, Q);
    }

    this->pCurrent = topPos;
    this->pMax = n;
    for (int i=0; i<nthread; i++)
      if (use16bit)
        workers.create_thread(boost::bind(&Query4::call64<uint16_t>, this, nthread, top_min,
                                          IdPair(n, k), boost::cref(rank), (uint16_t*)A, boost::ref(pc[i])));
      else
        workers.create_thread(boost::bind(&Query4::call64<uint32_t>, this, nthread, top_min,
                                          IdPair(n, k), boost::cref(rank), A, boost::ref(pc[i])));
    workers.join_all();

    TopKVector personCentrality;
    for (int i=0; i<nthread; i++)
      personCentrality.insert(personCentrality.end(), pc[i].begin(), pc[i].end());

    std::sort(personCentrality.begin(), personCentrality.end(), topk_comparator());
      
    std::string results = "";
    char result[64];
    for (TopKVector::reverse_iterator it=personCentrality.rbegin(); k>0 && it!=personCentrality.rend(); it++, --k) {
      sprintf(result, "%llu ", it->second);
      results += result;
    }

    if (!results.empty())
      results.resize(results.size()-1);
    return results;
  }

  
#define EXPAND(u) {                                     \
    if (!curF[u]) continue;                             \
    register BitMask bm = curF[u] & active;             \
    curF[u] = 0;                                        \
    if (!bm) continue;                                  \
    for(int k=this->E[u],e=this->E[u+1]; k<e; k++) {    \
      register uint32_t v = adjacencyList[k];           \
      register BitMask nextV = bm & (~VV[v]);           \
      if (nextV) {                                      \
        nextF[v] |= nextV;                              \
        VV[v] |= nextV;                                 \
        do {                                            \
          --r_p[__builtin_ctzl(nextV)];                 \
        } while (nextV &= (nextV-1));                   \
      }                                                 \
    }                                                   \
  }

#define UPDATE() {                                      \
    for (BitMask a=active; a; a&=(a-1)) {               \
      register int j = __builtin_ctzl(a);               \
      register BitMask nshift = ~(1UL << j);            \
      if (r_p[j]==0)                                    \
        active &= nshift;                               \
      else {                                            \
        s_p[j] += r_p[j];                               \
        if (s_p[j]>s_p_max[j]) {                        \
          s_p[j] = 0;                                   \
          active &= nshift;                             \
        }                                               \
      }                                                 \
    }                                                   \
  }

  template<typename IdType>
  void call64(int stride, double current_min, IdPair nk, const IdPairVector &rank, IdType *adjacencyList, TopKSet &personCentrality)
  {
    BitMask *F = (BitMask*)malloc(3*nk.first*sizeof(BitMask));
    BitMask *FF[2] = {F, F + nk.first};
    BitMask *VV    = F + 2*nk.first;

    uint32_t i=0;
    while (1) {
      {
        this->pMutex.lock();
        i = this->pCurrent;
        this->pCurrent += NBITS;
        this->pMutex.unlock();
      }
      if (i>=this->pMax) break;

      // initialize
      memset(F, 0, 3*nk.first*sizeof(BitMask));
      IdType   r_p[NBITS];
      uint32_t s_p[NBITS];
      uint32_t s_p_max[NBITS];
      double value[NBITS];
      BitMask *curF=FF[0], *nextF=FF[0];
      BitMask active = -1;
      for (BitMask j=0,shift=1; j<NBITS; j++, shift<<=1) {
        uint32_t p = rank.at(i+j).second;
        r_p[j] = this->R[p]-1;
        value[j] = (nk.first<2)?0:(double)r_p[j]*r_p[j]/(double)(nk.first-1);
        s_p[j] = r_p[j];
        s_p_max[j] = current_min==0?INT_MAX:value[j]/current_min;
        if (r_p[j]<2) {
          active &= ~shift;
          s_p[j] = 0;
        }
        VV[p] |= shift;
        nextF[p] |= shift;
      }

      // performing bfs for hop=1
      for (int hop=1; hop<2 && active; hop++, curF=nextF) {
        nextF = FF[hop%2];
        for (uint32_t j=0; j<NBITS; j++) {
          uint32_t u = rank.at(i+j).second;
          EXPAND(u);
        }
        UPDATE();
      }

      // performing bfs for the rest
      for (int hop=2; active; hop++, curF=nextF) {
        nextF = FF[hop%2];
        for (uint32_t u=0; u<nk.first; u++)
          EXPAND(u);
        UPDATE();
      }

      // finalize
      for (uint32_t j=0; j<NBITS; j++) {
        uint32_t p = rank.at(i+j).second;
        double cc = s_p[j]?(value[j]/s_p[j]):0;
        if (cc>=current_min) {
//          personCentrality.insert(std::make_pair(cc, SharedData::instance()->person->denormalized(this->D[p])));
          personCentrality.insert(std::make_pair(cc, this->D[p]));
          if (personCentrality.size()>nk.second) {
            personCentrality.erase(personCentrality.begin());
            current_min = personCentrality.begin()->first;
          }
        }
      }
    }
    free(F);
  }

  template<typename IdType>
  void compute_r_p(uint32_t pid, BitSet &personMap, uint32_t n, uint32_t *edgeIndex, IdType *adjacencyList, uint32_t *Q)
  {
    int front=0, count=0;
    personMap[pid] = 1;
    Q[count++] = pid;
    while (front<count) {
      uint32_t u = Q[front++];
      for(int k=edgeIndex[u]; k<edgeIndex[u+1]; k++) {
        uint32_t id = adjacencyList[k];
        if (!personMap[id]) {
          personMap[id] = 1;
          Q[count++] = id;
        }
      }
    }
    for (int i=0; i<count; i++)
      this->R[Q[i]] = count;
  }  

  template<typename IdType>
  double closeness_centrality(uint32_t pid, double kmin, uint32_t n, uint32_t *edgeIndex, IdType *adjacencyList, uint32_t *Q)
  {
    uint32_t r_p = this->R[pid];
    if (r_p==1) return 0.0;
    uint32_t s_p = 0;
    double value = (n<1)?0:(double)(r_p-1)*(r_p-1)/(double)(n-1);
    uint32_t s_p_max = kmin==0?INT_MAX:value/kmin;    
    int front=0, count=0;
    BitSet V(n);
    V[pid] = 1;
    Q[count++] = pid;
    for (int hop=1; front<count; hop++) {
      if (s_p+(r_p-count)*hop>s_p_max)
        return 0.0;
      for (int end=count; front<end; ++front) {
        uint32_t u = Q[front];
        for(int k=edgeIndex[u]; k<edgeIndex[u+1]; k++) {
          uint32_t id = adjacencyList[k];
          if (!V[id]) {
            V[id] = 1;
            Q[count++] = id;
          }
        }
      }
      s_p += hop*(count-front);
    }
    if (s_p==0)
      return 0.0;
    return value/s_p;
  }

  template<typename IdType>
  void call(IdPair range, int stride, double current_min, IdPair nk, const IdPairVector &rank, IdType *adjacencyList, uint32_t *Q, TopKSet &personCentrality)
  {
    for (uint32_t i=range.first; i<range.second; i+=stride) {
      uint32_t p = rank.at(i).second;
      double cc = this->closeness_centrality(p, current_min, nk.first, this->E, adjacencyList, Q);
      if (cc>=current_min) {
        //personCentrality.insert(std::make_pair(cc, SharedData::instance()->person->denormalized(this->D[p])));
        personCentrality.insert(std::make_pair(cc, this->D[p]));
        if (personCentrality.size()>nk.second) {
          personCentrality.erase(personCentrality.begin());
          current_min = personCentrality.begin()->first;
        }
      }
    }
  }
};
