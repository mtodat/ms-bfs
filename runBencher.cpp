//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#include "include/bench.hpp"
#include "include/TraceStats.hpp"

#define GEN_BENCH_BRANCH(X,CTYPE,WIDTH) \
   X(batchType==sizeof(CTYPE)*8&&batchWidth==WIDTH) { \
      bencher = new SpecializedBFSBenchmark<Query4::HugeBatchBfs<CTYPE,WIDTH,false>>("BatchBFS "+std::to_string(sizeof(CTYPE)*8)+" ("+std::to_string(WIDTH)+")"); \
      maxBatchSize = sizeof(CTYPE)*8*WIDTH; \
      bfsType = std::to_string(sizeof(CTYPE)*8)+"_"+std::to_string(WIDTH); \
   } \


int main(int argc, char** argv) {
    if(argc!=6 && argc!=7 && argc!=8) {
      FATAL_ERROR("Not enough parameters");
   }

   Queries queries = Queries::loadFromFile(std::string(argv[1]));
   const int numRuns = std::stoi(std::string(argv[2]));

   //size_t bfsLimit = std::numeric_limits<uint64_t>::max();
   size_t bfsLimit = argc>=7?std::stoi(std::string(argv[6])):std::numeric_limits<uint64_t>::max();
   bool checkNumTasks = argc>=8&&argv[7][0]=='f'?false:true;
   size_t numThreads = std::thread::hardware_concurrency()/2;
   if(argc>3) {
      numThreads = std::stoi(std::string(argv[3]));
   }
   LOG_PRINT("[Main] Using "<< numThreads <<" threads");

   size_t maxBatchSize;
   BFSBenchmark* bencher;
   std::string bfsType;
   Query4::PARABFSRunner::setThreadNum(numThreads);//Using threads inside BFS
   if(std::string(argv[4])=="naive") {
      bencher = new SpecializedBFSBenchmark<Query4::BFSRunner>("BFSRunner");
      maxBatchSize = 1;
      bfsType = "naive";
   } else if(std::string(argv[4])=="noqueue") {
      bencher = new SpecializedBFSBenchmark<Query4::NoQueueBFSRunner>("NoQueueBFSRunner");
      maxBatchSize = 1;
      bfsType = "noqueue";
   } else if(std::string(argv[4])=="scbfs") {
      bencher = new SpecializedBFSBenchmark<Query4::SCBFSRunner>("SCBFSRunner");
      maxBatchSize = 1;
      bfsType = "scbfs";
   } else if(std::string(argv[4])=="parabfs") {
      bencher = new SpecializedBFSBenchmark<Query4::PARABFSRunner>("PARABFSRunner");
      maxBatchSize = 1;
      bfsType = "parabfs";
      numThreads = 1;
   } else {
      const int batchType = std::stoi(std::string(argv[4]));
      const int batchWidth = std::stoi(std::string(argv[5]));
      GEN_BENCH_BRANCH(if,__m128i,8)
      GEN_BENCH_BRANCH(else if,__m128i,4)
      GEN_BENCH_BRANCH(else if,__m128i,1)
      #ifdef AVX2
      GEN_BENCH_BRANCH(else if,__m256i,2)
      GEN_BENCH_BRANCH(else if,__m256i,1)
      #endif
      GEN_BENCH_BRANCH(else if,uint64_t,8)
      GEN_BENCH_BRANCH(else if,uint64_t,1)
      GEN_BENCH_BRANCH(else if,uint32_t,16)
      GEN_BENCH_BRANCH(else if,uint32_t,1)
      GEN_BENCH_BRANCH(else if,uint16_t,32)
      GEN_BENCH_BRANCH(else if,uint16_t,1)
      GEN_BENCH_BRANCH(else if,uint8_t,64)
      GEN_BENCH_BRANCH(else if,uint8_t,1)
      else {
         exit(-1);
      }
   }

   // Allocate additional worker threads
   Workers workers(numThreads-1);

   for(unsigned i=0; i<queries.queries.size(); i++) {
      Query query = queries.queries[i];
      LOG_PRINT("[Main] Executing query "<<query.dataset);
      auto personGraph = Graph<Query4::PersonId>::loadFromPath(query.dataset);
      if(bfsLimit>personGraph.size()) {
         bfsLimit=personGraph.size();
      }
      if(checkNumTasks)
      {
         auto ranges = generateTasks(bfsLimit, personGraph.size(), maxBatchSize);
         auto desiredTasks=numThreads*3;
         if(ranges.size()<desiredTasks) {
            FATAL_ERROR("[Main] Not enough tasks! #Threads="<<numThreads<<", #Tasks="<<ranges.size()<<", #DesiredTasks="<<desiredTasks<<", #maxBatchSize="<<maxBatchSize);
         }
      }

      // Run benchmark
      std::cout<<"# Benchmarking "<<bencher->name<<" ... "<<std::endl<<"# ";
      for(int i=0; i<numRuns; i++) {
         bencher->initTrace(personGraph.numVertices, personGraph.numEdges, numThreads, bfsLimit, bfsType);
         bencher->run(7, personGraph, query.reference, workers, bfsLimit);
         std::cout<<bencher->lastRuntime()<<"ms ";
         std::cout.flush();
      }
      std::cout<<std::endl;

      std::cout<<bencher->getMinTrace()<<std::endl;
   }

   workers.close();

   if(std::string(argv[4])=="parabfs") {
     Query4::PARABFSRunner::finish();
   }
   return 0;
}
