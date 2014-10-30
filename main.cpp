//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#include "include/bench.hpp"
#include "include/TraceStats.hpp"

int main(int argc, char** argv) {
    if(argc<2) {
      FATAL_ERROR("Not enough parameters");
   }

   Queries queries = Queries::loadFromFile(std::string(argv[1]));
   const int numRuns = argc==2 ? 1 :  std::stoi(std::string(argv[2]));

   size_t numThreads = std::thread::hardware_concurrency()/2;
   if(argc>3) {
      numThreads = std::stoi(std::string(argv[3]));
   }
   LOG_PRINT("[Main] Using "<< numThreads <<" threads");

   size_t bfsLimit = std::numeric_limits<uint64_t>::max();
   if(argc>4) {
      bfsLimit = std::stoi(std::string(argv[4]));
      LOG_PRINT("[Main] Limit to "<< bfsLimit <<" bfs");
   }

   vector<std::unique_ptr<BFSBenchmark>> benchmarks;
   #ifdef AVX2
   benchmarks.push_back(std::unique_ptr<BFSBenchmark>(new SpecializedBFSBenchmark<Query4::HugeBatchBfs<__m256i,1,true>>("Huge Batch BFS Runner 256 SP    (width 1)")));
   benchmarks.push_back(std::unique_ptr<BFSBenchmark>(new SpecializedBFSBenchmark<Query4::HugeBatchBfs<__m256i,1,false>>("Huge Batch BFS Runner 256 NoSP  (width 1)")));
   benchmarks.push_back(std::unique_ptr<BFSBenchmark>(new SpecializedBFSBenchmark<Query4::HugeBatchBfs<__m256i,2,true>>("Huge Batch BFS Runner 256 SP    (width 2)")));
   benchmarks.push_back(std::unique_ptr<BFSBenchmark>(new SpecializedBFSBenchmark<Query4::HugeBatchBfs<__m256i,2,false>>("Huge Batch BFS Runner 256 NoSp  (width 2)")));
   benchmarks.push_back(std::unique_ptr<BFSBenchmark>(new SpecializedBFSBenchmark<Query4::HugeBatchBfs<__m256i,4,true>>("Huge Batch BFS Runner 256 SP    (width 4)")));
   benchmarks.push_back(std::unique_ptr<BFSBenchmark>(new SpecializedBFSBenchmark<Query4::HugeBatchBfs<__m256i,4,false>>("Huge Batch BFS Runner 256 NoSp  (width 4)")));
   // benchmarks.push_back(std::unique_ptr<BFSBenchmark>(new SpecializedBFSBenchmark<Query4::BatchBFSRunner256>("Batch BFS Runner 256")));
   #endif
   //benchmarks.push_back(std::unique_ptr<BFSBenchmark>(new SpecializedBFSBenchmark<Query4::HugeBatchBfs<__m128i,1>>("Huge Batch BFS Runner 128 (width 1)")));
   //benchmarks.push_back(std::unique_ptr<BFSBenchmark>(new SpecializedBFSBenchmark<Query4::HugeBatchBfs<uint64_t,1>>("Huge Batch BFS Runner 64 (width 1)")));
   benchmarks.push_back(std::unique_ptr<BFSBenchmark>(new SpecializedBFSBenchmark<Query4::HugeBatchBfs<__m128i,4,false>>("Huge Batch BFS Runner 128 (width 4)")));
   // benchmarks.push_back(std::unique_ptr<BFSBenchmark>(new SpecializedBFSBenchmark<Query4::HugeBatchBfs<uint64_t,8>>("Huge Batch BFS Runner 64 (width 8)")));
   //benchmarks.push_back(std::unique_ptr<BFSBenchmark>(new SpecializedBFSBenchmark<Query4::HugeBatchBfs<uint32_t,16>>("Huge Batch BFS Runner 32 (width 16)")));
   //benchmarks.push_back(std::unique_ptr<BFSBenchmark>(new SpecializedBFSBenchmark<Query4::HugeBatchBfs<uint16_t,32>>("Huge Batch BFS Runner 16 (width 32)")));
   //benchmarks.push_back(std::unique_ptr<BFSBenchmark>(new SpecializedBFSBenchmark<Query4::HugeBatchBfs<uint8_t,64>>("Huge Batch BFS Runner 8 (width 64)")));
   //benchmarks.push_back(std::unique_ptr<BFSBenchmark>(new SpecializedBFSBenchmark<Query4::HugeBatchBfs<uint8_t,1>>("Huge Batch BFS Runner 8 (width 1)")));
   //benchmarks.push_back(std::unique_ptr<BFSBenchmark>(new SpecializedBFSBenchmark<Query4::BatchBFSRunner128>("BatchBFSRunner128")));
   //benchmarks.push_back(std::unique_ptr<BFSBenchmark>(new SpecializedBFSBenchmark<Query4::HugeBatchBfs<uint64_t,2>>("Huge Batch BFS Runner 64 (width 2)")));
   //benchmarks.push_back(std::unique_ptr<BFSBenchmark>(new SpecializedBFSBenchmark<Query4::HugeBatchBfs<uint64_t,4>>("Huge Batch BFS Runner 64 (width 4)")));

   // benchmarks.push_back(std::unique_ptr<BFSBenchmark>(new SpecializedBFSBenchmark<Query4::BatchBFSRunner>("BatchBFSRunner64")));
   // benchmarks.push_back(std::unique_ptr<BFSBenchmark>(new SpecializedBFSBenchmark<Query4::NoQueueBFSRunner>("NoQueueBFSRunner")));
   // benchmarks.push_back(std::unique_ptr<BFSBenchmark>(new SpecializedBFSBenchmark<Query4::BFSRunner>("BFSRunner")));

   size_t maxBatchSize=0;
   for(auto& benchmark : benchmarks) {
      if(benchmark->batchSize()>maxBatchSize) {
         maxBatchSize = benchmark->batchSize();
      }
   }
   LOG_PRINT("[Main] Max batch size: "<<maxBatchSize);

   // Allocate additional worker threads
   Workers workers(numThreads-1);

   // Run benchmarks
   uint32_t minAvgRuntime=std::numeric_limits<uint32_t>::max();
   for(unsigned i=0; i<queries.queries.size(); i++) {
      Query query = queries.queries[i];
      LOG_PRINT("[Main] Executing query "<<query.dataset);
      auto personGraph = Graph<Query4::PersonId>::loadFromPath(query.dataset);
      {
         auto ranges = generateTasks(bfsLimit, personGraph.size(), maxBatchSize);
         auto desiredTasks=numThreads*4;
         if(ranges.size()<desiredTasks) {
            FATAL_ERROR("[Main] Not enough tasks! #Threads="<<numThreads<<", #Tasks="<<ranges.size()<<", #DesiredTasks="<<desiredTasks);
         }
      }
      // XXX: Do one warmup run?
      for(const auto& b : benchmarks) {
         cout<<"# Benchmarking "<<b->name<<" ... ";
         cout.flush();
         for(int a=0; a<numRuns; a++) {
            b->initTrace(personGraph.numVertices, personGraph.numEdges, numThreads, bfsLimit, "NotWorkingHere");
            b->run(7, personGraph, query.reference, workers, bfsLimit);
            cout<<b->lastRuntime()<<"ms ";
            cout.flush();
         }

         if(b->avgRuntime()<minAvgRuntime) {
            minAvgRuntime=b->avgRuntime();
            cout<<" new best => "<<minAvgRuntime;
         }

         cout<<" ... avg: "<<b->avgRuntime()<<" rel: "<< b->avgRuntime()/(double)minAvgRuntime<<"x"<<endl;
      }
   }

   workers.close();

   // Print final results
   for(const auto& b : benchmarks) {
      cout<<b->avgRuntime()<<"\t"<<(b->avgRuntime()/(double)minAvgRuntime)<<" # (ms, factor) "<<b->name<<endl;
   }

   return 0;
}