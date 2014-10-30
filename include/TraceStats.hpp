//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#pragma once

#include "log.hpp"
#include <atomic>
#include <array>
#include <cstdint>
#include <iostream>
#include <sstream>
#include <mutex>

template<size_t numBits, size_t maxRounds=20>
class TraceStats {
   size_t numVertices;
   size_t numEdges;
   size_t numTraversedEdges;
   size_t batchSize;
   size_t runnerType;
   size_t typeSize;
   size_t width;
   size_t numThreads;
   size_t maxBfs;
   std::string bfsType;

   std::array<std::atomic<size_t>, maxRounds> numRounds;
   std::array<std::atomic<size_t>, maxRounds> roundDurations;
   std::array<std::array<std::atomic<size_t>, numBits>, maxRounds> roundVisitBits;
   std::array<std::array<std::atomic<size_t>, numBits>, maxRounds> roundFriendBits;

   TraceStats() : numRounds(), roundDurations(), roundVisitBits(), roundFriendBits() {

   }
public:
   static TraceStats& getStats(bool reset=false) {
      static TraceStats* globalStats;
      static std::mutex m;
      std::lock_guard<std::mutex> lock(m);
      if(reset && globalStats != nullptr) {
         globalStats = nullptr;
      }
      if(globalStats == nullptr) {
         globalStats = new TraceStats();
      }
      return *globalStats;
   }

   void init(size_t numVertices, size_t numEdges, size_t batchSize, size_t runnerType, size_t typeSize, size_t width, size_t numThreads, size_t maxBfs, std::string bfsType) {
      this->numVertices = numVertices;
      this->numEdges = numEdges;
      this->numTraversedEdges = 0;
      this->batchSize = batchSize;
      this->runnerType = runnerType;
      this->typeSize = typeSize;
      this->width = width;
      this->numThreads = numThreads;
      this->maxBfs = maxBfs;
      this->bfsType = bfsType;
   }

   void traceRound(size_t round) {
      numRounds[round]++;
   }

   void addRoundDuration(size_t round, size_t duration) {
      roundDurations[round] += duration;
   }

   void addRoundVisitBits(size_t round, size_t visitBits, size_t count) {
      roundVisitBits[round][visitBits]+=count;
   }

   void addRoundFriendBits(size_t round, size_t friendBits, size_t count) {
      roundFriendBits[round][friendBits]+=count;
   }

   void setNumTraversedEdges(size_t c) {
      numTraversedEdges = c;
   }

   std::string prelude(std::string metric, size_t totalDuration) {
      return "["+metric+"]\t"+std::to_string(numVertices)+"\t"+std::to_string(numEdges)+"\t"+std::to_string(numTraversedEdges)+"\t"+std::to_string(batchSize)+"\t"+std::to_string(numThreads)
         +"\t"+std::to_string(runnerType)+"\t"+std::to_string(typeSize)+"\t"+std::to_string(width)+"\t"+std::to_string(totalDuration)+"\t"+std::to_string(maxBfs)+"\t"+bfsType;
   }

   std::string print(size_t totalDuration) {
      size_t lastRound = 0;
      for (int i = 1; i < maxRounds; ++i) {
         if(numRounds[i]>0) { lastRound = i; }
      }

      std::stringstream ss;
      ss<<prelude("TROUNDS",totalDuration)<<"\t"<<lastRound<<std::endl;
      for (int i = 1; i < lastRound; ++i) {
         ss<<prelude("TDUR",totalDuration)<<"\t"<<i<<"\t"<<numRounds[i]<<"\t"<<roundDurations[i]<<" ms"<<std::endl;
      }

      #ifdef TRACE
      for (int i = 1; i < lastRound; ++i) {
         for (int j = 0; j < numBits; ++j) {
            ss<<prelude("TVBITS",totalDuration)<<"\t"<<i<<"\t"<<numRounds[i]<<"\t"<<j<<"\t"<<roundVisitBits[i][j]<<std::endl;
            ss<<prelude("TFBITS",totalDuration)<<"\t"<<i<<"\t"<<numRounds[i]<<"\t"<<j<<"\t"<<roundFriendBits[i][j]<<std::endl;
         }
      }
      #endif

      std::string traceStr = ss.str();
      return traceStr;
   }

   void reset() {
      throw;
   }
};
