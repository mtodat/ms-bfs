//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#pragma once

#include "base.hpp"

#include <cstdint>
#include <array>
#include <vector>

using namespace std;

namespace Query4 {

   struct BatchStatistics {
      #ifdef STATISTICS
      size_t step;

      uint32_t ABC[512]; //How many of each?
      std::array<std::vector<uint32_t>,512> EFG; //In which step?
      // std::vector<std::vector<uint32_t>> numTouchedPersonInDistance; //Person -> Distance -> Number
      uint32_t maxStatisticsDistance = 10; //Max distance for statistics
      #endif

      #ifdef OVER_TIME_STATISTICS
      std::vector<std::vector<uint32_t>> iterationUsages;
      std::vector<uint32_t> curIterationUse;
      #endif

      BatchStatistics()
      #ifdef STATISTICS
       : step(0)
      #endif
      { }

      #ifdef STATISTICS
      void inline log(PersonId/* person*/, uint32_t/* nextDistance */, size_t count) {
         // (numTouchedPersonInDistance[person])[nextDistance-1]++;



         #ifdef OVER_TIME_STATISTICS
         curIterationUse.push_back(count);
         #endif

         ABC[count-1]++;
         step++;

         cout<<step<<" "<<count<<"\n";

         #ifdef FULL_STATISTICS
         EFG[count-1].push_back(step);
         #endif
      }
      #else
      void inline log(PersonId, uint32_t, size_t) {
      }
      #endif

      void inline finishBatch() {
         #ifdef STATISTICS
         #ifdef OVER_TIME_STATISTICS
         iterationUsages.push_back(move(curIterationUse));
         curIterationUse.clear();
         #endif
         step=0;
         #endif
      }

      void print() {

         #ifdef STATISTICS
         #ifdef OVER_TIME_STATISTICS
         cout<<"Average batch usages:\n";
         for(std::vector<uint32_t>& b : iterationUsages) {
            uint64_t sum=0;
            for(uint32_t i : b) {
               sum+=i;
            }
            cout<<sum/b.size()<<"\n";
         }

         cout<<"Detailed iteratation usage:\n";
         for(std::vector<uint32_t>& b : iterationUsages) {
            for(uint32_t i : b) {
               cout<<i<<"\n";
            }
         }
         #endif

         cout<<"ABC:"<<endl;
         for(int a=0;a<512;a++) {
            cout<<"  "<<a+1<<": "<<ABC[a]<<"\n";
         }

         // cout<<endl<<"Distances:"<<endl;
         // for(uint32_t p=1; p<state->subgraph.size(); p++) {
         //    for(int a=0; a<maxStatisticsDistance; a++) {
         //       cout<<(numTouchedPersonInDistance[p])[a]<<" "<<p<<" "<<a<<"\n";
         //    }
         // }
         #endif

         // #ifdef FULL_STATISTICS
         // cout<<endl<<"EFG:"<<endl;
         // const uint32_t step=100;
         // const uint32_t num=7000;
         // for(int a=0;a<256;a++) {
         //    sort(EFG[a].begin(), EFG[a].end());

         //    auto iter=EFG[a].cbegin();
         //    for(uint32_t b=0; b<num; b++) {
         //       int n=0;
         //       while(iter!=EFG[a].cend() && *iter / step == b) {
         //          iter++;
         //          n++;
         //       }
         //       cout<<a<<" "<<b*step<<" "<<log(n)<<endl;
         //    }
         //    int n=0;
         //    while(iter!=EFG[a].cend()) {
         //       iter++;
         //       n++;
         //    }
         //    cout<<a<<" "<<step*num+step<<" "<<log(n)<<endl;
         // }
         // #endif
      }
   };
}