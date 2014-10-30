//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#pragma once

#include <iostream>

namespace tschrono {
   typedef uint64_t Time;

   Time now();

   struct TimeFrame {
      Time startTime;
      Time endTime;
      Time duration;

      void start();
      void end();
   };
}

#ifdef DEBUG
   #ifdef NDBGPRINT
      #define LOG_PRINT(X)
   #else
      #define LOG_PRINT(X) std::cerr<<tschrono::now()<<" "<<X<<std::endl
   #endif
#else
   #define LOG_PRINT(X)
#endif

#define FATAL_ERROR(X) std::cerr<<"FATAL: "<<X<<std::endl; throw -1
