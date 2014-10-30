//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#pragma once

#include "scheduler.hpp"

struct Workers {

   Scheduler scheduler;
   std::vector<std::thread> threads;

   Workers(uint32_t numWorkers);

   void assist(Scheduler& scheduler);
   void close();
};
