//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#include "include/worker.hpp"
#include "include/log.hpp"

Workers::Workers(uint32_t numWorkers) : scheduler() {
   LOG_PRINT("[Workers] Allocating worker pool with "<< numWorkers << " workers.");
   for (unsigned i = 0; i < numWorkers; ++i) {
      Executor* executor = new Executor(scheduler,i+1, false);
      threads.emplace_back(&Executor::start, executor);
   } 
}

void Workers::assist(Scheduler& tasks) {
   for (unsigned i = 0; i < threads.size(); ++i) {
      scheduler.schedule(LambdaRunner::createLambdaTask([&tasks, i] {
         Executor(tasks,i+1, false).run();
      }));
   }
}
void Workers::close() {
   scheduler.setCloseOnEmpty();
   for(auto& thread : threads) {
      thread.join();
   }
}
