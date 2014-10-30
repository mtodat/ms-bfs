//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#include "include/scheduler.hpp"
#include "include/log.hpp"

#include <atomic>
#include <pthread.h>

///--- Scheduler related methods
Scheduler::Scheduler() 
   : ioTasks(), workTasks(), numThreads(0), closeOnEmpty(false), currentlyEmpty(false), nextTaskId(0) {
}

Scheduler::~Scheduler() {
   assert(ioTasks.size()==0);
   assert(workTasks.size()==0);
}

void Scheduler::schedule(const std::vector<Task>& funcs, Priorities::Priority priority, bool isIO) {
   {
      std::lock_guard<std::mutex> lock(taskMutex);
      for (unsigned i = 0; i < funcs.size(); ++i) {
         Task* task = new Task(funcs[i]);

         if(isIO) {
            ioTasks.push(std::make_pair(TaskOrder(priority, nextTaskId++), task));
         } else {
            workTasks.push(std::make_pair(TaskOrder(priority, nextTaskId++), task));
         }
      }

      if(currentlyEmpty) {
         currentlyEmpty=false;
      }
   }

   taskCondition.notify_all();
}

void Scheduler::schedule(const Task& scheduleTask, Priorities::Priority priority, bool isIO) {
   {
      std::lock_guard<std::mutex> lock(taskMutex);
      Task* task = new Task(scheduleTask);

      if(isIO) {
         ioTasks.push(std::make_pair(TaskOrder(priority, nextTaskId++), task));
      } else {
         workTasks.push(std::make_pair(TaskOrder(priority, nextTaskId++), task));
      }

      if(currentlyEmpty) {
         currentlyEmpty=false;
      }
   }

   taskCondition.notify_one();
}

Task* Scheduler::getTask(bool preferIO) {
   std::unique_lock<std::mutex> lck(taskMutex);
   while(true) {
      // Try to acquire task
      if(!ioTasks.empty()||!workTasks.empty()) {
         Task* task;
         if((preferIO && !ioTasks.empty()) || workTasks.empty()) {
            task = ioTasks.top().second;
            ioTasks.pop();
         } else {
            task = workTasks.top().second;
            workTasks.pop();
         }
         assert(task!=nullptr);
         auto numTasks=ioTasks.size()+workTasks.size();
         if(numTasks==0&&!closeOnEmpty) {
            currentlyEmpty=true;
         }
         
         if(numTasks>0) { lck.unlock(); taskCondition.notify_one(); }
         else { lck.unlock(); }
         return task;
      } else {
         // Wait if no task is available
         if(closeOnEmpty) {
            break;
         } else {
            taskCondition.wait(lck);
         }
      }
   }

   return nullptr;
}

void Scheduler::setCloseOnEmpty() {
   closeOnEmpty=true;
   taskCondition.notify_all();
}

size_t Scheduler::size() {
   return ioTasks.size()+workTasks.size();
}

void Scheduler::registerThread() {
   numThreads++;
}

void Scheduler::unregisterThread() {
   numThreads--;
   threadsCondition.notify_all();
}

void Scheduler::waitAllFinished() {
   std::unique_lock<std::mutex> lck(threadsMutex);
   while(true) {
      if(closeOnEmpty && numThreads==0) {
         return;
      } else {
         threadsCondition.wait(lck);
      }
   }
}

// Executor related implementation
void Executor::run() {
   cpu_set_t cpuset;
   CPU_ZERO(&cpuset);
   CPU_SET(coreId, &cpuset);
   pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

   LOG_PRINT("[Executor] Starting");
   scheduler.registerThread();
   while(true) {
      auto task = scheduler.getTask(preferIO);
      if(task==nullptr) { break; }

      task->execute();
      delete task;
   }
   scheduler.unregisterThread();
   LOG_PRINT("[Executor] Stopping");
}

void* Executor::start(void* argument) {
   Executor* executor = static_cast<Executor*>(argument);
   executor->run();

   delete executor;
   return nullptr;
}

// Taskgroup related implementations
void TaskGroup::schedule(Task task) {
   tasks.push_back(task);
}

struct JoinRunner {
   Task fn;
   Task* joinTask;
   std::atomic<size_t>* semaphore;

   JoinRunner(Task fn, Task* joinTask, std::atomic<size_t>* semaphore) : fn(fn), joinTask(joinTask), semaphore(semaphore) {
   }

   static void* run(JoinRunner* runner) {
      runner->fn.execute();
      auto status=runner->semaphore->fetch_add(-1)-1;
      if(status==0) {
         runner->joinTask->execute();
         delete runner->semaphore;
         delete runner->joinTask;
         delete runner;
      }
      return nullptr;
   }
};

void TaskGroup::join(Task joinTask) {
   if(tasks.size()==0) {
      schedule(joinTask);
      return;
   }

   std::vector<Task> wrappedTasks;

   auto joinTaskPtr = new Task(joinTask);
   auto joinCounter = new std::atomic<size_t>(tasks.size());

   // Wrap existing functions to implement the join
   for (unsigned i = 0; i < tasks.size(); ++i) {
      JoinRunner* runner = new JoinRunner(tasks[i], joinTaskPtr, joinCounter);
      Task wrappedFn((void (*)(void*)) &(JoinRunner::run), runner, tasks[i].groupId);
      wrappedTasks.push_back(wrappedFn);
   }
   tasks = std::move(wrappedTasks);

}

std::vector<Task> TaskGroup::close() {
   return std::move(tasks);
}

///--- LambdaRunner related methods
LambdaRunner::LambdaRunner(std::function<void()>&& fn) : fn(std::move(fn)) {
}

Task LambdaRunner::createLambdaTask(std::function<void()>&& fn) {
   LambdaRunner* runner=new LambdaRunner(std::move(fn));
   return Task((void (*)(void*)) &(LambdaRunner::run), runner);
}

void* LambdaRunner::run(LambdaRunner* runner) {
   runner->fn();
   delete runner;
   return nullptr;
}
