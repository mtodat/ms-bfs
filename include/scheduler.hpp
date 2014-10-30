//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#pragma once

#include <vector>
#include <queue>
#include <assert.h>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <thread>

class Task {
   // XXX: Everything should be const
   private:
   void (*functionPtr)(void*);
   void* arguments;

   public:
   unsigned groupId;

   Task(void (*functionPtr)(void*),void* arguments, unsigned groupId=999)
      : functionPtr(functionPtr),arguments(arguments),groupId(groupId) {
   }

   ~Task() {
   }

   void execute() {
      (*functionPtr)(arguments);
   }
};

struct Priorities {
   enum Priority {
      LOW=10,
      DEFAULT=11,
      NORMAL=30,
      URGENT=50,
      CRITICAL=70,
      HYPER_CRITICAL=80
   };
};

struct TaskOrder {
   Priorities::Priority priority;
   unsigned insertion;

   TaskOrder(const Priorities::Priority priority,const unsigned insertion)
      : priority(priority), insertion(insertion) { }
};

typedef std::pair<TaskOrder,Task*> OrderedTask;

struct TaskOrderCmp {
   bool operator() (OrderedTask const &a, OrderedTask const &b) {
      // XXX: insertion order should be the other way round but leads to performance regression
      return a.first.priority < b.first.priority || ((a.first.priority == b.first.priority) && a.first.insertion > b.first.insertion);
   }
};

// Priority ordered scheduler
class Scheduler {
   std::priority_queue<OrderedTask, std::vector<OrderedTask>, TaskOrderCmp> ioTasks;
   std::priority_queue<OrderedTask, std::vector<OrderedTask>, TaskOrderCmp> workTasks;
   std::mutex taskMutex;
   std::condition_variable taskCondition;
   std::mutex threadsMutex;
   std::condition_variable threadsCondition;
   std::atomic<uint32_t> numThreads;
   volatile bool closeOnEmpty;
   volatile bool currentlyEmpty;
   volatile unsigned nextTaskId;

public:
   Scheduler();
   ~Scheduler();

   Scheduler(const Scheduler&) = delete;
   Scheduler(Scheduler&&) = delete;

   void schedule(const std::vector<Task>& funcs, Priorities::Priority priority=Priorities::DEFAULT, bool isIO=true);
   void schedule(const Task& task, Priorities::Priority priority=Priorities::DEFAULT, bool isIO=true);
   Task* getTask(bool preferIO=true);
   size_t size();
   void setCloseOnEmpty();
   void registerThread();
   void unregisterThread();
   void waitAllFinished();
};

// Simple executor that will run the tasks until no more exist
struct Executor {
   const bool preferIO;
   const uint32_t coreId;
   Scheduler& scheduler;

   Executor(Scheduler& scheduler, uint32_t coreId, bool preferIO) 
      : preferIO(preferIO), coreId(coreId), scheduler(scheduler) {
   }

   void run();

   static void* start(void* argument);
};

class TaskGroup {
   std::vector<Task> tasks;

public:
   void schedule(Task task);
   void join(Task joinTask);
   std::vector<Task> close();
};

/// Utility class to make conversion from lambda to task easier
class LambdaRunner {
   std::function<void()> fn;

   LambdaRunner(std::function<void()>&& fn);
public:
   static Task createLambdaTask(std::function<void()>&& fn);
   static void* run(LambdaRunner* runner);
};
