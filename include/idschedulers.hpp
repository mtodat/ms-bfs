//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#pragma once

#include "bfs/base.hpp"

#include <cstdint>
#include <vector>
#include <random>
#include <algorithm>
#include <queue>

struct RandomNodeOrdering {
   static void order(std::vector<Query4::PersonId>& ids, size_t /*batchSize*/, const Query4::PersonSubgraph& /*subgraph*/) {
      // Deterministic shuffeling
      std::random_device rd;
      std::mt19937 g(rd());
      g.seed(1987);
      std::shuffle(ids.begin(), ids.end(), g);     
   }
};

struct ComponentOrdering {
   static void order(std::vector<Query4::PersonId>& ids, size_t maxBfs, size_t /*batchSize*/, const Query4::PersonSubgraph& subgraph) {
      if(maxBfs>ids.size()) {
         maxBfs=ids.size();
      }
      // Sort by degree
      std::sort(ids.begin(), ids.begin()+maxBfs, [&subgraph](const Query4::PersonId a, const Query4::PersonId b) {
         return subgraph.componentSizes[subgraph.personComponents[a]]>subgraph.componentSizes[subgraph.personComponents[b]] || (subgraph.componentSizes[subgraph.personComponents[a]]==subgraph.componentSizes[subgraph.personComponents[b]] && subgraph.personComponents[a]<subgraph.personComponents[b]);
      });
   }
};

struct DegreeOrdering {
   static void order(std::vector<Query4::PersonId>& ids, size_t maxBfs, size_t /*batchSize*/, const Query4::PersonSubgraph& subgraph) {
      if(maxBfs>ids.size()) {
         maxBfs=ids.size();
      }
      // Sort by degree
      std::sort(ids.begin(), ids.begin()+maxBfs, [&subgraph](const Query4::PersonId a, const Query4::PersonId b) {
         return subgraph.personComponents[a]<subgraph.personComponents[b] || (subgraph.personComponents[a]==subgraph.personComponents[b] && subgraph.retrieve(a)->size() > subgraph.retrieve(b)->size());
      });
   }
};

struct TwoHopDegreeOrdering {
   static uint32_t countTwoHopNeighbors(Query4::PersonId p, const Query4::PersonSubgraph& subgraph) {
      uint32_t n = 0;
      auto bounds = subgraph.retrieve(p)->bounds();
      while(bounds.first != bounds.second) {
         n += subgraph.retrieve(*bounds.first)->size();
         bounds.first++;
      }
      return n;
   }

   static void order(std::vector<Query4::PersonId>& ids, size_t maxBfs, size_t /*batchSize*/, const Query4::PersonSubgraph& subgraph) {
      if(maxBfs>ids.size()) {
         maxBfs=ids.size();
      }
      // Sort by degree
      std::sort(ids.begin(), ids.begin()+maxBfs, [&subgraph](const Query4::PersonId a, const Query4::PersonId b) {
         return subgraph.personComponents[a]<subgraph.personComponents[b] || (subgraph.personComponents[a]==subgraph.personComponents[b] && countTwoHopNeighbors(a,subgraph) > countTwoHopNeighbors(b,subgraph));
      });
   }
};

struct NeighourDegreeOrdering {

   static void order(std::vector<Query4::PersonId>& ids, size_t /*batchSize*/, const Query4::PersonSubgraph& subgraph) {
      // Sort by degree
      std::sort(ids.begin(), ids.end(), [&subgraph](const Query4::PersonId a, const Query4::PersonId b) {
         return subgraph.retrieve(a)->size() > subgraph.retrieve(b)->size();
      });

      std::vector<Query4::PersonId> new_ids(ids.size());
      // Assign rounds by batching neighbours
      std::vector<uint8_t> assigned(subgraph.size());
      uint64_t addedFriends=0;
      size_t currentIx=0;
      for(Query4::Persons i=0; i<subgraph.size(); i++) {
         Query4::PersonId id = ids[i];

         // Skip persons that are already assigned
         if(assigned[id]) { continue; }

         // Add person
         new_ids[currentIx] = id;
         assigned[id]=true;
         currentIx++;

         // Add friends
         auto bounds = subgraph.retrieve(id)->bounds();
         while(bounds.first != bounds.second) {
            if(!assigned[*bounds.first]) {
               new_ids[currentIx] = *bounds.first;
               assigned[*bounds.first] = true;
               addedFriends++;
               currentIx++;
            }
            bounds.first++;
         }
      }

      LOG_PRINT("[Query4] Added friends "<<addedFriends);
      #ifdef DEBUG
      for(size_t i=0; i<subgraph.size(); i++) {
         assert(assigned[i]);
      }
      #endif

      swap(ids, new_ids);
   }
};

struct AdvancedNeighborOrdering {
   static void order(std::vector<Query4::PersonId>& ids, size_t batchSize, const Query4::PersonSubgraph& subgraph) {
      using namespace std;
      using NodeDegreePair = pair<Query4::PersonId, uint32_t>;

      auto degreePairSorter = [](const NodeDegreePair& p1, const NodeDegreePair& p2) -> bool {
         return p1.second>p2.second;
      };

      std::vector<Query4::PersonId> newIds(subgraph.size());
      std::vector<uint8_t> assigned(subgraph.size());

      priority_queue<pair<Query4::PersonId, uint32_t>, vector<NodeDegreePair>, decltype(degreePairSorter)> degreeQueue(degreePairSorter);
      for(Query4::PersonId p=0; p<subgraph.size(); p++) {
         degreeQueue.push(make_pair(p, subgraph.retrieve(p)->size()));
      }

      size_t currentIx=0;
      uint32_t numAssignedPersons=0;
      while(!degreeQueue.empty()) {
         const auto topElement = degreeQueue.top();
         degreeQueue.pop();
         if(assigned[topElement.first]) { continue; }

         uint32_t numUnprocessedFriends = 0;
         auto bounds = subgraph.retrieve(topElement.first)->bounds();
         while(bounds.first!=bounds.second) {
            if(!assigned[*bounds.first]) {
               numUnprocessedFriends++;
            }
            bounds.first++;
         }

         if(numUnprocessedFriends == topElement.second) {
            //We already have the correct number of unprocessed friends for this node
            queue<Query4::PersonId> assignedPersons;
            auto bounds = subgraph.retrieve(topElement.first)->bounds();
            while(bounds.first != bounds.second && numAssignedPersons < batchSize) {
               if(!assigned[*bounds.first]) {
                  assert(currentIx<subgraph.size());
                  newIds[currentIx++] = *bounds.first;
                  assigned[*bounds.first] = true;
                  assignedPersons.push(*bounds.first);
                  numAssignedPersons++;
               }
               bounds.first++;
            }
            if(bounds.first == bounds.second &&  numAssignedPersons < batchSize) {
               //All friends assigned
               assert(!assigned[topElement.first]);
               assert(currentIx<subgraph.size());
               newIds[currentIx++] = topElement.first;
               assigned[topElement.first]=true;
               numAssignedPersons++;
            }
            if(degreeQueue.top().second < batchSize - numAssignedPersons) {
               //Next person by degree has less friends left than we can fit into batch. Continue there
               continue;
            } else {
               while(numAssignedPersons < batchSize && !assignedPersons.empty()) {
                  const auto p = assignedPersons.front();
                  assignedPersons.pop();

                  auto bounds = subgraph.retrieve(p)->bounds();
                  while(bounds.first != bounds.second && numAssignedPersons < batchSize) {
                     if(!assigned[*bounds.first]) {
                        assert(currentIx<subgraph.size());
                        newIds[currentIx++] = *bounds.first;
                        assigned[*bounds.first] = true;
                        assignedPersons.push(*bounds.first);
                        numAssignedPersons++;
                     }
                     bounds.first++;
                  }
               }
               numAssignedPersons = 0;
            }
         } else {
            degreeQueue.push(make_pair(topElement.first, numUnprocessedFriends));
         }
      }

      #ifdef DEBUG
      for(size_t i=0; i<subgraph.size(); i++) {
         assert(assigned[i]);
      }
      #endif

      swap(ids, newIds);
   }
};