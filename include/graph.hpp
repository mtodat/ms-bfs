//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#pragma once

#include "log.hpp"
#include "queue.hpp"

#include <cstdint>
#include <cstddef>
#include <cassert>
#include <utility>
#include <vector>
#include <string>
#include <limits>
#include <unordered_map>

struct NodePair {
   uint64_t idA;
   uint64_t idB;

   NodePair() { }
   NodePair(uint64_t idA, uint64_t idB)
      : idA(idA), idB(idB)
   { }
};

struct GraphData {
   size_t numNodes;
   std::vector<NodePair> edges;
   std::unordered_map<uint64_t,uint64_t> revNodeRenaming;

   GraphData(const size_t numNodes, std::vector<NodePair> edges, std::unordered_map<uint64_t,uint64_t> revNodeRenaming)
      : numNodes(numNodes), edges(move(edges)), revNodeRenaming(std::move(revNodeRenaming)) {
   }

   GraphData(GraphData& other) = delete;
   GraphData(GraphData&& other) = default;

   static GraphData loadFromPath(const std::string& edgesFile);
};

template<class EntryType>
class SizedList {
public:
   typedef EntryType Size;
   typedef EntryType Entry;

private:
   Size count;

public:
   SizedList() {
   }

   /// Reads the size for the list at the pointer position
   inline const Size& size() const {
      return count;
   }

   inline Size& size() {
      return count;
   }

   /// Sets the size for the list at the pointer position
   void setSize(const Size count) {
      this->count = count;
   }

   const Entry* getPtr(const size_t index) const __attribute__ ((pure)) {
      const auto offsetPtr = reinterpret_cast<const uint8_t*>(this) + sizeof(Size) + sizeof(Entry)*index;
      return reinterpret_cast<const Entry*>(offsetPtr);
   }

   Entry* getPtr(const size_t index) __attribute__ ((pure)) {
      const auto offsetPtr = reinterpret_cast<uint8_t*>(this) + sizeof(Size) + sizeof(Entry)*index;
      return reinterpret_cast<Entry*>(offsetPtr);
   }

   std::pair<const Entry*,const Entry* const> bounds() const __attribute__ ((pure)) {
      const Entry* i=getPtr(0);
      return std::make_pair(i,i+count);
   }

   SizedList<Entry>* nextList(Size count) __attribute__ ((pure)) {
      auto offsetPtr = reinterpret_cast<uint8_t*>(this)+sizeof(Size)+sizeof(Entry)*count;
      return reinterpret_cast<SizedList<Entry>*>(offsetPtr);
   }
};

template<class IdType>
class Graph {
public:
   typedef IdType Id;
   typedef SizedList<Id>* Content;

   typedef uint32_t ComponentId;
   typedef uint64_t ComponentSize;

   const size_t numVertices;
   size_t numEdges;

   std::vector<ComponentId> personComponents;
   std::vector<ComponentSize> componentSizes;
   std::vector<ComponentSize> componentEdgeCount;
   ComponentSize maxComponentSize;

private:
   Content* table;

   std::unordered_map<uint64_t,uint64_t> revNodeRenaming;

public:
   uint8_t* data;

   Graph(size_t numVertices) : numVertices(numVertices), personComponents(numVertices), componentSizes(), componentEdgeCount(), maxComponentSize(), table(nullptr), data(nullptr) {
      // XXX: Maybe posix_memalign helps?
      table = new Content[numVertices]();
   }

   Graph(Graph& other) = delete;

   Graph(Graph&& other) : numVertices(other.numVertices), numEdges(other.numEdges), personComponents(other.personComponents), componentSizes(other.componentSizes), componentEdgeCount(other.componentEdgeCount), maxComponentSize(other.maxComponentSize), table(other.table), revNodeRenaming(std::move(other.revNodeRenaming)), data(other.data) {
      other.table=nullptr;
      other.data=nullptr;
   }

   ~Graph() {
      if(table) {
         delete[] table;
         table = nullptr;
      }

      if(data) {
         delete[] data;
         data = nullptr;
      }
   }

   IdType mapInternalNodeId(IdType id) const {
      const auto iter = revNodeRenaming.find(id);
      if(iter!=revNodeRenaming.cend()) {
         return iter->second;
      } else {
         throw -1;
      }
   }

   /// Inserts the data for the specified id into the index
   void insert(Id id, Content content) {
      assert(table!=nullptr);
      assert(id<numVertices);
      table[id] = std::move(content);
   }

   /// Retrieves the data for the specified id
   __attribute__ ((pure)) Content retrieve(Id id) const {
      assert(id<numVertices);
      assert(table[id]!=nullptr);
      return table[id];
   }

   inline IdType maxKey() const {
      return numVertices-1;
   }

   inline IdType size() const {
      return numVertices;
   }

   static Graph loadFromPath(const std::string& edgesFile) {
      GraphData graphData = GraphData::loadFromPath(edgesFile);
      IdType numPersons = graphData.numNodes;
      std::vector<NodePair>& edges = graphData.edges;

      // Build graph
      Graph personGraph(numPersons);
      personGraph.revNodeRenaming = std::move(graphData.revNodeRenaming);
      const size_t dataSize = (numPersons+edges.size())*sizeof(IdType);
      uint8_t* data = new uint8_t[dataSize]();
      {
         SizedList<IdType>* neighbours = reinterpret_cast<SizedList<IdType>*>(data);
         size_t ix=0;
         for(IdType person=0; person<numPersons; person++) {
            assert(reinterpret_cast<uint8_t*>(neighbours)<data+dataSize);
            IdType* insertPtr = neighbours->getPtr(0);
            IdType count=0;
            while(ix<edges.size() && edges[ix].idA==person) {
               *insertPtr = edges[ix].idB;
               insertPtr++;
               count++;
               ix++;
            }

            neighbours->setSize(count);
            personGraph.insert(person, neighbours);
            neighbours = neighbours->nextList(count);
         }
      }

      personGraph.data = data;
      personGraph.numEdges = edges.size();

      LOG_PRINT("[LOADING] Created person graph of size: "<< dataSize/1024<<" kb");

      #ifdef DEBUG
      uint64_t retrievableFriends = 0;
      for(IdType person=0; person<numPersons; person++) {
         retrievableFriends += personGraph.retrieve(person)->size();
      }
      assert(retrievableFriends==edges.size());
      #endif

      personGraph.analyzeGraph();

      return std::move(personGraph);      
   }

private:
   void analyzeGraph() {
      const auto graphSize = size();

      componentSizes.push_back(std::numeric_limits<ComponentSize>::max()); // Component 0 is invalid
      componentEdgeCount.push_back(std::numeric_limits<ComponentSize>::max()); // Component 0 is invalid

      // Identify connected components by running bfs
      awfy::FixedSizeQueue<IdType> toVisit = awfy::FixedSizeQueue<IdType>(graphSize);

      size_t trivialComponents=0;
      ComponentId componentId=1;
      for(IdType node=0; node<graphSize; node++) {
         if(personComponents[node]!=0) { continue; }

         ComponentSize componentSize=1;
         personComponents[node]=componentId;
         toVisit.push_back_pos()=node;

         uint64_t componentNeighborCount=0;

         // Do BFS for one connected component
         do {
            const IdType curNode = toVisit.front();
            toVisit.pop_front();

            const auto curNeighbours=retrieve(curNode);
            componentNeighborCount += curNeighbours->size();

            auto neighbourBounds = curNeighbours->bounds();
            while(neighbourBounds.first != neighbourBounds.second) {
               const IdType curNeighbour=*neighbourBounds.first;
               ++neighbourBounds.first;
               if (personComponents[curNeighbour]!=0) { continue; }
               personComponents[curNeighbour]=componentId;
               componentSize++;
               toVisit.push_back_pos() = curNeighbour;
            }
         } while(!toVisit.empty());

         componentEdgeCount.push_back(componentNeighborCount);
         componentSizes.push_back(componentSize);
         if(componentSize>maxComponentSize) {
            maxComponentSize = componentSize;
         }
         if(componentSize<5) {
            trivialComponents++;
         } else {
            std::cout<<"# C "<<componentSize<<std::endl;
         }

         componentId++;

         // Check for overflow
         assert(componentId>0);
      }

      LOG_PRINT("[Query4] Max component size "<< maxComponentSize);
      std::cout<<"# Found number components "<< componentId-1<<" ("<<trivialComponents<<" are of size < 5)."<<std::endl;
   }
};