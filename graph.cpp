//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#include "include/tokenizer.hpp"
#include "include/graph.hpp"
#include "include/io.hpp"

#include <algorithm>

using namespace std;

GraphData GraphData::loadFromPath(const std::string& edgesFile) {
   // Count number of persons (excluding header line)
   size_t numEdges = io::fileLines(edgesFile)-1;
   LOG_PRINT("[LOADING] Number of edges: "<<numEdges);


   // Load edges data from file
   LOG_PRINT("[LOADING] Loading edges from file: "<< edgesFile);
   io::MmapedFile file(edgesFile, O_RDONLY);

   Tokenizer tokenizer(file.mapping, file.size);
   tokenizer.skipLine(); // Skip header line

   vector<NodePair> edges;
   edges.reserve(numEdges);

   std::unordered_map<uint64_t,uint64_t> nodeRenaming;
   std::unordered_map<uint64_t,uint64_t> revNodeRenaming;
   uint64_t nextNodeId=0;

   auto mapExternalNodeId = [&nodeRenaming, &revNodeRenaming, &nextNodeId](uint64_t id) -> uint64_t {
      if(nodeRenaming.find(id)==nodeRenaming.end()) {
         nodeRenaming[id] = nextNodeId;
         revNodeRenaming[nextNodeId] = id;
         return nextNodeId++;
      } else {
         return nodeRenaming[id];
      }
   };

   while(!tokenizer.isFinished()) {
      assert(edges.size()<numEdges*2);
      NodePair pair;
      pair.idA = tokenizer.readId('|');
      pair.idB = tokenizer.readId('\n');
      if(pair.idA == pair.idB) {
         continue; //No self-edges
      }

      //Add undirected
      edges.push_back(pair);
      edges.push_back(NodePair(pair.idB, pair.idA));
   }

   // Reading edges
   LOG_PRINT("[LOADING] Read edges");
   std::sort(edges.begin(), edges.end(), [](const NodePair& a, const NodePair& b) {
      return a.idA<b.idA||(a.idA==b.idA && a.idB<b.idB);
   });

   //Remove duplicates
   vector<NodePair> uniqueEdges(edges.size());
   size_t e=0;
   NodePair last(numeric_limits<uint64_t>::max(), numeric_limits<uint64_t>::max());
   for(const NodePair& a : edges) {
      if(last.idA!=a.idA || last.idB!=a.idB) {
         last = a;
         uniqueEdges[e++] = NodePair(mapExternalNodeId(a.idA), a.idB);
      }
   }
   uniqueEdges.resize(e);
   for(NodePair& a : uniqueEdges) {
      a.idB = mapExternalNodeId(a.idB);
   }
   LOG_PRINT("[LOADING] Sorted edges");

   LOG_PRINT("[LOADING] Number of nodes: "<<nextNodeId);

   return GraphData(nextNodeId, move(uniqueEdges), move(revNodeRenaming));
}
