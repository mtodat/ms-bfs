//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#ifndef PERSON_GRAPH_HPP
#define PERSON_GRAPH_HPP

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/dynamic_bitset.hpp>
#include <boost/thread.hpp>
#include <boost/timer/timer.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include "hsort.hpp"

#define NBITS 64
typedef boost::dynamic_bitset<uint64_t> BitSet;

struct PersonGraph
{
  typedef std::pair<uint32_t, uint32_t> IdPair;
  typedef std::vector<IdPair> IdPairVector;
  
  std::string graphFile; //full path to file that contains edge list
  std::string refResult;
  const char  * queryFile;
  size_t        personSize;

  std::vector<uint32_t> edgeIndex;
  std::vector<uint32_t> adjacencyList;
  std::vector<uint32_t> adjacencyWeight;
//  BitSet personMap;

  PersonGraph(const char *queryFile)
  {
    if (queryFile) {
      this->readInputFile(queryFile);
      this->readPersonKnowsPerson(this->graphFile);
    }
  }

  void readInputFile(const char *queryFile)
  {
    std::string f = std::string(queryFile);
    boost::iostreams::mapped_file_source file(f);
    const char *cur = file.data();
    const char *end = cur + file.size();
    
    size_t pSize;
    for (pSize=0; *cur>='0' && *cur<='9'; cur++)
      pSize = pSize*10 + (*cur-'0');
    this->personSize = pSize;
    //personMap.resize(pSize);
    //for (int i=0; i<pSize; i++)
    //  personMap[i] = 0;
    while(*cur==' ') {
       cur++;
    }
    
    size_t length = 0;
    const char* start = cur;
    while(*cur != ' ') {
       length++;
       cur++;
    }
    this->graphFile = std::string(start, length);

    while(*cur==' ') {
       cur++;
    }
 
    length = 0;
    start = cur;
    while(cur!=end && *cur != ' ') {
       length++;
       cur++;
    }
    this->refResult = std::string(start, length);
  } 
 
  void readPersonKnowsPerson(std::string graphfile)
  {
    IdPairVector adjacencyPair;
    {
      boost::iostreams::mapped_file_source file(graphFile);
      const char *cur = file.data();
      const char *end = cur + file.size();
      while (*cur!='\n') cur++;
      IdPair ip;
      while (++cur<end) {
        uint64_t p;
        for (p=0; *cur>='0' && *cur<='9'; cur++)
          p = p*10 + (*cur-'0');
        ip.first = p;
      //  personMap[p] = 1;
        for (p=0,cur++; *cur>='0' && *cur<='9'; cur++)
          p = p*10 + (*cur-'0');
        ip.second = p;     
      //  personMap[p] = 1;
        while (*cur!='\n') cur++;        
        adjacencyPair.push_back(ip);
      }
    }
    this->adjacencyList.resize(adjacencyPair.size());
    this->adjacencyWeight.resize(adjacencyPair.size());

    {
      hsort::sort(adjacencyPair.begin(), adjacencyPair.end());

      this->edgeIndex.clear();
      this->edgeIndex.resize(this->personSize+1, 0);

      unsigned last = 0;
      for (unsigned i=0; i<adjacencyPair.size(); i++) {
        if (i==adjacencyPair.size()-1 || adjacencyPair[i].first!=adjacencyPair[i+1].first) {
          this->edgeIndex[adjacencyPair[i].first+1] = i+1-last;
          last = i+1;
        }
        this->adjacencyList[i] = adjacencyPair[i].second;
        this->adjacencyWeight[i] = 0;
      }
      for (unsigned i=0; i<this->personSize; i++)
        this->edgeIndex[i+1] += this->edgeIndex[i];
    }
  }
};

#endif
