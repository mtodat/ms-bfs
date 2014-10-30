//Copyright (C) 2014 by Manuel Then, Moritz Kaufmann, Fernando Chirigati, Tuan-Anh Hoang-Vu, Kien Pham, Alfons Kemper, Huy T. Vo
//
//Code must not be used, distributed, without written consent by the authors
#include <boost/thread.hpp>
#include "person_graph.hpp"
#include "msbfs.hpp"
#include "parabfs.hpp"
#include "naive.hpp"
#define DEBUG 0


int main(int argc, char**argv)
{
  const char *queryFile  = argv[1];
  int topk = 7;
  if (argc < 4)
  {
    printf("Wrong arguments!\n");
    return 0;
  }

  int nThread = atoi(argv[2]);
  int nRun = atoi(argv[3]);
  uint32_t nVertex = 0;
  if (argc == 6) 
    nVertex = atoi(argv[5]);

  if (strcmp(argv[4], "para") == 0)
  {
    PersonGraph * pg = new PersonGraph(queryFile);
    ParaBFS *parabfs = new ParaBFS(pg, topk, nThread, nVertex);
    for(int i=0; i<nRun; i++)
    {
      boost::timer::auto_cpu_timer t;
      parabfs->perform();
    }
  }

  if (strcmp(argv[4], "msbfs") == 0)
  {
    PersonGraph * pg = new PersonGraph(queryFile);
    //Query4 *query4 = new Query4(pg, topk, nVertex);
    Query4 *query4 = new Query4(pg, topk);
    for(int i=0; i<nRun; i++)
    {
      boost::timer::auto_cpu_timer t;
      query4->perform();
    }
  }

  if (strcmp(argv[4], "naive") == 0)
  {
    PersonGraph * pg = new PersonGraph(queryFile);
    NaiveBFS *naive = new NaiveBFS(pg, topk, nThread, nVertex);
    for(int i=0; i<nRun; i++)
    {
      boost::timer::auto_cpu_timer t;
      naive->perform();
    }
  }

  return 0; 
}
