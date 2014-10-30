This is the experimental framework to evaluate the MS-BFS agorithm and its related work.
Following are the instructions to compile and run the source code:

* Compile:
./compile

* Usage:
./runBencher [nRun] [nThreads] [BFSType] [bWidth] (nSources) (force)
  - nRun:     Number of execution
  - nThreads: Number of threads
  - BFSType:  Type of BFSs (naive, scbfs, noqueue, parabfs, 16, 32, 64, 128, 256)
               where the numbers refer to MS-BFS using the defined datatype, e.g.
               128 executes MS-BFS with SSE registers
  - bWidth:   Number of registers that are used per vertex for MS-BFS, e.g. 4 with
               the 128 BFSType runs 512 concurrent BFSs
  - nSources: (optional) Number of sources
  - force:    (optional) Set 'f' if you want to force it to execute even for a low
              number of sources
Note: Discard the last two arguments if you want to run BFSs for all sources.

* Example:
./runBencher test_queries/ldbc10k.txt 3 8 naive 1 20 f
./runBencher test_queries/ldbc10k.txt 1 32 256 2   (only works when compiled for architecture core-avx2)
