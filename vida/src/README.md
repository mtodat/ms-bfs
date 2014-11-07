# Compile:
    ./compile.sh

# Usage:
    ./query x y z t
        x: path to input file (ldbci1k.txt, ldbc10k.txt, ldbc100k.txt, etc)
        y: number of threads
        z: Number of runs
        t: type of bfs:
            naive: naive BFS
            para: parallel BFS, single socket implementation from “Scalable Graph Exploration on Multicore Processors, Agarwal et. al., SC'10"
        w: number of sources (if not provided, run for all sources). Not applicable for msbfs.

# Example: 
    ./query data/ldbc10k.txt 8 3 para 50000
    ./query data/ldbc10k.txt 8 3 para
    ./query data/ldbc10k.txt 8 3 naive 50000
    ./query data/ldbc10k.txt 8 3 msbfs
