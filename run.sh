#!/bin/bash

bin/cleanUp.py
bin/runMaxSingleDest.py
bin/cleanUp.py

# usage: bin/runAllOnce.py group_count process_per_group destination_count client_per_destination duration warmup enable_debug enablee_profiling
bin/runAllOnce.py 1 3 1 1 2 1 False False

bin/runMaxMultiDest.py
