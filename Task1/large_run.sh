#!/bin/bash
# this is for running page ranking algorithm on large dataset
# assumes that the local spark directory is directly under /mydata/
/mydata/spark-3.2.1-bin-hadoop3.2/bin/spark-submit large_dataset_page_rank.py $1 $2
