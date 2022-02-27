## Task 1 Instructions
Run the following command to execute the task such that 
input_file represents the input file path of dataset and output_file
represents the output file path or location of where you would like output
to be produced at. Note that the input and output file paths should be in hdfs 
in the following format: hdfs://{master node private IP}:9000/{location in hdfs}

The first command is for running program on small dataset while
second command is for running on the large dataset

<b> ./small_run.sh input_file output_file </b>

Example: ./small_run.sh hdfs://10.10.1.1:9000/web-BerkStan.txt hdfs://10.10.1.1:9000/Task1_small_dataset

<b> ./large_run.sh input_file output_file </b>

Example: ./large_run.sh hdfs://10.10.1.1:9000/enwiki-pages-articles/ hdfs://10.10.1.1:9000/Task1_large_dataset