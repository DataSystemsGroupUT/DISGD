




# DISGD
A distributed shared-nothing variant of the incremental stochastic gradient descent algorithm.DISGD is built on top of [Flink](https://flink.apache.org/) based on its [Streaming API](https://ci.apache.org/projects/flink/flink-docs-stable/dev/datastream_api.html).

# IDEA
- DISGD is a scalalble version form [ISGD](https://link.springer.com/chapter/10.1007/978-3-319-08786-3_41) and Morever it accomplish the goal to run in distributed environment it outperforms the recall accuracy of ISGD with respect to the replication factor. 
- DISGD applies splitting and repitlication mechanism that starts with choosing the ni parameter which is the replication factor of how many the item vector should be existed over nodes and based on it nc(number of nodes in the clutser) is caluclated nc=(n^2)i+w.ni where w∈N0.

 ![Test Image 1](DISGD.png | width=10)



# HOW TO RUN #
- Import the maven project into your IDE
- You can unpack the data (csv files) in the data folder to folder on your computer.The model accepts any dataset following format <user,item,rate,timeStamp> like [Movielens](https://grouplens.org/datasets/movielens/) or[Netflix](https://www.kaggle.com/netflix-inc/netflix-prize-data)
- Some basic preprocessing needed:
  - Order the dataset chronologically according to TimeStamp
  - Filter out any records with rating less than 5 as the model based on positive feedback
- You can pass the parameters below with the run command
  - --input "path to input file" --output "path to output file" --ni "The replication factor" --records "how many records from the dataset"
    * input: Path to the input file that should have timestamps as long value
    * Output: Path to the output file where the recall output is written
    * ni: The replication factor and based on it nc(number of nodes or parallelism factor) is calculated
    * records: An optional parameter if you want to end the job after certain records from the dataset
