




# DISGD
A distributed shared-nothing variant of the incremental stochastic gradient descent algorithm.DISGD is built on top of [Flink](https://flink.apache.org/) based on its [Streaming API](https://ci.apache.org/projects/flink/flink-docs-stable/dev/datastream_api.html).
# HOW TO RUN #

- Import the maven project into your IDE
- You can unpack the data (csv files) in the data folder to folder on your computer.The model accepts any dataset following format <user,item,rate,timeStamp> like [Movielens](https://grouplens.org/datasets/movielens/) or[Netflix](https://www.kaggle.com/netflix-inc/netflix-prize-data)
- Some basic preprocessing needed:
  - Order the dataset chronologically according to TimeStamp
  - Filter out any records with rating less than 5 as the model based on positive feedback
