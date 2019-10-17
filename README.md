# DISGD
A distributed shared-nothing variant of the incremental stochastic gradient descent algorithm.DISGD is built on top of [Flink](https://flink.apache.org/) based on its [Streaming API](https://ci.apache.org/projects/flink/flink-docs-stable/dev/datastream_api.html).
# HOW TO RUN
Import the maven project into your IDE.

You can unpack the data (csv files) in the data folder to folder on your computer.The model accepts any dataset following format <user,item,rate,timeStamp> like [Movielens](https://grouplens.org/datasets/movielens/) or[Netflix](https://www.kaggle.com/netflix-inc/netflix-prize-data). The datasets needs basic preprocessing 
*Order the records chronologically accorditing to the timestamp
*Filter out any records with ratings less than five as the model deal with positive feedback.
      

You can check the example run in ~\src\main\java\ee\ut\cs\dsg\adaptivewatermark\flink\StreamingJob

You can pass a command line arguments as

--input "path to input file" --output "path to output file" --adaptive true --allowedLateness 100 --oooThreshold 1 --sensitivity 1 --sensitivityChangeRate 1.0 --windowWidth 1000 --period 10 parameters:

Input: Path to the input file that should have timestamps as long value,
Output: Path to the output file that shall contain the window start and end and the number of elements assigned to the window,
Adaptive: if set to true will run the adaptive watermark generator. Otherwise, if will run the baseline periodic watermark generator,
Allowed lateness (m): This parameter is relevant to the periodic watermark generator. It is the number of milliseconds by which the watermark is delayed behind the max timestamp seen,
Out-of-order threshold (l): This parameter is used for the adaptive watermark generator. It controls the percentage of out of order (late) records with respect to the total number of records in a chunk. By default it is set to 1.0 meaning no restriction on out-of-order records,
Sensitivity (delta): This parameter is used to control how sensitive ADWIN is to the change in data arrival rate. Default value is 1 which means most sensitive,
Sensitivity change rate: the rate by which the sensitivity parameter is decreased/increased in case of late arrivals go above the threshold (l),
Window width: In milliseconds, controls the width of the tumbling time window in the pipeline. Default is 1000 milliseconds,
Period (s): In milliseconds controls the periodic watermark assigner interval between watermark generations.

