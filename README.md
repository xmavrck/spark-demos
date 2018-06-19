# spark-demos
These are some basic demos for spark 

# Enviroment
- Scala 2.11.8
- Spark 2.3.0

# Installation
- Go to command line or terminal and run below command to build the executable jar
sbt assembly

- And then Just run this command to execute your spark job
$SPARK_HOME/spark-submit --class com.xenonstack.examples.spark.SparkWordCountDemo <-your-executable-jar-path->
