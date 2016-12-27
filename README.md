# spark-examples

Note:  This is a research project and not intended for production.

This project demonstrates the following:
* SparkPi - Basic "hello world" job using Apache SparkPi example
* WordCount - Basic word count example demonstrating file IO
* Process - Example of basic processing in a map function.
* CsvMerge - Example of taking 2 CSV files and merging them on the appId field.

We then take this a step further to research Pentaho capabilities by wiring up Steps/Hops (creating a DAG graph) to perform a data transformation.
* Transformation1 - Uses Spark IO and basic Spark Joins (Spark SQL library)
* Transformation2 - Uses a DataGrid step which is built at runtime in the Spark library

## Projects
* spark-application
  * Application that will run on cluster (submitted by spark-runner tests)
* spark-engine
  * Library conceptualizes steps/hops for Spark
* spark-runner
  * Wrapper for the SparkLauncher that is tailored for running tests locally
  
## Building & Running
This project has a dependency on the Apache Spark Launcher.  Unzip it to the location of your choice and set the ```SPARK_HOME``` environment variable.

Note:  For simplicity purposes these tests run locally.
 