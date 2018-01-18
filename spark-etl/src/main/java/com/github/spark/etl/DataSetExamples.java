package com.github.spark.etl;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;

public class DataSetExamples implements Serializable {

  private SparkContext sc;
  private SparkSession ss;

  public DataSetExamples( SparkContext sc ) {
    this.sc = sc;
    //  Spark Context shouls set the following see
    //    SparkConf sparkConf = new SparkConf();
    //    sparkConf.set( "spark.eventLog.enabled", "true" );
    //    sparkConf.set( "spark.driver.host", "localhost" );
    //    SparkContext sparkContext = new SparkContext( "local[*]", "SparkTest", sparkConf );
    this.ss = SparkSession.builder()
      .appName("DataSetExamples")
      .getOrCreate();
  }

  public Dataset<Row> createBaseDataSet(String fileName) {
    Dataset<Row> result = this.ss.read()
      .format("com.databricks.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(fileName);
    return result;
  }

}



