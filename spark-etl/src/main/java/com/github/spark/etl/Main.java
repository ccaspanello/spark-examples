package com.github.spark.etl;

import org.apache.spark.SparkContext;

/**
 * Created by ccaspanello on 1/16/18.
 */
public class Main {

  public static void main(String[] args){
    SparkContext sparkContext = SparkContext.getOrCreate();
    SparkPi sparkPi = new SparkPi(sparkContext );
    sparkPi.run( 10 );
  }

}
