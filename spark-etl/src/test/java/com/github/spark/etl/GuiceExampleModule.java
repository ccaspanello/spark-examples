package com.github.spark.etl;

import com.google.inject.Binder;
import com.google.inject.Module;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

/**
 * Created by ccaspanello on 12/29/17.
 */
public class GuiceExampleModule implements Module {

  @Override
  public void configure( Binder binder ) {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set( "spark.eventLog.enabled", "true" );
    sparkConf.set( "spark.driver.host", "localhost" );
    SparkContext sparkContext = new SparkContext( "local[*]", "SparkTest", sparkConf );

    binder.bind( SparkContext.class ).toInstance( sparkContext );
  }

}

