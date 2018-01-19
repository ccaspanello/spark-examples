package com.github.spark.etl;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * Created by ccaspanello on 1/16/18.
 */
public class SubTransFunction implements MapFunction<WeatherData, WeatherData> {

  public SubTransFunction() {
  }

  @Override
  public WeatherData call( WeatherData value ) throws Exception {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set( "spark.eventLog.enabled", "true" );
    sparkConf.set( "spark.driver.host", "localhost" );
    sparkConf.set("spark.driver.allowMultipleContexts", "true");
    SparkContext sparkContext = new SparkContext( "local[*]", "SparkTest", sparkConf );

    SparkSession session = new SparkSession( sparkContext );
    List<WeatherData> list = Arrays.asList( value );
    Dataset<WeatherData> data = session.createDataset( list, Encoders.bean( WeatherData.class ) );
    data.map( (MapFunction<WeatherData, WeatherData>) value1 -> {
      value1.setLongitude( "badLongitude" );
      value1.setLatitude( "badLatitude" );
      return value1;
    }, Encoders.bean( WeatherData.class ) );
    return data.first();
  }
}
