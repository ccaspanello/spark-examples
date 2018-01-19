package com.github.spark.etl;

import org.apache.spark.SparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

/**
 * Created by ccaspanello on 1/16/18.
 */
@Guice(modules = GuiceExampleModule.class)
public class WeatherTransformationTest {

  private static final Logger LOG = LoggerFactory.getLogger( WeatherTransformationTest.class );

  @Inject
  private SparkContext sc;

  @Test
  public void test(){
    WeatherTransformation weatherTransformation = new WeatherTransformation( sc );
    weatherTransformation.execute("/Users/ccaspanello/Desktop/722030-12844-2016");

  }


}
