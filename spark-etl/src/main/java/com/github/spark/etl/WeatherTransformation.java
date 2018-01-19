package com.github.spark.etl;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.Beans;
import java.io.Serializable;
import java.util.Arrays;

import static org.apache.spark.sql.functions.col;

/**
 * Created by ccaspanello on 1/16/18.
 */
public class WeatherTransformation implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger( WeatherTransformation.class );
  private SparkContext sc;

  public WeatherTransformation( SparkContext sc ) {
    this.sc = sc;
  }

  public void execute( String inputData ) {

    SparkSession session = new SparkSession( sc );
    session.sparkContext().setJobGroup( "SubTrans", "Desc", false );

    sc.setJobGroup( "Parent", "Parent Transformation", false );
    Dataset<WeatherData> parent = session
      .read()
      .text( inputData )
      .mapPartitions( new FixedWidthFunction(), Encoders.bean( WeatherData.class ) )
      .filter( col( "sourceFlag" ).equalTo( "7" ) )
      .limit( 10 );

    sc.setJobGroup( "Child", "Child Transformation", false );
    Dataset<WeatherData> child = parent.map( new SubTransFunction(), Encoders.bean( WeatherData.class ) );


    sc.setJobGroup( "Parent2", "Parent2 Transformation", false );
    Dataset<WeatherData> parent2 = session.createDataset( child.rdd(), Encoders.bean( WeatherData.class ) );

    for ( WeatherData row : parent2.collectAsList() ) {
      LOG.info( "{}", row.toString() );
    }

    LOG.info( "count: {}", parent2.count() );
  }

  public void executeSubTransformation() {

  }


}
