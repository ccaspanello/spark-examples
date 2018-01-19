package com.github.spark.etl;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by ccaspanello on 1/16/18.
 */
public class FixedWidthFunction implements MapPartitionsFunction<Row, WeatherData> {

  @Override
  public Iterator<WeatherData> call( Iterator<Row> iterator ) throws Exception {
    List<WeatherData> result = new ArrayList<>(  );
    while(iterator.hasNext()){
      WeatherData weatherData = WeatherData.createFromString( iterator.next().getString( 0 ) );
      result.add( weatherData );
    }
    return result.iterator();
  }
}