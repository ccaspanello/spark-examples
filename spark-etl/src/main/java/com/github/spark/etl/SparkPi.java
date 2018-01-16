package com.github.spark.etl;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ccaspanello on 12/29/17.
 */
public class SparkPi implements Serializable {

  private SparkContext sc;

  public SparkPi( SparkContext sc ) {
    this.sc = sc;
  }

  public double run(int slices){
    JavaSparkContext jsc = new JavaSparkContext( sc );
    int n = 100000 * slices;
    List<Integer> l = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      l.add(i);
    }

    JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

    int count = dataSet.map( (Function<Integer, Integer>) integer -> {
      double x = Math.random() * 2 - 1;
      double y = Math.random() * 2 - 1;
      return (x * x + y * y < 1) ? 1 : 0;
    } ).reduce( (Function2<Integer, Integer, Integer>) ( integer, integer2 ) -> integer + integer2 );
    return  4.0 * count / n;
  }
}
