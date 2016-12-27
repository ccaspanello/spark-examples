package com.github.ccaspanello.spark.application.examples;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Word Count Example from Apache Documents
 *
 * Created by ccaspanello on 12/15/2016.
 */
public class WordCount implements Serializable {

    public void execute(SparkSession session, String input, String output) {

        JavaSparkContext context = new JavaSparkContext(session.sparkContext());

        JavaRDD<String> textFile = context.textFile(input);

        JavaRDD<String> words = textFile.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> pairs = words.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((Function2<Integer, Integer, Integer>) (a, b) -> a + b);

        counts.saveAsTextFile(output);
    }
}
