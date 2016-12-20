package com.github.ccaspanello.spark.application.examples;

import com.github.ccaspanello.spark.application.io.IOHandler;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Word Count Example from Apache Documents
 *
 * Created by ccaspanello on 12/15/2016.
 */
public class WordCount implements Serializable {

    private IOHandler ioHandler;

    public void execute(JavaSparkContext context, String input, String output) {

        ioHandler = new IOHandler(context.hadoopConfiguration());
        ioHandler.deleteFolder(output);

        JavaRDD<String> textFile = context.textFile(input);

        JavaRDD<String> words = textFile.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> pairs = words.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((Function2<Integer, Integer, Integer>) (a, b) -> a + b);

        counts.saveAsTextFile(output);
    }
}
