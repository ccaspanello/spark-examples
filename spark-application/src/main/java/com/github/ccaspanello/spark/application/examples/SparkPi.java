package com.github.ccaspanello.spark.application.examples;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Spark Pi Exampe from Apache Documents
 *
 * Created by ccaspanello on 12/15/2016.
 */
public class SparkPi implements Serializable  {

    public void execute(JavaSparkContext context, int slices) {
        int n = 100000 * slices;
        List<Integer> l = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            l.add(i);
        }

        JavaRDD<Integer> dataSet = context.parallelize(l, slices);

        int count = dataSet.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) {
                double x = Math.random() * 2 - 1;
                double y = Math.random() * 2 - 1;
                return (x * x + y * y <= 1) ? 1 : 0;
            }
        }).reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) {
                return integer + integer2;
            }
        });

        System.out.println("**************************************************");
        System.out.println("Pi is roughly " + 4.0 * count / n);
        System.out.println("**************************************************");
    }
}
