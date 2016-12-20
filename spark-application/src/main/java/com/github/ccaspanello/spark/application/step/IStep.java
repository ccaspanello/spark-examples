package com.github.ccaspanello.spark.application.step;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Set;

/**
 * Step Interface
 *
 * Created by ccaspanello on 12/19/2016.
 */
public interface IStep extends Serializable {

    /**
     * Executes Step Logic
     */
    void execute();

    //<editor-fold desc="Getters & Setters">
    void setSparkSession(SparkSession spark);

    SparkSession getSparkSession();

    IStepMeta getStepMeta();

    void setIncoming(Set<IStep> incoming);

    void setOutgoing(Set<IStep> outgoing);

    Dataset<Row> getData();
    //</editor-fold>
}
