package com.github.ccaspanello.spark.engine.step;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Set;

/**
 * Created by ccaspanello on 12/19/2016.
 */
public abstract class BaseStep<E extends IStepMeta> implements IStep {

    private SparkSession sparkSession;
    private E meta;
    private Set<IStep> incoming;
    private Set<IStep> outgoing;
    private Dataset<Row> data;

    public BaseStep(E meta){
        this.meta = meta;
    }

    //<editor-fold desc="Getters & Setters">
    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public void setSparkSession(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public E getStepMeta(){
        return meta;
    }


    public Set<IStep> getIncoming() {
        return incoming;
    }

    @Override
    public void setIncoming(Set<IStep> incoming) {
        this.incoming = incoming;
    }

    public Set<IStep> getOutgoing() {
        return outgoing;
    }

    @Override
    public void setOutgoing(Set<IStep> outgoing) {
        this.outgoing = outgoing;
    }

    public Dataset<Row> getData() {
        return data;
    }

    public void setData(Dataset<Row> data) {
        this.data = data;
    }
    //</editor-fold>
}
