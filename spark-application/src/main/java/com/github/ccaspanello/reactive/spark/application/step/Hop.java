package com.github.ccaspanello.reactive.spark.application.step;

import org.jgrapht.graph.DefaultEdge;

/**
 * Created by ccaspanello on 12/19/2016.
 */
public class Hop extends DefaultEdge {

    public IStep getSourceStep() {
        return (IStep) getSource();
    }

    public IStep getTargetStep() {
        return (IStep) getTarget();
    }
}
