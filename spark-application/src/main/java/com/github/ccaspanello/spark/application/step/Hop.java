package com.github.ccaspanello.spark.application.step;

import org.jgrapht.graph.DefaultEdge;

/**
 * Hop
 *
 * Wraps the DefaultEdge to define the to/from steps.  This wrapper will cast the source/target to Steps which by
 * default are not accessible.
 *
 * TODO Is there a better way to do this?
 *
 * Created by ccaspanello on 12/19/2016.
 */
public class Hop extends DefaultEdge {

    //<editor-fold desc="Getters & Setters">
    public IStep incomingSteps() {
        return (IStep) getSource();
    }

    public IStep outgoingSteps() {
        return (IStep) getTarget();
    }
    //</editor-fold>
}
