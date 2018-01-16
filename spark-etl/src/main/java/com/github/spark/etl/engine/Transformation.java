package com.github.spark.etl.engine;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class Transformation {

  private static final Logger LOG = LoggerFactory.getLogger( Transformation.class );

  private DirectedGraph<IStep, Hop> graph;

  public Transformation() {
    graph = new DefaultDirectedGraph<>( Hop.class );
  }

  public void addStep( IStep step ) {
    graph.addVertex( step );
  }

  public void addHop(IStep from, IStep to) {
    graph.addEdge( from, to );
  }

  public Set<Hop> incomingStepsOf( IStep step ) {
    return graph.incomingEdgesOf( step );
  }

  public Set<Hop> outgoingStepsOf( IStep step ) {
    return graph.outgoingEdgesOf( step );
  }

  public DirectedGraph<IStep,Hop> getGraph() {
    return graph;
  }
}
