package com.github.spark.etl;

import com.github.spark.etl.engine.Hop;
import com.github.spark.etl.engine.IStep;
import com.github.spark.etl.engine.Transformation;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by ccaspanello on 1/15/18.
 */
public class TransformationRunner {

  private static final Logger LOG = LoggerFactory.getLogger( TransformationRunner.class );
  private SparkContext sc;

  public TransformationRunner( SparkContext sc ) {
    this.sc = sc;
  }

  public void execute( Transformation transformation ) {

    SparkSession sparkSession = new SparkSession( sc );
    LOG.warn( "STEP ORDER" );
    LOG.warn( "=============================" );
    List<IStep> executionPlan = new ArrayList<>();
    TopologicalOrderIterator<IStep, Hop> orderIterator = new TopologicalOrderIterator<>( transformation.getGraph() );
    while ( orderIterator.hasNext() ) {
      IStep step = orderIterator.next();
      LOG.warn( "Step -> {}", step.getStepMeta().getName() );
      Set<Hop> incoming = transformation.incomingStepsOf( step );
      Set<Hop> outgoing = transformation.outgoingStepsOf( step );

      LOG.warn( "   - Incoming: {}", incoming.size() );
      LOG.warn( "   - Outgoing: {}", outgoing.size() );

      Set<IStep> incomingSteps = new HashSet<>();
      for ( Hop hop : incoming ) {
        incomingSteps.add( hop.incomingSteps() );
      }

      Set<IStep> outgoingSteps = new HashSet<>();
      for ( Hop hop : outgoing ) {
        outgoingSteps.add( hop.outgoingSteps() );
      }

      incomingSteps.stream().forEach( s -> LOG.warn( "  -> Incoming: {}", s.getStepMeta().getName() ) );
      outgoingSteps.stream().forEach( s -> LOG.warn( "  -> Outgoing: {}", s.getStepMeta().getName() ) );

      step.setIncoming( incomingSteps );
      step.setOutgoing( outgoingSteps );

      executionPlan.add( step );
    }

    LOG.warn( "RUNNING STEPS" );
    LOG.warn( "=============================" );
    for ( IStep step : executionPlan ) {
      LOG.warn( "***** -> {}", step.getStepMeta().getName() );
      step.setSparkSession( sparkSession );
      step.execute();
    }
  }


}
