package com.github.ccaspanello.spark.application.examples;

import com.github.ccaspanello.spark.application.step.Hop;
import com.github.ccaspanello.spark.application.step.csvinput.CsvInputMeta;
import com.github.ccaspanello.spark.application.step.csvinput.CsvInputStep;
import com.github.ccaspanello.spark.application.step.csvoutput.CsvOutputMeta;
import com.github.ccaspanello.spark.application.step.csvoutput.CsvOutputStep;
import com.github.ccaspanello.spark.application.step.IStep;
import com.github.ccaspanello.spark.application.step.lookup.LookupMeta;
import com.github.ccaspanello.spark.application.step.lookup.LookupStep;
import org.apache.spark.sql.SparkSession;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Transformation Example
 *
 * Create a Graph with Steps/Hops which defines how a transformation will run.
 *
 * Created by ccaspanello on 12/19/2016.
 */
public class TransformationExample {

    public static Logger LOG = LoggerFactory.getLogger(TransformationExample.class);

    public void execute(SparkSession spark, String sInputUser, String sInputApp, String sOutput) {

        // Create Meta Model (Note this could be deserialized/serialized into XML to be sent acorss the wire)
        CsvInputMeta inputStepMeta1 = new CsvInputMeta("Input User");
        inputStepMeta1.setFilename(sInputUser);

        CsvInputMeta inputStepMeta2 = new CsvInputMeta("Input App");
        inputStepMeta2.setFilename(sInputApp);

        LookupMeta lookupMeta = new LookupMeta("Lookup");
        lookupMeta.setLeftStep(inputStepMeta1.getName());
        lookupMeta.setRightStep(inputStepMeta2.getName());
        lookupMeta.setField("appId");

        CsvOutputMeta outputStepMeta = new CsvOutputMeta("Output");
        outputStepMeta.setFilename(sOutput);

        // Create Steps out of the Step Meta Models
        CsvInputStep csvInputStep1 = new CsvInputStep(inputStepMeta1);
        CsvInputStep csvInputStep2 = new CsvInputStep(inputStepMeta2);
        LookupStep lookupStep = new LookupStep(lookupMeta);
        CsvOutputStep csvOutputStep = new CsvOutputStep(outputStepMeta);

        // Wire Steps into a DAG
        DirectedGraph<IStep, Hop> graph = new DefaultDirectedGraph<>(Hop.class);

        // Steps
        graph.addVertex(csvInputStep1);
        graph.addVertex(csvInputStep2);
        graph.addVertex(lookupStep);
        graph.addVertex(csvOutputStep);

        // Hops
        graph.addEdge(csvInputStep1, lookupStep);
        graph.addEdge(csvInputStep2, lookupStep);
        graph.addEdge(lookupStep, csvOutputStep);

        LOG.warn("STEP ORDER");
        LOG.warn("=============================");
        List<IStep> executionPlan = new ArrayList<>();
        TopologicalOrderIterator<IStep, Hop> orderIterator = new TopologicalOrderIterator<>(graph);
        while (orderIterator.hasNext()) {
            IStep step = orderIterator.next();
            LOG.warn("Step -> {}", step.getStepMeta().getName());
            Set<Hop> incoming = graph.incomingEdgesOf(step);
            Set<Hop> outgoing = graph.outgoingEdgesOf(step);

            LOG.warn("   - Incoming: {}", incoming.size());
            LOG.warn("   - Outgoing: {}", outgoing.size());

            Set<IStep> incomingSteps = new HashSet<>();
            for(Hop hop : incoming){
                incomingSteps.add(hop.incomingSteps());
            }

            Set<IStep> outgoingSteps = new HashSet<>();
            for(Hop hop : outgoing){
                outgoingSteps.add(hop.outgoingSteps());
            }

            incomingSteps.stream().forEach(s -> LOG.warn("  -> Incoming: {}", s.getStepMeta().getName()));
            outgoingSteps.stream().forEach(s -> LOG.warn("  -> Outgoing: {}", s.getStepMeta().getName()));

            step.setIncoming(incomingSteps);
            step.setOutgoing(outgoingSteps);

            executionPlan.add(step);
        }

        LOG.warn("RUNNING STEPS");
        LOG.warn("=============================");
        for(IStep step : executionPlan){
            LOG.warn("***** -> {}", step.getStepMeta().getName());
            step.setSparkSession(spark);
            step.execute();
        }
    }
}
