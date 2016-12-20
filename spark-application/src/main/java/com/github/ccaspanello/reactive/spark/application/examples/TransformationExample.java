package com.github.ccaspanello.reactive.spark.application.examples;

import com.github.ccaspanello.reactive.spark.application.step.Hop;
import com.github.ccaspanello.reactive.spark.application.step.csvinput.CsvInputMeta;
import com.github.ccaspanello.reactive.spark.application.step.csvinput.CsvInputStep;
import com.github.ccaspanello.reactive.spark.application.step.csvoutput.CsvOutputMeta;
import com.github.ccaspanello.reactive.spark.application.step.csvoutput.CsvOutputStep;
import com.github.ccaspanello.reactive.spark.application.step.IStep;
import com.github.ccaspanello.reactive.spark.application.step.lookup.LookupMeta;
import com.github.ccaspanello.reactive.spark.application.step.lookup.LookupStep;
import org.apache.spark.sql.SparkSession;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.tools.nsc.doc.model.Def;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by ccaspanello on 12/19/2016.
 */
public class TransformationExample {

    public static Logger LOG = LoggerFactory.getLogger(TransformationExample.class);

    public void execute(SparkSession spark, String sInputUser, String sInputApp, String sOutput) {

        CsvInputMeta inputStepMeta1 = new CsvInputMeta("Input User");
        inputStepMeta1.setFilename(sInputUser);

        CsvInputMeta inputStepMeta2 = new CsvInputMeta("Input App");
        inputStepMeta2.setFilename(sInputApp);

        LookupMeta lookupMeta = new LookupMeta("Lookup");
        lookupMeta.setLeftSide(inputStepMeta1.getName());
        lookupMeta.setRightSide(inputStepMeta2.getName());

        CsvOutputMeta outputStepMeta = new CsvOutputMeta("Output");
        outputStepMeta.setFilename(sOutput);

        CsvInputStep csvInputStep1 = new CsvInputStep(inputStepMeta1);
        CsvInputStep csvInputStep2 = new CsvInputStep(inputStepMeta2);
        LookupStep lookupStep = new LookupStep(lookupMeta);
        CsvOutputStep csvOutputStep = new CsvOutputStep(outputStepMeta);

        DirectedGraph<IStep, Hop> g = new DefaultDirectedGraph<>(Hop.class);
        g.addVertex(csvInputStep1);
        g.addVertex(csvInputStep2);
        g.addVertex(lookupStep);
        g.addVertex(csvOutputStep);

        // add edges to create linking structure
        g.addEdge(csvInputStep1, lookupStep);
        g.addEdge(csvInputStep2, lookupStep);
        g.addEdge(lookupStep, csvOutputStep);

        LOG.warn("STEP ORDER");
        LOG.warn("=============================");
        List<IStep> executionPlan = new ArrayList<>();
        TopologicalOrderIterator<IStep, Hop> orderIterator = new TopologicalOrderIterator<>(g);
        while (orderIterator.hasNext()) {
            IStep step = orderIterator.next();
            LOG.warn("Step -> {}", step.getStepMeta().getName());
            Set<Hop> incoming = g.incomingEdgesOf(step);
            Set<Hop> outgoing = g.outgoingEdgesOf(step);

            LOG.warn("   - Incoming: {}", incoming.size());
            LOG.warn("   - Outgoing: {}", outgoing.size());

            Set<IStep> incomingSteps = new HashSet<>();
            for(Hop hop : incoming){
                incomingSteps.add(hop.getSourceStep());
            }

            Set<IStep> outgoingSteps = new HashSet<>();
            for(Hop hop : outgoing){
                outgoingSteps.add(hop.getTargetStep());
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

//        Dataset<Row> inputUser = sparkSession.read().format("com.databricks.spark.csv").option("header", true).option("inferSchema", true).load(sInputUser);
//        Dataset<Row> inputApp = sparkSession.read().format("com.databricks.spark.csv").option("header", true).option("inferSchema", true).load(sInputApp);
//        inputUser.join(inputApp, "appId").write().format("com.databricks.spark.csv").save(sOutput);
    }
}
