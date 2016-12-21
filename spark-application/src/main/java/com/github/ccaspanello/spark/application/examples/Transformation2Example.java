package com.github.ccaspanello.spark.application.examples;

import com.github.ccaspanello.spark.engine.step.Hop;
import com.github.ccaspanello.spark.engine.step.IStep;
import com.github.ccaspanello.spark.engine.step.csvinput.CsvInputMeta;
import com.github.ccaspanello.spark.engine.step.csvinput.CsvInputStep;
import com.github.ccaspanello.spark.engine.step.csvoutput.CsvOutputMeta;
import com.github.ccaspanello.spark.engine.step.csvoutput.CsvOutputStep;
import com.github.ccaspanello.spark.engine.step.datagrid.Column;
import com.github.ccaspanello.spark.engine.step.datagrid.DataGridMeta;
import com.github.ccaspanello.spark.engine.step.datagrid.DataGridStep;
import com.github.ccaspanello.spark.engine.step.lookup.LookupMeta;
import com.github.ccaspanello.spark.engine.step.lookup.LookupStep;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.StringType;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Transformation Example
 * <p>
 * Create a Graph with Steps/Hops which defines how a transformation will run.
 * <p>
 * Created by ccaspanello on 12/19/2016.
 */
public class Transformation2Example {

    public static Logger LOG = LoggerFactory.getLogger(Transformation2Example.class);

    public void execute(SparkSession spark, String sOutput) {

        // Create Meta Model (Note this could be deserialized/serialized into XML to be sent acorss the wire)
        DataGridMeta assetMeta = new DataGridMeta("Asset");
        assetMeta.getColumns().add(new Column("assetId", DataTypes.IntegerType));
        assetMeta.getColumns().add(new Column("asset", DataTypes.StringType));
        assetMeta.getColumns().add(new Column("employeeId", DataTypes.IntegerType));
        assetMeta.getData().add(Arrays.asList("1", "Computer", "11"));
        assetMeta.getData().add(Arrays.asList("2", "Monitor", "11"));
        assetMeta.getData().add(Arrays.asList("3", "Mouse", "11"));
        assetMeta.getData().add(Arrays.asList("4", "Keyboard", "11"));

        DataGridMeta employeeMeta = new DataGridMeta("Employee");
        employeeMeta.getColumns().add(new Column("employeeId", DataTypes.IntegerType));
        employeeMeta.getColumns().add(new Column("employee", DataTypes.StringType));
        employeeMeta.getData().add(Arrays.asList("11", "Chris"));

        LookupMeta lookupMeta = new LookupMeta("Lookup");
        lookupMeta.setLeftStep(assetMeta.getName());
        lookupMeta.setRightStep(employeeMeta.getName());
        lookupMeta.setField("employeeId");

        CsvOutputMeta outputStepMeta = new CsvOutputMeta("Output");
        outputStepMeta.setFilename(sOutput);

        // Create Steps out of the Step Meta Models
        DataGridStep assetStep = new DataGridStep(assetMeta);
        DataGridStep employeeStep = new DataGridStep(employeeMeta);
        LookupStep lookupStep = new LookupStep(lookupMeta);
        CsvOutputStep csvOutputStep = new CsvOutputStep(outputStepMeta);

        // Wire Steps into a DAG
        DirectedGraph<IStep, Hop> graph = new DefaultDirectedGraph<>(Hop.class);

        // Steps
        graph.addVertex(assetStep);
        graph.addVertex(employeeStep);
        graph.addVertex(lookupStep);
        graph.addVertex(csvOutputStep);

        // Hops
        graph.addEdge(assetStep, lookupStep);
        graph.addEdge(employeeStep, lookupStep);
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
            for (Hop hop : incoming) {
               incomingSteps.add(hop.incomingSteps());
            }

            Set<IStep> outgoingSteps = new HashSet<>();
            for (Hop hop : outgoing) {
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
        for (IStep step : executionPlan) {
            LOG.warn("***** -> {}", step.getStepMeta().getName());
            step.setSparkSession(spark);
            step.execute();
        }
    }
}
