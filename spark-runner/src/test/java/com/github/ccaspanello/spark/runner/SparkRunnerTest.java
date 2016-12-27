package com.github.ccaspanello.spark.runner;

import org.apache.commons.io.FileUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by ccaspanello on 12/27/2016.
 */
public class SparkRunnerTest {

    private static final Logger LOG = LoggerFactory.getLogger(SparkRunnerTest.class);

    //private static final String CLUSTER_CONFIG = "C:/Users/ccaspanello/Desktop/Cluster Config/svqxbdcn6cdh57spark";

    private SparkRunner sparkRunner;
    private File jar;

    private String csvInputUser;
    private String csvInputApp;
    private String csvOutput;

    @Before
    public void before() {
        Map<String, String> env = new HashMap<>();
        env.put(Constants.SPARK_HOME, System.getenv(Constants.SPARK_HOME));
        //env.put(Constants.HADOOP_CONF_DIR, CLUSTER_CONFIG);
        env.put(Constants.HADOOP_USER_NAME, "devuser");
        env.put(Constants.SPARK_CONF_DIR, System.getenv(Constants.SPARK_HOME) + "/kettleConfig");

        sparkRunner = new SparkRunner(env);
        File projectRoot = new File(System.getProperty("buildDirectory"));
        jar = new File(projectRoot.getParentFile(), "spark-application/target/spark-application-1.0-SNAPSHOT.jar");
        LOG.info("{}", jar.getPath());

        URL url1 = this.getClass().getClassLoader().getResource("csv/input/InputUser.csv");
        URL url2 = this.getClass().getClassLoader().getResource("csv/input/InputApp.csv");
        URL url3 = this.getClass().getClassLoader().getResource("csv");
        csvInputUser = url1.toString();
        csvInputApp = url2.toString();
        csvOutput = url3.toString() + "/output";
        LOG.info("input1: {}", csvInputUser);
        LOG.info("input2: {}", csvInputApp);
        LOG.info("output: {}", csvOutput);
    }

    @Test
    public void testSparkPi() {
        SparkAppHandle.State state = sparkRunner.run(jar, "SPARKPI", "10");
        assertEquals(SparkAppHandle.State.FINISHED, state);
    }

    @Test
    public void testWordCountLocal() throws Exception {
        URL url = this.getClass().getClassLoader().getResource("mobydick");
        String input = url.toString() + "/input";
        String output = url.toString() + "/output";
        LOG.info("input: {}", input);
        LOG.info("output: {}", output);

        // Cleanup Output Directory
        cleanupLocalOutput(output);
        SparkAppHandle.State state = sparkRunner.run(jar, "WORDCOUNT", input, output);
        assertEquals(SparkAppHandle.State.FINISHED, state);
    }

    @Test
    public void testProcessExample() {
        SparkAppHandle.State state = sparkRunner.run(jar, "PROCESS");
        assertEquals(SparkAppHandle.State.FINISHED, state);
    }

    @Test
    public void testCsvMerge() throws Exception {
        String output = csvOutput + "-CsvMerge";
        cleanupLocalOutput(output);
        SparkAppHandle.State state = sparkRunner.run(jar, "CSV", csvInputUser, csvInputApp, output);
        assertEquals(SparkAppHandle.State.FINISHED, state);
    }

    @Test
    public void testTransformation1() throws Exception {
        String output = csvOutput + "-Transformation1";
        cleanupLocalOutput(output);
        SparkAppHandle.State state = sparkRunner.run(jar, "TRANS", csvInputUser, csvInputApp, output);
        assertEquals(SparkAppHandle.State.FINISHED, state);
    }

    @Test
    public void testTransformation2() throws Exception {
        String output = csvOutput + "-Transformation2";
        cleanupLocalOutput(output);
        SparkAppHandle.State state = sparkRunner.run(jar, "TRANS2", output);
        assertEquals(SparkAppHandle.State.FINISHED, state);
    }

    private void cleanupLocalOutput(String file) throws Exception {
        // Apache Commons I/O Util does not support Schemas
        File folder = new File(file.replace("file:/", ""));
        FileUtils.deleteDirectory(folder);
    }
}
