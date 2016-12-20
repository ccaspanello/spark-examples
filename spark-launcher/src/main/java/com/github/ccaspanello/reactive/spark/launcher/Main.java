package com.github.ccaspanello.reactive.spark.launcher;


import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ccaspanello on 12/8/2016.
 */
public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private static final String SPARK_HOME = "C:/DevTools/spark-2.0.1-bin-hadoop2.7";
    private static final String SPARK_CONF_DIR = SPARK_HOME + "/kettleConfig";
    private static String CLUSTER_CONFIG = "C:/Users/ccaspanello/Desktop/Cluster Config/svqxbdcn6cdh57spark";

    public static void main(String[] args) {

        String hdfsInput = "hdfs://svqxbdcn6cdh57sparkn1.pentahoqa.com:8020/wordcount/input";
        String hdfsOutput = "hdfs://svqxbdcn6cdh57sparkn1.pentahoqa.com:8020/wordcount/output";

        String input = "file:/C:/Users/ccaspanello/Desktop/mobydick/input";
        String output = "file:/C:/Users/ccaspanello/Desktop/mobydick/output";

        try {
            LOG.info("************************* START *************************");

            String driverClass = "com.github.ccaspanello.reactive.spark.application.SparkDriver";

            File root = new File(System.getProperty("user.dir"));
            File jar = new File(root, "spark-application/target/spark-application-1.0-SNAPSHOT.jar");
            File lib = new File(root, "spark-application/target/lib");

            createConfigFile(lib);

            LOG.info("Running Directory: {}", root.getAbsolutePath());
            LOG.info("JAR Location:      {}", jar.getAbsolutePath());
            LOG.info("JAR Exists:        {}", jar.exists());
            LOG.info("Driver Class:      {}", driverClass);

            if (!jar.exists()) {
                LOG.info("JAR does not exist.  Debug from project root or modify jar location.");
                return;
            }

            Map<String, String> env = new HashMap<>();
            env.put("SPARK_HOME", SPARK_HOME);
            env.put("HADOOP_CONF_DIR", CLUSTER_CONFIG);
            env.put("HADOOP_USER_NAME", "devuser");
            env.put("SPARK_CONF_DIR", SPARK_CONF_DIR);
            //env.put("SPARK_JAVA_OPTS", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005");

            SparkLauncher launcher = new SparkLauncher(env);
            launcher.setVerbose(false);
            launcher.setAppName("Spark-Local");
            launcher.setAppResource(jar.getAbsolutePath());
            launcher.setMainClass(driverClass);
            launcher.setMaster("local");

            //launcher.addAppArgs("SPARKPI", "10");
            //launcher.addAppArgs("WORDCOUNT", input, output);
            //launcher.addAppArgs("WORDCOUNT", hdfsInput, hdfsOutput);

//            String csvInputUser = "file:/C:/Users/ccaspanello/Desktop/AEL/InputUser.csv";
//            String csvInputApp = "file:/C:/Users/ccaspanello/Desktop/AEL/InputApp.csv";
//            String csvOutput = "file:/C:/Users/ccaspanello/Desktop/AEL/Output.txt";
//            launcher.addAppArgs("CSV", csvInputUser, csvInputApp, csvOutput);

            String csvInputUser = "file:/C:/Users/ccaspanello/Desktop/AEL/InputUser.csv";
            String csvInputApp = "file:/C:/Users/ccaspanello/Desktop/AEL/InputApp.csv";
            String csvOutput = "file:/C:/Users/ccaspanello/Desktop/AEL/Output.txt";
            launcher.addAppArgs("TRANS", csvInputUser, csvInputApp, csvOutput);

            SparkAppHandle sparkHandle = launcher.startApplication();

            LOG.info("Job Submitted");
            while (!sparkHandle.getState().isFinal()) {
//                LOG.info("State:  {}", sparkHandle.getState());
                Thread.sleep(100);
            }

            LOG.info("Job {}!", sparkHandle.getState());

            LOG.info("*************************  END  *************************");
        } catch (Exception e) {
            LOG.error("************************* ERROR *************************", e);
        }
    }

    private static void createConfigFile(File lib) {
        try {
            File confDir = new File(SPARK_CONF_DIR);
            if (confDir.exists()) {
                FileUtils.deleteDirectory(confDir);
            }
            confDir.mkdirs();
            File conf = new File(confDir, "spark-defaults.conf");

            StringBuilder sb = new StringBuilder();

            // Add Concatinated List of Kettle Jars from HDFS
            sb.append("spark.jars   ");
            File[] jars = lib.listFiles();
            for (File jar : jars) {
                sb.append("file:/"+jar.getPath().replace("\\","/"));
                sb.append(",");
            }

            OutputStream outputStream = new FileOutputStream(conf);
            IOUtils.write(sb.toString(), outputStream);
            outputStream.close();
        } catch (IOException e) {
            throw new RuntimeException("Unable to create Kettle Conf for Spark.", e);
        }
    }

}
