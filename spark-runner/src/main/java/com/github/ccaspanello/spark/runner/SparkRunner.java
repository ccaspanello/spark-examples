package com.github.ccaspanello.spark.runner;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

/**
 * Created by ccaspanello on 12/27/2016.
 */
public class SparkRunner {

    private static final Logger LOG = LoggerFactory.getLogger(SparkRunner.class);

    private Map<String, String> env;


    public SparkRunner(Map<String, String> env) {
        this.env = env;
    }

    public SparkAppHandle.State run(File jar, String... args) {

        try {
            LOG.info("************************* START *************************");
            String driverClass = "com.github.ccaspanello.spark.application.SparkDriver";

            // Assume supporting libs are in a lib folder next to the JAR.
            File lib = new File(jar.getParentFile(), "lib");
            createConfigFile(lib);

            if (!jar.exists()) {
                LOG.info("JAR does not exist.  Debug from project root or modify jar location.");
                return SparkAppHandle.State.FAILED;
            }

            SparkLauncher launcher = new SparkLauncher(env);
            launcher.setVerbose(false);
            launcher.setAppName("Spark-Local");
            launcher.setAppResource(jar.getAbsolutePath());
            launcher.setMainClass(driverClass);
            launcher.setMaster("local");
            launcher.addAppArgs(args);

            SparkAppHandle sparkHandle = launcher.startApplication();

            LOG.info("Job Submitted");
            while (!sparkHandle.getState().isFinal()) {
                //LOG.info("State:  {}", sparkHandle.getState());
                Thread.sleep(100);
            }

            return sparkHandle.getState();

        } catch (Exception e) {
            throw new RuntimeException("Unexpected error trying to run spark application.");
        }
    }

    /**
     * Creates a custom configuration file.  This contains spark.jars which is a concatinated list of jars required to run
     * the application.  This is to get around a command line lenght limitation in windows.
     *
     * @param lib
     */
    private void createConfigFile(File lib) {
        try {
            String sparkConfDir = env.get(Constants.SPARK_CONF_DIR);
            File confDir = new File(sparkConfDir);
            if (confDir.exists()) {
                FileUtils.deleteDirectory(confDir);
            }
            confDir.mkdirs();
            File conf = new File(confDir, "spark-defaults.conf");

            StringBuilder sb = new StringBuilder();
            sb.append("spark.jars   ");
            File[] jars = lib.listFiles();
            for (File jar : jars) {
                sb.append("file:/" + jar.getPath().replace("\\", "/"));
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
