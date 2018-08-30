package com.gfyulx.DI.hadoop.service;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.deploy.SparkSubmit;

import com.gfyulx.DI.common.common;
import com.gfyulx.DI.hadoop.service.util.HadoopUriFinder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class SparkMain extends Submit {

    public static final String SPARK_MAIN_CLASS_NAME = "action.hadoop.SparkMain";
    public static final String TASK_USER_PRECEDENCE = "mapreduce.task.classpath.user.precedence"; // hadoop-2
    public static final String TASK_USER_CLASSPATH_PRECEDENCE = "mapreduce.user.classpath.first";  // hadoop-1
    public static final String SPARK_MASTER = "spark.master";
    public static final String SPARK_MODE = "spark.mode";
    public static final String SPARK_OPTS = "spark.spark-opts";  //只能有一个？？
    public static final String SPARK_JOB_NAME = "spark.name";
    public static final String SPARK_CLASS = "spark.class";
    public static final String SPARK_JAR = "spark.jar";   //hdfs路径的jar包,主jar包
    //public static final String SPARK_JARS="spark.jars";  //需要多个上传的jars包
    public static final String MAPRED_CHILD_ENV = "mapred.child.env";
    private static final String CONF_OOZIE_SPARK_SETUP_HADOOP_CONF_DIR = "action.spark.setup.hadoop.conf.dir";


    @VisibleForTesting
    static final Pattern[] SPARK_JOB_IDS_PATTERNS = {
            Pattern.compile("Submitted application (application[0-9_]*)")};
    @VisibleForTesting
    static final Pattern SPARK_ASSEMBLY_JAR_PATTERN = Pattern
            .compile("^spark-assembly((?:(-|_|(\\d+\\.))\\d+(?:\\.\\d+)*))*\\.jar$");
    @VisibleForTesting
    static final Pattern SPARK_YARN_JAR_PATTERN = Pattern
            .compile("^spark-yarn((?:(-|_|(\\d+\\.))\\d+(?:\\.\\d+)*))*\\.jar$");
    static final String HIVE_SITE_CONF = "hive-site.xml";
    static final String SPARK_LOG4J_PROPS = "spark-log4j.properties";

    private static final Pattern[] PYSPARK_DEP_FILE_PATTERN = {Pattern.compile("py4\\S*src.zip"),
            Pattern.compile("pyspark.zip")};

    static final String EXTERNAL_CHILD_IDS = "spark_externalChildIDs";

    public SparkMain(Configuration config) {
        super(config);
    }


    protected void run(final String[] args,Configuration config) throws Exception {
        Configuration conf = new Configuration();
        conf.addResource(config);
        //setYarnTag(conf);
        final String logFile = setUpSparkLog4J(conf);

        //解析配置文件为sparksubmit的参数
        final SparkArgsExtractor sparkArgsExtractor = new SparkArgsExtractor(conf);
        final List<String> sparkArgs = sparkArgsExtractor.extract(args);

        if (sparkArgsExtractor.isPySpark()) {
            createPySparkLibFolder();
        }

        System.out.println("Spark Action Main class        : " + SparkSubmit.class.getName());
        System.out.println();
        System.out.println("Spark action configuration");
        System.out.println("=================================================================");
        System.out.println();

        for (final String arg : sparkArgs) {
            System.out.println("                    " + arg);
        }
        System.out.println();

        try {
            runSpark(sparkArgs.toArray(new String[sparkArgs.size()]));
        } finally {
            System.out.println("\n<<< Invocation of Spark command completed <<<\n");
            writeExternalChildIDs(logFile, SPARK_JOB_IDS_PATTERNS, "Spark");
        }
    }

    protected static void writeExternalChildIDs(String logFile, Pattern[] patterns, String name) {
        // Harvesting and recording Hadoop Job IDs
        String jobIds = common.getHadoopJobIds(logFile, patterns);
        if (jobIds != null) {
            File externalChildIdsFile = new File(System.getProperty(EXTERNAL_CHILD_IDS));
            try (OutputStream externalChildIdsStream = new FileOutputStream(externalChildIdsFile)) {
                externalChildIdsStream.write(jobIds.getBytes());
                System.out.println("Hadoop Job IDs executed by " + name + ": " + jobIds);
                System.out.println();
            } catch (IOException e) {
                System.out.println("WARN: Error while writing to external child ids file: " +
                        System.getProperty(EXTERNAL_CHILD_IDS));
                e.printStackTrace(System.out);
            }
        } else {
            System.out.println("No child hadoop job is executed.");
        }
    }

    /**
     * SparkActionExecutor sets the SPARK_HOME environment variable to the local directory.
     * Spark is looking for the pyspark.zip and py4j-VERSION-src.zip files in the python/lib folder under SPARK_HOME.
     * This function creates the subfolders and copies the zips from the local folder.
     *
     * @throws IOException if there is an error during file copy
     */
    private void createPySparkLibFolder() throws IOException {
        final File pythonLibDir = new File("python/lib");
        if (!pythonLibDir.exists()) {
            pythonLibDir.mkdirs();
            System.out.println("PySpark lib folder " + pythonLibDir.getAbsolutePath() + " folder created.");
        }

        for (final Pattern fileNamePattern : PYSPARK_DEP_FILE_PATTERN) {
            final File file = getMatchingPyFile(fileNamePattern);
            final File destination = new File(pythonLibDir, file.getName());
            FileUtils.copyFile(file, destination);
            System.out.println("Copied " + file + " to " + destination.getAbsolutePath());
        }
    }

    /**
     * Searches for a file in the current directory that matches the given pattern.
     * If there are multiple files matching the pattern returns one of them.
     *
     * @param fileNamePattern the pattern to look for
     * @return the file if there is one
     */
    private File getMatchingPyFile(final Pattern fileNamePattern) {
        final File f = getMatchingFile(fileNamePattern);
        if (f != null) {
            return f;
        }
        return null;
    }

    /**
     * Searches for a file in the current directory that matches the given
     * pattern. If there are multiple files matching the pattern returns one of
     * them.
     *
     * @param fileNamePattern the pattern to look for
     * @return the file if there is one else it returns null
     */
    static File getMatchingFile(final Pattern fileNamePattern) {
        final File localDir = new File(".");

        final String[] localFileNames = localDir.list();
        if (localFileNames == null) {
            return null;
        }

        for (final String fileName : localFileNames) {
            if (fileNamePattern.matcher(fileName).find()) {
                return new File(fileName);
            }
        }
        return null;
    }

    private void runSpark(final String[] args) throws Exception {
        System.out.println("=================================================================");
        System.out.println();
        System.out.println(">>> Invoking Spark class now >>>");
        System.out.println();
        System.out.flush();
        SparkSubmit.main(args);
    }

    private String setUpSparkLog4J(final Configuration actionConf) throws IOException {
        // Logfile to capture job IDs
        final String hadoopJobId = System.getProperty("launcher.job.id");
        if (hadoopJobId == null) {
            throw new RuntimeException("Launcher Hadoop Job ID system,property not set");
        }
        final String logFile = new File("spark-" + hadoopJobId + ".log").getAbsolutePath();
        Properties hadoopProps = new Properties();

        // Preparing log4j configuration
        URL log4jFile = Thread.currentThread().getContextClassLoader().getResource("log4j.properties");
        if (log4jFile != null) {
            // getting hadoop log4j configuration
            hadoopProps.load(log4jFile.openStream());
        }

        final String logLevel = actionConf.get("spark.log.level", "INFO");
        final String rootLogLevel = actionConf.get("action.rootLogger.level", "INFO");

        hadoopProps.setProperty("log4j.rootLogger", rootLogLevel + ", A");
        hadoopProps.setProperty("log4j.logger.org.apache.spark", logLevel + ", A, jobid");
        hadoopProps.setProperty("log4j.additivity.org.apache.spark", "false");
        hadoopProps.setProperty("log4j.appender.A", "org.apache.log4j.ConsoleAppender");
        hadoopProps.setProperty("log4j.appender.A.layout", "org.apache.log4j.PatternLayout");
        hadoopProps.setProperty("log4j.appender.A.layout.ConversionPattern", "%d [%t] %-5p %c %x - %m%n");
        hadoopProps.setProperty("log4j.appender.jobid", "org.apache.log4j.FileAppender");
        hadoopProps.setProperty("log4j.appender.jobid.file", logFile);
        hadoopProps.setProperty("log4j.appender.jobid.layout", "org.apache.log4j.PatternLayout");
        hadoopProps.setProperty("log4j.appender.jobid.layout.ConversionPattern", "%d [%t] %-5p %c %x - %m%n");
        hadoopProps.setProperty("log4j.logger.org.apache.hadoop.mapred", "INFO, jobid");
        hadoopProps.setProperty("log4j.logger.org.apache.hadoop.mapreduce.Job", "INFO, jobid");
        hadoopProps.setProperty("log4j.logger.org.apache.hadoop.yarn.client.api.impl.YarnClientImpl", "INFO, jobid");

        final String localProps = new File(SPARK_LOG4J_PROPS).getAbsolutePath();
        try (OutputStream os1 = new FileOutputStream(localProps)) {
            hadoopProps.store(os1, "");
        }
        PropertyConfigurator.configure(SPARK_LOG4J_PROPS);
        return logFile;
    }

    /**
     * Convert URIs into the default format which Spark expects
     * Also filters out duplicate entries
     *
     * @param files
     * @return
     * @throws IOException
     * @throws URISyntaxException
     */
    static Map<String, URI> fixFsDefaultUrisAndFilterDuplicates(final URI[] files) throws IOException, URISyntaxException {
        final Map<String, URI> map = new LinkedHashMap<>();
        if (files == null) {
            return map;
        }
        final FileSystem fs = FileSystem.get(new Configuration(true));
        for (int i = 0; i < files.length; i++) {
            final URI fileUri = files[i];
            final Path p = new Path(fileUri);
            map.put(p.getName(), HadoopUriFinder.getFixedUri(fs, fileUri));
        }
        return map;
    }
}
