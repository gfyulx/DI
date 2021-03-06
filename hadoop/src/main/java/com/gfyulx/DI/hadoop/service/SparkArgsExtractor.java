package com.gfyulx.DI.hadoop.service;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import com.gfyulx.DI.hadoop.service.util.SparkOptionsSplitter;
import com.gfyulx.DI.hadoop.service.util.JarFilter;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.regex.Pattern;

class SparkArgsExtractor {
    private static final Pattern SPARK_DEFAULTS_FILE_PATTERN = Pattern.compile("spark-defaults.conf");
    private static final String FILES_OPTION = "--files";
    private static final String ARCHIVES_OPTION = "--archives";
    private static final String LOG4J_CONFIGURATION_JAVA_OPTION = "-Dlog4j.configuration=";
    private static final String SECURITY_TOKENS_HADOOPFS = "spark.yarn.security.tokens.hadoopfs.enabled";
    private static final String SECURITY_TOKENS_HIVE = "spark.yarn.security.tokens.hive.enabled";
    private static final String SECURITY_TOKENS_HBASE = "spark.yarn.security.tokens.hbase.enabled";
    private static final String SECURITY_CREDENTIALS_HADOOPFS = "spark.yarn.security.credentials.hadoopfs.enabled";
    private static final String SECURITY_CREDENTIALS_HIVE = "spark.yarn.security.credentials.hive.enabled";
    private static final String SECURITY_CREDENTIALS_HBASE = "spark.yarn.security.credentials.hbase.enabled";
    private static final String PWD = "$PWD" + File.separator + "*";
    private static final String MASTER_OPTION = "--master";
    private static final String MODE_OPTION = "--deploy-mode";
    private static final String JOB_NAME_OPTION = "--name";
    private static final String CLASS_NAME_OPTION = "--class";
    private static final String VERBOSE_OPTION = "--verbose";
    private static final String DRIVER_CLASSPATH_OPTION = "--driver-class-path";
    private static final String EXECUTOR_CLASSPATH = "spark.executor.extraClassPath=";
    private static final String DRIVER_CLASSPATH = "spark.driver.extraClassPath=";
    private static final String EXECUTOR_EXTRA_JAVA_OPTIONS = "spark.executor.extraJavaOptions=";
    private static final String DRIVER_EXTRA_JAVA_OPTIONS = "spark.driver.extraJavaOptions=";
    private static final Pattern SPARK_VERSION_1 = Pattern.compile("^1.*");
    private static final String SPARK_YARN_JAR = "spark.yarn.jar";
    private static final String SPARK_YARN_JARS = "spark.yarn.jars";
    private static final String OPT_SEPARATOR = "=";
    private static final String OPT_VALUE_SEPARATOR = ",";
    private static final String CONF_OPTION = "--conf";
    private static final String MASTER_OPTION_YARN_CLUSTER = "yarn-cluster";
    private static final String MASTER_OPTION_YARN_CLIENT = "yarn-client";
    private static final String MASTER_OPTION_YARN = "yarn";
    private static final String DEPLOY_MODE_CLUSTER = "cluster";
    private static final String DEPLOY_MODE_CLIENT = "client";
    private static final String SPARK_YARN_TAGS = "spark.yarn.tags";
    private static final String OPT_PROPERTIES_FILE = "--properties-file";

    private boolean pySpark = false;
    private final Configuration actionConf;

    SparkArgsExtractor(final Configuration actionConf) {
        this.actionConf = actionConf;
    }

    boolean isPySpark() {
        return pySpark;
    }

    List<String> extract(final String[] mainArgs) throws  IOException, URISyntaxException {
        final List<String> sparkArgs = new ArrayList<>();

        sparkArgs.add(MASTER_OPTION);
        final String master = actionConf.get(SparkMain.SPARK_MASTER);
        sparkArgs.add(master);

        // In local mode, everything runs here in the Launcher Job.
        // In yarn-client mode, the driver runs here in the Launcher Job and the
        // executor in Yarn.
        // In yarn-cluster mode, the driver and executor run in Yarn.
        final String sparkDeployMode = actionConf.get(SparkMain.SPARK_MODE);
        if (sparkDeployMode != null) {
            sparkArgs.add(MODE_OPTION);
            sparkArgs.add(sparkDeployMode);
        }
        final boolean yarnClusterMode = master.equals(MASTER_OPTION_YARN_CLUSTER)
                || (master.equals(MASTER_OPTION_YARN) && sparkDeployMode != null && sparkDeployMode.equals(DEPLOY_MODE_CLUSTER));
        final boolean yarnClientMode = master.equals(MASTER_OPTION_YARN_CLIENT)
                || (master.equals(MASTER_OPTION_YARN) && sparkDeployMode != null && sparkDeployMode.equals(DEPLOY_MODE_CLIENT));

        sparkArgs.add(JOB_NAME_OPTION);
        sparkArgs.add(actionConf.get(SparkMain.SPARK_JOB_NAME));

        final String className = actionConf.get(SparkMain.SPARK_CLASS);
        if (className != null) {
            sparkArgs.add(CLASS_NAME_OPTION);
            sparkArgs.add(className);
        }

        String jarPath = actionConf.get(SparkMain.SPARK_JAR);
        if (jarPath != null && jarPath.endsWith(".py")) {
            pySpark = true;
        }

        boolean addedLog4jDriverSettings = false;
        boolean addedLog4jExecutorSettings = false;
        final StringBuilder driverClassPath = new StringBuilder();
        final StringBuilder executorClassPath = new StringBuilder();
        final StringBuilder userFiles = new StringBuilder();
        final StringBuilder userArchives = new StringBuilder();
        final StringBuilder executorExtraJavaOptions = new StringBuilder();
        final StringBuilder driverExtraJavaOptions = new StringBuilder();

        //解析spark_opts参数
        final String sparkOpts = actionConf.get(SparkMain.SPARK_OPTS);
        System.out.println(sparkOpts);
        if (StringUtils.isNotEmpty(sparkOpts)) {
            final List<String> sparkOptions = SparkOptionsSplitter.splitSparkOpts(sparkOpts);
            for (int i = 0; i < sparkOptions.size(); i++) {
                String opt = sparkOptions.get(i);
                System.out.println(opt);
                boolean addToSparkArgs = true;
                if (yarnClusterMode || yarnClientMode) {
                    if (opt.startsWith(EXECUTOR_CLASSPATH)) {
                        appendWithPathSeparator(opt.substring(EXECUTOR_CLASSPATH.length()), executorClassPath);
                        addToSparkArgs = false;
                    }
                    if (opt.startsWith(DRIVER_CLASSPATH)) {
                        appendWithPathSeparator(opt.substring(DRIVER_CLASSPATH.length()), driverClassPath);
                        addToSparkArgs = false;
                    }
                    if (opt.equals(DRIVER_CLASSPATH_OPTION)) {
                        // we need the next element after this option
                        appendWithPathSeparator(sparkOptions.get(i + 1), driverClassPath);
                        // increase i to skip the next element.
                        i++;
                        addToSparkArgs = false;
                    }
                }

                if (opt.startsWith(EXECUTOR_EXTRA_JAVA_OPTIONS) || opt.startsWith(DRIVER_EXTRA_JAVA_OPTIONS)) {
                    if (!opt.contains(LOG4J_CONFIGURATION_JAVA_OPTION)) {
                        opt += " " + LOG4J_CONFIGURATION_JAVA_OPTION + SparkMain.SPARK_LOG4J_PROPS;
                    } else {
                        System.out.println("Warning: Spark Log4J settings are overwritten." +
                                " Child job IDs may not be available");
                    }

                    if (opt.startsWith(EXECUTOR_EXTRA_JAVA_OPTIONS)) {
                        appendWithPathSeparator(opt.substring(EXECUTOR_EXTRA_JAVA_OPTIONS.length()), executorExtraJavaOptions);
                        addToSparkArgs = false;
                        addedLog4jExecutorSettings = true;
                    } else {
                        appendWithPathSeparator(opt.substring(DRIVER_EXTRA_JAVA_OPTIONS.length()), driverExtraJavaOptions);
                        addToSparkArgs = false;
                        addedLog4jDriverSettings = true;
                    }
                }
                if (opt.startsWith(FILES_OPTION)) {
                    final String userFile;
                    if (opt.contains(OPT_SEPARATOR)) {
                        userFile = opt.substring(opt.indexOf(OPT_SEPARATOR) + OPT_SEPARATOR.length());
                    }
                    else {
                        userFile = sparkOptions.get(i + 1);
                        i++;
                    }
                    if (userFiles.length() > 0) {
                        userFiles.append(OPT_VALUE_SEPARATOR);
                    }
                    userFiles.append(userFile);
                    addToSparkArgs = false;
                }
                if (opt.startsWith(ARCHIVES_OPTION)) {
                    final String userArchive;
                    if (opt.contains(OPT_SEPARATOR)) {
                        userArchive = opt.substring(opt.indexOf(OPT_SEPARATOR) + OPT_SEPARATOR.length());
                    }
                    else {
                        userArchive = sparkOptions.get(i + 1);
                        i++;
                    }
                    if (userArchives.length() > 0) {
                        userArchives.append(OPT_VALUE_SEPARATOR);
                    }
                    userArchives.append(userArchive);
                    addToSparkArgs = false;
                }
                if (addToSparkArgs) {
                    sparkArgs.add(opt);
                }
                else if (sparkArgs.get(sparkArgs.size() - 1).equals(CONF_OPTION)) {
                    sparkArgs.remove(sparkArgs.size() - 1);
                }
            }
        }

        if ((yarnClusterMode || yarnClientMode)) {
            // Include the current working directory (of executor container)
            // in executor classpath, because it will contain localized
            // files
            appendWithPathSeparator(PWD, executorClassPath);
            appendWithPathSeparator(PWD, driverClassPath);

            sparkArgs.add(CONF_OPTION);
            sparkArgs.add(EXECUTOR_CLASSPATH + executorClassPath.toString());

            sparkArgs.add(CONF_OPTION);
            sparkArgs.add(DRIVER_CLASSPATH + driverClassPath.toString());

            sparkArgs.add(CONF_OPTION);
            sparkArgs.add(EXECUTOR_EXTRA_JAVA_OPTIONS + executorExtraJavaOptions.toString());

            sparkArgs.add(CONF_OPTION);
            sparkArgs.add(DRIVER_EXTRA_JAVA_OPTIONS + driverExtraJavaOptions.toString());
        }

        if (!addedLog4jExecutorSettings) {
            sparkArgs.add(CONF_OPTION);
            sparkArgs.add(EXECUTOR_EXTRA_JAVA_OPTIONS + LOG4J_CONFIGURATION_JAVA_OPTION + SparkMain.SPARK_LOG4J_PROPS);
        }
        if (!addedLog4jDriverSettings) {
            sparkArgs.add(CONF_OPTION);
            sparkArgs.add(DRIVER_EXTRA_JAVA_OPTIONS + LOG4J_CONFIGURATION_JAVA_OPTION + SparkMain.SPARK_LOG4J_PROPS);
        }
        final File defaultConfFile = SparkMain.getMatchingFile(SPARK_DEFAULTS_FILE_PATTERN);
        if (defaultConfFile != null) {
            sparkArgs.add(OPT_PROPERTIES_FILE);
            sparkArgs.add(SPARK_DEFAULTS_FILE_PATTERN.toString());
        }

        if ((yarnClusterMode || yarnClientMode)) {
            final Map<String, URI> fixedFileUrisMap =
                    SparkMain.fixFsDefaultUrisAndFilterDuplicates(DistributedCache.getCacheFiles(actionConf));
            //fixedFileUrisMap.put(SparkMain.SPARK_LOG4J_PROPS, new Path(SparkMain.SPARK_LOG4J_PROPS).toUri());
            //fixedFileUrisMap.put(SparkMain.HIVE_SITE_CONF, new Path(SparkMain.HIVE_SITE_CONF).toUri());
            addUserDefined(userFiles.toString(), fixedFileUrisMap);
            final Collection<URI> fixedFileUris = fixedFileUrisMap.values();
            final JarFilter jarFilter = new JarFilter(fixedFileUris, jarPath);
            jarFilter.filter();
            jarPath = jarFilter.getApplicationJar();

            final String cachedFiles = StringUtils.join(fixedFileUris, OPT_VALUE_SEPARATOR);
            if (cachedFiles != null && !cachedFiles.isEmpty()) {
                sparkArgs.add(FILES_OPTION);
                sparkArgs.add(cachedFiles);
            }
            final Map<String, URI> fixedArchiveUrisMap = SparkMain.fixFsDefaultUrisAndFilterDuplicates(DistributedCache.
                    getCacheArchives(actionConf));
            addUserDefined(userArchives.toString(), fixedArchiveUrisMap);
            final String cachedArchives = StringUtils.join(fixedArchiveUrisMap.values(), OPT_VALUE_SEPARATOR);
            if (cachedArchives != null && !cachedArchives.isEmpty()) {
                sparkArgs.add(ARCHIVES_OPTION);
                sparkArgs.add(cachedArchives);
            }
            setSparkYarnJarsConf(sparkArgs, jarFilter.getSparkYarnJar(), jarFilter.getSparkVersion());
        }

        if (!sparkArgs.contains(VERBOSE_OPTION)) {
            sparkArgs.add(VERBOSE_OPTION);
        }

        sparkArgs.add(jarPath);
        sparkArgs.addAll(Arrays.asList(mainArgs));

        return sparkArgs;
    }

    private void appendWithPathSeparator(final String what, final StringBuilder to) {
        if (to.length() > 0) {
            to.append(File.pathSeparator);
        }
        to.append(what);
    }

    private void addUserDefined(final String userList, final Map<String, URI> urisMap) {
        if (userList != null) {
            for (final String file : userList.split(OPT_VALUE_SEPARATOR)) {
                if (!StringUtils.isEmpty(file)) {
                    final Path p = new Path(file);
                    urisMap.put(p.getName(), p.toUri());
                }
            }
        }
    }

    /**
     * Sets spark.yarn.jars for Spark 2.X. Sets spark.yarn.jar for Spark 1.X.
     *
     * @param sparkArgs
     * @param sparkYarnJar
     * @param sparkVersion
     */
    private void setSparkYarnJarsConf(final List<String> sparkArgs, final String sparkYarnJar, final String sparkVersion) {
        if (SPARK_VERSION_1.matcher(sparkVersion).find()) {
            // In Spark 1.X.X, set spark.yarn.jar to avoid
            // multiple distribution
            sparkArgs.add(CONF_OPTION);
            sparkArgs.add(SPARK_YARN_JAR + OPT_SEPARATOR + sparkYarnJar);
        } else {
            // In Spark 2.X.X, set spark.yarn.jars
            sparkArgs.add(CONF_OPTION);
            sparkArgs.add(SPARK_YARN_JARS + OPT_SEPARATOR + sparkYarnJar);
        }
    }
}
