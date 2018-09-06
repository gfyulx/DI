package com.gfyulx.DI.hadoop.service.action;


import com.gfyulx.DI.hadoop.service.action.params.SparkJarTaskParam;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.deploy.SparkSubmit;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ClassName:  SparkProgramRunnerImpl
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/9/6 16:01
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class SparkProgramRunnerImpl  {
    //private static final Logger LOG = LoggerFactory.getLogger(SparkProgramRunnerImpl.class);
    private static final Pattern SPARK_DEFAULTS_FILE_PATTERN = Pattern.compile("spark-defaults.conf");
    private static final String FILES_OPTION = "--files";
    private static final String ARCHIVES_OPTION = "--archives";
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
    static final Pattern[] SPARK_JOB_IDS_PATTERNS = {
            Pattern.compile("Submitted application (application[0-9_]*)")};
    static final Pattern SPARK_ASSEMBLY_JAR_PATTERN = Pattern
            .compile("^spark-assembly((?:(-|_|(\\d+\\.))\\d+(?:\\.\\d+)*))*\\.jar$");
    static final Pattern SPARK_YARN_JAR_PATTERN = Pattern
            .compile("^spark-yarn((?:(-|_|(\\d+\\.))\\d+(?:\\.\\d+)*))*\\.jar$");
    private static final Pattern[] PYSPARK_DEP_FILE_PATTERN = {Pattern.compile("py4\\S*src.zip"),
            Pattern.compile("pyspark.zip")};

    private static final Pattern OPTION_KEY_PREFIX = Pattern.compile("\\s*--[a-zA-Z0-9.]+[\\-a-zA-Z0-9.]*[=]?");
    private static final String VALUE_HAS_QUOTES_AT_ENDS_REGEX = "[a-zA-Z0-9.]+=\".+\"";
    private static final String VALUE_HAS_QUOTES_IN_BETWEEN_REGEX =
            "[a-zA-Z0-9.]+=.*(\\w\\s+\"\\w+[\\s+\\w]*\"|\"\\w+[\\s+\\w]*\"\\s+\\w)+.*";


    //static final String EXTERNAL_CHILD_IDS = "spark_externalChildIDs";

    public boolean run(SparkJarTaskParam param) throws Exception {

        //解析sparksubmit的参数
        final List<String> sparkArgs = extract(param);
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
            System.getProperty("HADOOP_CONF_DIR");
            //System.out.println(System.getProperty("HADOOP_CONF_DIR"));
            runSpark(sparkArgs.toArray(new String[sparkArgs.size()]));
        } catch (IllegalArgumentException e) {
            System.out.println("HADOOP_CONF_DIR need be set in local env" + e);
            return false;
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            //System.out.println(ex.getCause());
            ex.printStackTrace();
        } finally {
            System.out.println("\n<<< Invocation of Spark command completed <<<\n");
        }
        return new Boolean(true);
    }

    private void runSpark(final String[] args) throws Exception {
        System.out.println("=================================================================");
        System.out.println();
        System.out.println(">>> Invoking Spark class now >>>");
        System.out.println();
        System.out.flush();
        System.out.println(System.getProperty("SPARK_HOME"));
        SparkSubmit.main(args);
    }

    private List<String> extract(SparkJarTaskParam param) throws IOException, URISyntaxException {
        final List<String> sparkArgs = new ArrayList<String>();

        sparkArgs.add(MASTER_OPTION);
        final String master = param.getSparkMaster();
        sparkArgs.add(master);

        final String sparkDeployMode = param.getMode();
        if (sparkDeployMode != null) {
            sparkArgs.add(MODE_OPTION);
            sparkArgs.add(sparkDeployMode);
        }
        final boolean yarnClusterMode = master.equals(MASTER_OPTION_YARN_CLUSTER)
                || (master.equals(MASTER_OPTION_YARN) && sparkDeployMode != null && sparkDeployMode.equals(DEPLOY_MODE_CLUSTER));
        final boolean yarnClientMode = master.equals(MASTER_OPTION_YARN_CLIENT)
                || (master.equals(MASTER_OPTION_YARN) && sparkDeployMode != null && sparkDeployMode.equals(DEPLOY_MODE_CLIENT));

        sparkArgs.add(JOB_NAME_OPTION);
        sparkArgs.add(param.getAppName());

        final String className = param.getMainClass();
        if (className != null) {
            sparkArgs.add(CLASS_NAME_OPTION);
            sparkArgs.add(className);
        }

        final StringBuilder driverClassPath = new StringBuilder();
        final StringBuilder executorClassPath = new StringBuilder();
        final StringBuilder userFiles = new StringBuilder();
        final StringBuilder userArchives = new StringBuilder();
        final StringBuilder executorExtraJavaOptions = new StringBuilder();
        final StringBuilder driverExtraJavaOptions = new StringBuilder();
        final String sparkOpts = param.getOptions();
        //System.out.println(sparkOpts);
        if (StringUtils.isNotEmpty(sparkOpts)) {
            final List<String> sparkOptions = splitSparkOpts(sparkOpts);
            for (int i = 0; i < sparkOptions.size(); i++) {
                String opt = sparkOptions.get(i);
                //System.out.println(opt);
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
                        i++;
                        addToSparkArgs = false;
                    }
                }

                if (opt.startsWith(EXECUTOR_EXTRA_JAVA_OPTIONS) || opt.startsWith(DRIVER_EXTRA_JAVA_OPTIONS)) {
                    if (opt.startsWith(EXECUTOR_EXTRA_JAVA_OPTIONS)) {
                        appendWithPathSeparator(opt.substring(EXECUTOR_EXTRA_JAVA_OPTIONS.length()), executorExtraJavaOptions);
                        addToSparkArgs = false;
                    } else {
                        appendWithPathSeparator(opt.substring(DRIVER_EXTRA_JAVA_OPTIONS.length()), driverExtraJavaOptions);
                        addToSparkArgs = false;
                    }
                }

                if (opt.startsWith(FILES_OPTION)) {
                    final String userFile;
                    if (opt.contains(OPT_SEPARATOR)) {
                        userFile = opt.substring(opt.indexOf(OPT_SEPARATOR) + OPT_SEPARATOR.length());
                    } else {
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
                    } else {
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
                } else if (sparkArgs.get(sparkArgs.size() - 1).equals(CONF_OPTION)) {
                    sparkArgs.remove(sparkArgs.size() - 1);
                }
            }
        }
        if ((yarnClusterMode || yarnClientMode)) {
            // Include the current working directory (of executor container)
            // in executor classpath, because it will contain localized
            // files

            if (userFiles != null && userFiles.length()>0) {
                sparkArgs.add(FILES_OPTION);
                sparkArgs.add(userFiles.toString());

            }

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

        String jarPath = param.getJarPath();
        sparkArgs.add(jarPath);
        String[] mainArgs = param.getArgs();
        if( mainArgs!=null && mainArgs.length>0) {
            sparkArgs.addAll(Arrays.asList(mainArgs));
        }
        return sparkArgs;
    }

    static List<String> splitSparkOpts(final String sparkOpts) {
        final List<String> result = new ArrayList<String>();
        final Matcher matcher = OPTION_KEY_PREFIX.matcher(sparkOpts);
        int start = 0, end;
        while (matcher.find()) {
            end = matcher.start();

            if (start > 0) {
                final String maybeQuotedValue = sparkOpts.substring(start, end).trim();
                if (StringUtils.isNotEmpty(maybeQuotedValue)) {
                    result.add(unquoteEntirelyQuotedValue(maybeQuotedValue));
                }
            }
            String sparkOpt = matchSparkOpt(sparkOpts, matcher);
            if (sparkOpt.endsWith("=")) {
                sparkOpt = sparkOpt.replaceAll("=", "");
            }
            result.add(sparkOpt);
            start = matcher.end();
        }
        final String maybeEntirelyQuotedValue = sparkOpts.substring(start).trim();
        if (StringUtils.isNotEmpty(maybeEntirelyQuotedValue)) {
            result.add(unquoteEntirelyQuotedValue(maybeEntirelyQuotedValue));
        }
        return result;
    }

    private static String matchSparkOpt(final String sparkOpts, final Matcher matcher) {
        return sparkOpts.substring(matcher.start(), matcher.end()).trim();
    }

    private static String unquoteEntirelyQuotedValue(final String maybeEntirelyQuotedValue) {
        final boolean hasQuotesAtEnds = maybeEntirelyQuotedValue.matches(VALUE_HAS_QUOTES_AT_ENDS_REGEX);
        final boolean hasQuotesInBetween = maybeEntirelyQuotedValue.matches(VALUE_HAS_QUOTES_IN_BETWEEN_REGEX);
        final boolean isEntirelyQuoted = hasQuotesAtEnds && !hasQuotesInBetween;

        if (isEntirelyQuoted) {
            return maybeEntirelyQuotedValue.replaceAll("\"", "");
        }
        return maybeEntirelyQuotedValue;
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
