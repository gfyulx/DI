package com.gfyulx.DI.hadoop.service.action;

import com.google.common.base.Preconditions;
import com.gfyulx.DI.hadoop.service.action.params.HiveScriptTaskParam;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hive.beeline.BeeLine;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.TeeOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ClassName: HiveProgramRunnerImpl
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date: 2018/8/30 10:57
 * @Copyright: 2018 gfyulx
 */
public class HiveProgramRunnerImpl {
    //private static final Logger LOG = LoggerFactory.getLogger(HiveProgramRunnerImpl.class);
    //从日志文件中获取jobid的正则
    static final Pattern[] HIVE2_JOB_IDS_PATTERNS = {
            Pattern.compile("Ended Job = (job_\\S*)"),
            Pattern.compile("Submitted application (application[0-9_]*)"),
            Pattern.compile("Running with YARN Application = (application[0-9_]*)")
    };
    protected static String[] HADOOP_SITE_FILES = new String[]
            {"core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml"};

    //*不允许在Beeline中执行的参数
    private static final Set<String> DISALLOWED_BEELINE_OPTIONS = new HashSet<String>();

    static {
        DISALLOWED_BEELINE_OPTIONS.add("-u");
        DISALLOWED_BEELINE_OPTIONS.add("-n");
        DISALLOWED_BEELINE_OPTIONS.add("-p");
        DISALLOWED_BEELINE_OPTIONS.add("-d");
        DISALLOWED_BEELINE_OPTIONS.add("-e");
        DISALLOWED_BEELINE_OPTIONS.add("-f");
        DISALLOWED_BEELINE_OPTIONS.add("-a");
        DISALLOWED_BEELINE_OPTIONS.add("--help");
    }

    public static final String MAPREDUCE_JOB_TAGS = "scheduler.hive.mr.tags";
    public String logFile;

    private String HQL = "";


    public boolean run(HiveScriptTaskParam param) throws Exception {
        List<String> arguments = new ArrayList<>();

        String jdbc = param.getHiveServerUrl();
        arguments.add("-u");
        arguments.add(jdbc);
        String name = param.getUserName();
        arguments.add("-n");
        arguments.add(name);
        String password = param.getPassword();

        if (password != null) {
            arguments.add("-p");
            arguments.add(password);
        }
        //this is the default driver
        arguments.add("-d");
        arguments.add("org.apache.hive.jdbc.HiveDriver");

        //make HQL to  a file
        String fileName = createHQLFile(param.getScript());
        arguments.add("-f");
        arguments.add(fileName);
        arguments.add("--hiveconf");
        arguments.add("hive.execution.engine=mr");
        //参数分隔用空格或者\t
        String vars = param.getHiveVars();
        if (!(vars==null ||vars.isEmpty())) {
            String[] splitVars = vars.split("\\s{1,}|\t");
            for (String var : splitVars) {
                if (var.length() > 0) {
                    arguments.add("--hivevar");
                    arguments.add(var);
                }
            }
        }
        arguments.add("-a");
        arguments.add("delegationToken");

        //固定配置，用于获取特定类别的yarn调度子进程ID获取
        arguments.add("--hiveconf");
        arguments.add("mapreduce.job.tags=" + this.MAPREDUCE_JOB_TAGS);
        System.out.println("geneteror hive parater:" + arguments);
        //System.out.println(arguments);
        //用于获取实际运行在yarn上的jobid
        logFile = new String("hivejob_" + System.currentTimeMillis() + ".log");
        //runBeeline
        try {
            runBeeline(arguments.toArray(new String[arguments.size()]), logFile);
        } catch (Exception e) {
            System.out.println(e.getMessage() + e.getCause());
            return false;
        }
        finally {
            System.out.println("<<< Invocation of Beeline command completed <<<");
            String jobId=getHadoopJobIds(logFile,HIVE2_JOB_IDS_PATTERNS);
            System.out.print("hadoopJobId:"+jobId);
        }
        return true;
    }

    private void runBeeline(String[] args, String logFile) throws Exception {
        // We do this instead of calling BeeLine.main so we can duplicate the error stream for harvesting Hadoop child job IDs
        BeeLine beeLine = new BeeLine();
        beeLine.setErrorStream(new PrintStream(new TeeOutputStream(System.err, new FileOutputStream(logFile))));
        //测试获取所有的输出结果
        beeLine.setOutputStream(new PrintStream(new TeeOutputStream(System.out, new FileOutputStream(logFile)), true));
        int status = beeLine.begin(args, null);
        if (status != 0) {
            System.exit(status);
        }
    }

    private String createHQLFile(String query) throws IOException {
        String filename = "hive2-query-" + System.currentTimeMillis() + ".hql";
        System.out.println(System.getProperty("user.dir"));
        File f = new File(System.getProperty("user.dir")+"/"+filename);
        //File f = new File(filename);
        FileUtils.writeStringToFile(f, query, "UTF-8");
        return System.getProperty("user.dir")+"\\"+filename;
    }
    protected static String getHadoopJobIds(String logFile, Pattern[] patterns) {
        Set<String> jobIds = new LinkedHashSet<String>();
        if (!new File(logFile).exists()) {
            System.err.println("Log file: " + logFile + "  not present. Therefore no Hadoop job IDs found.");
        }
        else {
            try (BufferedReader br = new BufferedReader(new FileReader(logFile))) {
                String line = br.readLine();
                while (line != null) {
                    extractJobIDs(line, patterns, jobIds);
                    line = br.readLine();
                }
            } catch (IOException e) {
                System.out.println("WARN: Error getting Hadoop Job IDs. logFile: " + logFile);
                e.printStackTrace(System.out);
            }
        }
        return jobIds.isEmpty() ? null : StringUtils.join(jobIds, ",");
    }
    protected static void extractJobIDs(String line, Pattern[] patterns, Set<String> jobIds) {
        Preconditions.checkNotNull(line);
        for (Pattern pattern : patterns) {
            Matcher matcher = pattern.matcher(line);
            if (matcher.find()) {
                String jobId = matcher.group(1);
                if (StringUtils.isEmpty(jobId) || jobId.equalsIgnoreCase("NULL")) {
                    continue;
                }
                jobId = jobId.replaceAll("application", "job");
                jobIds.add(jobId);
            }
        }
    }

    public boolean kill(String jobId) throws Exception {
        Configuration conf = new Configuration();
        conf = loadConf();
        YarnClient yarnClient=YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
        String[] parts = jobId.split("_");
        long timeScope=0L;
        int id=0;
        if (parts.length == 3 && parts[0].equals("job")) {
            timeScope=Long.parseLong(parts[1]);
            id=Integer.parseInt(parts[2]);
        }else{
            throw new Exception("jobId format wrong!");
        }
        ApplicationId appId=ApplicationId.newInstance(timeScope,id) ;
        try {
            yarnClient.killApplication(appId);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public String getStatus(String jobId) throws Exception {
        Configuration conf = new Configuration();
        conf = loadConf();
        JobClient job = new JobClient(conf);
        RunningJob jobStatus = job.getJob(jobId);
        System.out.print(jobStatus.toString());
        int statusInt = jobStatus.getJobStatus().getRunState();
        return jobStatus.getJobStatus().getJobRunState(statusInt);
    }

    public static Configuration loadConfigFiles(String[] fileNames) {
        Configuration config = new Configuration();
        for (String configFile : fileNames) {
            File file = new File(configFile);
            if (file.exists()) {
                try {
                    URL url = file.toURI().toURL();
                    config.addResource(url);
                    System.out.println("load configfile:" + configFile);
                } catch (Exception e) {
                    System.err.print(e.getMessage());
                }
            } else {
                System.out.print("configfile:" + configFile + "not exists!");
            }
        }
        return config;

    }
    public Configuration loadConf() {
        String configPath = new String();
        try {
            configPath = System.getProperty("HADOOP_CONF_DIR");
        } catch (IllegalArgumentException e) {
            System.out.println("HADOOP_CONF_DIR need be set in local env" + e);

        }
        List<String> fileNames = new ArrayList<>();
        for (String f : HADOOP_SITE_FILES) {
            fileNames.add(configPath + "\\" + f);
        }
        return loadConfigFiles(fileNames.toArray(new String[fileNames.size()]));
    }
}
