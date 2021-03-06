package com.gfyulx.DI.hadoop.service;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.gfyulx.DI.common.*;
import org.apache.commons.io.output.TeeOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.io.FileUtils;
import org.apache.hive.beeline.BeeLine;

/**
 * @ClassName: HiveSqlDeploy
 * @Description: 提交一个hql 语句到集群上
 * @author: gfyulx
 * @date: 2018/8/15 15:18
 * @Copyright: 2018 gfyulx
 */

/**
 * 使用该类应传递1.远程集群的配置文件2.包含jdburl ,username,password信息
 */
public class HiveSqlDeploy extends Submit {
    private static final Logger LOG = LoggerFactory.getLogger(HiveSqlDeploy.class);
    //从日志文件中获取jobid的正则
    static final Pattern[] HIVE2_JOB_IDS_PATTERNS = {
            Pattern.compile("Ended Job = (job_\\S*)"),
            Pattern.compile("Submitted application (application[0-9_]*)"),
            Pattern.compile("Running with YARN Application = (application[0-9_]*)")
    };
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
    public static final String MAPREDUCE_JOB_TAGS = "DI.hive.mr.tags";
    public String logFile;

    private String HQL = "";

    /**
     * 将SQL转为scripts文件，执行beeline命令远程执行sql命令
     */
    public HiveSqlDeploy(String[] configFile, String hql) {
        super(configFile);
        this.HQL = hql;
    }

    //初始化hive参数，配置文件路径为绝对路径或相对路径
    //jdbcurl
    //username
    //password
    public void hiveInit(String hiveConf) throws Exception {
        List<String> arguments = new ArrayList<>();
        ResourceBundle resource = ResourceBundle.getBundle(hiveConf);
        try {
            String jdbc = resource.getString("hive2.jdbc.url");
            arguments.add("-u");
            arguments.add(jdbc);
            String name = resource.getString("user.name");
            arguments.add("-n");
            arguments.add(name);
            String password = resource.getString("user.password");

            if (password != null) {
                arguments.add("-p");
                arguments.add(password);
            }

        } catch (MissingResourceException er) {
            LOG.error("hive conf not found prop:" + er, er);
        }
        //this is the default driver
        arguments.add("-d");
        arguments.add("org.apache.hive.jdbc.HiveDriver");

        //make HQL to  a file
        String fileName = createHQLFile(this.HQL);
        arguments.add("-f");
        arguments.add(fileName);
        arguments.add("--hiveconf");
        arguments.add("hive.execution.engine=mr");
        //参数分隔用空格或者\t
        String vars = resource.getString("hive.vars");
        String[] splitVars=vars.split("\\s{1,}|\t");
        for(String var:splitVars){
            if (var.length()>0) {
                arguments.add("--hivevar");
                arguments.add(var);
            }
        }
        arguments.add("-a");
        arguments.add("delegationToken");


        //固定配置，用于获取特定类别的yarn调度子进程ID获取
        arguments.add("--hiveconf");
        arguments.add("mapreduce.job.tags=" + this.MAPREDUCE_JOB_TAGS);
        LOG.info("geneteror hive parater:"+arguments);
        //System.out.println(arguments);
        //用于获取实际运行在yarn上的jobid
        logFile=new String("hivejob_"+System.currentTimeMillis()+".log");
        //runBeeline
        try {
            runBeeline(arguments.toArray(new String[arguments.size()]), logFile);
        }//TODO need to catch exception like permession and so on
        finally {
            System.out.println("\n<<< Invocation of Beeline command completed <<<\n");
        }
    }
    private void runBeeline(String[] args, String logFile) throws Exception {
        // We do this instead of calling BeeLine.ZooKeeperBase so we can duplicate the error stream for harvesting Hadoop child job IDs
        BeeLine beeLine = new BeeLine();
        beeLine.setErrorStream(new PrintStream(new TeeOutputStream(System.err, new FileOutputStream(logFile))));
        //测试获取所有的输出结果
        beeLine.setOutputStream(new PrintStream(new TeeOutputStream(System.out, new FileOutputStream(logFile)),true));
        int status = beeLine.begin(args, null);

        if (status != 0) {
            System.exit(status);
        }
    }

    private String createHQLFile(String query) throws IOException {
        String filename = "hive2-query-" + System.currentTimeMillis() + ".hql";
        File f = new File(filename);
        FileUtils.writeStringToFile(f, query, "UTF-8");
        return filename;
    }
}

