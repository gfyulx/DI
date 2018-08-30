package com.gfyulx.DI.hadoop.service.action;


import com.gfyulx.DI.hadoop.service.action.params.HiveScriptTaskParam;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.TeeOutputStream;
import org.apache.hive.beeline.BeeLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * @ClassName: HiveProgramRunnerImpl
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date: 2018/8/30 10:57
 * @Copyright: 2018 gfyulx
 */
public class HiveProgramRunnerImpl {
    private static final Logger LOG = LoggerFactory.getLogger(HiveProgramRunnerImpl.class);
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
        String[] splitVars = vars.split("\\s{1,}|\t");
        for (String var : splitVars) {
            if (var.length() > 0) {
                arguments.add("--hivevar");
                arguments.add(var);
            }
        }
        arguments.add("-a");
        arguments.add("delegationToken");

        //固定配置，用于获取特定类别的yarn调度子进程ID获取
        arguments.add("--hiveconf");
        arguments.add("mapreduce.job.tags=" + this.MAPREDUCE_JOB_TAGS);
        LOG.info("geneteror hive parater:" + arguments);
        //System.out.println(arguments);
        //用于获取实际运行在yarn上的jobid
        logFile = new String("hivejob_" + System.currentTimeMillis() + ".log");
        //runBeeline
        try {
            runBeeline(arguments.toArray(new String[arguments.size()]), logFile);
        } catch (Exception e) {
            System.out.println(e.getMessage() + e.getCause());
            return false;
        } finally {
            System.out.println("\n<<< Invocation of Beeline command completed <<<\n");
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
        File f = new File(filename);
        FileUtils.writeStringToFile(f, query, "UTF-8");
        return filename;
    }
}
