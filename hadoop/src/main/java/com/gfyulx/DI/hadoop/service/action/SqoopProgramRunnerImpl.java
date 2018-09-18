package com.gfyulx.DI.hadoop.service.action;


import com.cloudera.sqoop.util.OptionsFileUtil;
import com.gfyulx.DI.hadoop.service.action.params.SqoopTaskParam;


import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.Sqoop;
import com.google.common.annotations.VisibleForTesting;
import static org.apache.sqoop.Sqoop.runSqoop;

/**
 * @ClassName:  SqoopProgramRunnerImpl
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/9/7 11:30
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class SqoopProgramRunnerImpl   {
    /**
     * enum TOOLS{
     * codegen,
     * eval,
     * export,
     * import,
     * import-all-tables,
     * help,
     * list-databases,
     * list-tables,
     * merge,
     * metastore,
     * job,
     * version
     * };
     */

    public static final String SQOOP_SITE_CONF = "sqoop-site.xml";
    @VisibleForTesting
    static final Pattern[] SQOOP_JOB_IDS_PATTERNS = {
            Pattern.compile("Job complete: (job_\\S*)"),
            Pattern.compile("Job (job_\\S*) has completed successfully"),
            Pattern.compile("Submitted application (application[0-9_]*)")
    };

    private static final String SQOOP_LOG4J_PROPS = "sqoop-log4j.properties";


    public boolean run(SqoopTaskParam param) throws Exception {
        System.out.println();
        System.out.println("Sqoop action configuration");
        System.out.println("=================================================================");

        String sqoopArgs = param.getCommand();
        if (sqoopArgs == null) {
            throw new RuntimeException("no sqoop command exists in params");
        }
        String[] args;
        StringTokenizer st = new StringTokenizer(sqoopArgs, " ");
        List<String> l = new ArrayList<String>();
        while (st.hasMoreTokens()) {
            l.add(st.nextToken());
        }
        args = l.toArray(new String[l.size()]);
        //需要配置HDFS信息。hdfs-site.xml
        Configuration config=new Configuration();
        try{
            String hadoopDir=System.getenv("HADOOP_CONF_DIR");
            if (hadoopDir==null || hadoopDir.isEmpty()){
                throw new Exception("HADOOP_CONF_DIR muset be set!");
            }
            String configFile=new String(hadoopDir+"/hdfs-site.xml");
            System.out.println(configFile);
            File file=new File(configFile);
            if (file.exists() ){
                config.addResource(configFile);
            }
        }catch(Exception er){
            er.printStackTrace();
            throw new Exception(er.getCause());
        }


        System.out.println("=================================================================");
        System.out.println(">>> Invoking Sqoop command line now >>>");
        System.out.println();
        System.out.flush();

        try {
            runSqoopJob(args,config);
        } catch (Exception ex) {
            System.out.print(ex.getMessage());
            return false;
        } finally {
            System.out.println("\n<<< Invocation of Sqoop command completed <<<\n");
        }
        return true;
    }

    protected void runSqoopJob(String[] args,Configuration conf) throws Exception {
        // running as from the command line
        //Sqoop.main(args);
        String[] expandedArgs = null;
        try {
            expandedArgs = OptionsFileUtil.expandArguments(args);
        } catch (Exception var7) {
            System.err.println(var7.getMessage());
            System.err.println("Try 'sqoop help' for usage.");
            throw new Exception(var7.getMessage());
        }
        String toolName = expandedArgs[0];
        Configuration pluginConf = com.cloudera.sqoop.tool.SqoopTool.loadPlugins(conf);
        com.cloudera.sqoop.tool.SqoopTool tool = com.cloudera.sqoop.tool.SqoopTool.getTool(toolName);
        if (null == tool) {
            System.err.println("No such sqoop tool: " + toolName + ". See 'sqoop help'.");
            throw new Exception("sqoop tool not support!");
        } else {
            Sqoop sqoop = new Sqoop(tool, pluginConf);
            Sqoop.runSqoop(sqoop, (String[]) Arrays.copyOfRange(expandedArgs, 1, expandedArgs.length));
        }

    }

}
