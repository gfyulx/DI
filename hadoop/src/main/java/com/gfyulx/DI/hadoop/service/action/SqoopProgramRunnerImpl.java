package com.gfyulx.DI.hadoop.service.action;

import com.google.common.annotations.VisibleForTesting;
import com.gfyulx.DI.hadoop.service.action.params.SqoopTaskParam;
import org.apache.sqoop.Sqoop;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

/**
 * @ClassName:  SqoopProgramRunnerImpl
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/8/30 10:52
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class SqoopProgramRunnerImpl  {

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


        System.out.println("=================================================================");
        System.out.println(">>> Invoking Sqoop command line now >>>");
        System.out.println();
        System.out.flush();

        try {
            runSqoopJob(args);
        } catch (Exception ex) {
            System.out.print(ex.getMessage());
            return false;
        } finally {
            System.out.println("\n<<< Invocation of Sqoop command completed <<<\n");
        }
        return true;
    }

    protected void runSqoopJob(String[] args) throws Exception {
        // running as from the command line
        Sqoop.main(args);
    }

}
