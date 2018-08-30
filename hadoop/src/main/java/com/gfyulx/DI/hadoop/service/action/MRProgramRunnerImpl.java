package com.gfyulx.DI.hadoop.service.action;


import com.gfyulx.DI.hadoop.service.action.params.MRTaskParam;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName:  MRProgramRunnerImpl
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/8/30 10:59
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class MRProgramRunnerImpl {
    private static final Logger LOG = LoggerFactory.getLogger(MRProgramRunnerImpl.class);
    protected static String[] HADOOP_SITE_FILES = new String[]
            {"core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml"};


    public boolean run(MRTaskParam param) throws Exception {

        Configuration conf = new Configuration();
        String configPath = new String();
        try {
            configPath = System.getProperty("HADOOP_CONF_DIR");
        } catch (IllegalArgumentException e) {
            System.out.println("HADOOP_CONF_DIR need be set in local env" + e);

        }
        List<String> fileNames = new ArrayList<>();
        for (String f : HADOOP_SITE_FILES) {
            fileNames.add(configPath + f);
        }
        conf = loadConfigFiles(fileNames.toArray(new String[fileNames.size()]));

        List<String> arguments = new ArrayList<>();
        String vars = param.getOptions();
        String[] splitVars = vars.split(",");
        for (String var : splitVars) {
            if (var.length() > 0 && var.contains("=")) {
                String[] keyValue = var.split("=");
                conf.set(keyValue[0], keyValue[1]);
            }
        }

        Job job = Job.getInstance(conf);
        job.setJar(param.getJarPath());
        boolean waitForCompletion = job.waitForCompletion(true);
        return waitForCompletion;
    }

    public static Configuration loadConfigFiles(String[] fileNames) {
        Configuration config = new Configuration();
        for (String configFile : fileNames) {
            File file = new File(configFile);
            if (file.exists()) {
                config.addResource(configFile);
                LOG.debug("load configfile:" + configFile);
            }
        }
        return config;

    }
}
