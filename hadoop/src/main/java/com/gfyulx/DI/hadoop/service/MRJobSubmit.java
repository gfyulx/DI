package com.gfyulx.DI.hadoop.service;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: MRJobSubmit
 * @Description: MR类任务的提交设置
 * @author: gfyulx
 * @date: 2018/8/20 10:42
 * @Copyright: 2018 gfyulx
 */
public class MRJobSubmit {
    //private static final Logger LOG = LoggerFactory.getLogger(MRJobSubmit.class);

    public void submit(Configuration config) throws Exception {
        Configuration conf = new Configuration();
        conf.addResource(config);
        Job job = Job.getInstance(conf);
        job.setJarByClass(MRJobSubmit.class);
        job.setJar("d:\\code\\java\\gfyulx\\DI\\out\\artifacts\\collageCount\\collageCount.jar");
        job.setMapperClass(MapCollageCount.class);
        job.setReducerClass(ReduceCollageCount.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        Path inputPath = new Path("/user/root/data/s_college_info.csv");
        Path outputPath = new Path("/user/root/data/out/collageInfo");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath);
        }
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean waitForCompletion = job.waitForCompletion(true);
    }
    /*
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
    */
}
