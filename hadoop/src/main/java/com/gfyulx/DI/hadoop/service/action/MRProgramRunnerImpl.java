package com.gfyulx.DI.hadoop.service.action;


import com.gfyulx.DI.hadoop.service.action.params.MRTaskParam;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
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
        //fow windows10 test;
        String os = System.getProperty("os.name");
        //System.out.println(os);
        if (os.toLowerCase().startsWith("win")) {
            conf.set("mapreduce.app-submission.cross-platform", "true");
        }
        List<String> arguments = new ArrayList<>();
        String vars = param.getOptions();
        if (vars != null && !vars.isEmpty()) {
            String[] splitVars = vars.split(",");
            for (String var : splitVars) {
                if (var.length() > 0 && var.contains("=")) {
                    String[] keyValue = var.split("=");
                    conf.set(keyValue[0], keyValue[1]);
                }
            }
        } else {
            throw new Exception("params options can't be null");
        }

        try {
            //加载jar包

            File jarFile = new File(param.getJarPath());
            if (jarFile != null) {
                Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
                boolean accessible = method.isAccessible();
                if (accessible == false) {
                    method.setAccessible(true);
                }
                URLClassLoader classLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
                try {
                    URL url = jarFile.toURI().toURL();
                    method.invoke(classLoader, url);
                } finally {
                    method.setAccessible(accessible);
                }
            }

            Job job = Job.getInstance(conf);
            job.setJar(param.getJarPath());
            //check required arguments
            if (conf.get("mapreduce.job.map.class") == null) {
                throw new Exception("mapreduce.job.map.class is required");
            }
            Class mapClass = Class.forName(conf.get("mapreduce.job.map.class"));
            job.setMapperClass(mapClass);

            if (conf.get("mapreduce.job.reduce.class") == null) {
                throw new Exception("mapreduce.job.reduce.class is required");
            }
            Class reduceClass = Class.forName(conf.get("mapreduce.job.reduce.class"));
            job.setReducerClass(reduceClass);

            if (conf.get("mapreduce.job.output.key.class") == null) {
                if (conf.get("mapreduce.job.map.output.key.class") == null) {
                    throw new Exception("mapreduce.job.output.key.class or mapreduce.job.map.output.key.class is required");
                }
                Class mapKeyClass = Class.forName(conf.get("mapreduce.job.map.output.key.class"));
                job.setMapOutputKeyClass(mapKeyClass);
                if (conf.get("mapreduce.job.reduce.output.key.class") == null) {
                    throw new Exception("mapreduce.job.output.key.class or mapreduce.job.reduce.output.key.class is required");
                }
                Class reduceKeyClass = Class.forName(conf.get("mapreduce.job.reduce.output.key.class"));
                job.setOutputKeyClass(reduceKeyClass);
            } else {
                Class keyClass = Class.forName(conf.get("mapreduce.job.output.key.class"));
                job.setMapOutputKeyClass(keyClass);
                job.setOutputKeyClass(keyClass);
            }

            if (conf.get("mapreduce.job.output.value.class") == null) {
                if (conf.get("mapreduce.job.map.output.value.class") == null) {
                    throw new Exception("mapreduce.job.output.value.class or mapreduce.job.map.output.value.class is required");
                }
                Class mapValueClass = Class.forName(conf.get("mapreduce.job.map.output.value.class"));
                job.setMapOutputValueClass(mapValueClass);
                if (conf.get("mapreduce.job.reduce.output.value.class") == null) {
                    throw new Exception("mapreduce.job.output.value.class or mapreduce.job.reduce.output.value.class is required");
                }
                Class reduceValueClass = Class.forName(conf.get("mapreduce.job.reduce.output.value.class"));
                job.setOutputValueClass(reduceValueClass);
            } else {
                Class valueClass = Class.forName(conf.get("mapreduce.job.output.value.class"));
                job.setMapOutputValueClass(valueClass);
                job.setOutputValueClass(valueClass);
            }

            if (conf.get("mapreduce.input.fileinputformat.inputdir") == null) {
                throw new Exception("mapreduce.input.fileinputformat.inputdir is required");
            }
            if (conf.get("mapreduce.output.fileoutputformat.outputdir") == null) {
                throw new Exception("mapreduce.input.fileinputformat.inputdir is required");
            }
            Path inputPath = new Path(conf.get("mapreduce.input.fileinputformat.inputdir"));
            Path outputPath = new Path(conf.get("mapreduce.output.fileoutputformat.outputdir"));
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(outputPath)) {
                fs.delete(outputPath);
            }
            //MR的输入和输出源格式设置为文件
            FileInputFormat.setInputPaths(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);

            boolean waitForCompletion = job.waitForCompletion(true);
            return waitForCompletion;
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(e.getMessage());
        }
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
