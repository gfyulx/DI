package com.gfyulx.DI.hadoop.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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

public class MRMain {
    public void main()throws Exception{
        MRJobSubmit test=new MRJobSubmit();
        System.setProperty("hadoop.home.dir","D:\\code\\hadoopwinutil\\hadoop-2.6.3");
        String[] configFiles = new String[]{"core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml"};
        System.setProperty("HADOOP_USER_NAME","hdfs");
        System.setProperty("hdp.version","2.6.1.0-129");

        //System.setProperty("hdp.version","current");
        Configuration config=HadoopConfiguration.loadConfigFiles(configFiles);
        config.set("mapreduce.app-submission.cross-platform","true");
        config.set("mapred.remote.os","Linux");
        config.set("yarn.application.classpath","$CLASSPATH:./lib/");
        //config.set("yarn.nodemanager.delete.debug-delay-sec","600");
        //config.set("mapred.jar","d:\\code\\java\\gfyulx\\DI\\out\\artifacts\\collageCount\\collageCount.jar");
        ///其间出现log 设置的错误，原因为job.setJarByClass设置在远程运行时，jar包并没上传，需要设置为job.setJar方生效.
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
}
