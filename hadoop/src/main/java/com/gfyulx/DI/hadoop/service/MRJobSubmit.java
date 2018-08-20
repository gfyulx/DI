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

import java.net.URI;

/**
 * @ClassName: MRJobSubmit
 * @Description: MR类任务的提交设置
 * @author: gfyulx
 * @date: 2018/8/20 10:42
 * @Copyright: 2018 gfyulx
 */
public class MRJobSubmit {

    public void submit() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "HDFS://fj-c7-188.linewell.com:9200");
        Job job = Job.getInstance(conf);
        job.setJarByClass(MRJobSubmit.class);
        job.setMapperClass(MapCollageCount.class);
        job.setReducerClass(ReduceCollageCount.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        Path inputPath = new Path("/data/ s_college_info.csv");
        Path outputPath = new Path("data/out/collageInfo");
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
        if (!fs.exists(outputPath)) {
            fs.mkdirs(outputPath);
        }
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean waitForCompletion = job.waitForCompletion(true);
    }
}
