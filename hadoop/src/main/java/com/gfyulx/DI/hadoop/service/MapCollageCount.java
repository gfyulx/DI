package com.gfyulx.DI.hadoop.service;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName:  MapCollageCount
 * @Description: 各省高校分布情况统计map
 * @author: gfyulx
 * @date:   2018/8/20 10:20
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class MapCollageCount extends Mapper<LongWritable,Text,Text,LongWritable>{
    protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {
        String [] contents=value.toString().split(",");
        context.write(new Text(contents[1]),new LongWritable(1));
    }
}
