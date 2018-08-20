package com.gfyulx.DI.hadoop.service;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName:  ReduceCollageCount
 * @Description: 高校分布统计Reduce
 * @author: gfyulx
 * @date:   2018/8/20 10:27
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class ReduceCollageCount extends Reducer<Text,LongWritable,Text,LongWritable>{
    protected void reduce(Text key,Iterable<LongWritable>value,Context context) throws IOException,InterruptedException {
        int sum=0;
        for(LongWritable v:value){
            sum+=v.get();
        }
        context.write(key,new LongWritable(sum));
    }
}
