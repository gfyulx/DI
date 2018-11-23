package com.gfyulx.DI.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.api.java.function.*;

import java.util.Arrays;
import java.util.List;

/**
 * @ClassName:  SparkTestAll
 * @Description: spark练习类
 * @author: gfyulx
 * @date:   2018/9/18 13:49
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class SparkTestAll {

    public void createSparkProg(){
        SparkConf conf=new SparkConf().setAppName("sparkTest").setMaster("yarn-cluster");
        JavaSparkContext sc=new JavaSparkContext(conf);
        List<String> data=Arrays.asList("I'm a world count test lines");
        JavaRDD<String>words=sc.parallelize(data,10);  //numslices:partitions
        //hadoop datasets
        JavaRDD<String>dataLines=sc.textFile("data.txt");
        JavaRDD<Integer>linesLength=dataLines.map(s->s.length());
        int totalLength=linesLength.reduce((a,b)->a+b);
        linesLength.persist(StorageLevel.MEMORY_ONLY());  //持久化在内存,持久化操作通常应定义在reduce之前?

        //function: 在function.*中定义的各个函数接口，可以内嵌的方式实现或者使用lambada函数。
        JavaRDD<Integer> lineLengths = dataLines.map(new Function<String, Integer>() {
            public Integer call(String s) { return s.length(); }
        });
        totalLength = lineLengths.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) { return a + b; }
        });
        //内部类:如果需要访问变量，变量需要定义为finally
        class GetLength implements Function<String, Integer> {
            public Integer call(String s) { return s.length(); }
        }
        class Sum implements Function2<Integer, Integer, Integer> {
            public Integer call(Integer a, Integer b) { return a + b; }
        }

        JavaRDD<String> lines = sc.textFile("data.txt");
        JavaRDD<Integer> lineLengths1 = lines.map(new GetLength());
        int totalLength1 = lineLengths.reduce(new Sum());

        //lambda:
        dataLines.map(s->s.length()).reduce((a,b)->a + b);

        //对RDD的操作均是在executor中的，所以不要使用全局变量应用到单个RDD操作中，无法返回预期的结果
        //如果要用全局变量对每个RDD的操作，应使用累加器：Accumulator.
        //在driver中打印RDD的信息时，使用rdd.collect().foreach(println) 或者rdd.take(100).foreach(println)
        //注意内存问题！




    }

}
