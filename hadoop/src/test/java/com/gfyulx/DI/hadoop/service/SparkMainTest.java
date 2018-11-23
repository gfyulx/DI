package com.gfyulx.DI.hadoop.service;


import org.apache.hadoop.conf.Configuration;

import org.apache.spark.SparkConf;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;

import static org.junit.Assert.*;
import org.apache.hadoop.io.Writable;
public class SparkMainTest {
    @Test
    public void SparkMainTest() throws Exception {

        //System.setProperty("hadoop.home.dir", "D:\\code\\hadoopwinutil\\hadoop-2.6.3");
        String[] configFiles = new String[]{"core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml"};
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("hdp.version", "2.6.1.0-129");
        System.setProperty("launcher.job.id", "TestSampler");
        //System.setProperty("HADOOP_CONF_DIR", "./");
        //System.setProperty("SPARK_HOME", "D:\\code\\java\\lib");


        SparkConf sparkconf = new SparkConf();

        Configuration config = new Configuration();
        //Configuration config = HadoopConfiguration.loadConfigFiles(configFiles);
        config.set("mapreduce.app-submission.cross-platform", "true");
        //config.set("yarn.application.classpath", "D:\\code\\java\\lib\\jars\\*");

        config.set("spark.yarn.jars","");
        config.set("spark.master", "yarn");
        config.set("spark.mode", "cluster");
        config.set("spark.name", "samplerTest");
        config.set("spark.class", "com.gfyulx.DI.spark.Sampler");
        config.set("spark.jars", "D:\\code\\java\\wordcount.txt");
        config.set("spark.jar", "D:\\code\\java\\gfyulx\\DI\\out\\artifacts\\sampleer\\sampleer.jar");

        //config.set("spark.spark-opts","--files D:\\code\\java\\wordcount.txt");
        config.set("spark.spark-opts","--files D:\\code\\java\\wordcount.txt --conf spark.executor.extraJavaOptions=-Dhdp.version=2.6.1.0-129 --conf spark.driver.extraJavaOptions=-Dhdp.version=2.6.1.0-129");





        SparkMain test = new SparkMain(config);
        String[] args = {"--arg", "wordcount.txt"};
        //String[] args = {"wordcount.txt"};
        //Configuration conf=new Configuration();
        test.run(args, config);
        //export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
    }
}
