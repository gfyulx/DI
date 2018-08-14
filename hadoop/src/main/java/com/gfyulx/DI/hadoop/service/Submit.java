package com.gfyulx.DI.hadoop.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.gfyulx.DI.hadoop.service.HadoopConfiguration.loadConfigFiles;

public class Submit {
    private static final Logger LOG = LoggerFactory.getLogger(Submit.class);
    Configuration config;
    private String[] configFiles;
    private String[] jars;


    public void Submit(String[] configFiles){
        this.config = loadConfigFiles(configFiles);

    }

    //根据configuration对象提交yarn任务
    //异常处理中应包含任务提交成功或者失败，失败原因
    public String submit() throws InterruptedException, IOException, ClassNotFoundException {
        Job job=Job.getInstance();
        Configuration config=job.getConfiguration();
        config.addResource(this.config);
        return job.getJobID().toString();
    }

    //是否上传配置文件
    public void addJar(String[] jarFiles){

    }
    //提交参数
}
