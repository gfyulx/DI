package com.gfyulx.DI.hadoop.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.gfyulx.DI.hadoop.service.HadoopConfiguration.loadConfigFiles;
import com.gfyulx.DI.hadoop.service.util.loadJarToHDFS;

/**
 * @ClassName:  Submit
 * @Description: hadoop yarn提交jar任务入口
 * 这里是提交jar包的入口 ，在jar包中实现的各种类型的class主入口。
 * @author: gfyulx
 * @date:   2018/8/15 13:58
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class Submit {
    private static final Logger LOG = LoggerFactory.getLogger(Submit.class);
    Configuration config;
    private String[] configFiles;
    private String[] jars;


    public Submit(Configuration config){
        this.config=config;

    }
    public Submit(String[] configFiles){
        this.config = new Configuration(loadConfigFiles(configFiles));
    }

    //根据configuration对象提交yarn任务
    //异常处理中应包含任务提交成功或者失败，失败原因
    //config中应包含各类的配置设置
    public String deploy() throws InterruptedException, IOException, ClassNotFoundException {
        Job job=Job.getInstance(this.config);
        //Configuration config=job.getConfiguration();
        //config.addResource(this.config);
        job.waitForCompletion(true);
        return job.getJobID().toString();
    }

    //是否上传配置文件
    public void addJar(String[] jarFiles,String dstPath)throws IOException{
        loadJarToHDFS load=new loadJarToHDFS(this.config);
        load.load(jarFiles,dstPath);
    }
    //提交参数
    public void jvmParamAdd(String[] args){

    }
}
