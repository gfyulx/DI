package com.gfyulx.DI.hadoop.service;


import org.apache.hadoop.service.AbstractService;
//import org.apache.hadoop.util.Shell.runCommand;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;
import com.gfyulx.DI.hadoop.service.MRJobSubmit;
import com.gfyulx.DI.hadoop.service.HadoopConfiguration;
import org.apache.hadoop.conf.Configuration;
public class MRJobSubmitTest {
    @Test
    public void TestMRJobSubmit() throws Exception{
        MRJobSubmit test=new MRJobSubmit();
        System.setProperty("hadoop.home.dir","D:\\code\\hadoopwinutil\\hadoop-2.6.3");
        String[] configFiles = new String[]{"core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml"};
        System.setProperty("HADOOP_USER_NAME","hdfs");
        System.setProperty("hdp.version","2.6.1.0-129");

        System.out.println(System.getProperty("user.dir"));
        //System.setProperty("hdp.version","current");
        Configuration config=HadoopConfiguration.loadConfigFiles(configFiles);
        config.set("mapreduce.app-submission.cross-platform","true");
        config.set("mapred.remote.os","Linux");
        config.set("yarn.application.classpath","$CLASSPATH:./lib/");
        System.out.println(config.get("fs.defaultFS"));
        //config.set("yarn.nodemanager.delete.debug-delay-sec","600");
        //config.set("mapred.jar","d:\\code\\java\\gfyulx\\DI\\out\\artifacts\\collageCount\\collageCount.jar");
        ///其间出现log 设置的错误，原因为job.setJarByClass设置在远程运行时，jar包并没上传，需要设置为job.setJar方生效.
        test.submit(config);
    }

}
