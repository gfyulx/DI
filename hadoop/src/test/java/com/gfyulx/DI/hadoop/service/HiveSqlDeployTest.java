package com.gfyulx.DI.hadoop.service;


import org.junit.Test;

import com.gfyulx.DI.hadoop.service.HiveSqlDeploy;

import java.io.File;
import java.io.IOException;


public class HiveSqlDeployTest {
    @Test
    public void TestdeplogyHive() {
        //String HQL = "insert into test values(\"Kitty1\");";
        String HQL = "select * from test";
        String[] configFiles = new String[]{"core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml"};
        String hiveconf = "hivejob";

        System.out.println(Class.class.getResource("/").getPath());
        System.out.println(System.getProperty("user.dir"));
        //System.out.println(System.getProperty("classpath"));
        File directory = new File("");//设定为当前文件夹
        try {
            System.out.println(directory.getCanonicalPath());//获取标准的路径
            System.out.println(directory.getAbsolutePath());//获取绝对路径
        } catch (Exception e) {
            System.out.println("err" + e);
        }
        System.setProperty("hadoop.home.dir","D:\\code\\hadoopwinutil\\hadoop-common-2.2.0-bin-master");
        System.out.println(System.getenv("HADOOP_HOME"));

        try {
            HiveSqlDeploy hiveObject = new HiveSqlDeploy(configFiles, HQL);
            hiveObject.hiveInit(hiveconf);
        } catch (Exception er) {
            System.out.print("error" + er);
        }
        System.out.println("end test ");

    }


}
