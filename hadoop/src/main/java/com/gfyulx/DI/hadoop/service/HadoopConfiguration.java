package com.gfyulx.DI.hadoop.service;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import java.io.*;
import java.util.Map;


/**
 * @ClassName: Configuration
 * @Description: 配置hadoop集群相关的配置信息，生成client提交需要的配置。
 * @author: gfyulx
 * @date: 2018/8/14 10:37
 * @Copyright: Fujian Linewell Software Co., Ltd. All rights reserved.
 * 注意：本内容仅限于福建南威软件股份有限公司内部传阅，禁止外泄以及用于其他的商业目的
 */
public class HadoopConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(Configuration.class);
    protected static String[] HADOOP_SITE_FILES = new String[]
            {"core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml"};
    protected static String PROJECT_CONF_PATH="./";
    Configuration config;

    HadoopConfiguration(Configuration config) {
        this.config = config;
    }
    /**
     * @param fileNames
     * @return
     * @throws IOException
     */
    public static Configuration loadConfigFiles(String[] fileNames) {
        Configuration config = new Configuration();
        for (String configFile : fileNames) {
            File file = new File(configFile);
            if (file.exists()) {
                config.addResource(configFile);
                LOG.debug("load configfile:" + configFile);
            }
        }
        return config;

    }

    /**
     * add config from string
     * @param configStr
     * @return
     */
    public Configuration loadConfigString(String configStr) {
        Configuration config = new Configuration();
        try {
            config.addResource(configStr);
            LOG.debug("load configfile:" + configStr);
        } catch (Exception e) {
            LOG.error("parse config fail:", e);
        }
        return config;
    }


    //set conf
    public void  setAttr(String name,String value){
        this.config.set(name,value);
    }


    public void setAttr(Map<String,String> attrs){
        for(Map.Entry<String,String> entry:attrs.entrySet()){
            this.config.set(entry.getKey(), entry.getValue());
        }
    }
    //get conf
    public String getAttr(String name){
        return this.config.get(name,"NULL");
    }



}
