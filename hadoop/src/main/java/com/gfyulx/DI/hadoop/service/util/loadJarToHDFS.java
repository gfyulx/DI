package com.gfyulx.DI.hadoop.service.util;


import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

/**
 * @ClassName: loadJarToHDFS
 * @Description: 上传文件到hdfs路径
 * @author: gfyulx
 * @date: 2018/8/15 11:28
 * @Copyright: 2018 gfyulx
 */
public class loadJarToHDFS {
    private static final Logger LOG = LoggerFactory.getLogger(loadJarToHDFS.class);
    Configuration config;
    private String[] fileList;
    private String dstPath;

    public void loadJarToHDFS(Configuration config) {
        this.config = config;
    }

    public boolean load(String[] fileList, String dstPath) throws IOException {
        if (this.config == null) {
            LOG.error("need to init hadoop cluster configuration first");
            return false;
        }
        FileSystem fs = FileSystem.get(this.config);
        try {
            Path hdfsPath = new Path(dstPath);
            if (!(fs.exists(hdfsPath))) {
                fs.mkdirs(hdfsPath);
            }
            for (String file : fileList) {
                File f = new File(file);
                if (f.exists()) {
                    Path localPath = new Path(file);
                    fs.copyFromLocalFile(localPath, hdfsPath);
                    LOG.info("load file:" + file + "to hdfs:" + dstPath);
                } else {
                    LOG.warn("file not exists!:", file);
                }
            }
        } finally {
            fs.close();
        }
        return true;
    }

}
