package com.gfyulx.DI.hadoop.service.action;

import com.gfyulx.DI.hadoop.service.action.params.HdfsTaskParam;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName:  HdfsProgramRunnerImpl
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/8/30 10:56
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class HdfsProgramRunnerImpl  {
    private Configuration fsConfig;
    protected static String[] HADOOP_SITE_FILES = new String[]
            {"core-site.xml", "hdfs-site.xml"};


    public boolean run(HdfsTaskParam param) throws Exception {
        String delePath = param.getDeletePaths();
        String creatPath = param.getCreatePaths();
        String creatFile = param.getCreateFiles();
        String moveEntities = param.getMoveEntities();

        String configPath = new String();
        try {
            configPath = System.getProperty("HADOOP_CONF_DIR");
        } catch (IllegalArgumentException e) {
            System.out.println("HADOOP_CONF_DIR need be set in local env" + e);

        }
        this.fsConfig = new Configuration();
        List<String> fileNames = new ArrayList<>();
        for (String f : HADOOP_SITE_FILES) {
            File file = new File(f);
            if (file.exists()) {
                this.fsConfig.addResource(f);
            }

        }
        try {
            if (delePath.length() > 0) {
                if (delePath.contains(",")) {
                    for (String subPath : delePath.split(",")) {
                        delete(subPath);
                    }
                } else {
                    delete(delePath);
                }
            }

            if (creatPath.length() > 0) {
                if (creatPath.contains(",")) {
                    for (String subPath : creatPath.split(",")) {
                        mkdir(subPath);
                    }
                } else {
                    mkdir(creatPath);
                }
            }

            if (creatFile.length() > 0) {
                if (creatFile.contains(",")) {
                    for (String subFile : creatFile.split(",")) {
                        touchz(subFile);
                    }
                } else {
                    touchz(creatFile);
                }
            }

            if (moveEntities.length() > 0) {
                if (moveEntities.contains(",")) {
                    for (String subEntity : moveEntities.split(",")) {
                        move(subEntity);
                    }
                } else {
                    move(moveEntities);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
        return true;
    }


    void mkdir(String pathStr) throws Exception {
        try {
            Path path = new Path(pathStr);
            FileSystem fs = FileSystem.get(this.fsConfig);
            if (!fs.exists(path)) {
                if (!fs.mkdirs(path)) {
                    throw new Exception("mkdir, path [{0}] could not create directory" + path);
                }
            }
        } catch (Exception ex) {
            throw new Exception(ex.getMessage());
        }
    }

    public void delete(String pathStr) throws Exception {
        try {
            Path path = new Path(pathStr);
            FileSystem fs = FileSystem.get(this.fsConfig);
            if (fs.exists(path)) {
                if (!fs.delete(path, true)) {
                    throw new Exception("delete, path [{0}] could not delete path" + path);
                }
            }
        } catch (Exception ex) {
            throw new Exception(ex.getMessage());
        }
    }

    public void move(String moveEntity) throws Exception {
        try {
            if (moveEntity.contains(":")) {
                String source = moveEntity.split(":")[0];
                String target = moveEntity.split(":")[1];
                FileSystem fs = FileSystem.get(this.fsConfig);
                Path sPath = new Path(source);
                Path tPath = new Path(target);
                if ((sPath == null || source.length() == 0) || (!fs.exists(sPath))) {
                    throw new Exception("move, source path [{0}] does not exist" + source);
                }
                if (fs.isDirectory(sPath) && (fs.exists(tPath) && fs.isFile(tPath))) {
                    throw new Exception("move, could not move sources direct to dest file!");
                }
                if (!fs.rename(sPath, tPath)) {
                    throw new Exception("move, could not move [{0}] to [{1}]" + source + target);
                }
            }
        } catch (Exception ex) {
            throw new Exception(ex.getMessage());
        }

    }

    void touchz(String pathStr) throws Exception {
        try {
            Path path = new Path(pathStr);
            FileSystem fs = FileSystem.get(this.fsConfig);
            FileStatus st;
            if (fs.exists(path)) {
                st = fs.getFileStatus(path);
                if (st.isDir()) {
                    throw new Exception(path.toString() + " is a directory");
                } else if (st.getLen() != 0) {
                    throw new Exception(path.toString() + " must be a zero-length file");
                }
            }
            FSDataOutputStream out = fs.create(path);
            out.close();
        } catch (Exception ex) {
            throw new Exception(ex.getMessage());
        }
    }
}

