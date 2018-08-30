package com.gfyulx.DI.hadoop.service.action.params;

import java.io.Serializable;

/**
 * @ClassName:  SparkJarTaskParam
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/8/30 10:55
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class SparkJarTaskParam implements Serializable {
    private String sparkMaster;
    private String mode;
    private String jarPath;
    private String appName;
    private String mainClass;
    private String options;
    private String[] args;

    public SparkJarTaskParam() {
    }

    public String[] getArgs() {
        return args;
    }

    public void setArgs(String[] args) {
        this.args = args;
    }

    public String getSparkMaster() {
        return sparkMaster;
    }

    public void setSparkMaster(String sparkMaster) {
        this.sparkMaster = sparkMaster;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getJarPath() {
        return jarPath;
    }

    public void setJarPath(String jarPath) {
        this.jarPath = jarPath;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getMainClass() {
        return mainClass;
    }

    public void setMainClass(String mainClass) {
        this.mainClass = mainClass;
    }

    public String getOptions() {
        return options;
    }

    public void setOptions(String options) {
        options = options;
    }

    @Override
    public String toString() {
        return "SparkMaster:" + sparkMaster + " mode:" + mode + " jarpath:" + jarPath + " appName:" + appName + " mainClass:" + mainClass + " options:" + options;
    }
}
