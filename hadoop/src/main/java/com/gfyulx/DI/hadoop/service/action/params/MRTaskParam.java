package com.gfyulx.DI.hadoop.service.action.params;

import java.io.Serializable;

/**
 * @ClassName:  MRTaskParam
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/8/30 10:55
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class MRTaskParam implements Serializable {
    private String jarPath;
    private String options;

    public MRTaskParam() {
    }

    public String getJarPath() {
        return jarPath;
    }

    public void setJarPath(String jarPath) {
        this.jarPath = jarPath;
    }

    public String getOptions() {
        return options;
    }

    public void setOptions(String options) {
        this.options = options;
    }
}
