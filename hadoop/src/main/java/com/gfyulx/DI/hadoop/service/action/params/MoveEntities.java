package com.gfyulx.DI.hadoop.service.action.params;

import java.io.Serializable;

/**
 * @ClassName:  MoveEntities
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/8/30 10:55
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class MoveEntities implements Serializable{
    private String srcPath;
    private String destPath;

    public String getSrcPath() {
        return srcPath;
    }

    public void setSrcPath(String srcPath) {
        this.srcPath = srcPath;
    }

    public String getDestPath() {
        return destPath;
    }

    public void setDestPath(String destPath) {
        this.destPath = destPath;
    }
}
