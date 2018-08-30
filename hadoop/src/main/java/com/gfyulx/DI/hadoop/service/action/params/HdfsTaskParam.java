package com.gfyulx.DI.hadoop.service.action.params;

import java.io.Serializable;

/**
 * @ClassName:  HdfsTaskParam   
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/8/30 10:56
 *
 * @Copyright: 2018 gfyulx
 * 
 */  
public class HdfsTaskParam implements Serializable {
    private String deletePaths;
    private String createPaths;
    private String createFiles;
    private String moveEntities;  //source:target,source1:target1,source3:target3

    public String getDeletePaths() {
        return deletePaths;
    }

    public void setDeletePaths(String deletePaths) {
        this.deletePaths = deletePaths;
    }

    public String getCreatePaths() {
        return createPaths;
    }

    public void setCreatePaths(String createPaths) {
        this.createPaths = createPaths;
    }

    public String getCreateFiles() {
        return createFiles;
    }

    public void setCreateFiles(String createFiles) {
        this.createFiles = createFiles;
    }

    public String getMoveEntities() {
        return moveEntities;
    }

    public void setMoveEntities(String moveEntities) {
        this.moveEntities = moveEntities;
    }
}
