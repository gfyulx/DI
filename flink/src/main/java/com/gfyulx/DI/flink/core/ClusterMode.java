package com.gfyulx.DI.flink.core;

/**
 * @ClassName:  ClusterMode
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/9 9:24
 *
 * @Copyright: 2018 gfyulx
 *
 */
public enum ClusterMode {

    local(0),standalone(1),yarn(2),yarnPer(3);

    private int type;

    ClusterMode(int type){
        this.type = type;
    }

}
