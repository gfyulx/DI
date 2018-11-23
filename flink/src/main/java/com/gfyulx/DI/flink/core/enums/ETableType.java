package com.gfyulx.DI.flink.core.enums;

/**
 * @ClassName:  ETableType
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/9 14:06
 *
 * @Copyright: 2018 gfyulx
 *
 */
public enum ETableType {
    //源表
    SOURCE(1),
    //目的表
    SINK(2);

    int type;

    ETableType(int type){
        this.type = type;
    }

    public int getType() {
        return type;
    }
}
