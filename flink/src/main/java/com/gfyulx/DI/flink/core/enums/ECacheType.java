package com.gfyulx.DI.flink.core.enums;

/**
 * @ClassName:  ECacheType
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/9 14:06
 *
 * @Copyright: 2018 gfyulx
 *
 */
public enum ECacheType {
    NONE, LRU, ALL;

    public static boolean isValid(String type){
        for(ECacheType tmpType : ECacheType.values()){
            if(tmpType.name().equalsIgnoreCase(type)){
                return true;
            }
        }

        return false;
    }
}
