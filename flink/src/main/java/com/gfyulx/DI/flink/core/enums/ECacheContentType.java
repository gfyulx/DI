
package com.gfyulx.DI.flink.core.enums;

/**
 * @ClassName:  ECacheContentType
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/9 14:04
 *
 * @Copyright: 2018 gfyulx
 *
 */

public enum ECacheContentType {

    MissVal(0),
    SingleLine(1),
    MultiLine(2);

    int type;

    ECacheContentType(int type){
        this.type = type;
    }

    public int getType(){
        return this.type;
    }
}
