
package com.gfyulx.DI.flink.core.side.cache;

import com.gfyulx.DI.flink.core.enums.ECacheContentType;

/**
 * @ClassName:  CacheObj
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/9 14:09
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class CacheObj {

    private ECacheContentType type;

    private Object content;

    private CacheObj(ECacheContentType type, Object content){
        this.type = type;
        this.content = content;
    }

    public static CacheObj buildCacheObj(ECacheContentType type, Object content){
        return new CacheObj(type, content);
    }

    public ECacheContentType getType() {
        return type;
    }

    public void setType(ECacheContentType type) {
        this.type = type;
    }

    public Object getContent() {
        return content;
    }

    public void setContent(Object content) {
        this.content = content;
    }
}
