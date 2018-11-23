
package com.gfyulx.DI.flink.core.side;

import com.gfyulx.DI.flink.core.enums.ECacheContentType;
import com.gfyulx.DI.flink.core.side.cache.CacheObj;

/**
 * @ClassName:  CacheMissVal
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/8 14:11
 *
 * @Copyright: 2018 gfyulx
 *
 */

public class CacheMissVal {

    private static CacheObj missObj = CacheObj.buildCacheObj(ECacheContentType.MissVal, null);

    public static CacheObj getMissKeyObj(){
        return missObj;
    }
}
