
package com.gfyulx.DI.flink.core.sink;

import com.gfyulx.DI.flink.core.table.TargetTableInfo;

/**
 * @ClassName:  IStreamSinkGener   
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/9 14:16
 *
 * @Copyright: 2018 gfyulx
 * 
 */  
public interface IStreamSinkGener<T> {

    T genStreamSink(TargetTableInfo targetTableInfo);
}
