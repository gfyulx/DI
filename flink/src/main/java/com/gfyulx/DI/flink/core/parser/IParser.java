
package com.gfyulx.DI.flink.core.parser;

/**
 * @ClassName:  IParser   
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/9 14:09
 *
 * @Copyright: 2018 gfyulx
 * 
 */  

public interface IParser {

    /**
     * 是否满足该解析类型
     * @param sql
     * @return
     */
    boolean verify(String sql);

    /***
     * 解析sql
     * @param sql
     * @param sqlTree
     */
    void parseSql(String sql, SqlTree sqlTree);
}
