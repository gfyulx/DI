package com.gfyulx.DI.flink.core.table;

import java.util.regex.Matcher;

/**
 * @ClassName:  ITableFieldDealHandler
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/9 11:17
 *
 * @Copyright: 2018 gfyulx
 *
 */
public interface ITableFieldDealHandler {

    void dealPrimaryKey(Matcher matcher, TableInfo tableInfo);
}
