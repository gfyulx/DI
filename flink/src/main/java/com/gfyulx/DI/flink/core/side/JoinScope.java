package com.gfyulx.DI.flink.core.side;


import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * @ClassName:  JoinScope
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/8 14:12
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class JoinScope {

    private List<ScopeChild> children = Lists.newArrayList();

    private Map<String, ScopeChild> aliasMap = Maps.newHashMap();

    public void addScope(ScopeChild scopeChild){
        children.add(scopeChild);
        aliasMap.put(scopeChild.getAlias(), scopeChild);
    }

    public ScopeChild getScope(String tableAlias){
        return aliasMap.get(tableAlias);
    }

    public List<ScopeChild> getChildren() {
        return children;
    }

    public TypeInformation getFieldType(String tableName, String fieldName){
        ScopeChild scopeChild = aliasMap.get(tableName);
        if(scopeChild == null){
            throw new RuntimeException("can't find ");
        }

        RowTypeInfo rowTypeInfo = scopeChild.getRowTypeInfo();
        int index = rowTypeInfo.getFieldIndex(fieldName);
        if(index == -1){
            throw new RuntimeException("can't find field: " + fieldName);
        }

        return rowTypeInfo.getTypeAt(index);
    }

    public static class ScopeChild{

        private String alias;

        private String tableName;

        private RowTypeInfo rowTypeInfo;

        public String getAlias() {
            return alias;
        }

        public void setAlias(String alias) {
            this.alias = alias;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public RowTypeInfo getRowTypeInfo() {
            return rowTypeInfo;
        }

        public void setRowTypeInfo(RowTypeInfo rowTypeInfo) {
            this.rowTypeInfo = rowTypeInfo;
        }
    }
}
