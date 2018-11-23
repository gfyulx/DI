package com.gfyulx.DI.flink.core.side;

import org.apache.flink.calcite.shaded.com.google.common.collect.HashBasedTable;

/**
 * @ClassName:  FieldReplaceInfo
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/8 14:12
 *
 * @Copyright: 2018 gfyulx
 *
 */

public class FieldReplaceInfo {

    private HashBasedTable<String, String, String> mappingTable;

    private String targetTableName = null;

    private String targetTableAlias = null;

    public void setMappingTable(HashBasedTable<String, String, String> mappingTable) {
        this.mappingTable = mappingTable;
    }

    public HashBasedTable<String, String, String> getMappingTable() {
        return mappingTable;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    public String getTargetTableAlias() {
        return targetTableAlias;
    }

    public void setTargetTableAlias(String targetTableAlias) {
        this.targetTableAlias = targetTableAlias;
    }
}
