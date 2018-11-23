package com.gfyulx.DI.flink.core.enums;

/**
 * @ClassName:  ColumnType
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/9 14:04
 *
 * @Copyright: 2018 gfyulx
 *
 */



public enum ColumnType {
    STRING, VARCHAR, CHAR,
    INT, MEDIUMINT, TINYINT, DATETIME, SMALLINT, BIGINT,
    DOUBLE, FLOAT,
    BOOLEAN,
    DATE, TIMESTAMP, DECIMAL;

    public static ColumnType fromString(String type) {
        if(type == null) {
            throw new RuntimeException("null ColumnType!");
        }

        if(type.toUpperCase().startsWith("DECIMAL")) {
            return DECIMAL;
        }

        return valueOf(type.toUpperCase());
    }

}
