package com.gfyulx.DI.flink.service.stream;

public enum EnumType {

        STRING, VARCHAR, CHAR,
        INT, MEDIUMINT, TINYINT, DATETIME, SMALLINT, BIGINT,
        DOUBLE, FLOAT,
        BOOLEAN,
        DATE, TIMESTAMP, DECIMAL;

        public static EnumType fromString(String type) {
            if(type == null) {
                throw new RuntimeException("null ColumnType!");
            }

            if(type.toUpperCase().startsWith("DECIMAL")) {
                return DECIMAL;
            }

            return valueOf(type.toUpperCase());
        }

}
