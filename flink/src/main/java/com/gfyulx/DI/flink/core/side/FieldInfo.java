package com.gfyulx.DI.flink.core.side;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.Serializable;

/**
 * @ClassName:  FieldInfo
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/8 14:12
 *
 * @Copyright: 2018 gfyulx
 *
 */

public class FieldInfo implements Serializable {

    private static final long serialVersionUID = -1L;

    private String table;

    private String fieldName;

    private TypeInformation typeInformation;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public TypeInformation getTypeInformation() {
        return typeInformation;
    }

    public void setTypeInformation(TypeInformation typeInformation) {
        this.typeInformation = typeInformation;
    }
}
