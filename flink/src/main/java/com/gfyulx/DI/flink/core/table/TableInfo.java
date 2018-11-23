package com.gfyulx.DI.flink.core.table;

import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;

import java.util.List;

/**
 * @ClassName:  TableInfo
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/9 11:18
 *
 * @Copyright: 2018 gfyulx
 *
 */

public abstract class TableInfo {

    public static final String PARALLELISM_KEY = "parallelism";

    private String name;

    private String type;

    private String[] fields;

    private String[] fieldTypes;

    private Class<?>[] fieldClasses;

    private final List<String> fieldList = Lists.newArrayList();

    private final List<String> fieldTypeList = Lists.newArrayList();

    private final List<Class> fieldClassList = Lists.newArrayList();

    private List<String> primaryKeys;

    private Integer parallelism = 1;

    public String[] getFieldTypes() {
        return fieldTypes;
    }

    public abstract boolean check();

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String[] getFields() {
        return fields;
    }

    public Class<?>[] getFieldClasses() {
        return fieldClasses;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public void setParallelism(Integer parallelism) {
        if(parallelism == null){
            return;
        }

        if(parallelism <= 0){
            throw new RuntimeException("Abnormal parameter settings: parallelism > 0");
        }

        this.parallelism = parallelism;
    }

    public void addField(String fieldName){
        fieldList.add(fieldName);
    }

    public void addFieldClass(Class fieldClass){
        fieldClassList.add(fieldClass);
    }

    public void addFieldType(String fieldType){
        fieldTypeList.add(fieldType);
    }


    public void finish(){
        this.fields = fieldList.toArray(new String[fieldList.size()]);
        this.fieldClasses = fieldClassList.toArray(new Class[fieldClassList.size()]);
        this.fieldTypes = fieldTypeList.toArray(new String[fieldTypeList.size()]);
    }
}
