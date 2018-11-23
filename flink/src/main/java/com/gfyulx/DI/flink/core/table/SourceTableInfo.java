package com.gfyulx.DI.flink.core.table;

import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;

import java.util.Map;

/**
 * @ClassName:  SourceTableInfo
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/9 11:18
 *
 * @Copyright: 2018 gfyulx
 *
 */

public abstract class SourceTableInfo extends TableInfo {

    public static final String SOURCE_SUFFIX = "Source";

    private String eventTimeField;

    private Integer maxOutOrderness = 10;

    private Map<String, String> virtualFields = Maps.newHashMap();

    @Override
    public boolean check() {
       return true;
    }

    public String getEventTimeField() {
        return eventTimeField;
    }

    public void setEventTimeField(String eventTimeField) {
        this.eventTimeField = eventTimeField;
    }

    public int getMaxOutOrderness() {
        return maxOutOrderness;
    }

    public void setMaxOutOrderness(Integer maxOutOrderness) {
        if(maxOutOrderness == null){
            return;
        }

        this.maxOutOrderness = maxOutOrderness;
    }

    public Map<String, String> getVirtualFields() {
        return virtualFields;
    }

    public void setVirtualFields(Map<String, String> virtualFields) {
        this.virtualFields = virtualFields;
    }

    public void addVirtualField(String fieldName, String expression){
        virtualFields.put(fieldName, expression);
    }

    public String getAdaptSelectSql(){
        String fields = String.join(",", getFields());
        String virtualFieldsStr = "";

        if(virtualFields.size() == 0){
            return null;
        }

        for(Map.Entry<String, String> entry : virtualFields.entrySet()){
            virtualFieldsStr += entry.getValue() +" AS " + entry.getKey() + ",";
        }

        if(!Strings.isNullOrEmpty(virtualFieldsStr)){
            fields += "," + virtualFieldsStr.substring(0, virtualFieldsStr.lastIndexOf(","));
        }

        return String.format("select %s from %s", fields, getAdaptName());
    }

    public String getAdaptName(){
        return getName() + "_adapt";
    }
}
