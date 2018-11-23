package com.gfyulx.DI.flink.core.table;

import com.gfyulx.DI.flink.core.util.ClassUtil;
import com.gfyulx.DI.flink.core.util.DtStringUtil;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ClassName:  AbsTableParser
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/9 11:17
 *
 * @Copyright: 2018 gfyulx
 *
 */

public abstract class AbsTableParser {

    private static final String PRIMARY_KEY = "primaryKey";

    private static Pattern primaryKeyPattern = Pattern.compile("(?i)PRIMARY\\s+KEY\\s*\\((.*)\\)");

    public static Map<String, Pattern> keyPatternMap = Maps.newHashMap();

    public static Map<String, ITableFieldDealHandler> keyHandlerMap = Maps.newHashMap();

    static {
        keyPatternMap.put(PRIMARY_KEY, primaryKeyPattern);
        keyHandlerMap.put(PRIMARY_KEY, AbsTableParser::dealPrimaryKey);
    }

    protected boolean fieldNameNeedsUpperCase() {
        return true;
    }

    public abstract TableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props);

    public boolean dealKeyPattern(String fieldRow, TableInfo tableInfo){
        for(Map.Entry<String, Pattern> keyPattern : keyPatternMap.entrySet()){
            Pattern pattern = keyPattern.getValue();
            String key = keyPattern.getKey();
            Matcher matcher = pattern.matcher(fieldRow);
            if(matcher.find()){
                ITableFieldDealHandler handler = keyHandlerMap.get(key);
                if(handler == null){
                    throw new RuntimeException("parse field [" + fieldRow + "] error.");
                }

                handler.dealPrimaryKey(matcher, tableInfo);
                return true;
            }
        }

        return false;
    }

    public void parseFieldsInfo(String fieldsInfo, TableInfo tableInfo){

        String[] fieldRows = DtStringUtil.splitIgnoreQuotaBrackets(fieldsInfo, ",");
        for(String fieldRow : fieldRows){
            fieldRow = fieldRow.trim();
            if(fieldNameNeedsUpperCase()) {
                fieldRow = fieldRow.toUpperCase();
            }

            boolean isMatcherKey = dealKeyPattern(fieldRow, tableInfo);

            if(isMatcherKey){
                continue;
            }

            String[] filedInfoArr = fieldRow.split("\\s+");
            if(filedInfoArr.length < 2){
                throw new RuntimeException(String.format("table [%s] field [%s] format error.", tableInfo.getName(), fieldRow));
            }

            //Compatible situation may arise in space in the fieldName
            String[] filedNameArr = new String[filedInfoArr.length - 1];
            System.arraycopy(filedInfoArr, 0, filedNameArr, 0, filedInfoArr.length - 1);
            String fieldName = String.join(" ", filedNameArr);
            String fieldType = filedInfoArr[filedInfoArr.length - 1 ].trim();
            Class fieldClass = ClassUtil.stringConvertClass(fieldType);

            tableInfo.addField(fieldName);
            tableInfo.addFieldClass(fieldClass);
            tableInfo.addFieldType(fieldType);
        }

        tableInfo.finish();
    }

    public static void dealPrimaryKey(Matcher matcher, TableInfo tableInfo){
        String primaryFields = matcher.group(1);
        String[] splitArry = primaryFields.split(",");
        List<String> primaryKes = Lists.newArrayList(splitArry);
        tableInfo.setPrimaryKeys(primaryKes);
    }
}
