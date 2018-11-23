package com.gfyulx.DI.flink.core.util;

import com.gfyulx.DI.flink.core.enums.ColumnType;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ClassName:  DtStringUtil   
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/9 14:59
 *
 * @Copyright: 2018 gfyulx
 * 
 */  
public class DtStringUtil {

    private static final Pattern NO_VERSION_PATTERN = Pattern.compile("([a-zA-Z]+).*");

    /**
     * Split the specified string delimiter --- ignored quotes delimiter
     * @param str
     * @param delimiter
     * @return
     */
    public static List<String> splitIgnoreQuota(String str, char delimiter){
        List<String> tokensList = new ArrayList<>();
        boolean inQuotes = false;
        boolean inSingleQuotes = false;
        StringBuilder b = new StringBuilder();
        for (char c : str.toCharArray()) {
            if(c == delimiter){
                if (inQuotes) {
                    b.append(c);
                } else if(inSingleQuotes){
                    b.append(c);
                }else {
                    tokensList.add(b.toString());
                    b = new StringBuilder();
                }
            }else if(c == '\"'){
                inQuotes = !inQuotes;
                b.append(c);
            }else if(c == '\''){
                inSingleQuotes = !inSingleQuotes;
                b.append(c);
            }else{
                b.append(c);
            }
        }

        tokensList.add(b.toString());

        return tokensList;
    }

    /***
     * Split the specified string delimiter --- ignored in brackets and quotation marks delimiter
     * @param str
     * @param delimter
     * @return
     */
    public static String[] splitIgnoreQuotaBrackets(String str, String delimter){
        String splitPatternStr = delimter + "(?![^()]*+\\))(?![^{}]*+})(?![^\\[\\]]*+\\])(?=(?:[^\"]|\"[^\"]*\")*$)";
        return str.split(splitPatternStr);
    }

    public static String replaceIgnoreQuota(String str, String oriStr, String replaceStr){
        String splitPatternStr = oriStr + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)(?=(?:[^']*'[^']*')*[^']*$)";
        return str.replaceAll(splitPatternStr, replaceStr);
    }


    public static String col2string(Object column, String type) {
        String rowData = column.toString();
        ColumnType columnType = ColumnType.valueOf(type.toUpperCase());
        Object result = null;
        switch (columnType) {
            case TINYINT:
                result = Byte.valueOf(rowData);
                break;
            case SMALLINT:
                result = Short.valueOf(rowData);
                break;
            case INT:
                result = Integer.valueOf(rowData);
                break;
            case BIGINT:
                result = Long.valueOf(rowData);
                break;
            case FLOAT:
                result = Float.valueOf(rowData);
                break;
            case DOUBLE:
                result = Double.valueOf(rowData);
                break;
            case DECIMAL:
                result = new BigDecimal(rowData);
                break;
            case STRING:
            case VARCHAR:
            case CHAR:
                result = rowData;
                break;
            case BOOLEAN:
                result = Boolean.valueOf(rowData);
                break;
            case DATE:
                result = DateUtil.dateToString((java.util.Date)column);
                break;
            case TIMESTAMP:
                result = DateUtil.timestampToString((java.util.Date)column);
                break;
            default:
                throw new IllegalArgumentException();
        }
        return result.toString();
    }

    public static String getPluginTypeWithoutVersion(String engineType){

        Matcher matcher = NO_VERSION_PATTERN.matcher(engineType);
        if(!matcher.find()){
            return engineType;
        }

        return matcher.group(1);
    }

    /**
     * add specify params to dbUrl
     * @param dbUrl
     * @param addParams
     * @param isForce true:replace exists param
     * @return
     */
    public static String addJdbcParam(String dbUrl, Map<String, String> addParams, boolean isForce){

        if(Strings.isNullOrEmpty(dbUrl)){
            throw new RuntimeException("dburl can't be empty string, please check it.");
        }

        if(addParams == null || addParams.size() == 0){
            return dbUrl;
        }

        String[] splits = dbUrl.split("\\?");
        String preStr = splits[0];
        Map<String, String> params = Maps.newHashMap();
        if(splits.length > 1){
            String existsParamStr = splits[1];
            String[] existsParams = existsParamStr.split("&");
            for(String oneParam : existsParams){
                String[] kv = oneParam.split("=");
                if(kv.length != 2){
                    throw new RuntimeException("illegal dbUrl:" + dbUrl);
                }

                params.put(kv[0], kv[1]);
            }
        }

        for(Map.Entry<String, String> addParam : addParams.entrySet()){
            if(!isForce && params.containsKey(addParam.getKey())){
                continue;
            }

            params.put(addParam.getKey(), addParam.getValue());
        }

        //rebuild dbURL
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        for(Map.Entry<String, String> param : params.entrySet()){
            if(!isFirst){
                sb.append("&");
            }

            sb.append(param.getKey()).append("=").append(param.getValue());
            isFirst = false;
        }

        return preStr + "?" + sb.toString();
    }
}
