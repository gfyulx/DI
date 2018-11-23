package com.gfyulx.DI.flink.core.table;

import com.gfyulx.DI.flink.core.enums.ETableType;
import com.gfyulx.DI.flink.core.parser.CreateTableParser;
import com.gfyulx.DI.flink.core.side.SideTableInfo;
import com.gfyulx.DI.flink.core.side.StreamSideFactory;
import com.gfyulx.DI.flink.core.sink.StreamSinkFactory;
import com.gfyulx.DI.flink.core.source.StreamSourceFactory;
import com.gfyulx.DI.flink.core.util.MathUtil;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Maps;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ClassName:  TableInfoParserFactory
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/9 11:18
 *
 * @Copyright: 2018 gfyulx
 *
 */

public class TableInfoParserFactory {

    private final static String TYPE_KEY = "type";

    private final static String SIDE_TABLE_SIGN = "(?i)^PERIOD\\s+FOR\\s+SYSTEM_TIME$";

    private final static Pattern SIDE_PATTERN = Pattern.compile(SIDE_TABLE_SIGN);

    private static Map<String, AbsTableParser> sourceTableInfoMap = Maps.newConcurrentMap();

    private static Map<String, AbsTableParser> targetTableInfoMap = Maps.newConcurrentMap();

    private static Map<String, AbsTableParser> sideTableInfoMap = Maps.newConcurrentMap();

    //Parsing loaded plugin
    public static TableInfo parseWithTableType(int tableType, CreateTableParser.SqlParserResult parserResult,
                                               String localPluginRoot) throws Exception {
        AbsTableParser absTableParser = null;
        Map<String, Object> props = parserResult.getPropMap();
        String type = MathUtil.getString(props.get(TYPE_KEY));

        if(Strings.isNullOrEmpty(type)){
            throw new RuntimeException("create table statement requires property of type");
        }

        if(tableType == ETableType.SOURCE.getType()){
            boolean isSideTable = checkIsSideTable(parserResult.getFieldsInfoStr());

            if(!isSideTable){
                absTableParser = sourceTableInfoMap.get(type);
                if(absTableParser == null){
                    absTableParser = StreamSourceFactory.getSqlParser(type, localPluginRoot);
                    sourceTableInfoMap.put(type, absTableParser);
                }
            }else{
                absTableParser = sideTableInfoMap.get(type);
                if(absTableParser == null){
                    String cacheType = MathUtil.getString(props.get(SideTableInfo.CACHE_KEY));
                    absTableParser = StreamSideFactory.getSqlParser(type, localPluginRoot, cacheType);
                    sideTableInfoMap.put(type, absTableParser);
                }
            }

        }else if(tableType == ETableType.SINK.getType()){
            absTableParser = targetTableInfoMap.get(type);
            if(absTableParser == null){
                absTableParser = StreamSinkFactory.getSqlParser(type, localPluginRoot);
                targetTableInfoMap.put(type, absTableParser);
            }
        }

        if(absTableParser == null){
            throw new RuntimeException(String.format("not support %s type of table", type));
        }

        Map<String, Object> prop = Maps.newHashMap();

        //Shield case
        parserResult.getPropMap().forEach((key,val) -> prop.put(key.toLowerCase(), val));

        return absTableParser.getTableInfo(parserResult.getTableName(), parserResult.getFieldsInfoStr(), prop);
    }

    /**
     * judge dim table of PERIOD FOR SYSTEM_TIME
     * @param tableField
     * @return
     */
    private static boolean checkIsSideTable(String tableField){
        String[] fieldInfos = tableField.split(",");
        for(String field : fieldInfos){
            Matcher matcher = SIDE_PATTERN.matcher(field.trim());
            if(matcher.find()){
                return true;
            }
        }

        return false;
    }
}
