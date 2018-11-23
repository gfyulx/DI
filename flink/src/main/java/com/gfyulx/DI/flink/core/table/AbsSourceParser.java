package com.gfyulx.DI.flink.core.table;

import com.gfyulx.DI.flink.core.util.MathUtil;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ClassName:  AbsSourceParser
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/9 11:17
 *
 * @Copyright: 2018 gfyulx
 *
 */

public abstract class AbsSourceParser extends AbsTableParser {

    private static final String VIRTUAL_KEY = "virtualFieldKey";

    private static final String WATERMARK_KEY = "waterMarkKey";

    private static Pattern virtualFieldKeyPattern = Pattern.compile("(?i)^(\\S+\\([^\\)]+\\))\\s+AS\\s+(\\w+)$");

    private static Pattern waterMarkKeyPattern = Pattern.compile("(?i)^\\s*WATERMARK\\s+FOR\\s+(\\S+)\\s+AS\\s+withOffset\\(\\s*(\\S+)\\s*,\\s*(\\d+)\\s*\\)$");

    static {
        keyPatternMap.put(VIRTUAL_KEY, virtualFieldKeyPattern);
        keyPatternMap.put(WATERMARK_KEY, waterMarkKeyPattern);

        keyHandlerMap.put(VIRTUAL_KEY, AbsSourceParser::dealVirtualField);
        keyHandlerMap.put(WATERMARK_KEY, AbsSourceParser::dealWaterMark);
    }

    static void dealVirtualField(Matcher matcher, TableInfo tableInfo){
        SourceTableInfo sourceTableInfo = (SourceTableInfo) tableInfo;
        String fieldName = matcher.group(2);
        String expression = matcher.group(1);
        sourceTableInfo.addVirtualField(fieldName, expression);
    }

    static void dealWaterMark(Matcher matcher, TableInfo tableInfo){
        SourceTableInfo sourceTableInfo = (SourceTableInfo) tableInfo;
        String eventTimeField = matcher.group(1);
        //FIXME Temporarily resolve the second parameter row_time_field
        Integer offset = MathUtil.getIntegerVal(matcher.group(3));
        sourceTableInfo.setEventTimeField(eventTimeField);
        sourceTableInfo.setMaxOutOrderness(offset);
    }
}
