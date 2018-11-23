package com.gfyulx.DI.flink.core.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ClassName:  CreateFuncParser
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/9 14:06
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class CreateFuncParser implements IParser {

    private static final String funcPatternStr = "(?i)\\s*create\\s+(scala|table)\\s+function\\s+(\\S+)\\s+WITH\\s+(\\S+)";

    private static final Pattern funcPattern = Pattern.compile(funcPatternStr);

    @Override
    public boolean verify(String sql) {
        return funcPattern.matcher(sql).find();
    }

    @Override
    public void parseSql(String sql, SqlTree sqlTree) {
        Matcher matcher = funcPattern.matcher(sql);
        if(matcher.find()){
            String type = matcher.group(1);
            String funcName = matcher.group(2);
            String className = matcher.group(3);
            SqlParserResult result = new SqlParserResult();
            result.setType(type);
            result.setName(funcName);
            result.setClassName(className);
            sqlTree.addFunc(result);
        }
    }


    public static CreateFuncParser newInstance(){
        return new CreateFuncParser();
    }

    public static class SqlParserResult{

        private String name;

        private String className;

        private String type;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getClassName() {
            return className;
        }

        public void setClassName(String className) {
            this.className = className;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }


}
