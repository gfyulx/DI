package com.gfyulx.DI.flink.core.parser;

import com.gfyulx.DI.flink.core.enums.ETableType;
import com.gfyulx.DI.flink.core.table.TableInfo;
import com.gfyulx.DI.flink.core.table.TableInfoParserFactory;
import com.gfyulx.DI.flink.core.util.DtStringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;

import java.util.List;

/**
 * @ClassName:  SqlParser   
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/9 14:09
 *
 * @Copyright: 2018 gfyulx
 * 
 */  
public class SqlParser {

    private static final char SQL_DELIMITER = ';';

    private static String LOCAL_SQL_PLUGIN_ROOT;

    private static List<IParser> sqlParserList = Lists.newArrayList(CreateFuncParser.newInstance(),
            CreateTableParser.newInstance(), InsertSqlParser.newInstance());

    public static void setLocalSqlPluginRoot(String localSqlPluginRoot){
        LOCAL_SQL_PLUGIN_ROOT = localSqlPluginRoot;
    }

    /**
     * flink 支持的sql语法规范：
     * CREATE TABLE sls_stream() with ();
     * CREATE (TABLE|SCALA) FUNCTION fcnName WITH com.gfyulx.DI.com;
     * insert into tb1 select * from tb2;
     * @param sql
     */
    public static SqlTree parseSql(String sql) throws Exception {

        if(StringUtils.isBlank(sql)){
            throw new RuntimeException("sql is not null");
        }

        if(LOCAL_SQL_PLUGIN_ROOT == null){
            throw new RuntimeException("need to set local sql plugin root");
        }

        sql = sql.replaceAll("--.*", "")
                .replaceAll("\r\n", " ")
                .replaceAll("\n", " ")
                .replace("\t", " ").trim();

        List<String> sqlArr = DtStringUtil.splitIgnoreQuota(sql, SQL_DELIMITER);
        SqlTree sqlTree = new SqlTree();

        for(String childSql : sqlArr){
            if(Strings.isNullOrEmpty(childSql)){
                continue;
            }
            boolean result = false;
            for(IParser sqlParser : sqlParserList){
                if(!sqlParser.verify(childSql)){
                    continue;
                }

                sqlParser.parseSql(childSql, sqlTree);
                result = true;
            }

            if(!result){
                throw new RuntimeException(String.format("%s:Syntax does not support,the format of SQL like insert into tb1 select * from tb2.", childSql));
            }
        }

        //解析exec-sql
        if(sqlTree.getExecSqlList().size() == 0){
            throw new RuntimeException("sql no executable statement");
        }

        for(InsertSqlParser.SqlParseResult result : sqlTree.getExecSqlList()){
            List<String> sourceTableList = result.getSourceTableList();
            List<String> targetTableList = result.getTargetTableList();

            for(String tableName : sourceTableList){
                CreateTableParser.SqlParserResult createTableResult = sqlTree.getPreDealTableMap().get(tableName);
                if(createTableResult == null){
                    throw new RuntimeException("can't find table " + tableName);
                }

                TableInfo tableInfo = TableInfoParserFactory.parseWithTableType(ETableType.SOURCE.getType(),
                        createTableResult, LOCAL_SQL_PLUGIN_ROOT);
                sqlTree.addTableInfo(tableName, tableInfo);
            }

            for(String tableName : targetTableList){
                CreateTableParser.SqlParserResult createTableResult = sqlTree.getPreDealTableMap().get(tableName);
                if(createTableResult == null){
                    throw new RuntimeException("can't find table " + tableName);
                }

                TableInfo tableInfo = TableInfoParserFactory.parseWithTableType(ETableType.SINK.getType(),
                        createTableResult, LOCAL_SQL_PLUGIN_ROOT);
                sqlTree.addTableInfo(tableName, tableInfo);
            }
        }

        return sqlTree;
    }
}
