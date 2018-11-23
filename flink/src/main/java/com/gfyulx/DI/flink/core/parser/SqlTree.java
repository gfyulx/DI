package com.gfyulx.DI.flink.core.parser;


import com.gfyulx.DI.flink.core.table.TableInfo;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Maps;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * @ClassName:  SqlTree
 * @Description: sql语法树，包含sql语句，sql执行结果
 * @author: gfyulx
 * @date:   2018/11/9 14:07
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class SqlTree {

    private List<CreateFuncParser.SqlParserResult> functionList = Lists.newArrayList();

    private Map<String, CreateTableParser.SqlParserResult> preDealTableMap = Maps.newHashMap();

    private Map<String, TableInfo> tableInfoMap = Maps.newLinkedHashMap();

    private List<InsertSqlParser.SqlParseResult> execSqlList = Lists.newArrayList();

    public List<CreateFuncParser.SqlParserResult> getFunctionList() {
        return functionList;
    }

    public Map<String, CreateTableParser.SqlParserResult> getPreDealTableMap() {
        return preDealTableMap;
    }

    public List<InsertSqlParser.SqlParseResult> getExecSqlList() {
        return execSqlList;
    }

    public void addFunc(CreateFuncParser.SqlParserResult func){
        functionList.add(func);
    }

    public void addPreDealTableInfo(String tableName, CreateTableParser.SqlParserResult table){
        preDealTableMap.put(tableName, table);
    }

    public void addExecSql(InsertSqlParser.SqlParseResult execSql){
        execSqlList.add(execSql);
    }

    public Map<String, TableInfo> getTableInfoMap() {
        return tableInfoMap;
    }

    public void addTableInfo(String tableName, TableInfo tableInfo){
        tableInfoMap.put(tableName, tableInfo);
    }
}
