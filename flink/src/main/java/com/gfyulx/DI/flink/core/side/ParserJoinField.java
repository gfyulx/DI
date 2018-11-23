package com.gfyulx.DI.flink.core.side;

import org.apache.calcite.sql.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;

/**
 * @ClassName:  ParserJoinField
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/8 14:12
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class ParserJoinField {

    /**
     * Need to parse the fields of information and where selectlist
     * @return
     */
    public static List<FieldInfo> getRowTypeInfo(SqlNode sqlNode, JoinScope scope, boolean getAll){

        if(sqlNode.getKind() != SqlKind.SELECT){
            throw new RuntimeException("------not select node--------\n" + sqlNode.toString());
        }

        List<FieldInfo> fieldInfoList = Lists.newArrayList();
        if(getAll){
            return getAllField(scope);
        }

        SqlSelect sqlSelect = (SqlSelect)sqlNode;
        SqlNodeList sqlNodeList = sqlSelect.getSelectList();
        for(SqlNode fieldNode : sqlNodeList.getList()){
            SqlIdentifier identifier = (SqlIdentifier)fieldNode;
            if(!identifier.isStar()) {
                System.out.println(identifier);
                String tableName = identifier.getComponent(0).getSimple();
                String fieldName = identifier.getComponent(1).getSimple();
                TypeInformation<?> type = scope.getFieldType(tableName, fieldName);
                FieldInfo fieldInfo = new FieldInfo();
                fieldInfo.setTable(tableName);
                fieldInfo.setFieldName(fieldName);
                fieldInfo.setTypeInformation(type);
                fieldInfoList.add(fieldInfo);
            } else {
                //处理
                System.out.println("----------");
                int identifierSize = identifier.names.size();

                switch(identifierSize) {
                    case 1:
                        fieldInfoList.addAll(getAllField(scope));
                    default:
                        SqlIdentifier tableIdentify = identifier.skipLast(1);
                        JoinScope.ScopeChild scopeChild = scope.getScope(tableIdentify.getSimple());
                        if(scopeChild == null){
                            throw new RuntimeException("can't find table alias " + tableIdentify.getSimple());
                        }

                        RowTypeInfo field = scopeChild.getRowTypeInfo();
                        String[] fieldNames = field.getFieldNames();
                        TypeInformation<?>[] types = field.getFieldTypes();
                        for(int i=0; i< field.getTotalFields(); i++){
                            String fieldName = fieldNames[i];
                            TypeInformation<?> type = types[i];
                            FieldInfo fieldInfo = new FieldInfo();
                            fieldInfo.setTable(tableIdentify.getSimple());
                            fieldInfo.setFieldName(fieldName);
                            fieldInfo.setTypeInformation(type);
                            fieldInfoList.add(fieldInfo);
                        }
                }
            }
        }

        return fieldInfoList;
    }

    private static List<FieldInfo> getAllField(JoinScope scope){
        Iterator prefixId = scope.getChildren().iterator();
        List<FieldInfo> fieldInfoList = Lists.newArrayList();
        while(true) {
            JoinScope.ScopeChild resolved;
            RowTypeInfo field;
            if(!prefixId.hasNext()) {
                return fieldInfoList;
            }

            resolved = (JoinScope.ScopeChild)prefixId.next();
            field = resolved.getRowTypeInfo();
            String[] fieldNames = field.getFieldNames();
            TypeInformation<?>[] types = field.getFieldTypes();
            for(int i=0; i< field.getTotalFields(); i++){
                String fieldName = fieldNames[i];
                TypeInformation<?> type = types[i];
                FieldInfo fieldInfo = new FieldInfo();
                fieldInfo.setTable(resolved.getAlias());
                fieldInfo.setFieldName(fieldName);
                fieldInfo.setTypeInformation(type);
                fieldInfoList.add(fieldInfo);
            }
        }
    }

}
