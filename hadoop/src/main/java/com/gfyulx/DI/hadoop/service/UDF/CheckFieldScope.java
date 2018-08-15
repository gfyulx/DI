package com.gfyulx.DI.hadoop.service.UDF;

/**
 * @ClassName: CheckFieldScope
 * @Description: UDF检测某个字段的值域范围
 * @author: gfyulx
 * @date: 2018/8/15 9:54
 * @Copyright: 2018 gfyulx
 */

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.ArrayList;
import java.util.List;

public class CheckFieldScope extends GenericUDF {

    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("CheckFieldScope need two arguments");
        }
        ObjectInspector arg0 = arguments[0];
        ObjectInspector arg1 = arguments[1];
        if (!(arg0 instanceof StringObjectInspector) || (!(arg1 instanceof StringObjectInspector) && !(arg1 instanceof ListObjectInspector))) {
            throw new UDFArgumentException("first argument must be a string, second argument must be a list/array");
        }
        if (arg1 instanceof ListObjectInspector) {
            ListObjectInspector listObj = (ListObjectInspector) arg1;
            if (!(listObj.getListElementObjectInspector() instanceof StringObjectInspector)) {
                throw new UDFArgumentException("second argument must be a list of strings");
            }
        }

        //返回bool型结果。
        return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
    }

    public Object evaluate(DeferredObject[] args) throws HiveException {
        StringObjectInspector elementIO;

        elementIO = (StringObjectInspector) args[0];
        String checkField = elementIO.getPrimitiveJavaObject(args[0].get());
        List<String> list = new ArrayList<String>();
        if (args[1] instanceof StringObjectInspector) {
            StringObjectInspector elementArg1 = (StringObjectInspector) args[1];
            list.add(elementArg1.getPrimitiveJavaObject(args[1].get()));
        } else if (args[1] instanceof ListObjectInspector) {
            ListObjectInspector listIO;
            listIO = (ListObjectInspector) args[1];
            list = (List<String>) listIO.getList(args[1].get());
        }
        if ((checkField == null) || (list == null)) {
            return new Boolean(true);
        }
        int checkFlag = 0;
        for (String scope : list) {
            if (checkField.equals(scope)) {
                checkFlag = 1;
            }
        }
        return checkFlag == 1 ? new Boolean(true) : new Boolean(false);
    }

    @Override
    public String getDisplayString(String[] msg) {
        return ("CheckFidldScope,check the field value scope ");
    }


}
