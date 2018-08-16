package com.gfyulx.DI.hadoop.service.UDF;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @ClassName: ReplaceString
 * @Description: 自定义字符串替换函数
 * @author: gfyulx
 * @date: 2018/8/15 10:17
 * @Copyright: 2018 gfyulx
 */
public class ReplaceString extends UDF {
    public Text evaluate(Text input, String srcStr, String tarStr) {
        if (srcStr == null) {
            return input;
        }
        if (tarStr == null) {
            tarStr = new String("");
        }
        return new Text(input.toString().replaceAll(srcStr, tarStr));

    }
}
