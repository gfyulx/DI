package com.gfyulx.DI.flink.core.side;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.flink.calcite.shaded.com.google.common.collect.Sets;
import org.junit.Test;

import java.util.Set;

/**
 * @ClassName:  TestSideSqlParser
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/8 14:14
 *
 * @Copyright: 2018 gfyulx
 *
 */

public class TestSideSqlParser {

    @Test
    public void testSideSqlParser() throws SqlParseException {
        String sql = "select j1.id,j2.name,j1.info \n" +
                "   from\n" +
                "   (\n" +
                "   \tselect a.id,a.name,b.id \n" +
                "   \t\tfrom tab1 a join tab2 b\n" +
                "   \t\ton a.id = b.id  and a.proctime between b.proctime - interval '4' second and b.proctime + interval '4' second  \n" +
                "   ) j1\n" +
                "   join tab3 j2\n" +
                "   on j1.id = j2.id \n" +
                "   where j1.info like 'xc2'";

        Set<String> sideTbList = Sets.newHashSet("TAB3", "TAB4");


        SideSQLParser sideSQLParser = new SideSQLParser();
        sideSQLParser.getExeQueue(sql, sideTbList);
    }


}
