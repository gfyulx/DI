package com.gfyulx.DI.flink.core.side.operator;

import com.gfyulx.DI.flink.core.classloader.DtClassLoader;
import com.gfyulx.DI.flink.core.side.AllReqRow;
import com.gfyulx.DI.flink.core.side.FieldInfo;
import com.gfyulx.DI.flink.core.side.JoinInfo;
import com.gfyulx.DI.flink.core.side.SideTableInfo;
import com.gfyulx.DI.flink.core.util.PluginUtil;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;

/**
 * @ClassName:  SideWithAllCacheOperator
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/9 14:12
 *
 * @Copyright: 2018 gfyulx
 *
 */

public class SideWithAllCacheOperator {

    private static final String PATH_FORMAT = "%sallside";

    private static AllReqRow loadFlatMap(String sideType, String sqlRootDir, RowTypeInfo rowTypeInfo,
                                         JoinInfo joinInfo, List<FieldInfo> outFieldInfoList,
                                         SideTableInfo sideTableInfo) throws Exception {

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        String pathOfType = String.format(PATH_FORMAT, sideType);
        String pluginJarPath = PluginUtil.getJarFileDirPath(pathOfType, sqlRootDir);

        DtClassLoader dtClassLoader = (DtClassLoader) classLoader;
        PluginUtil.addPluginJar(pluginJarPath, dtClassLoader);
        String className = PluginUtil.getSqlSideClassName(sideType, "side", "All");

        return dtClassLoader.loadClass(className).asSubclass(AllReqRow.class).getConstructor(RowTypeInfo.class, JoinInfo.class, List.class, SideTableInfo.class)
                .newInstance(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);


    }

    public static DataStream getSideJoinDataStream(DataStream inputStream, String sideType, String sqlRootDir, RowTypeInfo rowTypeInfo, JoinInfo joinInfo,
                                                   List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) throws Exception {
        AllReqRow allReqRow = loadFlatMap(sideType, sqlRootDir, rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
        return inputStream.flatMap(allReqRow);
    }
}
