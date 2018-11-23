package com.gfyulx.DI.flink.core.side.operator;

import com.gfyulx.DI.flink.core.classloader.DtClassLoader;
import com.gfyulx.DI.flink.core.side.AsyncReqRow;
import com.gfyulx.DI.flink.core.side.FieldInfo;
import com.gfyulx.DI.flink.core.side.JoinInfo;
import com.gfyulx.DI.flink.core.side.SideTableInfo;
import com.gfyulx.DI.flink.core.util.PluginUtil;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName:  SideAsyncOperator
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/9 14:11
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class SideAsyncOperator {

    private static final String PATH_FORMAT = "%sasyncside";

    private static AsyncReqRow loadAsyncReq(String sideType, String sqlRootDir, RowTypeInfo rowTypeInfo,
                                            JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) throws Exception {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        String pathOfType = String.format(PATH_FORMAT, sideType);
        String pluginJarPath = PluginUtil.getJarFileDirPath(pathOfType, sqlRootDir);
        DtClassLoader dtClassLoader = (DtClassLoader) classLoader;
        PluginUtil.addPluginJar(pluginJarPath, dtClassLoader);
        String className = PluginUtil.getSqlSideClassName(sideType, "side", "Async");
        return dtClassLoader.loadClass(className).asSubclass(AsyncReqRow.class)
                .getConstructor(RowTypeInfo.class, JoinInfo.class, List.class, SideTableInfo.class).newInstance(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    public static DataStream getSideJoinDataStream(DataStream inputStream, String sideType, String sqlRootDir, RowTypeInfo rowTypeInfo, JoinInfo joinInfo,
                                                   List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) throws Exception {
        AsyncReqRow asyncDbReq = loadAsyncReq(sideType, sqlRootDir, rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
        //TODO How much should be set for the degree of parallelism? Timeout? capacity settings?
        return AsyncDataStream.orderedWait(inputStream, asyncDbReq, 10000, TimeUnit.MILLISECONDS, 10)
                .setParallelism(sideTableInfo.getParallelism());
    }
}
