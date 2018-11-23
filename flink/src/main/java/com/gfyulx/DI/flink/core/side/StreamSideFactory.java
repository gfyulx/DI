package com.gfyulx.DI.flink.core.side;

import com.gfyulx.DI.flink.core.classloader.DtClassLoader;
import com.gfyulx.DI.flink.core.enums.ECacheType;
import com.gfyulx.DI.flink.core.table.AbsSideTableParser;
import com.gfyulx.DI.flink.core.table.AbsTableParser;
import com.gfyulx.DI.flink.core.util.PluginUtil;

/**
 * @ClassName:  StreamSideFactory
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/8 14:13
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class StreamSideFactory {

    private static final String CURR_TYPE = "side";

    public static AbsTableParser getSqlParser(String pluginType, String sqlRootDir, String cacheType) throws Exception {

        String sideOperator = ECacheType.ALL.name().equals(cacheType) ? "all" : "async";
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        String pluginJarPath = PluginUtil.getSideJarFileDirPath(pluginType, sideOperator, "side", sqlRootDir);

        DtClassLoader dtClassLoader = (DtClassLoader) classLoader;
        PluginUtil.addPluginJar(pluginJarPath, dtClassLoader);
        String className = PluginUtil.getSqlParserClassName(pluginType, CURR_TYPE);

        Class<?> sideParser = dtClassLoader.loadClass(className);
        if(!AbsSideTableParser.class.isAssignableFrom(sideParser)){
            throw new RuntimeException("class " + sideParser.getName() + " not subClass of AbsSideTableParser");
        }

        return sideParser.asSubclass(AbsTableParser.class).newInstance();
    }
}
