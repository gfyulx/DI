
package com.gfyulx.DI.flink.core.sink;

import com.gfyulx.DI.flink.core.classloader.DtClassLoader;
import com.gfyulx.DI.flink.core.table.AbsTableParser;
import com.gfyulx.DI.flink.core.table.TargetTableInfo;
import com.gfyulx.DI.flink.core.util.PluginUtil;
import org.apache.flink.table.sinks.TableSink;

/**
 * @ClassName:  StreamSinkFactory
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/9 10:11
 *
 * @Copyright: 2018 gfyulx
 *
 */

public class StreamSinkFactory {

    public static String CURR_TYPE = "sink";

    private static final String DIR_NAME_FORMAT = "%ssink";

    public static AbsTableParser getSqlParser(String pluginType, String sqlRootDir) throws Exception {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        if(!(classLoader instanceof DtClassLoader)){
            throw new RuntimeException("it's not a correct classLoader instance, it's type must be DtClassLoader!");
        }

        DtClassLoader dtClassLoader = (DtClassLoader) classLoader;

        String pluginJarPath = PluginUtil.getJarFileDirPath(String.format(DIR_NAME_FORMAT, pluginType), sqlRootDir);

        PluginUtil.addPluginJar(pluginJarPath, dtClassLoader);
        String className = PluginUtil.getSqlParserClassName(pluginType, CURR_TYPE);
        Class<?> targetParser = dtClassLoader.loadClass(className);

        if(!AbsTableParser.class.isAssignableFrom(targetParser)){
            throw new RuntimeException("class " + targetParser.getName() + " not subClass of AbsTableParser");
        }

        return targetParser.asSubclass(AbsTableParser.class).newInstance();
    }

    public static TableSink getTableSink(TargetTableInfo targetTableInfo, String localSqlRootDir) throws Exception {

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if(!(classLoader instanceof DtClassLoader)){
            throw new RuntimeException("it's not a correct classLoader instance, it's type must be DtClassLoader!");
        }

        DtClassLoader dtClassLoader = (DtClassLoader) classLoader;

        String pluginType = targetTableInfo.getType();
        String pluginJarDirPath = PluginUtil.getJarFileDirPath(String.format(DIR_NAME_FORMAT, pluginType), localSqlRootDir);

        PluginUtil.addPluginJar(pluginJarDirPath, dtClassLoader);

        String className = PluginUtil.getGenerClassName(pluginType, CURR_TYPE);
        Class<?> sinkClass = dtClassLoader.loadClass(className);

        if(!IStreamSinkGener.class.isAssignableFrom(sinkClass)){
            throw new RuntimeException("class " + sinkClass + " not subClass of IStreamSinkGener");
        }

        IStreamSinkGener streamSinkGener = sinkClass.asSubclass(IStreamSinkGener.class).newInstance();
        Object result = streamSinkGener.genStreamSink(targetTableInfo);
        return (TableSink) result;
    }
}
