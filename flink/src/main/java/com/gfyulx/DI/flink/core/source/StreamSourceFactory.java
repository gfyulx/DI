

package com.gfyulx.DI.flink.core.source;


import com.gfyulx.DI.flink.core.classloader.DtClassLoader;
import com.gfyulx.DI.flink.core.table.AbsSourceParser;
import com.gfyulx.DI.flink.core.table.SourceTableInfo;
import com.gfyulx.DI.flink.core.util.DtStringUtil;
import com.gfyulx.DI.flink.core.util.PluginUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @ClassName:  StreamSourceFactory
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/6 9:26
 *
 * @Copyright: 2018 gfyulx
 *
 */

public class StreamSourceFactory {

    private static final String CURR_TYPE = "source";

    private static final String DIR_NAME_FORMAT = "%ssource";

    public static AbsSourceParser getSqlParser(String pluginType, String sqlRootDir) throws Exception {

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        String pluginJarPath = PluginUtil.getJarFileDirPath(String.format(DIR_NAME_FORMAT, pluginType), sqlRootDir);

        DtClassLoader dtClassLoader = (DtClassLoader) classLoader;
        PluginUtil.addPluginJar(pluginJarPath, dtClassLoader);

        String typeNoVersion = DtStringUtil.getPluginTypeWithoutVersion(pluginType);
        String className = PluginUtil.getSqlParserClassName(typeNoVersion, CURR_TYPE);
        Class<?> sourceParser = dtClassLoader.loadClass(className);
        if(!AbsSourceParser.class.isAssignableFrom(sourceParser)){
            throw new RuntimeException("class " + sourceParser.getName() + " not subClass of AbsSourceParser");
        }

        return sourceParser.asSubclass(AbsSourceParser.class).newInstance();
    }

    /**
     * The configuration of the type specified data source
     * @param sourceTableInfo
     * @return
     */
    public static Table getStreamSource(SourceTableInfo sourceTableInfo, StreamExecutionEnvironment env,
                                        StreamTableEnvironment tableEnv, String sqlRootDir) throws Exception {

        String sourceTypeStr = sourceTableInfo.getType();
        String typeNoVersion = DtStringUtil.getPluginTypeWithoutVersion(sourceTypeStr);
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        String pluginJarPath = PluginUtil.getJarFileDirPath(String.format(DIR_NAME_FORMAT, sourceTypeStr), sqlRootDir);
        String className = PluginUtil.getGenerClassName(typeNoVersion, CURR_TYPE);

        DtClassLoader dtClassLoader = (DtClassLoader) classLoader;
        PluginUtil.addPluginJar(pluginJarPath, dtClassLoader);
        Class<?> sourceClass = dtClassLoader.loadClass(className);

        if(!IStreamSourceGener.class.isAssignableFrom(sourceClass)){
            throw new RuntimeException("class " + sourceClass.getName() + " not subClass of IStreamSourceGener");
        }

        IStreamSourceGener sourceGener = sourceClass.asSubclass(IStreamSourceGener.class).newInstance();
        Object object = sourceGener.genStreamSource(sourceTableInfo, env, tableEnv);
        return (Table) object;
    }
}
