package com.gfyulx.DI.flink.core.util;

import com.gfyulx.DI.flink.core.classloader.DtClassLoader;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerationException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParseException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Properties;

/**
 * @ClassName:  PluginUtil
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/8 10:12
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class PluginUtil {

    private static String SP = File.separator;

    private static final String JAR_SUFFIX = ".jar";

    private static final String CLASS_PRE_STR = "com.gfyulx.DI.flink.sql";

    private static ObjectMapper objectMapper = new ObjectMapper();


    public static String getJarFileDirPath(String type, String sqlRootDir){
        String jarPath = sqlRootDir + SP + type;
        File jarFile = new File(jarPath);

        if(!jarFile.exists()){
            throw new RuntimeException(String.format("path %s not exists!!!", jarPath));
        }

        return jarPath;
    }

    public static String getSideJarFileDirPath(String pluginType, String sideOperator, String tableType, String sqlRootDir) throws MalformedURLException {
        String dirName = sqlRootDir + SP + pluginType + sideOperator + tableType.toLowerCase();
        File jarFile = new File(dirName);

        if(!jarFile.exists()){
            throw new RuntimeException(String.format("path %s not exists!!!", dirName));
        }

        return dirName;
    }

    public static String getGenerClassName(String pluginTypeName, String type) throws IOException {
        String pluginClassName = upperCaseFirstChar(pluginTypeName) + upperCaseFirstChar(type);
        return CLASS_PRE_STR  + "." + type.toLowerCase() + "." + pluginTypeName + "." + pluginClassName;
    }

    public static String getSqlParserClassName(String pluginTypeName, String type){

        String pluginClassName = upperCaseFirstChar(pluginTypeName) + upperCaseFirstChar(type) +  "Parser";
        return CLASS_PRE_STR  + "." + type.toLowerCase() + "." +  pluginTypeName + ".table." + pluginClassName;
    }


    public static String getSqlSideClassName(String pluginTypeName, String type, String operatorType){
        String pluginClassName = upperCaseFirstChar(pluginTypeName) + operatorType + "ReqRow";
        return CLASS_PRE_STR  + "." + type.toLowerCase() + "." +  pluginTypeName + "." + pluginClassName;
    }

    public static Map<String,Object> ObjectToMap(Object obj) throws Exception{
        return objectMapper.readValue(objectMapper.writeValueAsBytes(obj), Map.class);
    }

    public static <T> T jsonStrToObject(String jsonStr, Class<T> clazz) throws JsonParseException, JsonMappingException, JsonGenerationException, IOException{
        return  objectMapper.readValue(jsonStr, clazz);
    }

    public static Properties stringToProperties(String str) throws IOException{
        Properties properties = new Properties();
        properties.load(new ByteArrayInputStream(str.getBytes("UTF-8")));
        return properties;
    }

    public static URL getRemoteJarFilePath(String pluginType, String tableType, String remoteSqlRootDir) throws MalformedURLException {
        String dirName = pluginType + tableType.toLowerCase();
        String jarName = String.format("%s-%s.jar", pluginType, tableType.toLowerCase());
        return new URL("file:" + remoteSqlRootDir + SP + dirName + SP + jarName);
    }

    public static URL getRemoteSideJarFilePath(String pluginType, String sideOperator, String tableType, String remoteSqlRootDir) throws MalformedURLException {
        String dirName = pluginType + sideOperator + tableType.toLowerCase();
        String jarName = String.format("%s-%s-%s.jar", pluginType, sideOperator, tableType.toLowerCase());
        return new URL("file:" + remoteSqlRootDir + SP + dirName + SP + jarName);
    }

    public static String upperCaseFirstChar(String str){
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    public static void addPluginJar(String pluginDir, DtClassLoader classLoader) throws MalformedURLException {
        File dirFile = new File(pluginDir);
        if(!dirFile.exists() || !dirFile.isDirectory()){
            throw new RuntimeException("plugin path:" + pluginDir + "is not exist.");
        }

        File[] files = dirFile.listFiles(tmpFile -> tmpFile.isFile() && tmpFile.getName().endsWith(JAR_SUFFIX));
        if(files == null || files.length == 0){
            throw new RuntimeException("plugin path:" + pluginDir + " is null.");
        }

        for(File file : files){
            URL pluginJarURL = file.toURI().toURL();
            classLoader.addURL(pluginJarURL);
        }
    }

}
