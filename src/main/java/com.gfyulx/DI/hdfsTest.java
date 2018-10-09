package com.gfyulx.DI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.util.UUID;

public class hdfsTest {
    public static void main(String agrs[]) throws Exception {
        System.setProperty("HADOOP_CONF_DIR", "D:\\code\\java\\gfyulx\\DI\\hadoop\\src\\main\\resources");
        System.setProperty("HADOOP_USER_NAME", "root");
        System.setProperty("hdp.version", "2.6.1.0-129");
        Configuration conf = new Configuration();
        File file = new File("D:\\code\\java\\gfyulx\\DI\\hadoop\\src\\main\\resources\\core-site.xml");
        URL url1 = file.toURI().toURL();
        conf.addResource(url1);
        file = new File("D:\\code\\java\\gfyulx\\DI\\hadoop\\src\\main\\resources\\hdfs-site.xml");
        url1 = file.toURI().toURL();
        conf.addResource(url1);

        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
        boolean accessible = method.isAccessible();
        if (accessible == false) {
            method.setAccessible(true);
        }

        URLClassLoader classLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
        Path jarPath = new Path("hdfs://fj-c7-188.linewell.com:8020/user/root/data/collageCount.jar");
        URL url = jarPath.toUri().toURL();
        System.out.println(url.toString());
        // method.invoke(classLoader, url);
        /*
        ClassLoader cL = Thread.currentThread().getContextClassLoader();
        Class localLoadClass = cL.loadClass("com.gfyulx.DI.hadoop.service.MapCollageCount");
        System.out.println("local:" + localLoadClass);
        try {
            Class localLoadClass1 = classLoader.loadClass("com.gfyulx.DI.hadoop.service.MapCollageCount");
            System.out.println("local:" + localLoadClass1);
        }catch (Exception e){
            System.out.println(e);
        }
        */
       /*
        Class mapClass = Class.forName("com.gfyulx.DI.hadoop.service.MapCollageCount",false,URLClassLoader.getSystemClassLoader());
        Class testClass = Class.forName("java.net.URL",false,URLClassLoader.getSystemClassLoader());
        System.out.println(testClass.getName());
        */
        /**临时将jar包下载到本地的临时文件夹，并加载**/
        String s = UUID.randomUUID().toString();

        String projectPath = System.getProperty("user.dir");
        String os = System.getProperty("os.name");
        //System.out.println(os);
        if (os.toLowerCase().startsWith("win")) {
            projectPath.replaceAll("\\\\", "/");
            projectPath = projectPath + "/" + s;
        } else {
            projectPath = projectPath + "\\" + s;
        }
        File dirHandler = new File(projectPath);
        dirHandler.mkdir();
        System.out.println(projectPath);
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream inputStream = fs.open(jarPath);
        FileOutputStream localFile = new FileOutputStream(projectPath + "\\"+jarPath.getName());
        IOUtils.copy(inputStream, localFile);
        System.out.println(localFile.toString());

        File jarFile=new File(projectPath+"\\"+jarPath.getName());
        URL urlLocal = jarFile.toURI().toURL();
        method.invoke(classLoader, url);
        Class mapClass = Class.forName("com.gfyulx.DI.hadoop.service.MapCollageCount",false,URLClassLoader.getSystemClassLoader());
        System.out.println(mapClass.getName());
        deleteFile(dirHandler);
        System.out.println(mapClass.getName());

    }

    private static void deleteFile(File file) {
        if (file.exists()) {//判断文件是否存在
            if (file.isFile()) {//判断是否是文件
                file.delete();//删除文件
            } else if (file.isDirectory()) {//否则如果它是一个目录
                File[] files = file.listFiles();//声明目录下所有的文件 files[];
                for (int i = 0; i < files.length; i++) {//遍历目录下所有的文件
                    files[i].delete();//把每个文件用这个方法进行迭代
                }
                file.delete();//删除文件夹
            }
        } else {
            System.out.println("所删除的文件不存在");
        }
    }
}
