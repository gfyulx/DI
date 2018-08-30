package com.gfyulx.DI.hadoop.service.action;


import com.gfyulx.DI.hadoop.service.action.params.JavaTaskParam;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * @ClassName:  JavaProgramRunnerImpl
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/8/30 10:58
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class JavaProgramRunnerImpl  {

    public boolean run(JavaTaskParam param) throws Exception {
        Class<?> klass = null;
        try {
            File file = new File(param.getJarFile());
            URL url = file.toURI().toURL();
            ClassLoader loader = new URLClassLoader(new URL[]{url});//创建类加载器
            klass = loader.loadClass(param.getMainClazz());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Class not found", e);
        }
        System.out.println("Main class        : " + klass.getName());

        //参数分隔用空格或者\t
        String vars = param.getArguments();
        String[] splitVars = vars.split("\\s{1,}|\t");
        System.out.println();
        Method mainMethod = klass.getMethod("main", String[].class);
        try {
            mainMethod.invoke(null, (Object) splitVars);
        } catch (InvocationTargetException ex) {
            // Get rid of the InvocationTargetException and wrap the Throwable
            System.out.println(ex.getCause());
            ex.printStackTrace();
            return false;
        }
        return true;
    }
}

