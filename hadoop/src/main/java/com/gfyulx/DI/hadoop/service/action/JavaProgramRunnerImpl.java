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
        System.out.println("Main class: " + klass.getName());
        System.out.println();
        Method mainMethod;
        try {
            Object obj = klass.newInstance();
            Method[] methods = klass.getDeclaredMethods();
            for (Method methodl : methods) {
                System.out.println(methodl.getName());
                if (methodl.getName().equals("main")) {
                    Class<?>[] parameters = methodl.getParameterTypes();
                    if (parameters.length == 0) {
                        mainMethod = klass.getMethod("main");
                        mainMethod.invoke(obj, null);
                    } else {
                        //支持的main方法参数目前只支持String[] args;
                        mainMethod = klass.getMethod("main", String[].class);
                        //参数分隔用空格或者\t
                        String vars = param.getArguments();
                        if (vars != null && !vars.isEmpty()) {
                            String[] splitVars = vars.split("\\s{1,}|\t");
                            mainMethod.invoke(obj, (Object) splitVars);
                        }else{
                            throw new IllegalArgumentException("argument can't be null");
                        }
                    }
                    break;
                }
                throw new ClassNotFoundException("main");
            }
            //Method mainMethod = klass.getMethod("main");
            //Method mainMethod = klass.getMethod("main", String.class);

        } catch (InvocationTargetException ex) {
            // Get rid of the InvocationTargetException and wrap the Throwable
            System.out.println(ex.getCause());
            ex.printStackTrace();
            return false;
        }
        return true;
    }
}

