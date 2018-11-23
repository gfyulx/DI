package com.gfyulx.DI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;

public class slf4jTest {
    private static final Logger LOG = LoggerFactory.getLogger(slf4jTest.class);


    public static void main(String[] args) {
        LOG.warn("test!");
        LOG.info("test info");
        System.out.println(LOG.getClass());

        if (LOG.getClass().equals("org.slf4j.helpers.NOPLogger")){
            //set 一个文件输出

        }

        try {
            Enumeration<URL> resources = ClassLoader.getSystemResources("org/slf4j/impl/StaticLoggerBinder.class");
            while (resources.hasMoreElements()) {
                URL url = resources.nextElement();
                System.out.println(url.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
