package com.gfyulx.DI;

import java.util.ResourceBundle;

/**
 * @ClassName:  Application
 * @Description: 主入口
 * @author: gfyulx
 * @date:   2018/8/15 18:00
 *
 * @Copyright: 2018 gfyulx
 *
 */


public class Application {
            public static void main(String agrs[]) {
            System.out.print("all test!");

            String config = "hivejob";
            ResourceBundle resource = ResourceBundle.getBundle(config);
            System.out.println(resource.getString("hive2.jdbc.url"));
        }

}
