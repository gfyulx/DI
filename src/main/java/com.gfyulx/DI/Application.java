package com.gfyulx.DI;

import java.lang.reflect.Field;
import java.util.ResourceBundle;

/**
 * @ClassName: Application
 * @Description: 主入口
 * @author: gfyulx
 * @date: 2018/8/15 18:00
 * @Copyright: 2018 gfyulx
 */


public class Application {
    public static void main(String agrs[]) {
        System.out.println("all test!");
        String config = "hivejob";
        ResourceBundle resource = ResourceBundle.getBundle(config);
        String data = "";
        Test bb = new Test();
        try {
            if ((Object)bb instanceof String) {
                System.out.println("1 True");
            }
        } catch (Exception e) {

        }
        Field[] fields1=bb.getClass().getDeclaredFields();
        for(Field x:fields1){
            System.out.println(x.getType().getTypeName()+"  "+String.class.getClass().getClass());

            if ((Object)x instanceof  String){
                System.out.print("2 True");
            }

        }
        System.out.println("==========");
        System.out.println(data.getClass().getTypeName());
        System.out.println(String.class.getClass());
        System.out.println(bb.getClass());
        if (data.getClass().getTypeName() == String.class.getClass().getTypeName()) {
            System.out.println("True");
        }
        Test data1 = new Test();
        Field[] fields = data1.getClass().getDeclaredFields();
        for (Field x : fields) {
            System.out.println(x.getName() + " " + x.getType());
            if (!x.isAccessible()) {
                System.out.println(x.getName() + " " + x.getType());
            }
            //System.out.println(x.isAccessible());
        }
        System.out.println(resource.getString("hive2.jdbc.url"));
    }


}

class Test {
    String t1;
    Integer t2;
    Boolean t3;
    public String t4;
    boolean t5;
    Byte t6;
    Short t7;
    Long t8;
    public void add(String t1, String t2) {
        this.t1 = t1 + t2;
    }
    public void Test(String a,Integer b){
        this.t1=a;
        this.t2=b;
    }
}


