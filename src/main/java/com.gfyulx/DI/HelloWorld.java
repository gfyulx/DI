package com.gfyulx.DI;

public class HelloWorld {

    public static void main(String[] args) {
        System.out.println("Hello World!");
        for (String msg : args) {
            System.out.println("receive:" + msg);
        }
    }
}
