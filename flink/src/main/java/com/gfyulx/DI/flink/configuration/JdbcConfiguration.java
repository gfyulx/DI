package com.gfyulx.DI.flink.configuration;


import java.io.Serializable;

/**
 * @ClassName: JdbcConfiguration
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date: 2018/9/21 14:32
 * @Copyright: 2018 gfyulx
 */
public class JdbcConfiguration<T> implements Serializable{
    String driver = "com.mysql.jdbc.Driver";
    String url = "jdbc:mysql://";
    String username = "root";
    String password = "root";
    String sql = "";


    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getDriver() {
        return driver;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUrl() {
        return url;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getUsername() {
        return username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPassword() {
        return password;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

    public String toString() {
        return "JdbcDriverStruct:" + "Driver:" + driver + "\n" +
                "url:" + url + "\n" +
                "username:" + username + "\n" +
                "password:" + password + "\n" +
                "sql:" + sql;
    }

    ;
}


