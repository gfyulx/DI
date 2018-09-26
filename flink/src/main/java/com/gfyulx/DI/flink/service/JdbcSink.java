package com.gfyulx.DI.flink.service;

import com.gfyulx.DI.flink.configuration.JdbcConfiguration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;


/**
 * @ClassName: JdbcSink
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date: 2018/9/21 14:32
 * @Copyright: 2018 gfyulx
 */
public class JdbcSink<T> extends RichSinkFunction<T> {
    //public class JdbcSource<T> extends RichSourceFunction<T>  implements ResultTypeQueryable<T> {
    PreparedStatement ps;
    private Connection connection;
    JdbcConfiguration jdbcConf;
    T data;


    public JdbcSink(JdbcConfiguration conf, T data) {
        this.jdbcConf = conf;
        this.data = data;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String driver = jdbcConf.getDriver();
        String url = jdbcConf.getUrl();
        String username = jdbcConf.getUsername();
        String password = jdbcConf.getPassword();

        Class.forName(driver);
        connection = DriverManager.getConnection(url, username, password);
        String sql = jdbcConf.getSql();
        ps = connection.prepareStatement(sql);
    }


    @Override
    public void invoke(T sinkContext) throws Exception {
        try {
                Field[] fields = sinkContext.getClass().getDeclaredFields();
                for (int i = 0; i < fields.length; i++) {
                    fields[i].setAccessible(true);
                    Field x = fields[i];
                    Object obj=x.get(sinkContext);
                    ps.setObject(i+1,obj);
                }
                ps.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    @Override
    public void close() throws Exception {

        super.close();
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }
}
