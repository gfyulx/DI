package com.gfyulx.DI.flink.service;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import com.gfyulx.DI.flink.configuration.JdbcConfiguration;
import com.gfyulx.DI.flink.configuration.DataFormat;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @ClassName: JdbcSource
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date: 2018/9/21 14:32
 * @Copyright: 2018 gfyulx
 */
//public class JdbcSource<T> extends RichSourceFunction<T>  implements ResultTypeQueryable<T> {
public class JdbcSource<T> extends RichSourceFunction<T> {
    PreparedStatement ps;
    private Connection connection;
    JdbcConfiguration jdbcConf;
    T data;

    public JdbcSource(JdbcConfiguration conf, T data) {
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
    public void run(SourceContext<T> sourceContext) throws Exception {
        try {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                Field[] fields = data.getClass().getDeclaredFields();
                Object obj[] = new Object[fields.length];
                for (int i = 0; i < fields.length; i++) {
                    fields[i].setAccessible(true);
                    Field x = fields[i];
                    obj[i] = resultSet.getObject(x.getName());
                }
                //获取公共构造方法，并生成新的对象，如无带参数的构造方法，抛出异常
                Constructor[] c = data.getClass().getConstructors();
                for (Constructor x : c) {
                    //System.out.println(x.getParameterCount()+" "+fields.length+x.getParameterTypes());
                    if (x.getParameterCount() == fields.length) {
                        //T dataNew=(Class)T.newInstance();
                        Object dataNew = x.newInstance(obj);
                        sourceContext.collect((T) dataNew);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void cancel() {

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
