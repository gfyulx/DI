package com.gfyulx.DI.flink.configuration;

/**
 * @ClassName: DataFormat
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date: 2018/9/21 15:16
 * @Copyright: 2018 gfyulx
 */
public class DataFormat<T> {
    T data;

    public void DataFormat(T data){
        this.data=data;
    }
    public void setData(T data) {
        this.data = data;
    }

    public T getData() {
        return data;
    }
}
