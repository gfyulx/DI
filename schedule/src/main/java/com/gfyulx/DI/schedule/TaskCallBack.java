package com.gfyulx.DI.schedule;



public interface TaskCallBack {
    public void success(Class<?> T);

    public void fail(Class<?> T);

}
