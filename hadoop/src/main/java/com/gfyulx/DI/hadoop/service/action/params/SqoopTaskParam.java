package com.gfyulx.DI.hadoop.service.action.params;

import java.io.Serializable;

/**
 * @ClassName:  SqoopTaskParam
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/8/30 10:55
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class SqoopTaskParam implements Serializable {
    private String command;

    public SqoopTaskParam() {
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

}
