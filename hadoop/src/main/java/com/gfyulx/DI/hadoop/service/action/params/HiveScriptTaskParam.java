package com.gfyulx.DI.hadoop.service.action.params;

/**
 * @ClassName:  HiveScriptTaskParam
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/8/30 10:55
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class HiveScriptTaskParam {
    private String script;
    private String hiveServerUrl;
    private String userName;
    private String password;
    private String hiveVars;

    public HiveScriptTaskParam() {
    }

    public String getScript() {
        return script;
    }


    public void setScript(String script) {
        this.script = script;
    }


    public String getHiveVars() {
        return hiveVars;
    }


    public void setHiveVars(String hiveVars) {
        this.hiveVars = hiveVars;
    }

    public String getHiveServerUrl() {
        return hiveServerUrl;
    }

    public void setHiveServerUrl(String hiveServerUrl) {
        this.hiveServerUrl = hiveServerUrl;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
