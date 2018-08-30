package com.gfyulx.DI.hadoop.service.action.params;

/**
 * @ClassName:  JavaTaskParam
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/8/30 10:54
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class JavaTaskParam {
    private String jarFile;
    private String mainClazz;
    private String arguments;

    public String getJarFile() {
        return jarFile;
    }

    public void setJarFile(String jarFile) {
        this.jarFile = jarFile;
    }

    public String getMainClazz() {
        return mainClazz;
    }

    public void setMainClazz(String mainClazz) {
        this.mainClazz = mainClazz;
    }

    public String getArguments() {
        return arguments;
    }

    public void setArguments(String arguments) {
        this.arguments = arguments;
    }
}
