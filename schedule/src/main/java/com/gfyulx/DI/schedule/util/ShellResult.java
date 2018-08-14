package com.gfyulx.DI.schedule.util;


public class ShellResult {

    private boolean isEof = false;

    private boolean isTimeout = false;

    private String result;

    private int exitValue;

    public ShellResult(boolean isEof, boolean isTimeout, int exitValue, String result) {
        this.isEof = isEof;
        this.isTimeout = isTimeout;
        this.exitValue = exitValue;
        this.result = result;
    }

    public ShellResult(boolean isEof, int exitValue, String result) {
        this.isEof = isEof;
        this.exitValue = exitValue;
        this.result = result;
    }

    public void setTimeout(boolean isTimeout) {
        this.isTimeout = isTimeout;
    }

    public void setEof(boolean isEof) {
        this.isEof = isEof;
    }

    public boolean isTiemout() {
        return isTimeout;
    }

    public boolean isEof() {
        return isEof;
    }

    public String getResult() {
        return result;
    }

    public int getExitValue() {
        return exitValue;
    }

}
