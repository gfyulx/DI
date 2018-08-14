package com.gfyulx.DI.schedule.util;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;


public class SystemUtils {

    private String shellPath = SystemValue.getenv("SHELL");

    public static Log logger = LogFactory.getLog(SystemUtils.class);

    static class OsHolder {
        static SystemUtils instance = new SystemUtils();
    }

    public static SystemUtils getInstance() {
        return OsHolder.instance;
    }

    private SystemUtils() {
    }

    public String getenv(String key) {
        return SystemValue.getenv(key);
    }

    public static boolean isNumeric(String str) {
        for (int i = str.length(); --i >= 0; ) {
            if (!Character.isDigit(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public void executeNoreturn(String cmd) {
        if (cmd == null || cmd.length() == 0)
            return;
        Runtime r = Runtime.getRuntime();

        String shellPath = SystemValue.getenv("SHELL");

        String[] cmdSet = {shellPath, "-c", cmd};

        try {
            if (shellPath == null || shellPath.length() == 0)
                r.exec(cmd);
            else
                r.exec(cmdSet);
        } catch (IOException ioe) {
            logger.error(cmd, ioe);
        }
    }

    public ShellResult execute(String cmd) {
        return execute(cmd, 30);
    }

    public synchronized ShellResult execute(String cmd, long timeoutSec) {
        return executeForShellScript(cmd, timeoutSec);
    }

    public ShellResult executeForShellScript(String cmd, long timeoutSec) {
        Boolean isWaitFor = true;
        String[] cmdSet = {shellPath, "-c", cmd};
        Process p = null;
        OutputStream stdout = null, error = null;
        StringBuffer errmsg = new StringBuffer();
        try {
            if (shellPath == null || shellPath.length() == 0) {
                p = ProcessMethods.execute("cmd.exe /c" + cmd);
            } else {
                p = ProcessMethods.execute(cmdSet);
            }
            stdout = new ByteArrayOutputStream();
            error = new ByteArrayOutputStream();
            Thread tout = ProcessMethods.consumeProcessOutputStream(p, stdout);
            Thread eout = ProcessMethods.consumeProcessErrorStream(p, error);
            long idleTime = 1000 * timeoutSec;
            long startTime = System.currentTimeMillis();
            int exitValue = ProcessMethods.waitForOrKill(p, idleTime, isWaitFor);
            idleTime = idleTime - (System.currentTimeMillis() - startTime);
            if (idleTime <= 0) {
                idleTime = 1000 * 1;
            }
            startTime = System.currentTimeMillis();
            try {
                tout.join(idleTime);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            idleTime = idleTime - (System.currentTimeMillis() - startTime);
            if (idleTime <= 0) {
                idleTime = 1000 * 1;
            }
            try {
                eout.join(idleTime);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            String result = stdout.toString();
            if (result.endsWith("\n")) {
                result = result.substring(0, result.length() - 1);
            }
            if (exitValue == -1) {
                errmsg.delete(0, errmsg.length());
                errmsg.append("cmd[").append(cmd).append("] execute timeOut: ").append(timeoutSec).append(" S");
                return new ShellResult(false, exitValue, errmsg.toString());
            }
            return new ShellResult(false, exitValue, result);
        } catch (Exception e) {
            errmsg.append(cmd).append("execute error,errmsg=").append(e.getMessage());
            logger.error(errmsg.toString());
            if (stdout != null)
                return new ShellResult(false, 1111, errmsg.toString());
            else
                return new ShellResult(false, 1111, errmsg.toString());
        } finally {
            errmsg.delete(0, errmsg.length());
            try {
                if (error != null)
                    error.close();
                if (stdout != null)
                    stdout.close();
            } catch (Exception e) {
                errmsg.append(cmd).append(":").append(e.getMessage());
                logger.error(errmsg.toString(), e);
            }
            if (p != null) {
                ProcessMethods.closeStreams(p);
                p = null;
            }
        }
    }

}
