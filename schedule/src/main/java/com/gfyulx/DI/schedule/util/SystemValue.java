package com.gfyulx.DI.schedule.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;


public class SystemValue {

    private static Log logger = LogFactory.getLog(SystemValue.class);

    private static Map<String, String> map = new HashMap<String,String>();

    static {
        Process p = null;
        Runtime r = Runtime.getRuntime();
        String OS = System.getProperty("os.name").toLowerCase();
        logger.info("the systern version:" + OS);
        try {
            if (OS.indexOf("windows 9") > -1) {
                p = r.exec("command.com /c set");
            } else if ((OS.indexOf("nt") > -1)
                    || (OS.indexOf("windows 20") > -1) || (OS.indexOf("windows server 20") > -1)
                    || (OS.indexOf("windows xp") > -1) || (OS.indexOf("windows server xp") > -1)
                    || (OS.indexOf("windows 7") > -1) || (OS.indexOf("windows server 7") > -1)
                    || (OS.indexOf("windows vista") > -1) || (OS.indexOf("windows server vista") > -1)) {
                p = r.exec("cmd.exe /c set");
            } else {
                // Unix
                p = r.exec("env");
            }
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    p.getInputStream()));
            String line;
            while ((line = br.readLine()) != null) {
                String[] str = line.split("=");
                if (str.length == 2)
                    map.put(str[0], str[1]);
            }
            br.close();
            p.waitFor();
        } catch (IOException ioe) {
            logger.error(ioe);
            try {
                p.waitFor();
            } catch (InterruptedException e) {
                logger.error(e);
            }
        } catch (InterruptedException e) {
            logger.error(e);
        }
    }

    ;

    public static String getenv(String key) {
        return map.get(key);
    }

}
