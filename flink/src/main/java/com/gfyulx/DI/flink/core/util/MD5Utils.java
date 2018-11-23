package com.gfyulx.DI.flink.core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @ClassName:  MD5Utils
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/6 14:27
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class MD5Utils {

    private static Logger logger = LoggerFactory.getLogger(MD5Utils.class);

    public static String getMD5String(String value) {
        try {
            MessageDigest md = MessageDigest.getInstance("md5");
            byte[] e = md.digest(value.getBytes());
            return toHexString(e);
        } catch (NoSuchAlgorithmException e) {
            logger.error("MD5Utils || getMD5String" +e.toString());
            return value;
        }
    }

    private static String toHexString(byte bytes[]) {
        StringBuilder hs = new StringBuilder();
        String stmp = "";
        for (int n = 0; n < bytes.length; n++) {
            stmp = Integer.toHexString(bytes[n] & 0xff);
            if (stmp.length() == 1){
                hs.append("0").append(stmp);
            }else{
                hs.append(stmp);
            }
        }
        return hs.toString();
    }

}
