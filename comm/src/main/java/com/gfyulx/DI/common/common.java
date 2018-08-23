package com.gfyulx.DI.common;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.*;
/**
 * @ClassName: common
 * @Description: 通用工具方法集
 * @author: gfyulx
 * @date: 2018/8/15 15:48
 * @Copyright: 2018 gfyulx
 */
public class common {

    public static String getPattarnFromFile(String fileName, Pattern[] patterns) {
        Set<String> jobIds = new LinkedHashSet<String>();
        if (!new File(fileName).exists()) {
            System.err.println("Log file: " + fileName + "  not present.");
        } else {
            try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
                String line = br.readLine();
                while (line != null) {
                    extractJobIDs(line, patterns, jobIds);
                    line = br.readLine();
                }
            } catch (IOException e) {
                System.out.println("WARN: Error getting patterns from logFile: " + fileName);
                e.printStackTrace(System.out);
            }
        }
        return jobIds.isEmpty() ? null : StringUtils.join(jobIds, ",");


    }
    protected static void extractJobIDs(String line, Pattern[] patterns, Set<String> jobIds) {
        Preconditions.checkNotNull(line);
        for (Pattern pattern : patterns) {
            Matcher matcher = pattern.matcher(line);
            if (matcher.find()) {
                String jobId = matcher.group(1);
                if (StringUtils.isEmpty(jobId) || jobId.equalsIgnoreCase("NULL")) {
                    continue;
                }
                //jobId = jobId.replaceAll("application", "job");
                jobIds.add(jobId);
            }
        }
    }
    public static String getHadoopJobIds(String logFile, Pattern[] patterns) {
        Set<String> jobIds = new LinkedHashSet<String>();
        if (!new File(logFile).exists()) {
            System.err.println("Log file: " + logFile + "  not present. Therefore no Hadoop job IDs found.");
        }
        else {
            try (BufferedReader br = new BufferedReader(new FileReader(logFile))) {
                String line = br.readLine();
                while (line != null) {
                    extractJobIDs(line, patterns, jobIds);
                    line = br.readLine();
                }
            } catch (IOException e) {
                System.out.println("WARN: Error getting Hadoop Job IDs. logFile: " + logFile);
                e.printStackTrace(System.out);
            }
        }
        return jobIds.isEmpty() ? null : StringUtils.join(jobIds, ",");
    }
}
