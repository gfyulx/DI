package com.gfyulx.DI.hadoop.service;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;

/**
 * @ClassName:  YarnAppManager
 * @Description: 查询与管理yarn任务
 * @author: gfyulx
 * @date:   2018/8/15 15:52
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class YarnAppManager {
    private static final Log logger = LogFactory.getLog(YarnAppManager.class);

    public void kill(Configuration clusterConf, String appId) {
        ApplicationId yarnAppId = ConverterUtils.toApplicationId(appId);
        try {
            System.out.println();
            System.out.println("Killing existing jobs and starting over:");
            YarnClient yarnClient = YarnClient.createYarnClient();
            yarnClient.init(clusterConf);
            yarnClient.start();
            System.out.print("Killing job [" + yarnAppId + "] ... ");
            yarnClient.killApplication(yarnAppId);
            System.out.println("Done");
            System.out.println();
        } catch (YarnException ye) {
            throw new RuntimeException("Exception occurred while killing  job", ye);
        } catch (IOException ioe) {
            throw new RuntimeException("Exception occurred while killing job", ioe);
        }

    }

    /**
     * 查询任务状态，返回AppStateInfo
     *
     * @param AppId
     * @return
     */
    public ApplicationReport query(Configuration clusterConf, String appId) {
        ApplicationId yarnAppId = ConverterUtils.toApplicationId(appId);
        ApplicationReport appReport = null;
        try {
            YarnClient yarnClient = YarnClient.createYarnClient();
            yarnClient.init(clusterConf);
            yarnClient.start();
            appReport = yarnClient.getApplicationReport(yarnAppId);
        } catch (ApplicationNotFoundException e) {
            logger.warn("Application with id '" + yarnAppId +
                    "' doesn't exist in RM.");
        } catch (YarnException ye) {
            throw new RuntimeException("Exception occurred while query  job", ye);
        } catch (IOException ioe) {
            throw new RuntimeException("Exception occurred while query job", ioe);
        }
        return appReport;
    }

}