package com.gfyulx.DI.flink.core.util;

/**
 * @ClassName:  ConfigConstrant
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/5 14:19
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class ConfigConstrant {
	
    public static final String FLINK_CHECKPOINT_INTERVAL_KEY = "sql.checkpoint.interval";

    public static final String FLINK_CHECKPOINT_MODE_KEY = "sql.checkpoint.mode";

    public static final String FLINK_CHECKPOINT_TIMEOUT_KEY = "sql.checkpoint.timeout";

    public static final String FLINK_MAXCONCURRENTCHECKPOINTS_KEY = "sql.max.concurrent.checkpoints";

    public static final String FLINK_CHECKPOINT_CLEANUPMODE_KEY = "sql.checkpoint.cleanup.mode";

    public static final String FLINK_CHECKPOINT_DATAURI_KEY = "flinkCheckpointDataURI";

    public static final String SQL_ENV_PARALLELISM = "sql.env.parallelism";

    public static final String SQL_MAX_ENV_PARALLELISM = "sql.max.env.parallelism";
    
    public static final String MR_JOB_PARALLELISM = "mr.job.parallelism";
    
    public static final String SQL_BUFFER_TIMEOUT_MILLIS = "sql.buffer.timeout.millis";

    public static final String FLINK_TIME_CHARACTERISTIC_KEY = "time.characteristic";

}
