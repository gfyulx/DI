package com.gfyulx.DI.flink.core;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Collections;
import java.util.List;

/**
 * @ClassName:  MyLocalStreamEnvironment
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/9 11:24
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class MyLocalStreamEnvironment extends StreamExecutionEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(LocalStreamEnvironment.class);

    /** The configuration to use for the local cluster. */
    private final Configuration conf;

    public List<URL> getClasspaths() {
        return classpaths;
    }

    public void setClasspaths(List<URL> classpaths) {
        this.classpaths = classpaths;
    }

    private List<URL> classpaths = Collections.emptyList();

    /**
     * Creates a new local stream environment that uses the default configuration.
     */
    public MyLocalStreamEnvironment() {
        this(null);
    }

    /**
     * Creates a new local stream environment that configures its local executor with the given configuration.
     *
     * @param config The configuration used to configure the local executor.
     */
    public MyLocalStreamEnvironment(Configuration config) {
        if (!ExecutionEnvironment.areExplicitEnvironmentsAllowed()) {
            throw new InvalidProgramException(
                    "The LocalStreamEnvironment cannot be used when submitting a program through a client, " +
                            "or running in a TestEnvironment context.");
        }

        this.conf = config == null ? new Configuration() : config;
    }

    /**
     * Executes the JobGraph of the on a mini cluster of CLusterUtil with a user
     * specified name.
     *
     * @param jobName
     *            name of the job
     * @return The result of the job execution, containing elapsed time and accumulators.
     */
    @Override
    public JobExecutionResult execute(String jobName) throws Exception {
        // transform the streaming program into a JobGraph
        StreamGraph streamGraph = getStreamGraph();
        streamGraph.setJobName(jobName);

        JobGraph jobGraph = streamGraph.getJobGraph();
        jobGraph.setClasspaths(classpaths);

        Configuration configuration = new Configuration();
        configuration.addAll(jobGraph.getJobConfiguration());

        configuration.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, -1L);
        configuration.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, jobGraph.getMaximumParallelism());

        // add (and override) the settings with what the user defined
        configuration.addAll(this.conf);

        if (LOG.isInfoEnabled()) {
            LOG.info("Running job on local embedded Flink mini cluster");
        }

        LocalFlinkMiniCluster exec = new LocalFlinkMiniCluster(configuration, true);
        try {
            exec.start();
            return exec.submitJobAndWait(jobGraph, getConfig().isSysoutLoggingEnabled());
        }
        finally {
            transformations.clear();
            exec.stop();
        }
    }
}
