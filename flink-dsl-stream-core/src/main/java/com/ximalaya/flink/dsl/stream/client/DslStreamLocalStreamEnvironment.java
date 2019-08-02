package com.ximalaya.flink.dsl.stream.client;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/13
 **/

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.common.collect.Lists;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.File;
import java.net.URL;
import java.util.List;

/**
 * The DslStreamLocalStreamEnvironment is a StreamExecutionEnvironment that runs the program locally,
 * multi-threaded, in the JVM where the environment is instantiated. It spawns an embedded
 * Flink cluster in the background and executes the program on that cluster.
 *
 * <p>When this environment is instantiated, it uses a default parallelism of {@code 1}. The default
 * parallelism can be set via {@link #setParallelism(int)}.
 */
@Public
public class DslStreamLocalStreamEnvironment extends StreamExecutionEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(org.apache.flink.streaming.api.environment.LocalStreamEnvironment.class);

    private final Configuration configuration;

    private final List<URL> extendJars;

    /**
     * Creates a new mini cluster stream environment that uses the default configuration.
     */
    public DslStreamLocalStreamEnvironment() {
        this(new Configuration(),null);
    }

    /**
     * Creates a new mini cluster stream environment that configures its local executor with the given configuration.
     *
     * @param configuration The configuration used to configure the local executor.
     */
    public DslStreamLocalStreamEnvironment(@Nonnull Configuration configuration, List<URL> extendJars) {
        //todo
//        if (!ExecutionEnvironment.areExplicitEnvironmentsAllowed()) {
//            throw new InvalidProgramException(
//                    "The DslStreamLocalStreamEnvironment cannot be used when submitting a program through a client, " +
//                            "or running in a TestEnvironment context.");
//        }
        this.configuration = configuration;
        this.extendJars = extendJars;
        setParallelism(1);
    }

    protected Configuration getConfiguration() {
        return configuration;
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

        //todo 设置额外的依赖
        jobGraph.setClasspaths(extendJars);

        jobGraph.setClasspaths(Lists.newArrayList(new File("/Users/nali/Desktop/testUDFS/flink-udf-test-2.0.1.jar").toURL()));
        jobGraph.setAllowQueuedScheduling(true);

        Configuration configuration = new Configuration();
        configuration.addAll(jobGraph.getJobConfiguration());
        configuration.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, "0");

        // add (and override) the settings with what the user defined
        configuration.addAll(this.configuration);

        if (!configuration.contains(RestOptions.PORT)) {
            configuration.setInteger(RestOptions.PORT, 0);
        }

        int numSlotsPerTaskManager = configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS, jobGraph.getMaximumParallelism());

        MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
                .setConfiguration(configuration)
                .setNumSlotsPerTaskManager(numSlotsPerTaskManager)
                .build();

        if (LOG.isInfoEnabled()) {
            LOG.info("Running job on local embedded Flink mini cluster");
        }

        MiniCluster miniCluster = new MiniCluster(cfg);

        try {
            miniCluster.start();
            configuration.setInteger(RestOptions.PORT, miniCluster.getRestAddress().getPort());

            return miniCluster.executeJobBlocking(jobGraph);
        }
        finally {
            transformations.clear();
            miniCluster.close();
        }
    }

    public static void main(String[] args) throws Exception {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        configuration.setBoolean("fs.hdfs.impl.disable.cache", true);
        configuration.set("fs.default.name","192.168.60.38:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.copyFromLocalFile(new Path("/Users/nali/Desktop/testUDFS/flink-udf-test-2.0.1.jar"),new Path("/recsys/fxql/dynamicJars/jinzhongyong/flink-udf-test-2.0.1.jar"));

    }
}
