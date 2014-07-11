/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.groupon.mesos;

import java.io.Closeable;
import java.io.IOException;

import com.groupon.mesos.executor.InternalExecutorDriver;
import com.groupon.mesos.util.UPID;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.SlaveID;

public class JesosExecutorDriver
    extends InternalExecutorDriver
    implements ExecutorDriver, Closeable

{
    public JesosExecutorDriver(final Executor executor) throws IOException
    {
        this(executor,
             UPID.create(System.getenv("MESOS_SLAVE_PID")),
             SlaveID.newBuilder().setValue(System.getenv("MESOS_SLAVE_ID")).build(),
             FrameworkID.newBuilder().setValue(System.getenv("MESOS_FRAMEWORK_ID")).build(),
             ExecutorID.newBuilder().setValue(System.getenv("MESOS_EXECUTOR_ID")).build());
    }

    public JesosExecutorDriver(final Executor executor,
                                final UPID slaveUpid,
                                final SlaveID slaveId,
                                final FrameworkID frameworkId,
                                final ExecutorID executorId) throws IOException
    {
        super(executor, slaveUpid, slaveId, frameworkId, executorId);
    }
}
