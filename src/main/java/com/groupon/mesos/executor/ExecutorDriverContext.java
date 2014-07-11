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
package com.groupon.mesos.executor;

import static org.apache.mesos.Protos.Status.DRIVER_NOT_STARTED;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.groupon.mesos.util.NetworkUtil;
import com.groupon.mesos.util.UPID;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.Status;

import mesos.internal.Messages.StatusUpdate;

/**
 * Context for the Executor.
 */
class ExecutorDriverContext
{
    private final AtomicReference<Status> stateMachine = new AtomicReference<>(DRIVER_NOT_STARTED);
    private final BlockingQueue<SettableFuture<Status>> stateMachineFutures = new LinkedBlockingQueue<>();

    private final UPID driverUpid;

    private final UPID slaveUpid;
    private final SlaveID slaveId;
    private final FrameworkID frameworkId;
    private final ExecutorID executorId;

    private final ConcurrentMap<UUID, StatusUpdate> updates = new ConcurrentHashMap<>(16, 0.75f, 3);

    ExecutorDriverContext(final String hostName,
                          final UPID slaveUpid,
                          final SlaveID slaveId,
                          final FrameworkID frameworkId,
                          final ExecutorID executorId)
                    throws IOException
    {
        this.slaveUpid = slaveUpid;
        this.slaveId = slaveId;
        this.frameworkId = frameworkId;
        this.executorId = executorId;

        this.driverUpid = UPID.fromParts(UUID.randomUUID().toString(),
            HostAndPort.fromParts(hostName, NetworkUtil.findUnusedPort()));
    }

    UPID getSlaveUPID()
    {
        return slaveUpid;
    }

    SlaveID getSlaveId()
    {
        return slaveId;
    }

    FrameworkID getFrameworkId()
    {
        return frameworkId;
    }

    ExecutorID getExecutorId()
    {
        return executorId;
    }

    UPID getDriverUPID()
    {
        return driverUpid;
    }

    //
    // Status update management
    //

    void addUpdate(final UUID key, final StatusUpdate update)
    {
        updates.put(key, update);
    }

    void removeUpdate(final UUID key)
    {
        updates.remove(key);
    }

    Iterable<StatusUpdate> getUpdates()
    {
        return updates.values();
    }

    //
    // State machine management
    //

    synchronized void setStateMachine(final Status status)
    {
        final Status oldStatus = stateMachine.getAndSet(status);

        if (status != oldStatus) {
            List<SettableFuture<Status>> settableFutures = new ArrayList<>(stateMachineFutures.size());
            stateMachineFutures.drainTo(settableFutures);

            for (SettableFuture<Status> future : settableFutures) {
                future.set(status);
            }
        }
    }

    synchronized Status getStateMachine()
    {
        return stateMachine.get();
    }

    synchronized ListenableFuture<Status> waitForStateChange(final Status expectedStatus)
    {
        final SettableFuture<Status> future = SettableFuture.create();
        if (!isStateMachine(expectedStatus)) {
            // Current status is not the expected status. Return
            // it immediately.
            future.set(stateMachine.get());
        }
        else {
            // Current status is expected status: Queue up for a status change.
            stateMachineFutures.add(future);
        }
        return future;
    }

    synchronized boolean isStateMachine(final Protos.Status ... statusWanted)
    {
        final Protos.Status currentState = stateMachine.get();
        for (final Protos.Status status : statusWanted) {
            if (currentState == status) {
                return true;
            }
        }
        return false;
    }
}
