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
package com.groupon.mesos.scheduler;

import static com.google.common.base.Preconditions.checkNotNull;

import static org.apache.mesos.Protos.Status.DRIVER_ABORTED;
import static org.apache.mesos.Protos.Status.DRIVER_NOT_STARTED;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.groupon.mesos.util.Log;
import com.groupon.mesos.util.NetworkUtil;
import com.groupon.mesos.util.UPID;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.Status;

/**
 * Glues all the pieces of the scheduler together, keeps track of state etc. This is a too big and random collection of things.
 */
class SchedulerDriverContext
{
    private static final Log LOG = Log.getLog(SchedulerDriverContext.class);

    private final AtomicReference<Status> stateMachine = new AtomicReference<>(DRIVER_NOT_STARTED);
    private final AtomicReference<FrameworkInfo> frameworkInfo = new AtomicReference<>();
    private final AtomicReference<MasterInfo> masterInfo = new AtomicReference<>();
    private final AtomicReference<UPID> masterUpid = new AtomicReference<>();

    private final BlockingQueue<SettableFuture<Status>> stateMachineFutures = new LinkedBlockingQueue<>();

    private final AtomicBoolean connected = new AtomicBoolean();
    private final AtomicBoolean failover = new AtomicBoolean(false);
    private final UPID driverUpid;

    private final Table<OfferID, SlaveID, UPID> offerCache = HashBasedTable.create();
    private final Map<SlaveID, UPID> slaveCache = new ConcurrentHashMap<>(16, 0.75f, 3);

    SchedulerDriverContext(final FrameworkInfo frameworkInfo)
                    throws IOException
    {
        this.frameworkInfo.set(checkNotNull(frameworkInfo, "frameworkInfo is null"));

        this.driverUpid = UPID.fromParts(UUID.randomUUID().toString(),
            HostAndPort.fromParts(frameworkInfo.getHostname(), NetworkUtil.findUnusedPort()));

        // If the framework info sent in has an id, we are in failover mode from the start.
        failover.set(hasFrameworkId(frameworkInfo));
    }

    UPID getDriverUPID()
    {
        return driverUpid;
    }

    //
    // connected status
    //

    boolean connected()
    {
        return connected.getAndSet(true);
    }

    /**
     * Disconnect and return the previous state.
     */
    boolean disconnected()
    {
        return connected.getAndSet(false);
    }

    boolean isConnected()
    {
        return connected.get();
    }

    //
    // failover status
    //

    void setFailover(final boolean failover)
    {
        this.failover.set(failover);
    }

    boolean isFailover()
    {
        return failover.get();
    }

    //
    // Master information
    //

    synchronized MasterInfo getMaster()
    {
        return masterInfo.get();
    }

    synchronized MasterInfo connectedMaster()
    {
        if (isStateMachine(DRIVER_ABORTED)) {
            LOG.debug("driver is aborted!");
            return null;
        }

        if (!isConnected()) {
            LOG.debug("Not connected!");
            return null;
        }

        return masterInfo.get();
    }

    synchronized void setMaster(final MasterInfo newMasterInfo)
    {
        masterInfo.set(newMasterInfo);
        masterUpid.set(newMasterInfo == null ? null : UPID.create(newMasterInfo.getPid()));
    }

    synchronized UPID getMasterUPID()
    {
        return masterUpid.get();
    }

    void setFrameworkId(final FrameworkID frameworkId)
    {
        checkNotNull(frameworkId, "frameworkId is null");

        frameworkInfo.set(FrameworkInfo.newBuilder(frameworkInfo.get())
            .setId(frameworkId)
            .build());
    }

    //
    // Framework Id
    //

    FrameworkID getFrameworkId()
    {
        return frameworkInfo.get().getId();
    }

    boolean hasFrameworkId()
    {
        return hasFrameworkId(frameworkInfo.get());
    }

    FrameworkInfo getFrameworkInfo()
    {
        return frameworkInfo.get();
    }

    //
    // State machine management
    //

    synchronized void setStateMachine(final Status status)
    {
        final Status oldStatus = stateMachine.getAndSet(status);

        if (status != oldStatus) {
            // Fire all the futures waiting for a status change.
            final List<SettableFuture<Status>> settableFutures = new ArrayList<>(stateMachineFutures.size());
            stateMachineFutures.drainTo(settableFutures);

            for (final SettableFuture<Status> future : settableFutures) {
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

    //
    // Offer cache management
    //

    void addOffer(final OfferID offerId, final SlaveID slaveId, final UPID pid)
    {
        synchronized (offerCache) {
            offerCache.put(offerId, slaveId, pid);
        }
    }

    void removeAllOffers(final OfferID offerId)
    {
        synchronized (offerCache) {
            offerCache.row(offerId).clear();
        }
    }

    boolean hasOffers(final OfferID offerId)
    {
        synchronized (offerCache) {
            return offerCache.containsRow(offerId);
        }
    }

    boolean hasOffer(final OfferID offerId, final SlaveID slaveId)
    {
        synchronized (offerCache) {
            return offerCache.contains(offerId, slaveId);
        }
    }

    UPID getOffer(final OfferID offerId, final SlaveID slaveId)
    {
        synchronized (offerCache) {
            return offerCache.get(offerId, slaveId);
        }
    }

    //
    // Slave cache management
    //

    void addSlave(final SlaveID slaveId, final UPID upid)
    {
        slaveCache.put(slaveId, upid);
    }

    void removeSlave(final SlaveID slaveId)
    {
        slaveCache.remove(slaveId);
    }

    boolean containsSlave(final SlaveID slaveId)
    {
        return slaveCache.containsKey(slaveId);
    }

    UPID getSlaveUPID(final SlaveID slaveId)
    {
        return slaveCache.get(slaveId);
    }

    /**
     * Static helper for use in the c'tor
     */
    private static boolean hasFrameworkId(final FrameworkInfo frameworkInfo)
    {
        return frameworkInfo.hasId() && !"".equals(frameworkInfo.getId().getValue());
    }
}
