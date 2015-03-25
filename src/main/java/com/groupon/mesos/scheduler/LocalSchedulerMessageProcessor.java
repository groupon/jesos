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
import static com.google.common.base.Preconditions.checkState;

import static org.apache.mesos.Protos.Status.DRIVER_ABORTED;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.eventbus.Subscribe;
import com.groupon.mesos.scheduler.SchedulerMessageEnvelope.ExecutorToFrameworkMessageEnvelope;
import com.groupon.mesos.scheduler.SchedulerMessageEnvelope.FrameworkErrorMessageEnvelope;
import com.groupon.mesos.scheduler.SchedulerMessageEnvelope.FrameworkRegisteredMessageEnvelope;
import com.groupon.mesos.scheduler.SchedulerMessageEnvelope.FrameworkReregisteredMessageEnvelope;
import com.groupon.mesos.scheduler.SchedulerMessageEnvelope.LostSlaveMessageEnvelope;
import com.groupon.mesos.scheduler.SchedulerMessageEnvelope.RemoteMessageEnvelope;
import com.groupon.mesos.scheduler.SchedulerMessageEnvelope.RescindResourceOfferMessageEnvelope;
import com.groupon.mesos.scheduler.SchedulerMessageEnvelope.ResourceOffersMessageEnvelope;
import com.groupon.mesos.scheduler.SchedulerMessageEnvelope.StatusUpdateMessageEnvelope;
import com.groupon.mesos.util.Log;
import com.groupon.mesos.util.ManagedEventBus;
import com.groupon.mesos.util.UPID;

import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import mesos.internal.Messages.ExecutorToFrameworkMessage;
import mesos.internal.Messages.FrameworkErrorMessage;
import mesos.internal.Messages.FrameworkRegisteredMessage;
import mesos.internal.Messages.FrameworkReregisteredMessage;
import mesos.internal.Messages.LostSlaveMessage;
import mesos.internal.Messages.RescindResourceOfferMessage;
import mesos.internal.Messages.ResourceOffersMessage;
import mesos.internal.Messages.StatusUpdateAcknowledgementMessage;
import mesos.internal.Messages.StatusUpdateMessage;

/**
 * Local Message processing. Accepts messages from the outside and deliver them to the local scheduler.
 */
class LocalSchedulerMessageProcessor
{
    private static final Log LOG = Log.getLog(LocalSchedulerMessageProcessor.class);

    private final SchedulerDriverContext context;
    private final ManagedEventBus eventBus;
    private final boolean implicitAcknowledgements;

    LocalSchedulerMessageProcessor(final SchedulerDriverContext context,
                                   final ManagedEventBus eventBus,
                                   final boolean implicitAcknowledgements)
    {
        this.context = checkNotNull(context, "context is null");
        this.eventBus = checkNotNull(eventBus, "eventBus is null");
        this.implicitAcknowledgements = implicitAcknowledgements;
    }

    @Subscribe
    public void frameworkRegistered(final FrameworkRegisteredMessageEnvelope envelope)
    {
        checkState(envelope.getRecipient().equals(context.getDriverUPID()), "Received a remote message for local delivery");

        final FrameworkRegisteredMessage frameworkRegisteredMessage = envelope.getMessage();

        if (!masterIsValid(frameworkRegisteredMessage.getMasterInfo())) {
            return;
        }

        final FrameworkID frameworkId = frameworkRegisteredMessage.getFrameworkId();

        context.connected();
        context.setFailover(false);
        context.setFrameworkId(frameworkId);

        eventBus.post(new SchedulerCallback() {
            @Override
            public Runnable getCallback(final Scheduler scheduler, final SchedulerDriver schedulerDriver)
            {
                return new Runnable() {
                    @Override
                    public void run()
                    {
                        scheduler.registered(schedulerDriver, frameworkId, context.getMaster());
                    }
                };
            }
        });
    }

    @Subscribe
    public void frameworkReregistered(final FrameworkReregisteredMessageEnvelope envelope)
    {
        checkState(envelope.getRecipient().equals(context.getDriverUPID()), "Received a remote message for local delivery");

        final FrameworkReregisteredMessage frameworkReregisteredMessage = envelope.getMessage();

        if (!masterIsValid(frameworkReregisteredMessage.getMasterInfo())) {
            return;
        }

        final FrameworkID frameworkId = frameworkReregisteredMessage.getFrameworkId();

        checkState(frameworkId != null, "Received null framework reregistration message!");
        checkState(frameworkId.equals(context.getFrameworkId()), "Received framework reregistration for %s but expected %s", frameworkId.getValue(), context.getFrameworkId().getValue());

        context.connected();
        context.setFailover(false);

        eventBus.post(new SchedulerCallback() {
            @Override
            public Runnable getCallback(final Scheduler scheduler, final SchedulerDriver schedulerDriver)
            {
                return new Runnable() {
                    @Override
                    public void run()
                    {
                        scheduler.reregistered(schedulerDriver, context.getMaster());
                    }
                };
            }
        });
    }

    @Subscribe
    public void frameworkResourceOffers(final ResourceOffersMessageEnvelope envelope)
    {
        checkState(envelope.getRecipient().equals(context.getDriverUPID()), "Received a remote message for local delivery");

        final UPID sender = envelope.getSender();

        if (!driverIsConnected(sender)) {
            return;
        }

        final ResourceOffersMessage resourceOffersMessage = envelope.getMessage();
        final List<Offer> offers = resourceOffersMessage.getOffersList();
        final List<UPID> pids = ImmutableList.copyOf(Lists.transform(resourceOffersMessage.getPidsList(), UPID.getCreateFunction()));

        checkState(offers.size() == pids.size(), "Received %s offers but only %s pids!", offers.size(), pids.size());

        int pidIndex = 0;
        for (final Offer offer : offers) {
            context.addOffer(offer.getId(), offer.getSlaveId(), pids.get(pidIndex++));
        }

        eventBus.post(new SchedulerCallback() {
            @Override
            public Runnable getCallback(final Scheduler scheduler, final SchedulerDriver schedulerDriver)
            {
                return new Runnable() {
                    @Override
                    public void run()
                    {
                        scheduler.resourceOffers(schedulerDriver, resourceOffersMessage.getOffersList());
                    }
                };
            }
        });
    }

    @Subscribe
    public void frameworkRescindOffer(final RescindResourceOfferMessageEnvelope envelope)
    {
        checkState(envelope.getRecipient().equals(context.getDriverUPID()), "Received a remote message for local delivery");

        final UPID sender = envelope.getSender();

        if (!driverIsConnected(sender)) {
            return;
        }

        final RescindResourceOfferMessage rescindResourceOfferMessage = envelope.getMessage();
        context.removeAllOffers(rescindResourceOfferMessage.getOfferId());

        eventBus.post(new SchedulerCallback() {
            @Override
            public Runnable getCallback(final Scheduler scheduler, final SchedulerDriver schedulerDriver)
            {
                return new Runnable() {
                    @Override
                    public void run()
                    {
                        scheduler.offerRescinded(schedulerDriver, rescindResourceOfferMessage.getOfferId());
                    }
                };
            }
        });
    }

    @Subscribe
    public void frameworkStatusUpdate(final StatusUpdateMessageEnvelope envelope)
        throws IOException
    {
        checkState(envelope.getRecipient().equals(context.getDriverUPID()), "Received a remote message for local delivery");

        final UPID sender = envelope.getSender();

        if (!driverIsConnected(sender)) {
            return;
        }

        final StatusUpdateMessage statusUpdateMessage = envelope.getMessage();

        final FrameworkID frameworkId = context.getFrameworkId();
        final FrameworkID messageFrameworkId = statusUpdateMessage.getUpdate().getFrameworkId();

        checkState(frameworkId.equals(messageFrameworkId), "Received Message for framework %s, but local id is %s", messageFrameworkId, frameworkId);

        final TaskStatus.Builder taskStatusBuilder = TaskStatus.newBuilder(statusUpdateMessage.getUpdate().getStatus());
        final TaskStatus taskStatus;

        // If the update is driver-generated or master-generated, it does not require acknowledgement (from Mesos source code, sched.cpp).

        final boolean noAckRequired = envelope.getSender().equals(context.getDriverUPID()) || envelope.getSender().equals(context.getMasterUPID());

        if (noAckRequired) {
            taskStatus = taskStatusBuilder.clearUuid().build();
        }
        else {
            taskStatus = taskStatusBuilder.setUuid(statusUpdateMessage.getUpdate().getUuid()).build();
        }

        eventBus.post(new SchedulerCallback() {
            @Override
            public Runnable getCallback(final Scheduler scheduler, final SchedulerDriver schedulerDriver)
            {
                return new Runnable() {
                    @Override
                    public void run()
                    {
                        scheduler.statusUpdate(schedulerDriver, taskStatus);
                    }
                };
            }
        });

        if (implicitAcknowledgements && !noAckRequired) {
            final StatusUpdateAcknowledgementMessage statusUpdateAcknowledgementMessage = StatusUpdateAcknowledgementMessage.newBuilder()
                .setFrameworkId(frameworkId)
                .setSlaveId(statusUpdateMessage.getUpdate().getSlaveId())
                .setTaskId(taskStatus.getTaskId())
                .setUuid(statusUpdateMessage.getUpdate().getUuid())
                .build();

            final UPID pid = UPID.create(statusUpdateMessage.getPid());

            eventBus.post(new RemoteMessageEnvelope(context.getDriverUPID(), pid, statusUpdateAcknowledgementMessage));
        }
    }

    @Subscribe
    public void frameworkLostSlave(final LostSlaveMessageEnvelope envelope)
    {
        checkState(envelope.getRecipient().equals(context.getDriverUPID()), "Received a remote message for local delivery");

        final UPID sender = envelope.getSender();

        if (!driverIsConnected(sender)) {
            return;
        }

        final LostSlaveMessage lostSlaveMessage = envelope.getMessage();
        final SlaveID slaveId = lostSlaveMessage.getSlaveId();

        context.removeSlave(slaveId);

        eventBus.post(new SchedulerCallback() {
            @Override
            public Runnable getCallback(final Scheduler scheduler, final SchedulerDriver schedulerDriver)
            {
                return new Runnable() {
                    @Override
                    public void run()
                    {
                        scheduler.slaveLost(schedulerDriver, slaveId);
                    }
                };
            }
        });
    }

    @Subscribe
    public void frameworkFrameworkMessage(final ExecutorToFrameworkMessageEnvelope envelope)
    {
        checkState(envelope.getRecipient().equals(context.getDriverUPID()), "Received a remote message for local delivery");

        final UPID sender = envelope.getSender();

        if (!driverIsConnected(sender)) {
            return;
        }

        final ExecutorToFrameworkMessage executorToFrameworkMessage = envelope.getMessage();

        eventBus.post(new SchedulerCallback() {
            @Override
            public Runnable getCallback(final Scheduler scheduler, final SchedulerDriver schedulerDriver)
            {
                return new Runnable() {
                    @Override
                    public void run()
                    {
                        scheduler.frameworkMessage(schedulerDriver,
                            executorToFrameworkMessage.getExecutorId(),
                            executorToFrameworkMessage.getSlaveId(),
                            executorToFrameworkMessage.getData().toByteArray());
                    }
                };
            }
        });
    }

    @Subscribe
    public void frameworkError(final FrameworkErrorMessageEnvelope envelope)
    {
        checkState(envelope.getRecipient().equals(context.getDriverUPID()), "Received a remote message for local delivery");

        final UPID sender = envelope.getSender();

        if (!driverIsConnected(sender)) {
            return;
        }

        final FrameworkErrorMessage frameworkErrorMessage = envelope.getMessage();

        eventBus.post(new SchedulerCallback() {
            @Override
            public Runnable getCallback(final Scheduler scheduler, final SchedulerDriver schedulerDriver)
            {
                return new Runnable() {
                    @Override
                    public void run()
                    {
                        schedulerDriver.abort();
                        scheduler.error(schedulerDriver, frameworkErrorMessage.getMessage());
                    }
                };
            }
        });
    }

    private boolean masterIsValid(final MasterInfo masterInfo)
    {
        checkNotNull(masterInfo, "masterInfo is null");

        if (context.isStateMachine(DRIVER_ABORTED)) {
            LOG.warn("driver is aborted!");
            return false;
        }

        final MasterInfo currentMaster = context.getMaster();

        if (currentMaster == null) {
            LOG.warn("Received registration from  %s, but no master is leading, ignoring!", masterInfo.getId());
            return false;
        }

        if (!masterInfo.equals(currentMaster)) {
            LOG.warn("Received registration from %s, leading master is %s, ignoring!", masterInfo, currentMaster);
            return false;
        }

        return true;
    }

    private boolean driverIsConnected(final UPID messageSender)
    {
        final MasterInfo master = context.connectedMaster();

        if (master == null) {
            LOG.warn("Received message from  %s, but no master is leading, ignoring!", messageSender);
            return false;
        }

        // Master PID may have changed in the context in the meantime. Don't rely on the context
        // to be up to date but resolve the MasterInfo that was retrieved earlier.
        final UPID masterUpid = UPID.create(master.getPid());

        if (!masterUpid.equals(messageSender)) {
            LOG.warn("Received message from %s, leading master is %s, ignoring!", messageSender, masterUpid);
            return false;
        }

        return true;
    }
}
