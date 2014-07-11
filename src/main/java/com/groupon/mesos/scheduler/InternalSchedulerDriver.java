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

import static java.lang.String.format;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import static org.apache.mesos.Protos.Status.DRIVER_ABORTED;
import static org.apache.mesos.Protos.Status.DRIVER_NOT_STARTED;
import static org.apache.mesos.Protos.Status.DRIVER_RUNNING;
import static org.apache.mesos.Protos.Status.DRIVER_STOPPED;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.groupon.mesos.scheduler.SchedulerMessageEnvelope.RemoteMessageEnvelope;
import com.groupon.mesos.scheduler.SchedulerMessageEnvelope.StatusUpdateMessageEnvelope;
import com.groupon.mesos.util.CloseableExecutors;
import com.groupon.mesos.util.HttpProtocolReceiver;
import com.groupon.mesos.util.HttpProtocolSender;
import com.groupon.mesos.util.Log;
import com.groupon.mesos.util.TimeUtil;
import com.groupon.mesos.util.UPID;
import com.groupon.mesos.util.UUIDUtil;
import com.groupon.mesos.zookeeper.ZookeeperMasterDetector;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Credential;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Filters;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import mesos.internal.Messages.DeactivateFrameworkMessage;
import mesos.internal.Messages.FrameworkToExecutorMessage;
import mesos.internal.Messages.KillTaskMessage;
import mesos.internal.Messages.LaunchTasksMessage;
import mesos.internal.Messages.ReconcileTasksMessage;
import mesos.internal.Messages.RegisterFrameworkMessage;
import mesos.internal.Messages.ReregisterFrameworkMessage;
import mesos.internal.Messages.ResourceRequestMessage;
import mesos.internal.Messages.ReviveOffersMessage;
import mesos.internal.Messages.StatusUpdate;
import mesos.internal.Messages.StatusUpdateMessage;
import mesos.internal.Messages.UnregisterFrameworkMessage;

/**
 * Java implementation of {@link SchedulerDriver}.
 */
public abstract class InternalSchedulerDriver
    implements SchedulerDriver, Closeable
{
    private static final Log LOG = Log.getLog(InternalSchedulerDriver.class);

    private final Scheduler scheduler;
    private final Credential credential;

    private final ZookeeperMasterDetector detector;
    private final LocalSchedulerMessageProcessor localMessageProcessor;
    private final HttpProtocolReceiver receiver;
    private final HttpProtocolSender sender;

    private final ScheduledExecutorService callbackExecutor;
    private final ExecutorService eventBusExecutor;

    private final EventBus eventBus;

    private final Closer closer = Closer.create();

    private final SchedulerDriverContext context;

    /**
     * Creates a new driver for the specified scheduler. The master
     * must be specified as
     *
     *     zk://host1:port1,host2:port2,.../path
     *     zk://username:password@host1:port1,host2:port2,.../path
     *
     * The driver will attempt to "failover" if the specified
     * FrameworkInfo includes a valid FrameworkID.
     */
    protected InternalSchedulerDriver(final Scheduler scheduler,
                                      final FrameworkInfo frameworkInfo,
                                      final String master,
                                      final Credential credential)
                    throws IOException
    {
        this.scheduler = checkNotNull(scheduler, "scheduler is null");
        checkNotNull(frameworkInfo, "frameworkInfo is null");
        checkNotNull(master, "master is null");
        this.credential = credential;

        checkState(!master.equals("local"), "Java client can not launch a local cluster!");

        // TODO - Any volunteers to do the SASL dance?
        checkState(this.credential == null, "Credential is not supported yet.");

        final FrameworkInfo.Builder frameworkInfoBuilder = FrameworkInfo.newBuilder(frameworkInfo);

        if (!frameworkInfo.hasHostname()) {
            final InetAddress localHostName = InetAddress.getLocalHost();
            frameworkInfoBuilder.setHostname(localHostName.getHostName());
        }

        if (!frameworkInfo.hasUser() || "".equals(frameworkInfo.getUser())) {
            frameworkInfoBuilder.setUser(System.getProperty("user.name"));
        }

        context = new SchedulerDriverContext(frameworkInfoBuilder.build());

        this.callbackExecutor = closer.register(CloseableExecutors.decorate(Executors.newScheduledThreadPool(5, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("scheduler-callback-%d").build())));
        this.eventBusExecutor = closer.register(CloseableExecutors.decorate(Executors.newScheduledThreadPool(10, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("scheduler-driver-%d").build())));

        this.eventBus = new AsyncEventBus("mesos-scheduler", eventBusExecutor);

        this.localMessageProcessor = new LocalSchedulerMessageProcessor(context, eventBus);

        this.sender = closer.register(new HttpProtocolSender(context.getDriverUPID()));
        this.receiver = closer.register(new HttpProtocolReceiver(context.getDriverUPID(), SchedulerMessageEnvelope.class, eventBus));
        this.detector = closer.register(new ZookeeperMasterDetector(master, eventBus));
    }

    @Override
    public void close()
        throws IOException
    {
        stop();
    }

    private void driverStart()
    {
        eventBus.register(localMessageProcessor);
        eventBus.register(detector);
        eventBus.register(this);

        this.receiver.start();
        this.detector.start();
    }

    //
    // ========================================================================
    //
    // Mesos SchedulerDriver API
    //
    // ========================================================================
    //

    @Override
    public Status start()
    {
        if (!context.isStateMachine(DRIVER_NOT_STARTED)) {
            return context.getStateMachine();
        }

        try {
            driverStart();

            masterChanged(null);

            context.setStateMachine(DRIVER_RUNNING);
        }
        catch (final Exception e) {
            context.setStateMachine(DRIVER_ABORTED);
            LOG.error(e, "Failed to create scheduler process for '%s'", context.getDriverUPID());

            eventBus.post(new SchedulerCallback() {
                @Override
                public Runnable getCallback(final Scheduler scheduler, final SchedulerDriver schedulerDriver)
                {
                    return new Runnable() {
                        @Override
                        public void run()
                        {
                            scheduler.error(schedulerDriver, format("Failed to create scheduler process for '%s': %s", context.getDriverUPID(), e.getMessage()));
                        }
                    };
                }
            });
        }

        return context.getStateMachine();
    }

    @Override
    public Status stop()
    {
        return stop(false);
    }

    @Override
    public Status stop(final boolean failover)
    {
        Status status = context.getStateMachine();

        if (!context.isStateMachine(DRIVER_RUNNING, DRIVER_ABORTED)) {
            return status;
        }

        if (context.isConnected() && !failover) {
            final UnregisterFrameworkMessage message = UnregisterFrameworkMessage.newBuilder()
                .setFrameworkId(context.getFrameworkId())
                .build();
            eventBus.post(new RemoteMessageEnvelope(context.getDriverUPID(), context.getMasterUPID(), message));
        }

        try {
            closer.close();
        }
        catch (final IOException e) {
            LOG.warn(e, "While stopping");
        }

        try {
            callbackExecutor.awaitTermination(1, TimeUnit.DAYS);
        }
        catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        try {
            eventBusExecutor.awaitTermination(1, TimeUnit.DAYS);
        }
        catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        try {
            sender.drainRequests();
        }
        catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        context.setStateMachine(DRIVER_STOPPED);

        // If the driver was aborted, preserve that
        // state on the return.
        if (status != DRIVER_ABORTED) {
            status = DRIVER_STOPPED;
        }

        return status;
    }

    @Override
    public Status abort()
    {
        if (!context.isStateMachine(DRIVER_RUNNING)) {
            return context.getStateMachine();
        }

        if (context.isConnected()) {
            final DeactivateFrameworkMessage message = DeactivateFrameworkMessage.newBuilder().setFrameworkId(context.getFrameworkId()).build();
            eventBus.post(new RemoteMessageEnvelope(context.getDriverUPID(), context.getMasterUPID(), message));
        }

        context.setStateMachine(DRIVER_ABORTED);

        return context.getStateMachine();
    }

    @Override
    @SuppressWarnings("PMD.PreserveStackTrace")
    public Status join()
    {
        if (!context.isStateMachine(DRIVER_RUNNING)) {
            return context.getStateMachine();
        }

        final Future<Status> statusFuture = context.waitForStateChange(DRIVER_RUNNING);
        try {
            return statusFuture.get();
        }
        catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (final ExecutionException e) {
            final Throwable t = e.getCause();
            throw Throwables.propagate(t);
        }

        return context.getStateMachine();
    }

    @Override
    public Status run()
    {
        start();

        if (context.isStateMachine(DRIVER_RUNNING)) {
            join();
        }

        return context.getStateMachine();
    }

    @Override
    public Status killTask(final TaskID taskId)
    {
        checkNotNull(taskId, "taskId is null");

        if (!context.isStateMachine(DRIVER_RUNNING)) {
            return context.getStateMachine();
        }

        final KillTaskMessage message = KillTaskMessage.newBuilder()
            .setFrameworkId(context.getFrameworkId())
            .setTaskId(taskId)
            .build();
        eventBus.post(new RemoteMessageEnvelope(context.getDriverUPID(), context.getMasterUPID(), message));

        return context.getStateMachine();
    }

    @Override
    public Status launchTasks(final OfferID offerId, final Collection<TaskInfo> tasks)
    {
        checkNotNull(offerId, "offerId is null");
        checkNotNull(tasks, "tasks is null");

        if (!context.isStateMachine(DRIVER_RUNNING)) {
            return context.getStateMachine();
        }

        final LaunchTasksMessage message = LaunchTasksMessage.newBuilder()
            .setFrameworkId(context.getFrameworkId())
            .addOfferIds(offerId)
            .addAllTasks(tasks)
            .setFilters(Filters.newBuilder().build())
            .build();

        doLaunchTasks(message);
        return context.getStateMachine();
    }

    @Override
    public Status launchTasks(final OfferID offerId, final Collection<TaskInfo> tasks, final Filters filters)
    {
        checkNotNull(offerId, "offerId is null");
        checkNotNull(tasks, "tasks is null");
        checkNotNull(filters, "filters is null");

        if (!context.isStateMachine(DRIVER_RUNNING)) {
            return context.getStateMachine();
        }

        final LaunchTasksMessage message = LaunchTasksMessage.newBuilder()
            .setFrameworkId(context.getFrameworkId())
            .addOfferIds(offerId)
            .addAllTasks(tasks)
            .setFilters(filters)
            .build();

        doLaunchTasks(message);
        return context.getStateMachine();
    }

    @Override
    public Status launchTasks(final Collection<OfferID> offerIds, final Collection<TaskInfo> tasks)
    {
        checkNotNull(offerIds, "offerIds is null");
        checkNotNull(tasks, "tasks is null");

        if (!context.isStateMachine(DRIVER_RUNNING)) {
            return context.getStateMachine();
        }

        final LaunchTasksMessage message = LaunchTasksMessage.newBuilder()
            .setFrameworkId(context.getFrameworkId())
            .addAllOfferIds(offerIds)
            .addAllTasks(tasks)
            .setFilters(Filters.newBuilder().build())
            .build();

        doLaunchTasks(message);
        return context.getStateMachine();
    }

    @Override
    public Status launchTasks(final Collection<OfferID> offerIds, final Collection<TaskInfo> tasks, final Filters filters)
    {
        checkNotNull(offerIds, "offerIds is null");
        checkNotNull(tasks, "tasks is null");
        checkNotNull(filters, "filters is null");

        if (!context.isStateMachine(DRIVER_RUNNING)) {
            return context.getStateMachine();
        }

        final LaunchTasksMessage message = LaunchTasksMessage.newBuilder()
            .setFrameworkId(context.getFrameworkId())
            .addAllOfferIds(offerIds)
            .addAllTasks(tasks)
            .setFilters(filters)
            .build();

        doLaunchTasks(message);
        return context.getStateMachine();
    }

    @Override
    public Status declineOffer(final OfferID offerId, final Filters filters)
    {
        checkNotNull(offerId, "offerId is null");
        checkNotNull(filters, "filters is null");

        if (!context.isStateMachine(DRIVER_RUNNING)) {
            return context.getStateMachine();
        }

        final LaunchTasksMessage message = LaunchTasksMessage.newBuilder()
            .setFrameworkId(context.getFrameworkId())
            .addOfferIds(offerId)
            .setFilters(filters)
            .build();

        doLaunchTasks(message);
        return context.getStateMachine();
    }

    @Override
    public Status declineOffer(final OfferID offerId)
    {
        checkNotNull(offerId, "offerId is null");

        if (!context.isStateMachine(DRIVER_RUNNING)) {
            return context.getStateMachine();
        }

        final LaunchTasksMessage message = LaunchTasksMessage.newBuilder()
            .setFrameworkId(context.getFrameworkId())
            .addOfferIds(offerId)
            .setFilters(Filters.newBuilder().build())
            .build();

        doLaunchTasks(message);
        return context.getStateMachine();
    }

    @Override
    public Status reviveOffers()
    {
        if (!context.isStateMachine(DRIVER_RUNNING)) {
            return context.getStateMachine();
        }

        final ReviveOffersMessage message = ReviveOffersMessage.newBuilder()
            .setFrameworkId(context.getFrameworkId())
            .build();
        eventBus.post(new RemoteMessageEnvelope(context.getDriverUPID(), context.getMasterUPID(), message));

        return context.getStateMachine();
    }

    @Override
    public Status sendFrameworkMessage(final ExecutorID executorId, final SlaveID slaveId, final byte[] data)
    {
        checkNotNull(executorId, "executorId is null");
        checkNotNull(slaveId, "slaveId is null");
        checkNotNull(data, "data is null");

        if (!context.isStateMachine(DRIVER_RUNNING)) {
            return context.getStateMachine();
        }

        final FrameworkToExecutorMessage message = FrameworkToExecutorMessage.newBuilder()
            .setFrameworkId(context.getFrameworkId())
            .setExecutorId(executorId)
            .setSlaveId(slaveId)
            .setData(ByteString.copyFrom(data)).build();

        // If the UPID of that slave is known (from the slave cache, then send the message
        // directly to the slave, otherwise to the master and let the master sort it out.
        if (context.containsSlave(message.getSlaveId())) {
            final UPID slave = context.getSlaveUPID(message.getSlaveId());
            eventBus.post(new RemoteMessageEnvelope(context.getDriverUPID(), slave, message));
        }
        else {
            eventBus.post(new RemoteMessageEnvelope(context.getDriverUPID(), context.getMasterUPID(), message));
        }

        return context.getStateMachine();
    }

    @Override
    public Status reconcileTasks(final Collection<TaskStatus> statuses)
    {
        if (!context.isStateMachine(DRIVER_RUNNING)) {
            return context.getStateMachine();
        }

        final ReconcileTasksMessage message = ReconcileTasksMessage.newBuilder()
            .setFrameworkId(context.getFrameworkId())
            .addAllStatuses(statuses)
            .build();
        eventBus.post(new RemoteMessageEnvelope(context.getDriverUPID(), context.getMasterUPID(), message));

        return context.getStateMachine();
    }

    @Override
    public Status requestResources(final Collection<Protos.Request> requests)
    {
        checkNotNull(requests, "requests is null");

        if (!context.isStateMachine(DRIVER_RUNNING)) {
            return context.getStateMachine();
        }

        final ResourceRequestMessage message = ResourceRequestMessage.newBuilder()
            .setFrameworkId(context.getFrameworkId())
            .addAllRequests(requests)
            .build();
        eventBus.post(new RemoteMessageEnvelope(context.getDriverUPID(), context.getMasterUPID(), message));

        return context.getStateMachine();
    }

    //
    // Remote message delivery
    //

    @Subscribe
    public void sendMessage(final RemoteMessageEnvelope envelope) throws Exception
    {
        final Message message = envelope.getMessage();
        final UPID recipient = envelope.getRecipient();

        checkState(!recipient.equals(context.getDriverUPID()), "Received a message with local recipient! (%s)", message);

        sender.sendHttpMessage(recipient, message);
    }

    //
    // Scheduler callback delivery
    //

    @Subscribe
    public void processSchedulerCallback(final SchedulerCallback callback)
    {
        callbackExecutor.submit(callback.getCallback(scheduler, this));
    }

    //
    // Launch Tasks processing
    //

    private void doLaunchTasks(final LaunchTasksMessage message)
    {
        final MasterInfo masterInfo = context.connectedMaster();

        if (masterInfo == null) {
            loseAllTasks(message.getTasksList(), "Master disconnected");
            return;
        }

        final ImmutableList.Builder<TaskInfo> builder = ImmutableList.builder();

        for (TaskInfo taskInfo : message.getTasksList()) {
            if (taskInfo.hasExecutor() == taskInfo.hasCommand()) {
                loseTask(taskInfo, "TaskInfo must have either an 'executor' or a 'command'");
                continue; // for(...
            }

            if (taskInfo.hasExecutor()) {
                if (taskInfo.getExecutor().hasFrameworkId()) {
                    final FrameworkID executorFrameworkId = taskInfo.getExecutor().getFrameworkId();
                    if (!executorFrameworkId.equals(context.getFrameworkId())) {
                        loseTask(taskInfo, format("ExecutorInfo has an invalid FrameworkID (Actual: %s vs Expected: %s)", executorFrameworkId.getValue(), context.getFrameworkId().getValue()));
                        continue; // for(...
                    }
                }
                else {
                    // Executor present but not framework id. Set the framework id.
                    taskInfo = TaskInfo.newBuilder(taskInfo)
                                    .setExecutor(ExecutorInfo.newBuilder(taskInfo.getExecutor()).setFrameworkId(context.getFrameworkId()))
                                    .build();
                }
            }

            builder.add(taskInfo);
        }

        final List<TaskInfo> launchTasks = builder.build();

        for (final OfferID offer : message.getOfferIdsList()) {
            if (!context.hasOffers(offer)) {
                LOG.warn("Unknown offer %s ignored!", offer.getValue());
            }

            for (final TaskInfo launchTask : launchTasks) {
                if (context.hasOffer(offer, launchTask.getSlaveId())) {
                    context.addSlave(launchTask.getSlaveId(), context.getOffer(offer, launchTask.getSlaveId()));
                }
                else {
                    LOG.warn("Attempting to launch task %s with wrong slave id %s", launchTask.getTaskId().getValue(), launchTask.getSlaveId().getValue());
                }
            }
            context.removeAllOffers(offer);
        }

        final LaunchTasksMessage launchMessage = LaunchTasksMessage.newBuilder(message)
            .setFrameworkId(context.getFrameworkId())
            .clearTasks()
            .addAllTasks(launchTasks)
            .build();

        eventBus.post(new RemoteMessageEnvelope(context.getDriverUPID(), context.getMasterUPID(), launchMessage));
    }

    private void loseAllTasks(final Iterable<TaskInfo> taskInfos, final String reason)
    {
        for (final TaskInfo taskInfo : taskInfos) {
            loseTask(taskInfo, reason);
        }
    }

    private void loseTask(final TaskInfo taskInfo, final String reason)
    {
        final StatusUpdateMessage statusUpdate = StatusUpdateMessage.newBuilder()
            .setUpdate(StatusUpdate.newBuilder()
                .setFrameworkId(context.getFrameworkId())
                .setSlaveId(taskInfo.getSlaveId())
                .setExecutorId(taskInfo.getExecutor().getExecutorId())
                .setStatus(TaskStatus.newBuilder().setTaskId(taskInfo.getTaskId()).setState(TaskState.TASK_LOST).setMessage(reason))
                .setTimestamp(TimeUtil.currentTime())
                .setUuid(UUIDUtil.uuidBytes(UUID.randomUUID())))
            .build();

        eventBus.post(new StatusUpdateMessageEnvelope(context.getDriverUPID(), context.getDriverUPID(), statusUpdate));
    }

    //
    // Master connect / disconnect / reconnect handing.
    //

    private final FutureCallback<MasterInfo> masterInfoCallback = new FutureCallback<MasterInfo>() {
        @Override
        public void onSuccess(final MasterInfo masterInfo)
        {
            masterChanged(masterInfo);
        }

        @Override
        public void onFailure(final Throwable t)
        {
            LOG.warn(t, "Master detection failed!");
            masterChanged(null);
        }
    };

    private void masterChanged(final MasterInfo masterInfo)
    {
        if (context.isStateMachine(DRIVER_ABORTED)) {
            LOG.debug("driver is aborted!");
            return;
        }

        try {
            if (masterInfo != null) {
                LOG.debug("Master detected: %s", UPID.create(masterInfo.getPid()).asString());
            }
            else {
                LOG.debug("No master detected.");
            }

            context.setMaster(masterInfo);

            if (context.disconnected()) {
                callbackExecutor.submit(new Runnable() {
                    @Override
                    public void run()
                    {
                        scheduler.disconnected(InternalSchedulerDriver.this);
                    }
                });
            }

            if (masterInfo != null) {
                callbackExecutor.submit(new Runnable() {
                    @Override
                    public void run()
                    {
                        final MasterInfo master = context.getMaster();
                        if (context.isConnected() || master == null) {
                            return;
                        }

                        final FrameworkInfo frameworkInfo = context.getFrameworkInfo();

                        if (!context.hasFrameworkId()) {
                            final RegisterFrameworkMessage message = RegisterFrameworkMessage
                                .newBuilder()
                                .setFramework(frameworkInfo)
                                .build();
                            eventBus.post(new RemoteMessageEnvelope(context.getDriverUPID(), context.getMasterUPID(), message));
                        }
                        else {
                            final ReregisterFrameworkMessage message = ReregisterFrameworkMessage
                                .newBuilder()
                                .setFramework(frameworkInfo)
                                .setFailover(context.isFailover())
                                .build();
                            eventBus.post(new RemoteMessageEnvelope(context.getDriverUPID(), context.getMasterUPID(), message));
                        }

                        callbackExecutor.schedule(this, 1, TimeUnit.SECONDS);
                    }
                });
            }
        }
        finally {
            final ListenableFuture<MasterInfo> masterFuture = detector.detect(masterInfo);
            Futures.addCallback(masterFuture, masterInfoCallback);
        }
    }
}
