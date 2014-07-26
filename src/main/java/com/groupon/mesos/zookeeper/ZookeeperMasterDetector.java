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
package com.groupon.mesos.zookeeper;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Objects;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.InvalidProtocolBufferException;
import com.groupon.mesos.util.Log;
import com.groupon.mesos.util.ManagedEventBus;
import com.groupon.mesos.util.UPID;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.mesos.Protos.MasterInfo;

/**
 * Mesos master detector. Watches all the child nodes of the mesos directory in zookeeper, loads all the child nodes when anything
 * changes and uses the numerically lowest as master.
 *
 * Uses the event bus to decouple event submission from event execution, otherwise caller threads might end up running large swaths
 * of watcher specific codes (especially when the master changes). May be overkill but it seems to be the right thing.
 */
public class ZookeeperMasterDetector
    implements Closeable
{
    private static final Log LOG = Log.getLog(ZookeeperMasterDetector.class);

    private final String zookeeperPath;
    private final String user;
    private final String password;

    private final ZkClient client;
    private final ManagedEventBus eventBus;

    private final SortedMap<String, MasterInfo> nodeCache = new TreeMap<>();
    private final BlockingQueue<DetectMessage> futures = new LinkedBlockingQueue<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    public ZookeeperMasterDetector(final String master, final ManagedEventBus eventBus) throws IOException
    {

        checkNotNull(master, "master is null");
        this.eventBus = checkNotNull(eventBus, "eventBus is null");

        final URI zookeeperUri = URI.create(master);
        checkState(zookeeperUri.getScheme().equals("zk"), "Only zk:// URIs are supported (%s)", master);

        String authority = zookeeperUri.getAuthority();
        final int atIndex = authority.indexOf('@');
        if (atIndex != -1) {
            final List<String> userPass = ImmutableList.copyOf(Splitter.on(':').trimResults().split(authority.substring(0, atIndex)));
            checkState(userPass.size() == 2, "found %s for user name and password", userPass);
            user = userPass.get(0);
            password = userPass.get(1);
            authority = authority.substring(atIndex + 1);
        }
        else {
            user = null;
            password = null;
        }

        String zookeeperPath = zookeeperUri.getPath();
        while (zookeeperPath.endsWith("/")) {
            zookeeperPath = zookeeperPath.substring(0, zookeeperPath.length() - 1);
        }
        this.zookeeperPath = zookeeperPath;

        checkState(!zookeeperPath.equals(""), "A zookeeper path must be given! (%s)", zookeeperPath);

        checkState(user == null && password == null, "Current version of Zkclient does not support authentication!");

        this.client = new ZkClient(authority);
        this.client.setZkSerializer(new MasterInfoZkSerializer());
    }

    @Override
    public void close() throws IOException
    {
        if (running.getAndSet(false)) {
            this.client.close();
        }
    }

    public void start()
    {
        if (!running.getAndSet(true)) {
            processChildList(client.getChildren(zookeeperPath));

            client.subscribeChildChanges(zookeeperPath, new IZkChildListener() {
                @Override
                public void handleChildChange(final String parentPath, final List<String> currentChildren) throws Exception
                {
                    checkState(zookeeperPath.equals(parentPath), "Received Event for %s (expected %s)", parentPath, zookeeperPath);
                    processChildList(currentChildren);
                }
            });
        }
    }

    public ListenableFuture<MasterInfo> detect(final MasterInfo previous)
    {
        checkState(running.get(), "not running");

        final SettableFuture<MasterInfo> result = SettableFuture.create();
        eventBus.post(new DetectMessage(result, previous));
        return result;
    }

    @Subscribe
    public void processMasterUpdate(final MasterUpdateMessage message)
    {
        final Set<String> currentNodes = message.getNodes();
        final Set<String> nodesToRemove = ImmutableSet.copyOf(Sets.difference(nodeCache.keySet(), currentNodes));
        final Set<String> nodesToAdd = ImmutableSet.copyOf(Sets.difference(currentNodes, nodeCache.keySet()));

        for (final String node : nodesToAdd) {
            final String path = zookeeperPath + "/" + node;
            final MasterInfo masterInfo = client.readData(path);
            nodeCache.put(node, masterInfo);
        }

        for (final String node : nodesToRemove) {
            nodeCache.remove(node);
        }

        LOG.debug("Processed event, active nodes are %s", nodeCache.entrySet());

        final MasterInfo masterInfo = getMaster();

        if (masterInfo == null) {
            LOG.debug("No current master exists!");
        }
        else {
            LOG.debug("Current master is %s", UPID.create(masterInfo.getPid()).asString());
        }

        List<DetectMessage> detectMessages = new ArrayList<>(futures.size());
        futures.drainTo(detectMessages);

        for (DetectMessage detectMessage : detectMessages) {
            processDetect(detectMessage);
        }
    }

    @Subscribe
    public void processDetect(final DetectMessage message)
    {
        final SettableFuture<MasterInfo> future = message.getFuture();
        final MasterInfo previous = message.getPrevious();
        final MasterInfo currentLeader = getMaster();

        if (!Objects.equal(currentLeader, previous)) {
            LOG.debug("Master has changed: %s -> %s", previous, currentLeader);
            future.set(currentLeader);
        }
        else {
            LOG.debug("Master unchanged, queueing");
            futures.add(message);
        }
    }

    private void processChildList(final List<String> children)
    {
        final Set<String> masterNodes = ImmutableSet.copyOf(Iterables.filter(children, Predicates.containsPattern("^info_")));
        eventBus.post(new MasterUpdateMessage(masterNodes));
    }

    private MasterInfo getMaster()
    {
        if (nodeCache.isEmpty()) {
            return null;
        }
        final String key = nodeCache.firstKey();
        return nodeCache.get(key);
    }

    private static class MasterInfoZkSerializer implements ZkSerializer
    {
        @Override
        public byte[] serialize(final Object data) throws ZkMarshallingError
        {
            checkState(data instanceof MasterInfo, "%s is not a MasterInfo!", data.getClass().getSimpleName());
            return ((MasterInfo) data).toByteArray();
        }

        @Override
        public Object deserialize(final byte[] bytes) throws ZkMarshallingError
        {
            checkNotNull(bytes, "bytes is null");
            try {
                return MasterInfo.parseFrom(bytes);
            }
            catch (final InvalidProtocolBufferException e) {
                return new ZkMarshallingError(e);
            }
        }
    }
}
