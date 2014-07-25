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

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.InvalidProtocolBufferException;
import com.groupon.mesos.util.Log;
import com.groupon.mesos.util.UPID;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
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

    private final CuratorFramework curatorFramework;
    private final PathChildrenCache pathChildrenCache;
    private final EventBus eventBus;

    // TODO this can be replaced by pathChildrenCache I think
    private final SortedMap<String, MasterInfo> nodeCache = new TreeMap<>();
    private final BlockingQueue<SettableFuture<MasterInfo>> futures = new LinkedBlockingQueue<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    public ZookeeperMasterDetector(final String master, final EventBus eventBus) throws IOException
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
        // TODO - Curator supports authorization but the correct scheme, etc. needs to be constructed

        // TODO make configurable
        curatorFramework = CuratorFrameworkFactory.builder().connectString(authority).retryPolicy(new BoundedExponentialBackoffRetry(1000, 10000, 3)).build();
        pathChildrenCache = new PathChildrenCache(curatorFramework, zookeeperPath, true);  // TODO it appears that the data isn't currently used - but there is unused code that seems to want it
        PathChildrenCacheListener listener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event) throws Exception {
                if ( (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) || (event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) || (event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED)) {
                    // TODO - does this always have to be a complete list? PathChildrenCache returns changes for each individual node
                    processChildDataList(pathChildrenCache.getCurrentData());
                }
            }
        };
        pathChildrenCache.getListenable().addListener(listener);
    }

    @Override
    public void close() throws IOException
    {
        if (running.getAndSet(false)) {
            pathChildrenCache.close();
            curatorFramework.close();
        }
    }

    public void start()
    {
        if (!running.getAndSet(true)) {
            curatorFramework.start();
            try {
                pathChildrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
                processChildDataList(pathChildrenCache.getCurrentData());
            }
            catch ( Exception e ) {
                LOG.error("Could not start ZooKeeper cache", e);
                throw new RuntimeException(e);
            }
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
            final MasterInfo masterInfo = deserialize(pathChildrenCache.getCurrentData(path));
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

        List<SettableFuture<MasterInfo>> settableFutures = new ArrayList<>(futures.size());
        futures.drainTo(settableFutures);

        for (SettableFuture<MasterInfo> future : settableFutures) {
            future.set(masterInfo);
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
            futures.add(future);
        }
    }

    private void processChildDataList(final List<ChildData> childrenData)
    {
        List<String> children = Lists.transform(childrenData, new Function<ChildData, String>()
        {
            @Override
            public String apply(ChildData childData)
            {
                return childData.getPath();
            }
        });
        processChildList(children);
    }

    private void processChildList(final List<String> children)
    {
        // TODO - does this always have to be a complete list? PathChildrenCache returns changes for each individual node
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

    private MasterInfo deserialize(ChildData childData)
    {
        checkNotNull(childData, "childData is null");
        return deserialize(childData.getData());
    }

    private MasterInfo deserialize(final byte[] bytes)
    {
        checkNotNull(bytes, "bytes is null");
        try {
            return MasterInfo.parseFrom(bytes);
        }
        catch (final InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }
}
