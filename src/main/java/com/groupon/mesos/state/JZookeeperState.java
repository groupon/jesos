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
package com.groupon.mesos.state;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.groupon.mesos.state.JVariable.EMPTY_BYTES;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.groupon.mesos.util.Log;

import org.apache.mesos.state.State;
import org.apache.mesos.state.Variable;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.BadVersionException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import mesos.internal.state.State.Entry;

public class JZookeeperState implements State, Closeable
{
    private static final Log LOG = Log.getLog(JZookeeperState.class);

    private final AtomicBoolean closed = new AtomicBoolean();
    private final ExecutorService executor = Executors.newFixedThreadPool(10, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("JZookeeper-State-%d").build());

    private final ZooKeeper client;
    private final String path;

    public JZookeeperState(final String servers,
                           final long timeout,
                           final TimeUnit unit,
                           final String znode) throws IOException
    {
        this(servers, timeout, unit, znode, null, null);
    }

    public JZookeeperState(final String servers,
                           final long timeout,
                           final TimeUnit unit,
                           final String znode,
                           final String scheme,
                           final byte[] credentials) throws IOException
    {
        checkNotNull(servers, "servers is null");
        checkNotNull(unit, "unit is null");
        checkNotNull(znode, "znode is null");

        checkState(scheme == null && credentials == null, "Authentication is currently not supported!");

        this.client = new ZooKeeper(servers, Ints.checkedCast(unit.toMillis(timeout)), new StateWatcher());

        String path = znode.startsWith("/") ? znode : "/" + znode;
        path = path.endsWith("/") ? path.substring(0, path.length() - 1) : path;

        this.path = path;
    }

    @Override
    public void close() throws IOException
    {
        if (!closed.getAndSet(true)) {
            executor.shutdown();

            try {
                executor.awaitTermination(1, TimeUnit.DAYS);
            }
            catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            try {
                client.close();
            }
            catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public Future<Variable> fetch(final String name)
    {
        checkNotNull(name, "name is null");
        checkState(!closed.get(), "already closed");

        return executor.submit(new Callable<Variable>() {
            @Override
            public Variable call() throws Exception
            {
                final ZookeeperVariable var = load(getFullPath(name));
                if (var == null) {
                    return new ZookeeperVariable(name, EMPTY_BYTES);
                }
                else {
                    return var;
                }
            }
        });
    }

    @Override
    public Future<Variable> store(final Variable variable)
    {
        checkNotNull(variable, "variable is null");
        checkState(!closed.get(), "already closed");
        checkState(variable instanceof ZookeeperVariable, "can not process native variable, use ZookeeperVariable");

        final ZookeeperVariable v = (ZookeeperVariable) variable;

        checkState(v.asBytes().length < 1_048_576, "Entry size exceeds 1 MB");

        final String fullName = getFullPath(v.getName());

        return executor.submit(new Callable<Variable>() {
            @Override
            public Variable call() throws Exception
            {
                final ZookeeperVariable update = new ZookeeperVariable(v.getName(), v.value());

                ZookeeperVariable current = load(fullName);

                while (true) {
                    // Node does not exist. Create it.
                    if (current == null) {
                        LOG.debug("Node %s does not exist", fullName);
                        try {
                            createNodeRecursively(fullName, update.asBytes());
                            LOG.debug("Node %s successfully created", fullName);
                            return update;
                        }
                        catch (final NodeExistsException e) {
                            // Someone beat us to creating the node. Then load it.
                            LOG.debug("Lost Node %s race, reloading", fullName);
                            current = load(fullName);
                        }
                    }

                    if (current != null) {

                        if (!current.getUuid().equals(v.getUuid())) {
                            return null;
                        }

                        checkState(current.getZookeeperVersion() != null, "store with unknown zookeeper version (%s)", current.getEntry());

                        try {
                            client.setData(fullName, update.asBytes(), current.getZookeeperVersion());
                            return update;
                        }
                        catch (BadVersionException | NoNodeException e) {
                            // Version has changed under us or the node has disappeared. Retry (which will probably fail unless it was deleted).
                            LOG.debug("Could not change version %d, retry writing", current.getZookeeperVersion());
                        }
                    }

                    // Current could be null here if the node was deleted while we were not looking.
                    current = load(fullName);
                }
            }
        });
    }

    @Override
    public Future<Boolean> expunge(final Variable variable)
    {
        checkNotNull(variable, "variable is null");
        checkState(!closed.get(), "already closed");
        checkState(variable instanceof ZookeeperVariable, "can not process native variable, use ZookeeperVariable");

        final ZookeeperVariable v = (ZookeeperVariable) variable;
        final String fullName = getFullPath(v.getName());

        return executor.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception
            {
                ZookeeperVariable current = load(fullName);

                while (true) {
                    if (current == null) {
                        return false;
                    }

                    if (!current.getUuid().equals(v.getUuid())) {
                        return false;
                    }

                    checkState(current.getZookeeperVersion() != null, "expunge with unknown zookeeper version (%s)", current.getEntry());

                    try {
                        client.delete(fullName, current.getZookeeperVersion());
                        return true;
                    }
                    catch (BadVersionException | NoNodeException e) {
                        // Version has changed under us or the node has disappeared. Retry (which will probably fail unless it was deleted).
                        LOG.debug("Could not change version %d, retry expunging", current.getZookeeperVersion());
                    }

                    // Current could be null here if the node was deleted while we were not looking.
                    current = load(fullName);
                }
            }
        });
    }

    @Override
    public Future<Iterator<String>> names()
    {
        checkState(!closed.get(), "already closed");

        return executor.submit(new Callable<Iterator<String>>() {
            @Override
            public Iterator<String> call() throws Exception
            {
                // Not very memory efficient if there is a large number
                // of children. Ah, well. It is good enough.
                final List<String> children = client.getChildren(path, false);
                return children.iterator();
            }
        });
    }

    private ZookeeperVariable load(final String name) throws KeeperException, IOException
    {
        final Stat stat = new Stat();
        try {
            final Entry entry = Entry.parseFrom(client.getData(name, false, stat));
            return new ZookeeperVariable(entry, stat.getVersion());
        }
        catch (final NoNodeException e) {
            return null;
        }
        catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private String getFullPath(final String name)
    {
        return String.format("%s/%s", path, name);
    }

    private void createNodeRecursively(final String path, final byte[] data) throws KeeperException, InterruptedException {
        if (!path.isEmpty() && client.exists(path, false) == null) {
            final String parent = path.substring(0, path.lastIndexOf('/'));
            createNodeRecursively(parent, new byte[0]);
            client.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    private static class StateWatcher implements Watcher
    {

        @Override
        public void process(final WatchedEvent event)
        {
            LOG.info("Received watched event %s", event);
        }
    }

}
