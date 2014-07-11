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

import static org.iq80.leveldb.impl.Iq80DBFactory.asString;
import static org.iq80.leveldb.impl.Iq80DBFactory.bytes;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.groupon.mesos.util.Log;

import org.apache.mesos.state.State;
import org.apache.mesos.state.Variable;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.impl.Iq80DBFactory;

import mesos.internal.state.State.Entry;

public class JLevelDBState implements State, Closeable
{
    private static final Log LOG = Log.getLog(JLevelDBState.class);

    private final DB db;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ExecutorService executor = Executors.newFixedThreadPool(10, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("JLevelDB-State-%d").build());

    public JLevelDBState(final String path)
                    throws IOException
    {
        checkNotNull(path, "path is null");
        final Options options = new Options();
        options.createIfMissing(true);
        this.db = Iq80DBFactory.factory.open(new File(path), options);
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

            db.close();
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
                // Interning the string will make sure that all
                // synchronized blocks use the same java object monitor
                // for the same string value, which in turn serves
                // as poor man's row lock.
                final String internedName = name.intern();
                synchronized (internedName) {
                    final JVariable var = load(name);
                    if (var == null) {
                        return new JVariable(name, JVariable.EMPTY_BYTES);
                    }
                    else {
                        return var;
                    }
                }
            }
        });
    }

    @Override
    public Future<Variable> store(final Variable variable)
    {
        checkNotNull(variable, "variable is null");
        checkState(!closed.get(), "already closed");
        checkState(variable instanceof JVariable, "can not process native variable, use JVariable");

        final JVariable v = (JVariable) variable;

        return executor.submit(new Callable<Variable>() {
            @Override
            public Variable call() throws Exception
            {
                final WriteOptions writeOptions = new WriteOptions();
                writeOptions.sync(true);

                final String internedName = v.getName().intern();
                synchronized (internedName) {
                    final JVariable current = load(internedName);
                    if (current == null || current.getUuid().equals(v.getUuid())) {
                        final JVariable update = new JVariable(internedName, v.value());
                        final WriteBatch writeBatch = db.createWriteBatch();
                        writeBatch.delete(bytes(internedName));
                        writeBatch.put(bytes(internedName), update.getEntry().toByteArray());
                        db.write(writeBatch, writeOptions);
                        return update;
                    }
                    else {
                        return null;
                    }
                }
            }
        });
    }

    @Override
    public Future<Boolean> expunge(final Variable variable)
    {
        checkNotNull(variable, "variable is null");
        checkState(!closed.get(), "already closed");
        checkState(variable instanceof JVariable, "can not process native variable, use JVariable");

        final JVariable v = (JVariable) variable;

        return executor.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception
            {
                final WriteOptions writeOptions = new WriteOptions();
                writeOptions.sync(true);

                final String internedName = v.getName().intern();
                synchronized (internedName) {
                    final JVariable current = load(internedName);
                    if (current != null && current.getUuid().equals(v.getUuid())) {
                        db.delete(bytes(internedName));
                        return Boolean.TRUE;
                    }
                    else {
                        return Boolean.FALSE;
                    }
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
                return new ClosingIterator(db.iterator());
            }
        });
    }

    private JVariable load(final String name) throws IOException
    {
        final byte[] value = db.get(bytes(name));

        if (value == null) {
            return null;
        }
        else {
            final Entry entry = Entry.parseFrom(value);
            return new JVariable(entry);
        }
    }

    private static class ClosingIterator
        extends AbstractIterator<String>
        implements Iterator<String>, Closeable
    {
        private final DBIterator dbIterator;
        private final AtomicBoolean closed = new AtomicBoolean();

        private ClosingIterator(final DBIterator dbIterator)
        {
            this.dbIterator = checkNotNull(dbIterator, "dbIterator is null");
            this.dbIterator.seekToFirst();
        }

        @Override
        protected String computeNext()
        {
            if (!closed.get() && dbIterator.hasNext()) {
                final Map.Entry<byte[], byte[]> value = dbIterator.next();
                return asString(value.getKey());
            }
            else {
                if (!closed.getAndSet(true)) {
                    try {
                        dbIterator.close();
                    }
                    catch (final IOException ioe) {
                        LOG.warn(ioe, "while closing iterator");
                    }
                }
                return endOfData();
            }
        }

        @Override
        public void close() throws IOException
        {
            if (!closed.getAndSet(true)) {
                dbIterator.close();
            }
        }
    }
}
