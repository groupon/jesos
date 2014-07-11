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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.mesos.state.State;
import org.apache.mesos.state.Variable;
import org.junit.Test;

public abstract class AbstractTestState
{
    protected abstract State getState();

    @Test
    public void testNonExistentValue()
        throws Exception
    {
        final State state = getState();

        final Variable empty = state.fetch("does-not-exist").get();
        assertNotNull(empty);
        assertTrue(empty.value().length == 0);
    }

    @Test
    public void testSetAndGet()
        throws Exception
    {
        final State state = getState();

        final byte[] value = "The quick brown fox jumps over the lazy dog.".getBytes(StandardCharsets.UTF_8);

        final Variable var = state.fetch("someValue").get();
        assertTrue(var.value().length == 0);
        final Variable newVar = var.mutate(value);

        final JVariable storedVar = (JVariable) state.store(newVar).get();
        assertNotNull(storedVar);

        final JVariable retrievedVar = (JVariable) state.fetch("someValue").get();

        assertNotNull(retrievedVar);
        assertArrayEquals(newVar.value(), retrievedVar.value());

        assertArrayEquals(storedVar.value(), retrievedVar.value());
        assertEquals(storedVar.getName(), retrievedVar.getName());
        assertEquals(storedVar.getUuid(), retrievedVar.getUuid());
    }

    @Test
    public void testNames()
        throws Exception
    {
        final State state = getState();

        final byte[] value = "The quick brown fox jumps over the lazy dog.".getBytes(StandardCharsets.UTF_8);

        final ImmutableSortedSet.Builder<String> builder = ImmutableSortedSet.naturalOrder();

        for (int i = 0; i < 10; i++) {
            final String key = "name-" + UUID.randomUUID().toString();
            builder.add(key);

            final Variable var = state.fetch(key).get();
            assertTrue(var.value().length == 0);
            state.store(var.mutate(value)).get();
        }

        final SortedSet<String> keys = builder.build();

        final Iterator<String> it = state.names().get();

        final SortedSet<String> entries = ImmutableSortedSet.copyOf(it);

        assertEquals(keys, entries);
    }

    @Test
    public void testUpdateOk()
        throws Exception
    {
        final State state = getState();

        final byte[] value = "The quick brown fox jumps over the lazy dog.".getBytes(StandardCharsets.UTF_8);
        final byte[] newValue = "Ich esse Autos zum Abendbrot und mache Kopfsprung ins Sandbecken. Gruen. Rot. Pferderennen.".getBytes(StandardCharsets.UTF_8);

        final Variable var = state.fetch("someValue").get();
        assertTrue(var.value().length == 0);

        JVariable storedVar = (JVariable) state.store(var.mutate(value)).get();

        storedVar = (JVariable) state.store(storedVar.mutate(newValue)).get();
        assertNotNull(storedVar);

        final JVariable retrievedVar = (JVariable) state.fetch("someValue").get();
        assertNotNull(retrievedVar);

        assertArrayEquals(storedVar.value(), retrievedVar.value());
        assertEquals(storedVar.getName(), retrievedVar.getName());
        assertEquals(storedVar.getUuid(), retrievedVar.getUuid());
    }

    @Test
    public void testOldUpdateRefused()
        throws Exception
    {
        final State state = getState();

        final byte[] value = "The quick brown fox jumps over the lazy dog.".getBytes(StandardCharsets.UTF_8);
        final byte[] newValue = "Ich esse Autos zum Abendbrot und mache Kopfsprung ins Sandbecken. Gruen. Rot. Pferderennen.".getBytes(StandardCharsets.UTF_8);

        final Variable var = state.fetch("someValue").get();
        assertTrue(var.value().length == 0);

        JVariable storedVar = (JVariable) state.store(var.mutate(value)).get();

        storedVar = (JVariable) state.store(var.mutate(newValue)).get();
        assertNull(storedVar);
    }

    @Test
    public void testExpungeOk()
        throws Exception
    {
        final State state = getState();

        final byte[] value = "The quick brown fox jumps over the lazy dog.".getBytes(StandardCharsets.UTF_8);

        final Variable var = state.fetch("someValue").get();
        assertTrue(var.value().length == 0);

        final JVariable storedVar = (JVariable) state.store(var.mutate(value)).get();

        final boolean expunged = state.expunge(storedVar).get();
        assertTrue(expunged);

        final JVariable retrievedVar = (JVariable) state.fetch("someValue").get();

        assertNotNull(retrievedVar);
        assertEquals(0, retrievedVar.value().length);
    }

    @Test
    public void testOldExpungeRefused()
        throws Exception
    {
        final State state = getState();

        final byte[] value = "The quick brown fox jumps over the lazy dog.".getBytes(StandardCharsets.UTF_8);

        final Variable var = state.fetch("someValue").get();
        assertTrue(var.value().length == 0);

        final JVariable storedVar = (JVariable) state.store(var.mutate(value)).get();

        final boolean expunged = state.expunge(var).get();
        assertFalse(expunged);

        final JVariable retrievedVar = (JVariable) state.fetch("someValue").get();
        assertNotNull(retrievedVar);

        assertArrayEquals(storedVar.value(), retrievedVar.value());
        assertEquals(storedVar.getName(), retrievedVar.getName());
        assertEquals(storedVar.getUuid(), retrievedVar.getUuid());
    }

    @Test
    public void testExpungeNonExistentValue()
        throws Exception
    {
        final State state = getState();

        final Variable empty = state.fetch("does-not-exist").get();
        assertNotNull(empty);
        assertTrue(empty.value().length == 0);

        final boolean expunged = state.expunge(empty).get();
        assertFalse(expunged);
    }

    @Test
    public void testTenMonkeysPressTenKeys()
        throws Exception
    {
        final State state = getState();

        final byte[] value = "The quick brown fox jumps over the lazy dog.".getBytes(StandardCharsets.UTF_8);
        final byte[] newValue = "Ich esse Autos zum Abendbrot und mache Kopfsprung ins Sandbecken. Gruen. Rot. Pferderennen.".getBytes(StandardCharsets.UTF_8);

        final ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());

        final ImmutableList.Builder<ListenableFuture<String>> builder = ImmutableList.builder();

        for (int i = 0; i < 10; i++) {
            builder.add(executor.submit(new Callable<String>() {

                @Override
                public String call() throws Exception
                {
                    final String key = "key-" + UUID.randomUUID().toString();
                    final Variable var = state.fetch(key).get();
                    assertTrue(var.value().length == 0);

                    JVariable storedVar = (JVariable) state.store(var.mutate(value)).get();

                    storedVar = (JVariable) state.store(storedVar.mutate(newValue)).get();
                    assertNotNull(storedVar);

                    final JVariable retrievedVar = (JVariable) state.fetch(key).get();
                    assertNotNull(retrievedVar);

                    assertArrayEquals(storedVar.value(), retrievedVar.value());
                    assertEquals(storedVar.getName(), retrievedVar.getName());
                    assertEquals(storedVar.getUuid(), retrievedVar.getUuid());

                    return key;
                }
            }));
        }

        final List<String> keys = Futures.allAsList(builder.build()).get();

        for (final String key : keys) {
            final JVariable retrievedVar = (JVariable) state.fetch(key).get();
            assertNotNull(retrievedVar);

            assertArrayEquals(newValue, retrievedVar.value());
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
    }

    @Test
    public void testTenMonkeysHammerOnTenKeys()
        throws Exception
    {
        final State state = getState();

        final byte[] value = "The quick brown fox jumps over the lazy dog.".getBytes(StandardCharsets.UTF_8);
        final byte[] newValue = "Ich esse Autos zum Abendbrot und mache Kopfsprung ins Sandbecken. Gruen. Rot. Pferderennen.".getBytes(StandardCharsets.UTF_8);

        final ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        final ImmutableList.Builder<Variable> varBuilder = ImmutableList.builder();

        for (int i = 0; i < 10; i++) {
            final String key = "key-" + UUID.randomUUID().toString();
            final Variable var = state.fetch(key).get();
            assertTrue(var.value().length == 0);

            final Variable storedVar = state.store(var.mutate(value)).get();
            assertNotNull(storedVar);
            builder.add(key);
            varBuilder.add(storedVar);
        }

        final Set<String> keys = builder.build();
        final List<Variable> variables = varBuilder.build();

        final ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());

        final ImmutableList.Builder<ListenableFuture<Integer>> resultBuilder = ImmutableList.builder();

        for (int i = 0; i < 10; i++) {
            resultBuilder.add(executor.submit(new Callable<Integer>() {

                @Override
                public Integer call() throws Exception
                {
                    final ArrayList<Variable> vars = new ArrayList<>(variables);
                    Collections.shuffle(vars);

                    int updateCount = 0;
                    for (final Variable var : vars) {
                        final Variable storedVar = state.store(var.mutate(newValue)).get();
                        if (storedVar != null) {
                            updateCount++;
                            Thread.sleep(2L);
                        }
                    }

                    return updateCount;
                }
            }));
        }

        final List<Integer> results = Futures.allAsList(resultBuilder.build()).get();

        int finalTally = 0;
        for (final Integer result : results) {
            finalTally += result;
        }

        assertEquals(10, finalTally);

        for (final String key : keys) {
            final Variable retrievedVar = state.fetch(key).get();
            assertNotNull(retrievedVar);

            assertArrayEquals(newValue, retrievedVar.value());
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
    }

}
