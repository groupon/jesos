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

import static com.groupon.mesos.util.UUIDUtil.uuidBytes;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import com.google.protobuf.ByteString;

import org.apache.mesos.state.Variable;
import org.junit.Test;

import mesos.internal.state.State.Entry;

public class TestJVariable
{
    @Test
    public void testEntry()
    {
        final UUID uuid = UUID.randomUUID();
        final String name = "test-name";
        final byte[] value = "The quick brown fox jumps over the lazy dog.".getBytes(StandardCharsets.UTF_8);

        final Entry entry = Entry.newBuilder()
            .setName(name)
            .setValue(ByteString.copyFrom(value))
            .setUuid(uuidBytes(uuid))
            .build();

        final JVariable v = new JVariable(entry);

        assertEquals(name, v.getName());
        assertEquals(uuid, v.getUuid());
        assertArrayEquals(value, v.value());
    }

    @Test
    public void testNameValue()
    {
        final String name = "test-name";
        final byte[] value = "The quick brown fox jumps over the lazy dog.".getBytes(StandardCharsets.UTF_8);

        final JVariable v = new JVariable(name, value);

        assertEquals(name, v.getName());
        assertArrayEquals(value, v.value());
    }

    @Test
    public void testMutate()
    {
        final String name = "test-name";
        final byte[] value = "The quick brown fox jumps over the lazy dog.".getBytes(StandardCharsets.UTF_8);
        final byte[] newValue = "Ich esse Autos zum Abendbrot und mache Kopfsprung ins Sandbecken. Gruen. Rot. Pferderennen.".getBytes(StandardCharsets.UTF_8);

        final JVariable v = new JVariable(name, value);

        final Variable w = v.mutate(newValue);

        assertEquals(JVariable.class, w.getClass());
        assertArrayEquals(newValue, w.value());

        assertEquals(name, v.getName());
        assertArrayEquals(value, v.value());

        final JVariable v2 = (JVariable) w;

        assertEquals(v.getName(), v2.getName());
        assertEquals(v.getUuid(), v2.getUuid());
    }
}
