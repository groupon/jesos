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
import static com.groupon.mesos.util.UUIDUtil.bytesUuid;
import static com.groupon.mesos.util.UUIDUtil.uuidBytes;

import java.util.UUID;

import com.google.protobuf.ByteString;

import org.apache.mesos.state.Variable;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mesos.internal.state.State.Entry;

public class JVariable
    extends Variable
{
    static final byte[] EMPTY_BYTES = new byte[0];

    private final Entry entry;

    JVariable(final String name, final byte[] value)
    {
        checkNotNull(name, "name is null");
        checkNotNull(value, "value is null");

        this.entry = Entry.newBuilder()
            .setName(name)
            .setValue(ByteString.copyFrom(value))
            .setUuid(uuidBytes(UUID.randomUUID()))
            .build();
    }

    JVariable(final Entry entry)
    {
        this.entry = entry;
    }

    @Override
    public byte[] value()
    {
        return entry.getValue().toByteArray();
    }

    String getName()
    {
        return entry.getName();
    }

    UUID getUuid()
    {
        return bytesUuid(entry.getUuid());
    }

    Entry getEntry()
    {
        return entry;
    }

    @Override
    public Variable mutate(final byte[] value)
    {
        checkNotNull(value, "value is null");

        return new JVariable(Entry.newBuilder()
            .setName(entry.getName())
            .setValue(ByteString.copyFrom(value))
            .setUuid(entry.getUuid())
            .build());
    }

    @Override
    @SuppressFBWarnings("FI_NULLIFY_SUPER")
    @SuppressWarnings("PMD.EmptyFinalizer")
    protected void finalize()
    {
        // This method must override Variable.finalize() because
        // it would throw a stupid UnsatisfiedLinkError during
        // finalizing if the mesos native library is not present
        // (which is the whole point of this code).
        //
        // And then both findbugs and pmd start (rightfully) complaining.
    }
}
