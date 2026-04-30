/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.python.processtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Length-prefixed UTF-8 string serializer used as the namespace serializer for PyFlink PTF named
 * timers.
 *
 * <p>The wire format is a 4-byte big-endian length prefix followed by the UTF-8-encoded bytes of
 * the string. Chosen over Flink's {@code StringSerializer} (which uses a custom per-character
 * varint format) because the same bytes flow Java <-> Python through our owned encoder; the
 * Python side just packs {@code int.to_bytes(4, 'big') + name.encode('utf-8')} which is far
 * simpler than emulating Flink's StringValue varint encoding.
 *
 * <p>Used as the namespace type for the PTF user-timer service so that the timer name is
 * round-tripped through the timer fire/registration path.
 */
@Internal
public final class Utf8LengthPrefixedStringSerializer extends TypeSerializerSingleton<String> {

    private static final long serialVersionUID = 1L;

    public static final Utf8LengthPrefixedStringSerializer INSTANCE =
            new Utf8LengthPrefixedStringSerializer();

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public String createInstance() {
        return "";
    }

    @Override
    public String copy(String from) {
        return from;
    }

    @Override
    public String copy(String from, String reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(String record, DataOutputView target) throws IOException {
        if (record == null) {
            target.writeInt(0);
            return;
        }
        final byte[] bytes = record.getBytes(StandardCharsets.UTF_8);
        target.writeInt(bytes.length);
        target.write(bytes);
    }

    @Override
    public String deserialize(DataInputView source) throws IOException {
        final int len = source.readInt();
        if (len <= 0) {
            return "";
        }
        final byte[] bytes = new byte[len];
        source.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public String deserialize(String reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        final int len = source.readInt();
        target.writeInt(len);
        if (len > 0) {
            final byte[] tmp = new byte[len];
            source.readFully(tmp);
            target.write(tmp);
        }
    }

    @Override
    public TypeSerializerSnapshot<String> snapshotConfiguration() {
        return new Utf8LengthPrefixedStringSerializerSnapshot();
    }

    /** Snapshot of {@link Utf8LengthPrefixedStringSerializer}. */
    @SuppressWarnings("WeakerAccess")
    public static final class Utf8LengthPrefixedStringSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<String> {
        public Utf8LengthPrefixedStringSerializerSnapshot() {
            super(() -> Utf8LengthPrefixedStringSerializer.INSTANCE);
        }
    }

}
