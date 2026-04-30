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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.api.operators.python.process.timer.TimerRegistration;
import org.apache.flink.streaming.api.utils.PythonOperatorUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * PTF flavor of {@link TimerRegistration}: bridges the wire {@link Row} partition key to {@link
 * RowData} and adds three Java-side semantic gates required for full PTF parity with Java:
 *
 * <ol>
 *   <li><b>Replace-on-register</b> for named event/proc timers — a fresh registration with the
 *       same name cancels the prior occurrence (Java {@code TimeContext.registerOnTime(name, ts)}
 *       semantics). The PTF spec stores the most-recent ts per name in a hidden {@link MapState}.
 *   <li><b>{@code clear_timer(name)}</b> with sentinel timestamp {@code -1L} on the wire — looks
 *       up the actual ts from the {@code namedTimers} MapState before issuing the delete. Without
 *       this lookup the Python side has no way to recover the registered firing time.
 *   <li><b>{@code clear_all_timers()}</b> — a custom op byte (4) iterates both hidden states
 *       (named + unnamed) and deletes every pending timer for the current partition key.
 * </ol>
 *
 * <p>Unnamed timers also get tracked in a hidden {@link ListState} so {@code clear_all_timers}
 * has a complete picture of pending firings; otherwise the internal timer service alone has no
 * way to enumerate them.
 */
@Internal
public final class PtfTimerRegistration extends TimerRegistration {

    /** Custom op byte beyond {@link TimerRegistration.TimerOperandType}. */
    public static final byte OP_REGISTER_EVENT_TIMER = 0;

    public static final byte OP_REGISTER_PROC_TIMER = 1;
    public static final byte OP_DELETE_EVENT_TIMER = 2;
    public static final byte OP_DELETE_PROC_TIMER = 3;
    public static final byte OP_CLEAR_ALL_TIMERS = 4;

    /** Sentinel ts for {@code clear_timer(name)}: tells Java to look up the actual ts. */
    private static final long DELETE_TS_SENTINEL = -1L;

    private final RowRowConverter keyConverter;
    @SuppressWarnings("rawtypes")
    private final InternalTimerService internalTimerService;
    private final KeyedStateBackend<RowData> keyedStateBackend;
    private final KeyContext keyContext;
    private final TypeSerializer<Row> timerDataSerializer;
    private final TypeSerializer<String> namespaceSerializer;
    private final MapState<String, Long> namedTimers;
    private final ListState<Long> unnamedTimers;

    private final ByteArrayInputStreamWithPos bais;
    private final DataInputViewStreamWrapper baisWrapper;

    @SuppressWarnings("rawtypes")
    public PtfTimerRegistration(
            KeyedStateBackend<RowData> keyedStateBackend,
            InternalTimerService internalTimerService,
            KeyContext keyContext,
            TypeSerializer<String> namespaceSerializer,
            TypeSerializer<Row> timerDataSerializer,
            RowRowConverter keyConverter,
            MapState<String, Long> namedTimers,
            ListState<Long> unnamedTimers) {
        // Pass through to the parent so its own bais (used only when an unsupported op falls
        // back) is still primed with our serializers.
        super(keyedStateBackend, internalTimerService, keyContext, namespaceSerializer,
                timerDataSerializer);
        this.keyedStateBackend = keyedStateBackend;
        this.internalTimerService = internalTimerService;
        this.keyContext = keyContext;
        this.namespaceSerializer = namespaceSerializer;
        this.timerDataSerializer = timerDataSerializer;
        this.keyConverter = keyConverter;
        this.namedTimers = namedTimers;
        this.unnamedTimers = unnamedTimers;
        this.bais = new ByteArrayInputStreamWithPos();
        this.baisWrapper = new DataInputViewStreamWrapper(bais);
    }

    @Override
    protected Object convertExternalKey(Row externalKey) {
        return keyConverter.toInternal(externalKey);
    }

    /**
     * Re-implements {@link TimerRegistration#setTimer(byte[])} with PTF-specific semantics.
     * Delegates back to the parent only for the non-named, non-clear-all flavors.
     */
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void setTimer(byte[] serializedTimerData) {
        try {
            bais.setBuffer(serializedTimerData, 0, serializedTimerData.length);
            final Row timerData = timerDataSerializer.deserialize(baisWrapper);
            final byte opByte = (byte) timerData.getField(0);
            final long timestamp = (long) timerData.getField(2);
            final RowData key = (RowData) convertExternalKey((Row) timerData.getField(3));

            // Decode the namespace bytes (length-prefixed UTF-8). Empty -> unnamed.
            String name = "";
            final byte[] encodedNamespace = (byte[]) timerData.getField(4);
            if (encodedNamespace != null && encodedNamespace.length > 0) {
                bais.setBuffer(encodedNamespace, 0, encodedNamespace.length);
                name = namespaceSerializer.deserialize(baisWrapper);
            }

            synchronized (keyedStateBackend) {
                keyContext.setCurrentKey(key);
                PythonOperatorUtils.setCurrentKeyForTimerService(internalTimerService, key);

                switch (opByte) {
                    case OP_REGISTER_EVENT_TIMER:
                        registerEvent(name, timestamp);
                        break;
                    case OP_REGISTER_PROC_TIMER:
                        registerProc(name, timestamp);
                        break;
                    case OP_DELETE_EVENT_TIMER:
                        deleteEvent(name, timestamp);
                        break;
                    case OP_DELETE_PROC_TIMER:
                        deleteProc(name, timestamp);
                        break;
                    case OP_CLEAR_ALL_TIMERS:
                        clearAll();
                        break;
                    default:
                        throw new IllegalArgumentException(
                                "Unknown PTF timer op byte: " + opByte);
                }
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private void registerEvent(String name, long timestamp) throws Exception {
        if (name == null || name.isEmpty()) {
            // Unnamed: track ts in ListState so clear_all_timers can iterate.
            unnamedTimers.add(timestamp);
            internalTimerService.registerEventTimeTimer("", timestamp);
            return;
        }
        // Named with replace-on-register semantics: cancel any prior firing.
        final Long prior = namedTimers.get(name);
        if (prior != null) {
            internalTimerService.deleteEventTimeTimer(name, prior);
        }
        namedTimers.put(name, timestamp);
        internalTimerService.registerEventTimeTimer(name, timestamp);
    }

    private void registerProc(String name, long timestamp) throws Exception {
        if (name == null || name.isEmpty()) {
            unnamedTimers.add(timestamp);
            internalTimerService.registerProcessingTimeTimer("", timestamp);
            return;
        }
        final Long prior = namedTimers.get(name);
        if (prior != null) {
            internalTimerService.deleteProcessingTimeTimer(name, prior);
        }
        namedTimers.put(name, timestamp);
        internalTimerService.registerProcessingTimeTimer(name, timestamp);
    }

    private void deleteEvent(String name, long timestamp) throws Exception {
        long actualTs = timestamp;
        if (timestamp == DELETE_TS_SENTINEL && name != null && !name.isEmpty()) {
            final Long stored = namedTimers.get(name);
            if (stored == null) {
                return; // No such timer pending.
            }
            actualTs = stored;
        }
        internalTimerService.deleteEventTimeTimer(name == null ? "" : name, actualTs);
        if (name != null && !name.isEmpty()) {
            namedTimers.remove(name);
        }
    }

    private void deleteProc(String name, long timestamp) throws Exception {
        long actualTs = timestamp;
        if (timestamp == DELETE_TS_SENTINEL && name != null && !name.isEmpty()) {
            final Long stored = namedTimers.get(name);
            if (stored == null) {
                return;
            }
            actualTs = stored;
        }
        internalTimerService.deleteProcessingTimeTimer(name == null ? "" : name, actualTs);
        if (name != null && !name.isEmpty()) {
            namedTimers.remove(name);
        }
    }

    private void clearAll() throws Exception {
        // Delete every named timer pending for the current partition.
        final List<Map.Entry<String, Long>> namedSnapshot = new ArrayList<>();
        for (Map.Entry<String, Long> entry : namedTimers.entries()) {
            namedSnapshot.add(Map.entry(entry.getKey(), entry.getValue()));
        }
        for (Map.Entry<String, Long> entry : namedSnapshot) {
            // Best-effort: try both event-time and proc-time deletion. The internal timer
            // service ignores deletions of timers that aren't registered, so this is safe.
            internalTimerService.deleteEventTimeTimer(entry.getKey(), entry.getValue());
            internalTimerService.deleteProcessingTimeTimer(entry.getKey(), entry.getValue());
        }
        namedTimers.clear();

        // Delete every unnamed timer.
        final Iterable<Long> unnamedIter = unnamedTimers.get();
        if (unnamedIter != null) {
            final List<Long> unnamedSnapshot = new ArrayList<>();
            for (Long ts : unnamedIter) {
                unnamedSnapshot.add(ts);
            }
            for (Long ts : unnamedSnapshot) {
                internalTimerService.deleteEventTimeTimer("", ts);
                internalTimerService.deleteProcessingTimeTimer("", ts);
            }
            unnamedTimers.clear();
        }
    }
}
