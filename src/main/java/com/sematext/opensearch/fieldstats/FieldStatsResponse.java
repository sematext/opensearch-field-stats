/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sematext.opensearch.fieldstats;

import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class FieldStatsResponse extends BroadcastResponse {
    private Map<String, Map<String, FieldStats<?>>> indicesMergedFieldStats = new HashMap<>();
    private Map<String, String> conflicts = new HashMap<>();

    public FieldStatsResponse() {
    }

    public FieldStatsResponse(int totalShards, int successfulShards, int failedShards,
                              List<DefaultShardOperationFailedException> shardFailures,
                              Map<String, Map<String, FieldStats<?>>> indicesMergedFieldStats,
                              Map<String, String> conflicts) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.indicesMergedFieldStats = indicesMergedFieldStats;
        this.conflicts = conflicts;
    }

    public FieldStatsResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        indicesMergedFieldStats = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            int indexSize = in.readVInt();
            Map<String, FieldStats<?>> indexFieldStats = new HashMap<>(indexSize);
            indicesMergedFieldStats.put(key, indexFieldStats);
            for (int j = 0; j < indexSize; j++) {
                key = in.readString();
                FieldStats<?> value = FieldStats.readFrom(in);
                indexFieldStats.put(key, value);
            }
        }
        size = in.readVInt();
        conflicts = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            String value = in.readString();
            conflicts.put(key, value);
        }

    }

    @Nullable
    public Map<String, FieldStats<?>> getAllFieldStats() {
        return indicesMergedFieldStats.get("_all");
    }

    public Map<String, String> getConflicts() {
        return conflicts;
    }

    public Map<String, Map<String, FieldStats<?>>> getIndicesMergedFieldStats() {
        return indicesMergedFieldStats;
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("upgraded_indices");
        builder.endObject();
    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(indicesMergedFieldStats.size());
        for (Map.Entry<String, Map<String, FieldStats<?>>> entry1 : indicesMergedFieldStats.entrySet()) {
            out.writeString(entry1.getKey());
            int size = entry1.getValue().size();
            out.writeVInt(size);
            for (Map.Entry<String, FieldStats<?>> entry2 : entry1.getValue().entrySet()) {
                out.writeString(entry2.getKey());
                entry2.getValue().writeTo(out);
            }
        }
        out.writeVInt(conflicts.size());
        for (Map.Entry<String, String> entry : conflicts.entrySet()) {
            out.writeString(entry.getKey());
            out.writeString(entry.getValue());
        }
    }
}
