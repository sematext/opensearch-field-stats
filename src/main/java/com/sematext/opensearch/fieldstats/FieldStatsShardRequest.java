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


import org.opensearch.action.support.broadcast.BroadcastShardRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


public class FieldStatsShardRequest extends BroadcastShardRequest {

    private String[] fields;
    private boolean useCache;

    public FieldStatsShardRequest() {
    }

    public FieldStatsShardRequest(ShardId shardId, FieldStatsRequest request) {
        super(shardId, request);
        Set<String> fields = new HashSet<>(Arrays.asList(request.getFields()));
        for (IndexConstraint indexConstraint : request.getIndexConstraints()) {
            fields.add(indexConstraint.getField());
        }
        this.fields = fields.toArray(new String[fields.size()]);
        useCache = request.shouldUseCache();
    }

    public FieldStatsShardRequest(StreamInput in) throws IOException {
        super(in);
        fields = in.readStringArray();
        useCache = in.readBoolean();
    }

    public String[] getFields() {
        return fields;
    }

    public boolean shouldUseCache() {
        return useCache;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(fields);
        out.writeBoolean(useCache);
    }

}
