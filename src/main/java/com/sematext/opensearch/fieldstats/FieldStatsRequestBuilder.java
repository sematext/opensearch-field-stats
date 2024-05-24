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


import org.opensearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.opensearch.client.OpenSearchClient;


public class FieldStatsRequestBuilder extends
    BroadcastOperationRequestBuilder<FieldStatsRequest, FieldStatsResponse, FieldStatsRequestBuilder> {

    public FieldStatsRequestBuilder(OpenSearchClient client, FieldStatsAction action) {
        super(client, action, new FieldStatsRequest());
    }

    public FieldStatsRequestBuilder setFields(String... fields) {
        request().setFields(fields);
        return this;
    }

    public FieldStatsRequestBuilder setIndexContraints(IndexConstraint... fields) {
        request().setIndexConstraints(fields);
        return this;
    }

    public FieldStatsRequestBuilder setLevel(String level) {
        request().level(level);
        return this;
    }

    public FieldStatsRequestBuilder setUseCache(boolean useCache) {
        request().setUseCache(useCache);
        return this;
    }
}
