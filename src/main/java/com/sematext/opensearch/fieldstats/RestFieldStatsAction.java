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

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.*;
import static org.opensearch.rest.RestRequest.Method.PUT;
import static org.opensearch.rest.action.RestActions.buildBroadcastShardsHeader;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.action.RestBuilderListener;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RestFieldStatsAction extends BaseRestHandler {

  @Override
  public List<Route> routes() {
    return unmodifiableList(
            asList(
                    new Route(GET, "/_field_stats"),
                    new Route(POST, "/_field_stats"),
                    new Route(GET, "/{index}/_field_stats"),
                    new Route(POST, "/{index}/_field_stats")
            )
    );
  }

  @Override public String getName() {
    return "field_stats_action";
  }

  @Override
  public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
    if (request.hasContentOrSourceParam() && request.hasParam("fields")) {
      throw new IllegalArgumentException("can't specify a request body and [fields] request parameter, " +
          "either specify a request body or the [fields] request parameter");
    }

    FieldStatsRequestBuilder fieldStatsRequestBuilder = new FieldStatsRequestBuilder(client, FieldStatsAction.INSTANCE);
    final FieldStatsRequest fieldStatsRequest = fieldStatsRequestBuilder.request();
    fieldStatsRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
    fieldStatsRequest.indicesOptions(IndicesOptions.fromRequest(request, fieldStatsRequest.indicesOptions()));
    fieldStatsRequest.level(request.param("level", FieldStatsRequest.DEFAULT_LEVEL));
    if (request.hasContentOrSourceParam()) {
      try (XContentParser parser = request.contentOrSourceParamParser()) {
        fieldStatsRequest.source(parser);
      }
    } else {
      fieldStatsRequest.setFields(Strings.splitStringByCommaToArray(request.param("fields")));
    }



    return channel -> fieldStatsRequestBuilder.execute(new RestBuilderListener<FieldStatsResponse>(channel) {
      @Override
      public RestResponse buildResponse(FieldStatsResponse response, XContentBuilder builder) throws Exception {
        builder.startObject();
        buildBroadcastShardsHeader(builder, request, response);

        builder.startObject("indices");
        for (Map.Entry<String, Map<String, FieldStats<?>>> entry1 :
            response.getIndicesMergedFieldStats().entrySet()) {
          builder.startObject(entry1.getKey());
          builder.startObject("fields");
          for (Map.Entry<String, FieldStats<?>> entry2 : entry1.getValue().entrySet()) {
            builder.field(entry2.getKey());
            entry2.getValue().toXContent(builder, request);
          }
          builder.endObject();
          builder.endObject();
        }
        builder.endObject();
        if (response.getConflicts().size() > 0) {
          builder.startObject("conflicts");
          for (Map.Entry<String, String> entry : response.getConflicts().entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
          }
          builder.endObject();
        }
        builder.endObject();
        return new BytesRestResponse(RestStatus.OK, builder);
      }
    });
  }
}
