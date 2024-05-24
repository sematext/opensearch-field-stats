/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sematext.opensearch.fieldstats;

import static com.sematext.opensearch.fieldstats.IndexConstraint.Comparison.GT;
import static com.sematext.opensearch.fieldstats.IndexConstraint.Comparison.GTE;
import static com.sematext.opensearch.fieldstats.IndexConstraint.Comparison.LT;
import static com.sematext.opensearch.fieldstats.IndexConstraint.Comparison.LTE;
import static com.sematext.opensearch.fieldstats.IndexConstraint.Property.MAX;
import static com.sematext.opensearch.fieldstats.IndexConstraint.Property.MIN;
import static org.hamcrest.Matchers.equalTo;

import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.StreamsUtils;

import java.util.HashMap;
import java.util.Map;


public class FieldStatsRequestTests extends OpenSearchTestCase {

    public void testFieldsParsing() throws Exception {
        BytesArray data = new BytesArray(StreamsUtils.copyToBytesFromClasspath(
                "/com/sematext/opensearch/fieldstats/fieldstats-index-constraints-request.json"));
        FieldStatsRequest request = new FieldStatsRequest();
        request.source(createParser(JsonXContent.jsonXContent, data));

        assertThat(request.getFields().length, equalTo(5));
        assertThat(request.getFields()[0], equalTo("field1"));
        assertThat(request.getFields()[1], equalTo("field2"));
        assertThat(request.getFields()[2], equalTo("field3"));
        assertThat(request.getFields()[3], equalTo("field4"));
        assertThat(request.getFields()[4], equalTo("field5"));

        assertThat(request.getIndexConstraints().length, equalTo(8));
        assertThat(request.getIndexConstraints()[0].getField(), equalTo("field2"));
        assertThat(request.getIndexConstraints()[0].getValue(), equalTo("9"));
        assertThat(request.getIndexConstraints()[0].getProperty(), equalTo(MAX));
        assertThat(request.getIndexConstraints()[0].getComparison(), equalTo(GTE));
        assertThat(request.getIndexConstraints()[1].getField(), equalTo("field3"));
        assertThat(request.getIndexConstraints()[1].getValue(), equalTo("5"));
        assertThat(request.getIndexConstraints()[1].getProperty(), equalTo(MIN));
        assertThat(request.getIndexConstraints()[1].getComparison(), equalTo(GT));
        assertThat(request.getIndexConstraints()[2].getField(), equalTo("field4"));
        assertThat(request.getIndexConstraints()[2].getValue(), equalTo("a"));
        assertThat(request.getIndexConstraints()[2].getProperty(), equalTo(MIN));
        assertThat(request.getIndexConstraints()[2].getComparison(), equalTo(GTE));
        assertThat(request.getIndexConstraints()[3].getField(), equalTo("field4"));
        assertThat(request.getIndexConstraints()[3].getValue(), equalTo("g"));
        assertThat(request.getIndexConstraints()[3].getProperty(), equalTo(MAX));
        assertThat(request.getIndexConstraints()[3].getComparison(), equalTo(LTE));
        assertThat(request.getIndexConstraints()[4].getField(), equalTo("field5"));
        assertThat(request.getIndexConstraints()[4].getValue(), equalTo("2"));
        assertThat(request.getIndexConstraints()[4].getProperty(), equalTo(MAX));
        assertThat(request.getIndexConstraints()[4].getComparison(), equalTo(GT));
        assertThat(request.getIndexConstraints()[5].getField(), equalTo("field5"));
        assertThat(request.getIndexConstraints()[5].getValue(), equalTo("9"));
        assertThat(request.getIndexConstraints()[5].getProperty(), equalTo(MAX));
        assertThat(request.getIndexConstraints()[5].getComparison(), equalTo(LT));
        assertThat(request.getIndexConstraints()[6].getField(), equalTo("field1"));
        assertThat(request.getIndexConstraints()[6].getValue(), equalTo("2014-01-01"));
        assertThat(request.getIndexConstraints()[6].getProperty(), equalTo(MIN));
        assertThat(request.getIndexConstraints()[6].getComparison(), equalTo(GTE));
        assertThat(request.getIndexConstraints()[6].getOptionalFormat(), equalTo("date_optional_time"));
        assertThat(request.getIndexConstraints()[7].getField(), equalTo("field1"));
        assertThat(request.getIndexConstraints()[7].getValue(), equalTo("2015-01-01"));
        assertThat(request.getIndexConstraints()[7].getProperty(), equalTo(MAX));
        assertThat(request.getIndexConstraints()[7].getComparison(), equalTo(LT));
        assertThat(request.getIndexConstraints()[7].getOptionalFormat(), equalTo("date_optional_time"));
    }

    public void testFieldStatsBWC() throws Exception {
        int size = randomIntBetween(5, 20);
        Map<String, FieldStats<?> > stats = new HashMap<> ();
        for (int i = 0; i < size; i++) {
            stats.put(Integer.toString(i), FieldStatsTests.randomFieldStats());
        }

        FieldStatsShardResponse response = new FieldStatsShardResponse(new ShardId("test", "test", 0), stats);
        for (int i = 0; i < 10; i++) {
            BytesStreamOutput output = new BytesStreamOutput();
            response.writeTo(output);
            output.flush();
            StreamInput input = output.bytes().streamInput();
            FieldStatsShardResponse deserialized = new FieldStatsShardResponse(input);
            final Map<String, FieldStats<?>> expected;
            expected = deserialized.getFieldStats();
            assertEquals(expected.size(), deserialized.getFieldStats().size());
            assertThat(expected, equalTo(deserialized.getFieldStats()));
        }
    }
}

