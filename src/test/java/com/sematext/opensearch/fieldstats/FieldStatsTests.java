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

import static com.sematext.opensearch.fieldstats.IndexConstraint.Comparison.GT;
import static com.sematext.opensearch.fieldstats.IndexConstraint.Comparison.GTE;
import static com.sematext.opensearch.fieldstats.IndexConstraint.Comparison.LT;
import static com.sematext.opensearch.fieldstats.IndexConstraint.Comparison.LTE;
import static com.sematext.opensearch.fieldstats.IndexConstraint.Property.MAX;
import static com.sematext.opensearch.fieldstats.IndexConstraint.Property.MIN;
import static org.hamcrest.Matchers.containsString;

import org.apache.lucene.tests.geo.GeoTestUtil;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.common.joda.Joda;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.test.InternalSettingsPlugin;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import com.sematext.opensearch.plugin.FieldStatsPlugin;

public class FieldStatsTests extends OpenSearchSingleNodeTestCase {
  @Override
  protected Collection<Class<? extends Plugin>> getPlugins() {
    return pluginList(InternalSettingsPlugin.class, FieldStatsPlugin.class);
  }

  public void testByte() {
    testNumberRange("field1", "byte", 12, 18);
    testNumberRange("field1", "byte", -5, 5);
    testNumberRange("field1", "byte", -18, -12);
  }

  public void testShort() {
    testNumberRange("field1", "short", 256, 266);
    testNumberRange("field1", "short", -5, 5);
    testNumberRange("field1", "short", -266, -256);
  }

  public void testInteger() {
    testNumberRange("field1", "integer", 56880, 56890);
    testNumberRange("field1", "integer", -5, 5);
    testNumberRange("field1", "integer", -56890, -56880);
  }

  public void testLong() {
    testNumberRange("field1", "long", 312321312312412L, 312321312312422L);
    testNumberRange("field1", "long", -5, 5);
    testNumberRange("field1", "long", -312321312312422L, -312321312312412L);
  }

  private static String makeType(String type, boolean indexed, boolean docValues, boolean stored) {
    return new StringBuilder()
        .append("type=").append(type)
        .append(",index=").append(indexed)
        .append(",doc_values=").append(docValues)
        .append(",store=").append(stored).toString();
  }


  public FieldStatsRequestBuilder prepareFieldStats() {
    return new FieldStatsRequestBuilder(client(), FieldStatsAction.INSTANCE);
  }

  public void testString() {
    createIndex("test", Settings.EMPTY, "test",
        "field_index", makeType("keyword", true, false, false),
        "field_dv", makeType("keyword", false, true, false),
        "field_stored", makeType("keyword", false, true, true),
        "field_source", makeType("keyword", false, false, false));
    for (int value = 0; value <= 10; value++) {
      String keyword =  String.format(Locale.ENGLISH, "%03d", value);
      client().prepareIndex("test")
          .setSource("field_index", keyword,
              "field_dv", keyword,
              "field_stored", keyword,
              "field_source", keyword).get();
    }
    client().admin().indices().prepareRefresh().get();

    FieldStatsResponse result = prepareFieldStats()
        .setFields("field_index", "field_dv", "field_stored", "field_source").get();

    assertEquals(4, result.getAllFieldStats().size());
    for (String field : new String[] {"field_index", "field_dv", "field_stored"}) {
      FieldStats<?> stats = result.getAllFieldStats().get(field);
      assertEquals(stats.getMaxDoc(), 11L);
      assertEquals(stats.getDisplayType(),
          "string");
      if ("field_index".equals(field)) {
        assertEquals(stats.getMinValue(),
            new BytesRef(String.format(Locale.ENGLISH, "%03d", 0)));
        assertEquals(stats.getMaxValue(),
            new BytesRef(String.format(Locale.ENGLISH, "%03d", 10)));
        assertEquals(stats.getMinValueAsString(),
            String.format(Locale.ENGLISH, "%03d", 0));
        assertEquals(stats.getMaxValueAsString(),
            String.format(Locale.ENGLISH, "%03d", 10));
        assertEquals(stats.getDocCount(), 11L);
        assertEquals(stats.getDensity(), 100);
      } else {
        assertEquals(stats.getDocCount(), 0L);
        assertNull(stats.getMinValue());
        assertNull(stats.getMaxValue());
        assertEquals(stats.getDensity(), 0);
      }
    }
  }

  public void testDouble() {
    createIndex("test", Settings.builder().put("index.number_of_shards", 4).build(), "test",
        "field_index", makeType("double", true, false, false),
        "field_dv", makeType("double", false, true, false),
        "field_stored", makeType("double", false, true, true),
        "field_source", makeType("double", false, false, false));
    for (double value = -1; value <= 9; value++) {
      client().prepareIndex("test")
          .setSource("field_index", value, "field_dv", value, "field_stored", value, "field_source", value).get();
    }
    client().admin().indices().prepareRefresh().get();
    FieldStatsResponse result = prepareFieldStats()
        .setFields("field_index", "field_dv", "field_stored", "field_source").get();
    for (String field : new String[] {"field_index", "field_dv", "field_stored"}) {
      FieldStats<?> stats = result.getAllFieldStats().get(field);
      assertEquals(stats.getMaxDoc(), 11L);
      assertEquals(stats.getDisplayType(), "float");
      if ("field_index".equals(field)) {
        assertEquals(stats.getDocCount(), 11L);
        assertEquals(stats.getDensity(), 100);
        assertEquals(stats.getMinValue(), -1d);
        assertEquals(stats.getMaxValue(), 9d);
        assertEquals(stats.getMinValueAsString(), Double.toString(-1));
      } else {
        assertEquals(stats.getDocCount(), 0L);
        assertNull(stats.getMinValue());
        assertNull(stats.getMaxValue());
        assertEquals(stats.getDensity(), 0);
      }
    }
  }

  public void testHalfFloat() {
    createIndex("test", Settings.EMPTY, "test",
        "field_index", makeType("half_float", true, false, false),
        "field_dv", makeType("half_float", false, true, false),
        "field_stored", makeType("half_float", false, true, true),
        "field_source", makeType("half_float", false, false, false));
    for (float value = -1; value <= 9; value++) {
      client().prepareIndex("test")
          .setSource("field_index", value, "field_dv", value, "field_stored", value, "field_source", value).get();
    }
    client().admin().indices().prepareRefresh().get();

    FieldStatsResponse result = prepareFieldStats()
        .setFields("field_index", "field_dv", "field_stored", "field_source").get();
    for (String field : new String[] {"field_index", "field_dv", "field_stored"}) {
      FieldStats<?> stats = result.getAllFieldStats().get(field);
      assertEquals(stats.getMaxDoc(), 11L);
      assertEquals(stats.getDisplayType(), "float");
      if (field.equals("field_index")) {
        assertEquals(stats.getDocCount(), 11L);
        assertEquals(stats.getDensity(), 100);
        assertEquals(stats.getMinValue(), -1d);
        assertEquals(stats.getMaxValue(), 9d);
        assertEquals(stats.getMinValueAsString(), Float.toString(-1));
        assertEquals(stats.getMaxValueAsString(), Float.toString(9));
      } else {
        assertEquals(stats.getDocCount(), 0L);
        assertNull(stats.getMinValue());
        assertNull(stats.getMaxValue());
        assertEquals(stats.getDensity(), 0);
      }
    }
  }

  public void testFloat() {
    String fieldName = "field";
    createIndex("test", Settings.EMPTY, "test",
        "field_index", makeType("float", true, false, false),
        "field_dv", makeType("float", false, true, false),
        "field_stored", makeType("float", false, true, true),
        "field_source", makeType("float", false, false, false));
    for (float value = -1; value <= 9; value++) {
      client().prepareIndex("test")
          .setSource("field_index", value, "field_dv", value, "field_stored", value, "field_source", value).get();
    }
    client().admin().indices().prepareRefresh().get();

    FieldStatsResponse result = prepareFieldStats()
        .setFields("field_index", "field_dv", "field_stored", "field_source").get();
    for (String field : new String[]{"field_index", "field_dv", "field_stored"}) {
      FieldStats<?> stats = result.getAllFieldStats().get(field);
      assertEquals(stats.getMaxDoc(), 11L);
      assertEquals(stats.getDisplayType(), "float");
      if (field.equals("field_index")) {
        assertEquals(stats.getDocCount(), 11L);
        assertEquals(stats.getDensity(), 100);
        assertEquals(stats.getMinValue(), -1d);
        assertEquals(stats.getMaxValue(), 9d);
        assertEquals(stats.getMinValueAsString(), Float.toString(-1));
        assertEquals(stats.getMaxValueAsString(), Float.toString(9));
      } else {
        assertEquals(stats.getDocCount(), 0L);
        assertNull(stats.getMinValue());
        assertNull(stats.getMaxValue());
        assertEquals(stats.getDensity(), 0);
      }
    }
  }

  private void testNumberRange(String fieldName, String fieldType, long min, long max) {
    createIndex("test", Settings.EMPTY, "test", fieldName, "type=" + fieldType);
    // index=false
    createIndex("test1", Settings.EMPTY, "test", fieldName, "type=" + fieldType + ",index=false");
    // no value
    createIndex("test2", Settings.EMPTY, "test", fieldName, "type=" + fieldType);

    for (long value = min; value <= max; value++) {
      client().prepareIndex("test").setSource(fieldName, value).get();
    }
    client().admin().indices().prepareRefresh().get();

    FieldStatsResponse result = prepareFieldStats().setFields(fieldName).get();
    long numDocs = max - min + 1;

    assertEquals(result.getAllFieldStats().get(fieldName).getMaxDoc(), numDocs);
    assertEquals(result.getAllFieldStats().get(fieldName).getDocCount(), numDocs);
    assertEquals(result.getAllFieldStats().get(fieldName).getDensity(), 100);
    assertEquals(result.getAllFieldStats().get(fieldName).getMinValue(), min);
    assertEquals(result.getAllFieldStats().get(fieldName).getMaxValue(), max);
    assertEquals(result.getAllFieldStats().get(fieldName).getMinValueAsString(),
        java.lang.Long.toString(min));
    assertEquals(result.getAllFieldStats().get(fieldName).getMaxValueAsString(),
        java.lang.Long.toString(max));
    assertEquals(result.getAllFieldStats().get(fieldName).isSearchable(), true);
    assertEquals(result.getAllFieldStats().get(fieldName).isAggregatable(), true);
    if (fieldType.equals("float") || fieldType.equals("double") || fieldType.equals("half-float")) {
      assertEquals(result.getAllFieldStats().get(fieldName).getDisplayType(), "float");
    } else {
      assertEquals(result.getAllFieldStats().get(fieldName).getDisplayType(), "integer");
    }

    client().admin().indices().prepareDelete("test").get();
    client().admin().indices().prepareDelete("test1").get();
    client().admin().indices().prepareDelete("test2").get();
  }

  public void testMerge() {
    List<FieldStats<?>> stats = new ArrayList<>();
    stats.add(new FieldStats.Long(1, 1L, 1L, 1L, true, false, 1L, 1L));
    stats.add(new FieldStats.Long(1, 1L, 1L, 1L, true, false, 1L, 1L));
    stats.add(new FieldStats.Long(1, 1L, 1L, 1L, true, false, 1L, 1L));
    stats.add(new FieldStats.Long(0, 0, 0, 0, false, false));

    FieldStats<?> stat = new FieldStats.Long(1, 1L, 1L, 1L, true, false, 1L, 1L);
    for (FieldStats<?> otherStat : stats) {
      stat.accumulate(otherStat);
    }
    assertEquals(stat.getMaxDoc(), 4L);
    assertEquals(stat.getDocCount(), 4L);
    assertEquals(stat.getSumDocFreq(), 4L);
    assertEquals(stat.getSumTotalTermFreq(), 4L);
    assertEquals(stat.isSearchable(), true);
    assertEquals(stat.isAggregatable(), false);
    assertEquals(stat.getDisplayType(), "integer");
  }

  public void testNumberFiltering() {
    createIndex("test1", Settings.EMPTY, "test", "value", "type=long");
    client().prepareIndex("test1").setSource("value", 1L).get();
    createIndex("test2", Settings.EMPTY, "test", "value", "type=long");
    client().prepareIndex("test2").setSource("value", 3L).get();
    client().admin().indices().prepareRefresh().get();

    FieldStatsResponse response = prepareFieldStats()
        .setFields("value")
        .setLevel("indices")
        .get();
    assertEquals(response.getIndicesMergedFieldStats().size(), 2);
    assertEquals(response.getIndicesMergedFieldStats().get("test1").get("value").getMinValue(), 1L);
    assertEquals(response.getIndicesMergedFieldStats().get("test2").get("value").getMinValue(), 3L);

    response = prepareFieldStats()
        .setFields("value")
        .setIndexContraints(new IndexConstraint("value", MIN, GTE, "-1"),
            new IndexConstraint("value", MAX, LTE, "0"))
        .setLevel("indices")
        .get();
    assertEquals(response.getIndicesMergedFieldStats().size(), 0);

    response = prepareFieldStats()
        .setFields("value")
        .setIndexContraints(new IndexConstraint("value", MIN, GTE, "0"),
            new IndexConstraint("value", MAX, LT, "1"))
        .setLevel("indices")
        .get();
    assertEquals(response.getIndicesMergedFieldStats().size(), 0);

    response = prepareFieldStats()
        .setFields("value")
        .setIndexContraints(new IndexConstraint("value", MIN, GTE, "0"),
            new IndexConstraint("value", MAX, LTE, "1"))
        .setLevel("indices")
        .get();
    assertEquals(response.getIndicesMergedFieldStats().size(), 1);
    assertEquals(response.getIndicesMergedFieldStats().get("test1").get("value").getMinValue(), 1L);

    response = prepareFieldStats()
        .setFields("value")
        .setIndexContraints(new IndexConstraint("value", MIN, GTE, "1"),
            new IndexConstraint("value", MAX, LTE,  "2"))
        .setLevel("indices")
        .get();
    assertEquals(response.getIndicesMergedFieldStats().size(), 1);
    assertEquals(response.getIndicesMergedFieldStats().get("test1").get("value").getMinValue(), 1L);

    response = prepareFieldStats()
        .setFields("value")
        .setIndexContraints(new IndexConstraint("value", MIN, GT, "1"),
            new IndexConstraint("value", MAX, LTE, "2"))
        .setLevel("indices")
        .get();
    assertEquals(response.getIndicesMergedFieldStats().size(), 0);

    response = prepareFieldStats()
        .setFields("value")
        .setIndexContraints(new IndexConstraint("value", MIN, GT, "2"),
            new IndexConstraint("value", MAX, LTE, "3"))
        .setLevel("indices")
        .get();
    assertEquals(response.getIndicesMergedFieldStats().size(), 1);
    assertEquals(response.getIndicesMergedFieldStats().get("test2").get("value").getMinValue(), 3L);

    response = prepareFieldStats()
        .setFields("value")
        .setIndexContraints(new IndexConstraint("value", MIN, GTE, "3"),
            new IndexConstraint("value", MAX, LTE, "4"))
        .setLevel("indices")
        .get();
    assertEquals(response.getIndicesMergedFieldStats().size(), 1);
    assertEquals(response.getIndicesMergedFieldStats().get("test2").get("value").getMinValue(), 3L);

    response = prepareFieldStats()
        .setFields("value")
        .setIndexContraints(new IndexConstraint("value", MIN, GT, "3"),
            new IndexConstraint("value", MAX, LTE, "4"))
        .setLevel("indices")
        .get();
    assertEquals(response.getIndicesMergedFieldStats().size(), 0);

    response = prepareFieldStats()
        .setFields("value")
        .setIndexContraints(new IndexConstraint("value", MIN, GTE,  "1"),
            new IndexConstraint("value", MAX, LTE, "3"))
        .setLevel("indices")
        .get();
    assertEquals(response.getIndicesMergedFieldStats().size(), 2);
    assertEquals(response.getIndicesMergedFieldStats().get("test1").get("value").getMinValue(), 1L);
    assertEquals(response.getIndicesMergedFieldStats().get("test2").get("value").getMinValue(), 3L);

    response = prepareFieldStats()
        .setFields("value")
        .setIndexContraints(new IndexConstraint("value", MIN, GT, "1"),
            new IndexConstraint("value", MAX, LT, "3"))
        .setLevel("indices")
        .get();
    assertEquals(response.getIndicesMergedFieldStats().size(), 0);
  }

  public void testDateFiltering() {
    ZonedDateTime dateTime1 = ZonedDateTime.of(2014, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC"));
    String dateTime1Str = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.format(dateTime1);
    ZonedDateTime dateTime2 = ZonedDateTime.of(2014, 1, 2, 0, 0, 0, 0, ZoneId.of("UTC"));
    String dateTime2Str = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.format(dateTime2);

    createIndex("test1", Settings.EMPTY, "test", "value", "type=date", "value2", "type=date,index=false");
    client().prepareIndex("test1")
        .setSource("value", dateTime1Str, "value2", dateTime1Str).get();
    createIndex("test2", Settings.EMPTY, "test", "value", "type=date");
    client().prepareIndex("test2").setSource("value", dateTime2Str).get();
    client().admin().indices().prepareRefresh().get();

    FieldStatsResponse response = prepareFieldStats()
        .setFields("value")
        .setLevel("indices")
        .get();
    assertEquals(response.getIndicesMergedFieldStats().size(), 2);
    assertEquals(response.getIndicesMergedFieldStats().get("test1").get("value").getMinValue(),
        dateTime1.toInstant().toEpochMilli());
    assertEquals(response.getIndicesMergedFieldStats().get("test2").get("value").getMinValue(),
        dateTime2.toInstant().toEpochMilli());
    assertEquals(response.getIndicesMergedFieldStats().get("test1").get("value").getMinValueAsString(),
        dateTime1Str);
    assertEquals(response.getIndicesMergedFieldStats().get("test2").get("value").getMinValueAsString(),
        dateTime2Str);
    assertEquals(response.getIndicesMergedFieldStats().get("test2").get("value").getDisplayType(),
        "date");

    response = prepareFieldStats()
        .setFields("value")
        .setIndexContraints(new IndexConstraint("value", MIN, GTE, "2013-12-30T00:00:00.000Z"),
            new IndexConstraint("value", MAX, LTE, "2013-12-31T00:00:00.000Z"))
        .setLevel("indices")
        .get();
    assertEquals(response.getIndicesMergedFieldStats().size(), 0);

    response = prepareFieldStats()
        .setFields("value")
        .setIndexContraints(new IndexConstraint("value", MIN, GTE, "2013-12-31T00:00:00.000Z"),
            new IndexConstraint("value", MAX, LTE, "2014-01-01T00:00:00.000Z"))
        .setLevel("indices")
        .get();
    assertEquals(response.getIndicesMergedFieldStats().size(), 1);
    assertEquals(response.getIndicesMergedFieldStats().get("test1").get("value").getMinValue(),
        dateTime1.toInstant().toEpochMilli());
    assertEquals(response.getIndicesMergedFieldStats().get("test1").get("value").getMinValueAsString(),
        dateTime1Str);
    assertEquals(response.getIndicesMergedFieldStats().get("test1").get("value").getDisplayType(),
        "date");

    response = prepareFieldStats()
        .setFields("value")
        .setIndexContraints(new IndexConstraint("value", MIN, GT, "2014-01-01T00:00:00.000Z"),
            new IndexConstraint("value", MAX, LTE, "2014-01-02T00:00:00.000Z"))
        .setLevel("indices")
        .get();
    assertEquals(response.getIndicesMergedFieldStats().size(), 1);
    assertEquals(response.getIndicesMergedFieldStats().get("test2").get("value").getMinValue(),
        dateTime2.toInstant().toEpochMilli());
    assertEquals(response.getIndicesMergedFieldStats().get("test2").get("value").getMinValueAsString(),
        dateTime2Str);

    response = prepareFieldStats()
        .setFields("value")
        .setIndexContraints(new IndexConstraint("value", MIN, GT, "2014-01-02T00:00:00.000Z"),
            new IndexConstraint("value", MAX, LTE, "2014-01-03T00:00:00.000Z"))
        .setLevel("indices")
        .get();
    assertEquals(response.getIndicesMergedFieldStats().size(), 0);

    response = prepareFieldStats()
        .setFields("value")
        .setIndexContraints(new IndexConstraint("value", MIN, GTE, "2014-01-01T23:00:00.000Z"),
            new IndexConstraint("value", MAX, LTE, "2014-01-02T01:00:00.000Z"))
        .setLevel("indices")
        .get();
    assertEquals(response.getIndicesMergedFieldStats().size(), 1);
    assertEquals(response.getIndicesMergedFieldStats().get("test2").get("value").getMinValue(),
        dateTime2.toInstant().toEpochMilli());
    assertEquals(response.getIndicesMergedFieldStats().get("test2").get("value").getMinValueAsString(),
        dateTime2Str);
    assertEquals(response.getIndicesMergedFieldStats().get("test2").get("value").getDisplayType(),
        "date");

    response = prepareFieldStats()
        .setFields("value")
        .setIndexContraints(new IndexConstraint("value", MIN, GTE, "2014-01-01T00:00:00.000Z"))
        .setLevel("indices")
        .get();
    assertEquals(response.getIndicesMergedFieldStats().size(), 2);
    assertEquals(response.getIndicesMergedFieldStats().get("test1").get("value").getMinValue(),
        dateTime1.toInstant().toEpochMilli());
    assertEquals(response.getIndicesMergedFieldStats().get("test2").get("value").getMinValue(),
        dateTime2.toInstant().toEpochMilli());
    assertEquals(response.getIndicesMergedFieldStats().get("test1").get("value").getMinValueAsString(),
        dateTime1Str);
    assertEquals(response.getIndicesMergedFieldStats().get("test2").get("value").getMinValueAsString(),
        dateTime2Str);
    assertEquals(response.getIndicesMergedFieldStats().get("test2").get("value").getDisplayType(),
        "date");

    response = prepareFieldStats()
        .setFields("value")
        .setIndexContraints(new IndexConstraint("value", MAX, LTE, "2014-01-02T00:00:00.000Z"))
        .setLevel("indices")
        .get();
    assertEquals(response.getIndicesMergedFieldStats().size(), 2);
    assertEquals(response.getIndicesMergedFieldStats().get("test1").get("value").getMinValue(),
        dateTime1.toInstant().toEpochMilli());
    assertEquals(response.getIndicesMergedFieldStats().get("test2").get("value").getMinValue(),
        dateTime2.toInstant().toEpochMilli());
    assertEquals(response.getIndicesMergedFieldStats().get("test1").get("value").getMinValueAsString(),
        dateTime1Str);
    assertEquals(response.getIndicesMergedFieldStats().get("test2").get("value").getMinValueAsString(),
        dateTime2Str);
    assertEquals(response.getIndicesMergedFieldStats().get("test2").get("value").getDisplayType(), "date");

    response = prepareFieldStats()
        .setFields("value2")
        .setIndexContraints(new IndexConstraint("value2", MAX, LTE, "2014-01-02T00:00:00.000Z"))
        .setLevel("indices")
        .get();
    assertEquals(response.getIndicesMergedFieldStats().size(), 0);
  }

  public void testDateFiltering_optionalFormat() {
    createIndex("test1", Settings.EMPTY, "type", "value", "type=date,format=strict_date_optional_time");
    client().prepareIndex("test1").setSource("value", "2014-01-01T00:00:00.000Z").get();
    createIndex("test2", Settings.EMPTY, "type", "value", "type=date,format=strict_date_optional_time");
    client().prepareIndex("test2").setSource("value", "2014-01-02T00:00:00.000Z").get();
    client().admin().indices().prepareRefresh().get();

    DateTime dateTime1 = new DateTime(2014, 1, 1, 0, 0, 0, 0, DateTimeZone.UTC);
    DateTime dateTime2 = new DateTime(2014, 1, 2, 0, 0, 0, 0, DateTimeZone.UTC);
    FieldStatsResponse response = prepareFieldStats()
        .setFields("value")
        .setIndexContraints(new IndexConstraint("value", MIN, GT,
                String.valueOf(dateTime1.getMillis()), "epoch_millis"),
            new IndexConstraint("value", MAX, LTE, String.valueOf(dateTime2.getMillis()), "epoch_millis"))
        .setLevel("indices")
        .get();
    assertEquals(response.getIndicesMergedFieldStats().size(), 1);
    assertEquals(response.getIndicesMergedFieldStats().get("test2").get("value").getMinValueAsString(),
        "2014-01-02T00:00:00.000Z");
    assertEquals(response.getIndicesMergedFieldStats().get("test2").get("value").getDisplayType(),
        "date");

    try {
      prepareFieldStats()
          .setFields("value")
          .setIndexContraints(new IndexConstraint("value", MIN, GT,
              String.valueOf(dateTime1.getMillis()), "xyz"))
          .setLevel("indices")
          .get();
      fail("IllegalArgumentException should have been thrown");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Invalid format"));
    }
  }

  public void testEmptyIndex() {
    createIndex("test1", Settings.EMPTY, "type", "value", "type=date");
    FieldStatsResponse response = prepareFieldStats()
        .setFields("*")
        .setLevel("indices")
        .get();
    assertEquals(response.getSuccessfulShards(), response.getTotalShards());
    assertEquals(0, response.getFailedShards());
    assertEquals(response.getIndicesMergedFieldStats().size(), 1);
  }

  public void testOneEmptyIndex() {
    createIndex("test1", Settings.EMPTY, "type", "v", "type=date");
    createIndex("test2", Settings.EMPTY, "type", "v", "type=date");
    FieldStatsResponse response = prepareFieldStats()
            .setFields("v")
            .setLevel("indices")
            .get();
    assertEquals(response.getSuccessfulShards(), response.getTotalShards());
    assertEquals(0, response.getFailedShards());
    assertEquals(2, response.getIndicesMergedFieldStats().size());
  }

  public void testEmptyIndexNotExistingField() {
    createIndex("test1", Settings.EMPTY, "type", "value", "type=date");
    FieldStatsResponse response = prepareFieldStats()
        .setFields("foobar")
        .setLevel("indices")
        .get();
    assertEquals(response.getSuccessfulShards(), response.getTotalShards());
    assertEquals(0, response.getFailedShards());
    assertEquals(response.getIndicesMergedFieldStats().size(), 1);
  }

  public void testMetaFieldsNotIndexed() {
    createIndex("test", Settings.EMPTY);
    client().prepareIndex("test").setSource().get();
    client().admin().indices().prepareRefresh().get();

    FieldStatsResponse response = prepareFieldStats()
        .setFields("_id")
        .get();
    assertEquals(response.getAllFieldStats().size(), 1);
  }

  public void testSerialization() throws IOException {
    for (int i = 0; i < 20; i++) {
        assertSerialization(randomFieldStats());
    }
  }

  /**
   * creates a random field stats which does not guarantee that {@link FieldStats#maxValue} is greater than {@link FieldStats#minValue}
   **/
  public static FieldStats<?> randomFieldStats() throws UnknownHostException {
    int type = randomInt(5);
    switch (type) {
      case 0:
        if (randomBoolean()) {
          return new FieldStats.Long(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
              randomNonNegativeLong(), randomBoolean(), randomBoolean());
        } else {
          return new FieldStats.Long(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
              randomNonNegativeLong(), randomBoolean(), randomBoolean(), randomLong(), randomLong());
        }
      case 1:
        if (randomBoolean()) {
          return new FieldStats.Double(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
              randomNonNegativeLong(), randomBoolean(), randomBoolean());
        } else {
          return new FieldStats.Double(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
              randomNonNegativeLong(), randomBoolean(), randomBoolean(), randomDouble(), randomDouble());
        }
      case 2:
        if (randomBoolean()) {
          return new FieldStats.Date(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
              randomNonNegativeLong(), randomBoolean(), randomBoolean());
        } else {
          return new FieldStats.Date(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
              randomNonNegativeLong(), randomBoolean(), randomBoolean(), Joda.forPattern("basic_date"),
              new Date().getTime(), new Date().getTime());
        }
      case 3:
        if (randomBoolean()) {
          return new FieldStats.Text(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
              randomNonNegativeLong(), randomBoolean(), randomBoolean());
        } else {
          return new FieldStats.Text(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
              randomNonNegativeLong(), randomBoolean(), randomBoolean(),
              new BytesRef(randomAlphaOfLength(10)), new BytesRef(randomAlphaOfLength(20)));
        }
      case 4:
        if (randomBoolean()) {
          return new FieldStats.Ip(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
              randomNonNegativeLong(), randomBoolean(), randomBoolean());
        } else {
          return new FieldStats.Ip(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
              randomNonNegativeLong(), randomBoolean(), randomBoolean(),
              InetAddress.getByName("::1"), InetAddress.getByName("::1"));
        }
      case 5:
        if (randomBoolean()) {
          return new FieldStats.Ip(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
              randomNonNegativeLong(), randomBoolean(), randomBoolean());
        } else {
          return new FieldStats.Ip(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
              randomNonNegativeLong(), randomBoolean(), randomBoolean(),
              InetAddress.getByName("1.2.3.4"), InetAddress.getByName("1.2.3.4"));
        }
      default:
        throw new IllegalArgumentException("Invalid type");
    }
  }

  public void testGeopoint() {
    createIndex("test", Settings.EMPTY, "test",
        "field_index", makeType("geo_point", true, false, false));
    int numDocs = random().nextInt(20);
    for (int i = 0; i <= numDocs; ++i) {
      double lat = GeoTestUtil.nextLatitude();
      double lon = GeoTestUtil.nextLongitude();
      final String src = lat + "," + lon;
      client().prepareIndex("test").setSource("field_index", src).get();
    }

    client().admin().indices().prepareRefresh().get();
    FieldStatsResponse result = prepareFieldStats().setFields("field_index").get();
    FieldStats<?> stats = result.getAllFieldStats().get("field_index");
    assertEquals(stats.getDisplayType(), "geo_point");
  }

  private void assertSerialization(FieldStats<?> stats) throws IOException {
    BytesStreamOutput output = new BytesStreamOutput();
    stats.writeTo(output);
    output.flush();
    StreamInput input = output.bytes().streamInput();
    FieldStats<?> deserializedStats = FieldStats.readFrom(input);
    assertEquals(stats, deserializedStats);
    assertEquals(stats.hashCode(), deserializedStats.hashCode());
  }
}
