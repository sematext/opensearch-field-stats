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

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.Terms;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.opensearch.action.support.broadcast.TransportBroadcastAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.GeoPointFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class TransportFieldStatsAction extends
    TransportBroadcastAction<FieldStatsRequest, FieldStatsResponse, FieldStatsShardRequest, FieldStatsShardResponse> {

    private final IndicesService indicesService;

    @Inject
    public TransportFieldStatsAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                              TransportService transportService, ActionFilters actionFilters,
                                              IndexNameExpressionResolver indexNameExpressionResolver,
                                              IndicesService indicesService) {
        super(FieldStatsAction.NAME, clusterService, transportService,
            actionFilters, indexNameExpressionResolver, FieldStatsRequest::new,
            FieldStatsShardRequest::new, ThreadPool.Names.MANAGEMENT);
        this.indicesService = indicesService;
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected FieldStatsResponse newResponse(FieldStatsRequest request, AtomicReferenceArray shardsResponses,
                                             ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        Map<String, String> conflicts = new HashMap<>();
        Map<String, Map<String, FieldStats<?>>> indicesMergedFieldStats = new HashMap<>();
        List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>();
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardValue = shardsResponses.get(i);
            if (shardValue == null) {
                // simply ignore non active shards
            } else if (shardValue instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                shardFailures.add(
                    new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardValue)
                );
            } else {
                successfulShards++;
                FieldStatsShardResponse shardResponse = (FieldStatsShardResponse) shardValue;

                final String indexName;
                if ("cluster".equals(request.level())) {
                    indexName = "_all";
                } else if ("indices".equals(request.level())) {
                    indexName = shardResponse.getIndex();
                } else {
                    // should already have been caught by the FieldStatsRequest#validate(...)
                    throw new IllegalArgumentException("Illegal level option [" + request.level() + "]");
                }

                Map<String, FieldStats<?>> indexMergedFieldStats = indicesMergedFieldStats.get(indexName);
                if (indexMergedFieldStats == null) {
                    indicesMergedFieldStats.put(indexName, indexMergedFieldStats = new HashMap<>());
                }

                Map<String, FieldStats<?>> fieldStats = shardResponse.getFieldStats();
                for (Map.Entry<String, FieldStats<?>> entry : fieldStats.entrySet()) {
                    FieldStats<?> existing = indexMergedFieldStats.get(entry.getKey());
                    if (existing != null) {
                        if (existing.getType() != entry.getValue().getType()) {
                            if (conflicts.containsKey(entry.getKey()) == false) {
                                FieldStats<?>[] fields = new FieldStats<?>[] {entry.getValue(), existing};
                                Arrays.sort(fields, Comparator.comparingInt(FieldStats::getType));
                                conflicts.put(entry.getKey(),
                                    "Field [" + entry.getKey() + "] of type [" +
                                        fields[0].getDisplayType() +
                                        "] conflicts with existing field of type [" +
                                        fields[1].getDisplayType() +
                                        "] in other index.");
                            }
                        } else {
                            existing.accumulate(entry.getValue());
                        }
                    } else {
                        indexMergedFieldStats.put(entry.getKey(), entry.getValue());
                    }
                }
            }

            // Check the field with conflicts and remove them.
            for (String conflictKey : conflicts.keySet()) {
                Iterator<Map.Entry<String, Map<String, FieldStats<?>>>> iterator =
                    indicesMergedFieldStats.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, Map<String, FieldStats<?>>> entry = iterator.next();
                    if (entry.getValue().containsKey(conflictKey)) {
                        entry.getValue().remove(conflictKey);
                    }
                }
            }

        }

        if (request.getIndexConstraints().length != 0) {
            Set<String> fieldStatFields = new HashSet<>(Arrays.asList(request.getFields()));
            for (IndexConstraint indexConstraint : request.getIndexConstraints()) {
                Iterator<Map.Entry<String, Map<String, FieldStats<?>>>> iterator =
                    indicesMergedFieldStats.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, Map<String, FieldStats<?>>> entry = iterator.next();
                    FieldStats<?> indexConstraintFieldStats = entry.getValue().get(indexConstraint.getField());
                    if (indexConstraintFieldStats != null && indexConstraintFieldStats.match(indexConstraint)) {
                        // If the field stats didn't occur in the list of fields in the original request
                        // we need to remove the field stats, because it was never requested and was only needed to
                        // validate the index constraint.
                        if (fieldStatFields.contains(indexConstraint.getField()) == false) {
                            entry.getValue().remove(indexConstraint.getField());
                        }
                    } else {
                        // The index constraint didn't match or was empty,
                        // so we remove all the field stats of the index we're checking.
                        iterator.remove();
                    }
                }
            }
        }

        return new FieldStatsResponse(shardsResponses.length(), successfulShards, failedShards,
            shardFailures, indicesMergedFieldStats, conflicts);
    }

    @Override
    protected FieldStatsShardRequest newShardRequest(int numShards, ShardRouting shard, FieldStatsRequest request) {
        return new FieldStatsShardRequest(shard.shardId(), request);
    }

    @Override protected FieldStatsShardResponse readShardResponse(StreamInput in) throws IOException {
        return null;
    }

    @Override protected FieldStatsShardResponse shardOperation(FieldStatsShardRequest request, Task task)
        throws IOException {
        ShardId shardId = request.shardId();
        Map<String, FieldStats<?>> fieldStats = new HashMap<>();
        IndexService indexServices = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard shard = indexServices.getShard(shardId.id());
        try (Engine.Searcher searcher = shard.acquireSearcher("fieldstats")) {
            // Resolve patterns and deduplicate
            Set<String> fieldNames = new HashSet<>();
            for (String field : request.getFields()) {
                fieldNames.addAll(shard.mapperService().simpleMatchToFullName(field));
            }
            for (String field : fieldNames) {
                FieldStats<?> stats = getFieldStats(shard, searcher, field);
                if (stats != null) {
                    fieldStats.put(field, stats);
                }
            }
        } catch (Exception e) {
            throw ExceptionsHelper.convertToOpenSearchException(e);
        }
        return new FieldStatsShardResponse(shardId, fieldStats);
    }

    private FieldStats<?> getFieldStats(IndexShard shard, Engine.Searcher searcher, String field) throws Exception {
        MappedFieldType fieldType = shard.mapperService().fieldType(field);
        if (fieldType == null) {
            return null;
        }
        IndexReader ir = searcher.getIndexReader();
        if (fieldType instanceof NumberFieldMapper.NumberFieldType) {
            String numericName = fieldType.typeName();
            long size = PointValues.size(ir, field);
            int docCount = PointValues.getDocCount(ir, field);
            byte[] min = PointValues.getMinPackedValue(ir, field);
            byte[] max = PointValues.getMaxPackedValue(ir, field);
            if (size == 0) {
                if (numericName == NumberFieldMapper.NumberType.HALF_FLOAT.typeName() ||
                        numericName == NumberFieldMapper.NumberType.FLOAT.typeName() ||
                        numericName == NumberFieldMapper.NumberType.DOUBLE.typeName()) {
                    return new FieldStats.Double(ir.maxDoc(), 0, -1, -1, fieldType.isSearchable(), fieldType.isAggregatable());
                }
                return new FieldStats.Long(ir.maxDoc(), 0, -1, -1, fieldType.isSearchable(), fieldType.isAggregatable());
            }
            if (numericName == NumberFieldMapper.NumberType.LONG.typeName()) {
                return new FieldStats.Long(ir.maxDoc(), docCount, -1, size,
                        fieldType.isSearchable(), fieldType.isAggregatable(),
                        LongPoint.decodeDimension(min, 0), LongPoint.decodeDimension(max, 0));
            } else if (numericName == NumberFieldMapper.NumberType.INTEGER.typeName() ||
                    numericName == NumberFieldMapper.NumberType.BYTE.typeName() ||
                    numericName == NumberFieldMapper.NumberType.SHORT.typeName()) {
                return new FieldStats.Long(ir.maxDoc(), docCount, -1, size,
                        fieldType.isSearchable(), fieldType.isAggregatable(),
                        IntPoint.decodeDimension(min, 0), IntPoint.decodeDimension(max, 0));
            } else if (numericName == NumberFieldMapper.NumberType.HALF_FLOAT.typeName()) {
                return new FieldStats.Double(ir.maxDoc(), docCount, -1, size,
                        fieldType.isSearchable(), fieldType.isAggregatable(),
                        HalfFloatPoint.decodeDimension(min, 0), HalfFloatPoint.decodeDimension(max, 0));
            } else if (numericName == NumberFieldMapper.NumberType.FLOAT.typeName()) {
                return new FieldStats.Double(ir.maxDoc(), docCount, -1, size,
                        fieldType.isSearchable(), fieldType.isAggregatable(),
                        FloatPoint.decodeDimension(min, 0), FloatPoint.decodeDimension(max, 0));
            } else if (numericName == NumberFieldMapper.NumberType.DOUBLE.typeName()) {
                return new FieldStats.Double(ir.maxDoc(), docCount, -1, size,
                        fieldType.isSearchable(), fieldType.isAggregatable(),
                        DoublePoint.decodeDimension(min, 0), DoublePoint.decodeDimension(max, 0));
            }
        }

        if (fieldType instanceof DateFieldMapper.DateFieldType) {
            long size = PointValues.size(ir, field);
            if (size == 0) {
                return new FieldStats.Date(ir.maxDoc(), -1L, -1L, -1L, fieldType.isSearchable(), fieldType.isAggregatable());
            }
            int docCount = PointValues.getDocCount(ir, field);
            byte[] min = PointValues.getMinPackedValue(ir, field);
            byte[] max = PointValues.getMaxPackedValue(ir, field);
            return new FieldStats.Date(ir.maxDoc(),
                docCount,
                -1,
                size,
                fieldType.isSearchable(),
                fieldType.isAggregatable(),
                ((DateFieldMapper.DateFieldType) fieldType).dateTimeFormatter(),
                LongPoint.decodeDimension(min, 0),
                LongPoint.decodeDimension(max, 0)
            );
        }

        if (fieldType instanceof GeoPointFieldMapper.GeoPointFieldType) {
            final long size = PointValues.size(ir, field);
            if (size == 0) {
                return new FieldStats.GeoPoint(ir.maxDoc(), -1L, -1L, -1L, fieldType.isSearchable(), fieldType.isAggregatable());
            }
            final int docCount = PointValues.getDocCount(ir, field);
            byte[] min = PointValues.getMinPackedValue(ir, field);
            byte[] max = PointValues.getMaxPackedValue(ir, field);
            GeoPoint minPt = new GeoPoint(GeoEncodingUtils.decodeLatitude(min, 0),
                    GeoEncodingUtils.decodeLongitude(min, Integer.BYTES));
            GeoPoint maxPt = new GeoPoint(GeoEncodingUtils.decodeLatitude(max, 0),
                    GeoEncodingUtils.decodeLongitude(max, Integer.BYTES));
            return new FieldStats.GeoPoint(ir.maxDoc(), docCount, -1L, size, fieldType.isSearchable(), fieldType.isAggregatable(),
                    minPt, maxPt);

        }

        Terms terms = MultiTerms.getTerms(ir, field);
        if (terms == null) {
            return new FieldStats.Text(ir.maxDoc(), 0, 0, 0, fieldType.isSearchable(), fieldType.isAggregatable());
        }

        return new FieldStats.Text(ir.maxDoc(), terms.getDocCount(), terms.getSumDocFreq(), terms.getSumTotalTermFreq(),
                fieldType.isSearchable(), fieldType.isAggregatable(), terms.getMin(), terms.getMax());
    }


    @Override
    protected GroupShardsIterator<ShardIterator> shards(ClusterState clusterState, FieldStatsRequest request,
                                         String[] concreteIndices) {
        return clusterService.operationRouting().searchShards(clusterState, concreteIndices, null, null);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, FieldStatsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, FieldStatsRequest request,
                                                      String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.READ, concreteIndices);
    }
}
