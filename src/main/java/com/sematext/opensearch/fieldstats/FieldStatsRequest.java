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


import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ValidateActions;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class FieldStatsRequest extends BroadcastRequest<FieldStatsRequest> {
    public static final String DEFAULT_LEVEL = "cluster";

    private String[] fields = Strings.EMPTY_ARRAY;
    private String level = DEFAULT_LEVEL;
    private IndexConstraint[] indexConstraints = new IndexConstraint[0];
    private boolean useCache = true;

    public FieldStatsRequest(String... indices) {
        super(indices);
    }

    public FieldStatsRequest(StreamInput in) throws IOException {
        super(in);
        fields = in.readStringArray();
        int size = in.readVInt();
        indexConstraints = new IndexConstraint[size];
        for (int i = 0; i < size; i++) {
            indexConstraints[i] = new IndexConstraint(in);
        }
        level = in.readString();
        useCache = in.readBoolean();
    }

    public FieldStatsRequest(String[] indices, IndicesOptions indicesOptions) {
        super(indices, indicesOptions);
    }

    public String[] getFields() {
        return fields;
    }

    public void setFields(String[] fields) {
        if (fields == null) {
            throw new NullPointerException("specified fields can't be null");
        }
        this.fields = fields;
    }

    public void setUseCache(boolean useCache) {
        this.useCache = useCache;
    }

    public boolean shouldUseCache() {
        return useCache;
    }

    public IndexConstraint[] getIndexConstraints() {
        return indexConstraints;
    }

    public void setIndexConstraints(IndexConstraint[] indexConstraints) {
        if (indexConstraints == null) {
            throw new NullPointerException("specified index_constraints can't be null");
        }
        this.indexConstraints = indexConstraints;
    }

    public void source(XContentParser parser) throws IOException {
        List<IndexConstraint> indexConstraints = new ArrayList<>();
        List<String> fields = new ArrayList<>();
        String fieldName = null;
        Token token = parser.nextToken();
        assert token == Token.START_OBJECT;
        for (token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            switch (token) {
                case FIELD_NAME:
                    fieldName = parser.currentName();
                    break;
                case START_OBJECT:
                    if ("index_constraints".equals(fieldName)) {
                        parseIndexConstraints(indexConstraints, parser);
                    } else {
                        throw new IllegalArgumentException("unknown field [" + fieldName + "]");
                    }
                    break;
                case START_ARRAY:
                    if ("fields".equals(fieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token.isValue()) {
                                fields.add(parser.text());
                            } else {
                                throw new IllegalArgumentException("unexpected token [" + token + "]");
                            }
                        }
                    } else {
                        throw new IllegalArgumentException("unknown field [" + fieldName + "]");
                    }
                    break;
                default:
                    throw new IllegalArgumentException("unexpected token [" + token + "]");
            }
        }
        this.fields = fields.toArray(new String[fields.size()]);
        this.indexConstraints = indexConstraints.toArray(new IndexConstraint[indexConstraints.size()]);
    }

    private static void parseIndexConstraints(List<IndexConstraint> indexConstraints,
                                       XContentParser parser) throws IOException {
        Token token = parser.currentToken();
        assert token == Token.START_OBJECT;
        String field = null;
        String currentName = null;
        for (token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            if (token == Token.FIELD_NAME) {
                field = currentName = parser.currentName();
            } else if (token == Token.START_OBJECT) {
                for (Token fieldToken = parser.nextToken();
                     fieldToken != Token.END_OBJECT; fieldToken = parser.nextToken()) {
                    if (fieldToken == Token.FIELD_NAME) {
                        currentName = parser.currentName();
                    } else if (fieldToken == Token.START_OBJECT) {
                        List<IndexConstraint> tmpConstraints = new ArrayList<>();
                        IndexConstraint.Property property = IndexConstraint.Property.parse(currentName);
                        String tmpOptionalFormat = null;
                        IndexConstraint.Comparison comparison = null;
                        for (Token propertyToken = parser.nextToken();
                             propertyToken != Token.END_OBJECT; propertyToken = parser.nextToken()) {
                            if (propertyToken.isValue()) {
                                if ("format".equals(parser.currentName())) {
                                    tmpOptionalFormat = parser.text();
                                } else {
                                    tmpConstraints.add(new IndexConstraint(field, property,
                                        IndexConstraint.Comparison.parse(parser.currentName()),
                                        parser.text(), tmpOptionalFormat)
                                    );
                                }
                            }
                        }
                        String optionalFormat = tmpOptionalFormat;
                        indexConstraints.addAll(tmpConstraints.stream().map(it -> new IndexConstraint(
                            it.getField(), it.getProperty(), it.getComparison(), it.getValue(), optionalFormat
                        )).collect(Collectors.toList()));
                    } else {
                        throw new IllegalArgumentException("unexpected token [" + fieldToken + "]");
                    }
                }
            } else {
                throw new IllegalArgumentException("unexpected token [" + token + "]");
            }
        }
    }

    public String level() {
        return level;
    }

    public void level(String level) {
        this.level = level;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if ("cluster".equals(level) == false && "indices".equals(level) == false) {
            validationException =
                ValidateActions.addValidationError("invalid level option [" + level + "]", validationException);
        }
        if (fields == null || fields.length == 0) {
            validationException = ValidateActions.addValidationError("no fields specified", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(fields);
        out.writeVInt(indexConstraints.length);
        for (IndexConstraint indexConstraint : indexConstraints) {
            out.writeString(indexConstraint.getField());
            out.writeByte(indexConstraint.getProperty().getId());
            out.writeByte(indexConstraint.getComparison().getId());
            out.writeString(indexConstraint.getValue());
        }
        out.writeString(level);
        out.writeBoolean(useCache);
    }
}
