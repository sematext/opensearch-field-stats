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

import org.opensearch.test.fixture.AbstractHttpFixture;

import java.io.IOException;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

public class FieldStatsFixture extends AbstractHttpFixture {

  private final String message;

  private FieldStatsFixture(final String workingDir, final String message) {
    super(workingDir);
    this.message = Objects.requireNonNull(message);
  }

  @Override
  protected Response handle(final Request request) throws IOException {
    if ("GET".equals(request.getMethod()) && "/".equals(request.getPath())) {
      return new Response(200, TEXT_PLAIN_CONTENT_TYPE, message.getBytes(UTF_8));
    }
    return null;
  }

  public static void main(final String[] args) throws Exception {
    if (args == null || args.length != 2) {
      throw new IllegalArgumentException("FieldStatsFixture <working directory> <echo message>");
    }

    final FieldStatsFixture fixture = new FieldStatsFixture(args[0], args[1]);
    fixture.listen();
  }
}
