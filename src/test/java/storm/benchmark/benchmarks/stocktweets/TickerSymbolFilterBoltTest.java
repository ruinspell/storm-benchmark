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
 * limitations under the License
 */

package storm.benchmark.benchmarks.stocktweets;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import com.google.common.collect.Maps;
import org.testng.annotations.Test;

import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static storm.benchmark.benchmarks.stocktweets.DataCleanse.*;

public class TickerSymbolFilterBoltTest {
  private static final Fields ANY_FIELDS = new Fields("any_fields");
  private static final Map<String, String> ANY_TICKERBASE = Maps.newHashMap();

  @Test
  public void shouldSetFilterWhenPrepare() {
    TickerSymbolFilterBolt bolt = new TickerSymbolFilterBolt(ANY_FIELDS);
    Map conf = mock(Map.class);
    TopologyContext context = mock(TopologyContext.class);
    bolt.setTickerBase(ANY_TICKERBASE);

    assertThat(bolt.getFilter()).isNull();

    bolt.prepare(conf, context);

    assertThat(bolt.getFilter()).isNotNull().isInstanceOf(TickerSymbolFilter.class);
  }
}
