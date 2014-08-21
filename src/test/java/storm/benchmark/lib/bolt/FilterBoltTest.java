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

package storm.benchmark.lib.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import storm.benchmark.lib.operation.Filter;
import storm.benchmark.util.MockTupleHelpers;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class FilterBoltTest {
  private static final String ANY_FIELDS = "any_fields";
  private static final String ANY_VALUES = "any_values";
  private FilterBolt bolt;
  private Filter filter;
  private Tuple tuple;
  private BasicOutputCollector collector;
  private OutputFieldsDeclarer declarer;

  @BeforeMethod
  public void setUp() {
    filter = mock(Filter.class);
    tuple = MockTupleHelpers.mockAnyTuple();
    collector = mock(BasicOutputCollector.class);
    declarer = mock(OutputFieldsDeclarer.class);

  }

  @Test
  public void shouldDeclareOutputFields() {
    bolt = new FilterBolt(filter, new Fields(ANY_FIELDS));
    bolt.declareOutputFields(declarer);

    verify(declarer, times(1)).declare(any(Fields.class));
  }

  @Test
  public void shouldNotEmitIfFiltered() {
    bolt = new FilterBolt(filter, new Fields());

    when(filter.filter(tuple)).thenReturn(new Values());

    bolt.execute(tuple, collector);

    verifyZeroInteractions(collector);
  }

  @Test
  public void shouldEmitIfNotFiltered() {
    bolt = new FilterBolt(filter, new Fields(ANY_FIELDS));
    when(filter.filter(tuple)).thenReturn(new Values(ANY_VALUES));

    bolt.execute(tuple, collector);

    verify(collector, times(1)).emit(any(Values.class));
  }
}
