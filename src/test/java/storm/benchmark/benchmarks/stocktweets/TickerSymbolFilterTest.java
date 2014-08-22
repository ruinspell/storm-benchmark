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

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Map;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static storm.benchmark.benchmarks.stocktweets.DataCleanse.TickerSymbolFilter;

public class TickerSymbolFilterTest {
  Map<String, String> tickerBase = ImmutableMap.of(
    "aapl", "Apple",
    "gool", "Google"
  );

  @Test(dataProvider = "getStockTweets")
  public void testFilter(String original, String filtered) {
    TickerSymbolFilter filter = new TickerSymbolFilter();
    filter.setTickerBase(tickerBase);
    Tuple tuple = mock(Tuple.class);

    when(tuple.getString(0)).thenReturn(original);

    Values values = filter.filter(tuple);
    assertThat(values.size()).isEqualTo(1);
    assertThat(values.get(0)).isEqualTo(filtered);
  }

  @DataProvider
  public Object[][] getStockTweets() {
    return new Object[][] {
            { "2010\t$AAPL\t01\tup",  "2010\tApple\t01\tup"},
            { "2010\t$AAPL...\t02\tup", "2010\tApple\t02\tup"},
            { "2010\t$aapl\t03\tup", "2010\tApple\t03\tup"},
            { "2012\t$GOOL\t04\tup", "2012\tGoogle\t04\tup"}
    };
  }
}
