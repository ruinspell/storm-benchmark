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

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.benchmark.benchmarks.common.StormBenchmark;
import storm.benchmark.lib.bolt.FilterBolt;
import storm.benchmark.lib.operation.Filter;
import storm.benchmark.lib.spout.FileReadSpout;
import storm.benchmark.tools.FileReader;
import storm.benchmark.util.BenchmarkUtils;
import storm.benchmark.util.KafkaUtils;
import storm.kafka.KafkaSpout;
import storm.kafka.StringScheme;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class DataCleanse extends StormBenchmark {
  public final static String SPOUT_ID = "spout";
  public final static String SPOUT_NUM = "component.spout_num";
  public final static String FILTER_ID = "filter";
  public final static String FILTER_NUM = "component.filter_bolt_num";

  public static final int DEFAULT_SPOUT_NUM = 4;
  public static final int DEFAULT_FILTER_BOLT_NUM = 4;

  private static final Logger LOG = LoggerFactory.getLogger(DataCleanse.class);
  private IRichSpout spout;


  @Override
  public StormTopology getTopology(Config config) {
    final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
    final int filterBoltNum = BenchmarkUtils.getInt(config, FILTER_NUM, DEFAULT_FILTER_BOLT_NUM);
    spout = new KafkaSpout(KafkaUtils.getSpoutConfig(
            config, new SchemeAsMultiScheme(new StringScheme())));

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT_ID, spout, spoutNum);
    builder.setBolt(FILTER_ID, new TickerSymbolFilterBolt(new Fields("filter_tweets")),
            filterBoltNum).localOrShuffleGrouping(SPOUT_ID);
    return builder.createTopology();
  }

  static class TickerSymbolFilterBolt extends FilterBolt {
    private static final long serialVersionUID = -5166209349181923660L;

    private static final String DEFAULT_TICKER_SYMBOLS = "/resources/ticker_symbol.tsv";
    private Map<String, String> tickerBase;

    public TickerSymbolFilterBolt(Fields fields) {
      super(fields);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
      super.prepare(stormConf, context);
      // map from ticker symbol to company e.g. AAPL -> Apple
      TickerSymbolFilter filter = new TickerSymbolFilter();
      filter.setTickerBase(loadTickerBase());
      setFilter(filter);
    }

    public void setTickerBase(Map<String, String> tickerBase) {
      this.tickerBase = tickerBase;
    }

    private Map<String, String> loadTickerBase() {
      if (tickerBase != null) {
        return tickerBase;
      }
      // if not set, read from default ticker_symbol files.
      Map<String, String> tickerBase = new HashMap<String, String>();
      FileReader reader = new FileReader(DEFAULT_TICKER_SYMBOLS);
      reader.open();
      String line;
      while((line = reader.nextLine()) != null) {
        String[] words = line.split("\t");
        if (words.length != 2) {
          LOG.warn("invalid ticker symbols: " + words);
          continue;
        }
        LOG.debug("ticker: " + words[0] + ", company: " + words[1]);
        tickerBase.put(words[0].toLowerCase(), words[1]);
      }
      return tickerBase;
    }
  }

  static class TickerSymbolFilter implements Filter, Serializable {
    private static final long serialVersionUID = 1100622030911096941L;
    private Map<String, String> tickerBase = new HashMap<String, String>();

    public void setTickerBase(Map<String, String> tickerBase) {
      this.tickerBase = tickerBase;
    }

    /**
     * this filter replaces valid ticker symbols (starting with $) with company names
     * and throw out tweets wth invalid tickers or no keywords;
     *
     * @param tuple a tab separated tweet including timestamp, ticker symbol, tweet id and keywords
     * @return a tab separated tweet including timestamp, company name, tweet id and keywords
     */
    @Override
    public Values filter(Tuple tuple) {
      String tweet = tuple.getString(0);
      String[] words = tweet.split("\t");
      if (words.length == 4) {
        String second = words[1]; // ticker symbol
        String fourth = words[3]; // keywords
        if (second.startsWith("$") && !fourth.isEmpty()) {
          String ticker = second.substring(1).toLowerCase().replaceAll("[.]", "");
          String company = tickerBase.get(ticker);
          if (company != null) {
            words[1] = company;
            Joiner joiner = Joiner.on("\t").skipNulls();
            String filtered = joiner.join(words);
            System.out.println("filtered tweet: " + filtered);
            LOG.debug("filtered tweet: " + filtered);
            return new Values(filtered);
          }
        }
      }
      return new Values();
    }
  }
}
