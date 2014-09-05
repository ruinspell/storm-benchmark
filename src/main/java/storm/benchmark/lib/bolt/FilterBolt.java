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
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.benchmark.lib.operation.Filter;

public class FilterBolt extends BaseBasicBolt {
  private static final long serialVersionUID = -4957635695743420459L;
  private Filter filter;
  private Fields fields;

  public FilterBolt(Fields fields) {
    this.fields = fields;
  }

  public FilterBolt(Filter filter, Fields fields) {
    this.filter = filter;
    this.fields = fields;
  }

  /**
   * subclass should set filter at prepare stage if the filter
   * carries a lot of states
   */
  public void setFilter(Filter filter) {
    this.filter = filter;
  }

  public Filter getFilter() {
    return filter;
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    Values output = filter.filter(input);
    if (output != null) {
      if (output.size() != fields.size()) {
        throw new RuntimeException("filter values size mismatches declared output size");
      } else if (output.size() != 0) {
        collector.emit(output);
      }
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(fields);
  }
}
