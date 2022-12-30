/*
 * Copyright (c) 2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
 */

package py.coordinator.utils;

import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.PyService;
import py.monitor.common.CounterName;
import py.monitor.common.OperationName;
import py.monitor.common.UserDefineName;
import py.querylog.eventdatautil.EventDataWorker;


public class NetworkDelayRecorderImpl implements NetworkDelayRecorder {

  private static final Logger logger = LoggerFactory.getLogger(NetworkDelayRecorderImpl.class);
  private int timeWindowMs;
  private Map<String, SlidingTimeWindowArrayReservoir> recordMap;


  public NetworkDelayRecorderImpl(int timeWindowMs) {
    this.timeWindowMs = timeWindowMs;
    this.recordMap = new ConcurrentHashMap<>();
  }

  @Override
  public void recordDelay(String destIpAddress, Long delayMs) {
    SlidingTimeWindowArrayReservoir reservoir;
    if (!recordMap.containsKey(destIpAddress)) {
      reservoir = new SlidingTimeWindowArrayReservoir(timeWindowMs, TimeUnit.MILLISECONDS);
      recordMap.put(destIpAddress, reservoir);
    } else {
      reservoir = recordMap.get(destIpAddress);
    }
    reservoir.update(delayMs);
  }

  @Override
  public Double getMeanDelay(String destIpAddress) {
    SlidingTimeWindowArrayReservoir reservoir = recordMap.get(destIpAddress);
    Validate.notNull(reservoir);

    return reservoir.getSnapshot().getMean();
  }

  @Override
  public void outputAllMeanDelay(String myIpAddress) {

    Map<String, Long> counters = new HashMap<>();
    counters.put(CounterName.NETWORK_CONGESTION.name(), 0L);

    Map<String, String> userDefineParams = new HashMap<>();
    userDefineParams.put(UserDefineName.PingSourceIp.name(), myIpAddress);

    JSONArray jsonRet = new JSONArray();
    for (Map.Entry<String, SlidingTimeWindowArrayReservoir> entry : recordMap.entrySet()) {

      String destPingIp = entry.getKey();

      long delayMs = (long) entry.getValue().getSnapshot().getMean();

     
      JSONObject destJson = new JSONObject();
      destJson.put(UserDefineName.PingDestIp.toString(), destPingIp);
      destJson.put(UserDefineName.PingDelay.toString(), String.valueOf(delayMs));

      jsonRet.add(destJson);
    }

    EventDataWorker eventDataWorker = new EventDataWorker(PyService.COORDINATOR, userDefineParams);

    eventDataWorker.addToDefaultNameValues(UserDefineName.PingResult.name(), jsonRet.toString());
    String result = eventDataWorker.work(OperationName.NETWORK.name(), counters);

    logger.info("network delay output result:{}", result);
  }
}
