/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
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

package py.coordinator.logmanager;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.coordinator.log.BroadcastLog;


public class LogRecorderImpl implements LogRecorder {

  private static final Logger logger = LoggerFactory.getLogger(LogRecorderImpl.class);

 
 
 

  private Multimap<SegIdAndPageIndexKey, Long> logRecorder;

  public LogRecorderImpl() {
    logRecorder = Multimaps
        .synchronizedSetMultimap(HashMultimap.<SegIdAndPageIndexKey, Long>create());
  }

  @Override
  public boolean hasCommitAllLog() {
    return logRecorder.isEmpty();
  }

  @Override
  public void addCreatedLogs(SegId segId, Collection<BroadcastLog> logs) {
    if (logs.isEmpty()) {
      logger.info("no need to add any logs at:{}", segId);
      return;
    }

    for (BroadcastLog log : logs) {
      Long pageIndex = log.getPageIndexInSegment();
      SegIdAndPageIndexKey key = new SegIdAndPageIndexKey(segId, pageIndex);
      logRecorder.put(key, log.getLogId());
    }
  }

  @Override
  public void removeCreateLogs(SegId segId, Collection<BroadcastLog> logs) {
    if (logs.isEmpty()) {
      logger.info("no need to remove any logs at:{}", segId);
      return;
    }

    for (BroadcastLog log : logs) {
      Long pageIndex = log.getPageIndexInSegment();
      SegIdAndPageIndexKey key = new SegIdAndPageIndexKey(segId, pageIndex);
      logRecorder.remove(key, log.getLogId());
    }
  }

  @Override
  public List<Long> getCreatedLogs(SegId segId, Collection<Long> pageIndexList) {
    List<Long> returnLogs = new ArrayList<>();

    if (null == pageIndexList || pageIndexList.isEmpty()) {
      logger.info("no log to load at:{}", segId);
      return returnLogs;
    }

    for (Long pageIndex : pageIndexList) {
      SegIdAndPageIndexKey key = new SegIdAndPageIndexKey(segId, pageIndex);
      Collection<Long> logIds = logRecorder.get(key);
     
     
      returnLogs.addAll(logIds);
    }

    return returnLogs;
  }

  @Override
  public String printAllLogs() {
    StringBuilder printStringBuilder = new StringBuilder();
    for (Entry<SegIdAndPageIndexKey, Long> entry : logRecorder.entries()) {
      printStringBuilder
          .append("SegId:[ " + entry.getKey().getSegId() + "] pageIndex[ " + entry.getKey()
              .getPageIndex()
              + "], has" + " log:" + entry.getValue() + "\n");
    }
    return printStringBuilder.toString();
  }

  class SegIdAndPageIndexKey {

    private SegId segId;
    private Long pageIndex;

    public SegIdAndPageIndexKey(SegId segId, Long pageIndex) {
      this.segId = segId;
      this.pageIndex = pageIndex;
    }

    public SegId getSegId() {
      return segId;
    }

    public void setSegId(SegId segId) {
      this.segId = segId;
    }

    public Long getPageIndex() {
      return pageIndex;
    }

    public void setPageIndex(Long pageIndex) {
      this.pageIndex = pageIndex;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof SegIdAndPageIndexKey)) {
        return false;
      }

      SegIdAndPageIndexKey that = (SegIdAndPageIndexKey) o;

      if (!segId.equals(that.segId)) {
        return false;
      }
      return pageIndex.equals(that.pageIndex);
    }

    @Override
    public int hashCode() {
      int result = segId.hashCode();
      result = 31 * result + pageIndex.hashCode();
      return result;
    }
  }
}
