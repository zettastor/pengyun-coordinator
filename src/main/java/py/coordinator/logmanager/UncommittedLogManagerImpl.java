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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.lang.Validate;
import py.archive.segment.SegId;
import py.coordinator.worker.CommitLogWorker;


public class UncommittedLogManagerImpl implements UncommittedLogManager {

  private Map<VolumeIdAndSegIdKey, BlockingQueue<WriteIoContextManager>> mapSegIdToManagers;
  private CommitLogWorker commitLogWorker;

  public UncommittedLogManagerImpl() {
    this.mapSegIdToManagers = new ConcurrentHashMap<>();
  }

  @Override
  public boolean hasLogManagerToCommit(Long volumeId, SegId segId) {
    VolumeIdAndSegIdKey volumeIdAndSegIdKey = new VolumeIdAndSegIdKey(volumeId, segId);
    BlockingQueue<WriteIoContextManager> existManagers = mapSegIdToManagers
        .get(volumeIdAndSegIdKey);
    if (existManagers == null) {
      return false;
    }

    return (existManagers.size() > 0) ? true : false;
  }

  @Override
  public void addLogManagerToCommit(WriteIoContextManager manager) {
    Long volumeId = manager.getVolumeId();
    SegId segId = manager.getSegId();
    addLogManagerToCommitWithoutDriverCommitLogWork(manager);
   
    commitLogWorker.put(volumeId, segId, false);
  }

  @Override
  public void addLogManagerToCommit(Collection<WriteIoContextManager> managers) {
    if (managers == null || managers.isEmpty()) {
      return;
    }
    Long volumeId = null;
    SegId segId = null;
    for (WriteIoContextManager manager : managers) {
      if (volumeId == null) {
        volumeId = manager.getVolumeId();
      }
      if (segId == null) {
        segId = manager.getSegId();
      }
      addLogManagerToCommitWithoutDriverCommitLogWork(manager);
    }
   
    commitLogWorker.put(volumeId, segId, true);
  }

  private void addLogManagerToCommitWithoutDriverCommitLogWork(WriteIoContextManager manager) {
    Validate.isTrue(!manager.isAllFinalStatus());
    Long volumeId = manager.getVolumeId();
    SegId segId = manager.getSegId();
    VolumeIdAndSegIdKey volumeIdAndSegIdKey = new VolumeIdAndSegIdKey(volumeId, segId);
    BlockingQueue<WriteIoContextManager> existManagers = null;
    if (!mapSegIdToManagers.containsKey(volumeIdAndSegIdKey)) {
      existManagers = mapSegIdToManagers
          .putIfAbsent(volumeIdAndSegIdKey, new LinkedBlockingQueue<>());
    }
    if (existManagers == null) {
      existManagers = mapSegIdToManagers.get(volumeIdAndSegIdKey);
    }
    Validate.notNull(existManagers);
    existManagers.offer(manager);
  }

  @Override
  public List<WriteIoContextManager> pollLogManagerToCommit(Long volumeId, SegId segId,
      int maxCount) {
    List<WriteIoContextManager> logManagerToCommit = new ArrayList<>();
    VolumeIdAndSegIdKey volumeIdAndSegIdKey = new VolumeIdAndSegIdKey(volumeId, segId);
    BlockingQueue<WriteIoContextManager> existManagers = mapSegIdToManagers
        .get(volumeIdAndSegIdKey);
    if (existManagers == null) {
      return logManagerToCommit;
    }

    existManagers.drainTo(logManagerToCommit, maxCount);
    return logManagerToCommit;
  }

  @Override
  public List<WriteIoContextManager> pollLogManagerToCommit(Long volumeId, SegId segId) {
    List<WriteIoContextManager> logManagerToCommit = new ArrayList<>();
    VolumeIdAndSegIdKey volumeIdAndSegIdKey = new VolumeIdAndSegIdKey(volumeId, segId);
    BlockingQueue<WriteIoContextManager> existManagers = mapSegIdToManagers
        .get(volumeIdAndSegIdKey);
    if (existManagers == null) {
      return logManagerToCommit;
    }

    existManagers.drainTo(logManagerToCommit);
    return logManagerToCommit;
  }

  public void setCommitLogWorker(CommitLogWorker commitLogWorker) {
    this.commitLogWorker = commitLogWorker;
  }
}
