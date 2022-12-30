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
