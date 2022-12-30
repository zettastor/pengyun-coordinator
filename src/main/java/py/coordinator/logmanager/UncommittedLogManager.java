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

import java.util.Collection;
import java.util.List;
import py.archive.segment.SegId;
import py.coordinator.worker.CommitLogWorker;


public interface UncommittedLogManager {

  public void addLogManagerToCommit(WriteIoContextManager logManager);

  public void addLogManagerToCommit(Collection<WriteIoContextManager> managers);

  public List<WriteIoContextManager> pollLogManagerToCommit(Long volumeId, SegId segId);

  public List<WriteIoContextManager> pollLogManagerToCommit(Long volumeId, SegId segId,
      int maxCount);

  public boolean hasLogManagerToCommit(Long volumeId, SegId segId);

  public void setCommitLogWorker(CommitLogWorker commitLogWorker);

}
