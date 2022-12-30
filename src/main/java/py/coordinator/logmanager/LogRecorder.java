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
import py.coordinator.log.BroadcastLog;


public interface LogRecorder {

  public boolean hasCommitAllLog();

  public void addCreatedLogs(SegId segId, Collection<BroadcastLog> logs);

  public void removeCreateLogs(SegId segId, Collection<BroadcastLog> logs);

  public List<Long> getCreatedLogs(SegId segId, Collection<Long> pageIndexes);

  public String printAllLogs();
}
