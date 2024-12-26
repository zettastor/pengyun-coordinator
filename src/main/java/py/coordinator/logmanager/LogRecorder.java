

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
