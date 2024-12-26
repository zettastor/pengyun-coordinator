
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
