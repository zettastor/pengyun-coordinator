
package py.coordinator.worker;

import py.archive.segment.SegId;


public interface CommitLogWorker {


  public void start();


  public void stop();


  public boolean put(Long volumeId, SegId segId, boolean needDelay);

}
