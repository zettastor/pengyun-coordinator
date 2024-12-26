
package py.coordinator.logmanager;

import py.archive.segment.SegId;


public interface IoContextManagerCommon {

  public long getRequestId();

  public SegId getSegId();

  public Long getVolumeId();
}
