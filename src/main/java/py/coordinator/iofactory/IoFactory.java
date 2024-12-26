

package py.coordinator.iofactory;

import py.archive.segment.SegId;
import py.membership.IoActionContext;
import py.membership.SegmentMembership;
import py.volume.VolumeType;


public interface IoFactory {

  public IoActionContext generateIoMembers(SegmentMembership segmentMembership,
      VolumeType volumeType, SegId segId, 
      Long requestId);
}
