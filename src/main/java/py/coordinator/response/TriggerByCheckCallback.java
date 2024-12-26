
package py.coordinator.response;

import py.instance.InstanceId;


public interface TriggerByCheckCallback {

  public long getOriRequestId();

  public void triggeredByCheckCallback();

  public void markNeedUpdateMembership();

  public void noNeedUpdateMembership();

  public void resetNeedUpdateMembership();

  public void markDoneDirectly();

  public void markRequestFailed(InstanceId whoIsDisconnect);

  public void resetRequestFailedInfo();

  public boolean streamIO();

  public void doneForCommitLog();

  public void replaceLogUuidForNotCreateCompletelyLogs();

  public Long getVolumeId();
}
