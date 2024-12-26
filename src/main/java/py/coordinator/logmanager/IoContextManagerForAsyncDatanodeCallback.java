

package py.coordinator.logmanager;

import py.instance.InstanceId;
import py.membership.IoActionContext;
import py.membership.IoMember;


public interface IoContextManagerForAsyncDatanodeCallback extends IoContextManagerCommon {

  public IoActionContext getIoActionContext();

  public void markDelay();

  public void markDoneDirectly();

  public void markRequestFailed(InstanceId whoIsDisconnect);

  public void markNeedUpdateMembership();

  public void noNeedUpdateMembership();

  public void resetNeedUpdateMembership();

  public boolean streamIO();

  public void replaceLogUuidForNotCreateCompletelyLogs();

  public void markPrimaryRsp();

  public void markSecondaryRsp();

  public void processResponse(Object object, IoMember ioMember);
}
