
package py.coordinator.response;

import py.coordinator.lib.VolumeInfoHolder;
import py.instance.InstanceId;
import py.netty.core.AbstractMethodCallback;
import py.proto.Commitlog;


public class NotifyDatanodeStartOnlineMigrationCallback
    extends AbstractMethodCallback<Commitlog.PbStartOnlineMigrationResponse> {

  private VolumeInfoHolder volumeInfoHolder;
  private InstanceId notifyDatanode;

  public NotifyDatanodeStartOnlineMigrationCallback(VolumeInfoHolder volumeInfoHolder,
      InstanceId notifyDatanode) {
    this.volumeInfoHolder = volumeInfoHolder;
    this.notifyDatanode = notifyDatanode;
  }

  @Override
  public void complete(Commitlog.PbStartOnlineMigrationResponse object) {
    this.volumeInfoHolder.decNotifyDatanodeCount();
  }

  @Override
  public void fail(Exception e) {
    this.volumeInfoHolder.addFailedDatanode(this.notifyDatanode);
    this.volumeInfoHolder.decNotifyDatanodeCount();
  }

}
