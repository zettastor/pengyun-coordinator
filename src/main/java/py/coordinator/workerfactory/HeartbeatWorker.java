
package py.coordinator.workerfactory;

import io.netty.channel.Channel;


public interface HeartbeatWorker {

  public void doHeartbeatWork();

  public void heartbeatIfNeed(Channel channel);
}
