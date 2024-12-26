

package py.coordinator.lib;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;


public interface AsyncIoCallBack {

  public void ioRequestDone(Long volumeId, boolean result, ByteBuf byteBuf);

  public void setReadDstBuffer(ByteBuf buffer);

  public Channel getChannel();
}
