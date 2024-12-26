

package py.coordinator.nbd.request;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.NotImplementedException;


public class NbdNormalRequestHeader extends RequestHeader {

  public NbdNormalRequestHeader(ByteBuf buffer) {
    super(MagicType.NBD_NORMAL, buffer);

  }

  @Override
  public int getIoSum() {
    return 1;
  }

  @Override
  public long getNbdClientTimestamp() {
    throw new NotImplementedException("");
  }

  @Override
  public String toString() {
    return "NbdNormalRequestHeader{super=" + super.toString() + '}';
  }
}
