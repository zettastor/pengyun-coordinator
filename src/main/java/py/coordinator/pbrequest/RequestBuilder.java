
package py.coordinator.pbrequest;

import com.google.protobuf.AbstractMessage;
import py.coordinator.iorequest.iorequest.IoRequestType;


public interface RequestBuilder<T extends AbstractMessage> {

  public T getRequest();

  public IoRequestType getRequestType();
}
