

package py.coordinator.response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.instance.InstanceId;
import py.netty.core.AbstractMethodCallback;
import py.proto.Broadcastlog;


public class CheckRequestCallback extends AbstractMethodCallback<Broadcastlog.PbCheckResponse> {

  private static final Logger logger = LoggerFactory.getLogger(CheckRequestCallback.class);
  private final CheckRequestCallbackCollector checkRequestCallbackCollector;
  private final InstanceId passByMeToCheck;

  public CheckRequestCallback(CheckRequestCallbackCollector checkRequestCallbackCollector,
      InstanceId passByMeToCheck) {
    this.checkRequestCallbackCollector = checkRequestCallbackCollector;
    this.passByMeToCheck = passByMeToCheck;
  }

  @Override
  public void complete(Broadcastlog.PbCheckResponse object) {
    checkRequestCallbackCollector.complete(object, passByMeToCheck);
  }

  @Override
  public void fail(Exception e) {
    checkRequestCallbackCollector.fail(e, passByMeToCheck);
  }
}
