

package py.coordinator.response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.coordinator.lib.Coordinator;


public class GetMembershipCallbackForCommitLog extends GetMembershipCallbackCollector {

  private static final Logger logger = LoggerFactory
      .getLogger(GetMembershipCallbackForCommitLog.class);

  public GetMembershipCallbackForCommitLog(Coordinator coordinator, int sendCount, SegId segId,
      long requestId,
      TriggerByCheckCallback callback) {
    super(coordinator, sendCount, segId, requestId, callback);
  }

  @Override
  public void nextProcess() {
    super.nextProcess();
    logger.info("ori:{} at:{} get membership done", requestId, segId);
    callback.doneForCommitLog();
  }
}
