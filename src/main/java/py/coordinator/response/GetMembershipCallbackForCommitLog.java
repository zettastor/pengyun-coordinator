/*
 * Copyright (c) 2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
 */

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
