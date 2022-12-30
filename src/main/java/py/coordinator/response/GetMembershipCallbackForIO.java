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

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.coordinator.lib.Coordinator;
import py.coordinator.logmanager.ReadIoContextManager;
import py.coordinator.logmanager.WriteIoContextManager;
import py.coordinator.pbrequest.ReadRequestBuilder;
import py.coordinator.pbrequest.WriteRequestBuilder;


public class GetMembershipCallbackForIO extends GetMembershipCallbackCollector {

  private static final Logger logger = LoggerFactory.getLogger(GetMembershipCallbackForIO.class);

  private WriteIoContextManager writeIoContextManager;
  private ReadIoContextManager readIoContextManager;

  private ReadRequestBuilder readRequestBuilder;
  private WriteRequestBuilder writeRequestBuilder;

  private boolean isWrite;



  public GetMembershipCallbackForIO(Coordinator coordinator, int sendCount,
      WriteIoContextManager writeIoContextManager, WriteRequestBuilder writeRequestBuilder) {
    this(coordinator, sendCount, writeIoContextManager, writeRequestBuilder, null, null,
        writeIoContextManager.getSegId(), writeIoContextManager.getRequestId(),
        writeIoContextManager);
    Validate.notNull(writeRequestBuilder);
    Validate.notNull(writeIoContextManager);
    this.isWrite = true;
  }



  public GetMembershipCallbackForIO(Coordinator coordinator, int sendCount,
      ReadIoContextManager readIoContextManager,
      ReadRequestBuilder readRequestBuilder) {
    this(coordinator, sendCount, null, null, readIoContextManager, readRequestBuilder,
        readIoContextManager.getSegId(), readIoContextManager.getRequestId(), readIoContextManager);
    Validate.notNull(readIoContextManager);
    Validate.notNull(readRequestBuilder);
    this.isWrite = false;
  }

  private GetMembershipCallbackForIO(Coordinator coordinator, int sendCount,
      WriteIoContextManager writeIoContextManager, WriteRequestBuilder writeRequestBuilder,
      ReadIoContextManager readIoContextManager, ReadRequestBuilder readRequestBuilder, SegId segId,
      long requestId, TriggerByCheckCallback callback) {

    super(coordinator, sendCount, segId, requestId, callback);

    this.writeIoContextManager = writeIoContextManager;
    this.writeRequestBuilder = writeRequestBuilder;

    this.readIoContextManager = readIoContextManager;
    this.readRequestBuilder = readRequestBuilder;
  }

  @Override
  public void nextProcess() {
    super.nextProcess();
    if (isWrite) {
      logger.info("ori:{} at:{} after update membership, going to resend write",
          writeIoContextManager.getOriRequestId(), writeIoContextManager.getSegId());
      coordinator.resendWriteAfterRetrieveMembership(writeIoContextManager, writeRequestBuilder);
    } else {
      logger.info("ori:{} at:{} after update membership, going to resend read",
          readIoContextManager.getOriRequestId(), readIoContextManager.getSegId());
      coordinator.resendReadAfterRetrieveMembership(readIoContextManager, readRequestBuilder);
    }
  }
}
