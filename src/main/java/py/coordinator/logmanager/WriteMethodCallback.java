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

package py.coordinator.logmanager;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.LoggerTracer;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.coordinator.lib.Coordinator;
import py.membership.IoMember;
import py.proto.Broadcastlog;
import py.proto.Broadcastlog.PbWriteResponse;


public class WriteMethodCallback extends IoMethodCallback<PbWriteResponse> {

  private static final Logger logger = LoggerFactory.getLogger(WriteMethodCallback.class);
  private static String className = null;
  private Broadcastlog.PbWriteResponse response;

  public WriteMethodCallback(IoContextManagerForAsyncDatanodeCallback ioContextManager,
      AtomicInteger readCounter, Coordinator coordinator,
      IoMember ioMember) {
    super(ioContextManager, readCounter, coordinator, ioMember);

  }



  public String getClassName() {
    if (className == null) {
      className = getClass().getSimpleName();
    }

    return className;
  }

  @Override
  public void complete(Broadcastlog.PbWriteResponse object) {
    logger.debug("ori:{} receive an response:{}", ioContextManager.getRequestId(), object);
    this.response = object;
    if (logger.isInfoEnabled()) {
      StringBuilder sb = new StringBuilder();
      IoMember ioMember = getIoMember();
      sb.append(ioMember.getMemberIoStatus());
      sb.append(", ori:");
      sb.append(String.valueOf(ioContextManager.getRequestId()));
      sb.append(", log results : [");
      List<Broadcastlog.PbWriteResponseUnit> list = object.getResponseUnitsList();
      for (Broadcastlog.PbWriteResponseUnit pbWriteResponseUnit : list) {
        sb.append("(");
        sb.append(pbWriteResponseUnit.getLogUuid()).append(":")
            .append(pbWriteResponseUnit.getLogResult());
        sb.append("),");
      }
      String msg = sb.toString();
      logger.debug(msg);
      LoggerTracer.getInstance().mark(ioContextManager.getRequestId(), className, msg);
    }
    if (CoordinatorConfigSingleton.getInstance().isTraceAllLogs()) {
      Broadcastlog.PbIoUnitResult logResult = null;
      if (!object.getResponseUnitsList().isEmpty()) {
        int lastIndex = object.getResponseUnitsList().size() - 1;
        logResult = object.getResponseUnitsList().get(lastIndex).getLogResult();
      }

      LoggerTracer.getInstance()
          .mark(ioContextManager.getRequestId(), className,
              "ori:{} last log write result:[{}] from:{} at:{}",
              ioContextManager.getRequestId(), logResult, getIoMember().getEndPoint(),
              ioContextManager.getSegId());
    }

    if (getMemberIoStatus().isPrimary()) {
      ioContextManager.markPrimaryRsp();
    } else {
      ioContextManager.markSecondaryRsp();
    }

    try {
      super.complete(object);
    } catch (Throwable t) {
      logger.error("caught an exception", t);
    }
  }

  @Override
  public void fail(Exception e) {
    logger.info("ori:{} caught an exception: {}, {}", ioContextManager.getRequestId(),
        e.getClass().getSimpleName(), e.toString());

    if (getMemberIoStatus().isPrimary()) {
      ioContextManager.markPrimaryRsp();
    } else {
      ioContextManager.markSecondaryRsp();
    }

    try {
      super.fail(e);
    } catch (Throwable t) {
      logger.error("caught an exception", t);
    }
  }

  @Override
  public Broadcastlog.PbWriteResponse getResponse() {
    return response;
  }

  @Override
  public boolean weakGoodResponse() {
    throw new NotImplementedException("not implement here");
  }

}
