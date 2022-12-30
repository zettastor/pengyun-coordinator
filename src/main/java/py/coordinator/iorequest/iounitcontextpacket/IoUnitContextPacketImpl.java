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

package py.coordinator.iorequest.iounitcontextpacket;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.coordinator.iorequest.iorequest.IoRequestType;
import py.coordinator.iorequest.iounitcontext.IoUnitContext;


public class IoUnitContextPacketImpl implements IoUnitContextPacket {

  private static final Logger logger = LoggerFactory.getLogger(IoUnitContextPacketImpl.class);
  private final Long volumeId;
  private final int logicalSegIndex;
  private final IoRequestType requestType;
  private List<IoUnitContext> ioContexts;



  public IoUnitContextPacketImpl(Long volumeId, List<IoUnitContext> ioContexts, int logicalSegIndex,
      IoRequestType requestType) {
    this.volumeId = volumeId;
    this.ioContexts = ioContexts;
    this.logicalSegIndex = logicalSegIndex;
    this.requestType = requestType;
  }

  public List<IoUnitContext> getIoContext() {
    return ioContexts;
  }

  public int getLogicalSegIndex() {
    return logicalSegIndex;
  }

  @Override
  public IoRequestType getRequestType() {
    return requestType;
  }

  @Override
  public void complete() {
    logger.debug("all request units are done, {}", ioContexts);
  }

  @Override
  public Long getVolumeId() {
    return volumeId;
  }

  @Override
  public void releaseReference() {
    if (ioContexts != null) {
      if (!ioContexts.isEmpty()) {
        for (IoUnitContext ioUnitContext : ioContexts) {
          ioUnitContext.releaseReference(false);
        }
      }
      ioContexts.clear();
      ioContexts = null;
    }
  }

  @Override
  public String toString() {
    return "IOUnitContextPacketImpl [ioContexts=" + ioContexts + ", logicalSegIndex="
        + logicalSegIndex
        + ", requestType=" + requestType + "]";
  }

  public Integer getSnapshotId() {
    return 0;
  }

  public boolean hasSnapshotId() {
    return false;
  }
}
