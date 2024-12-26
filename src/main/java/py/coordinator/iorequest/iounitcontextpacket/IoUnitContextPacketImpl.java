/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
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
