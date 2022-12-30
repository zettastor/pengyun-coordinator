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

package py.coordinator.lib;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.coordinator.iorequest.iorequest.IoRequestType;
import py.coordinator.iorequest.iounit.IoUnit;
import py.coordinator.iorequest.iounitcontext.IoUnitContext;
import py.coordinator.iorequest.iounitcontextpacket.IoUnitContextPacketImpl;
import py.coordinator.nbd.request.NbdRequestType;
import py.exception.StorageException;


public class IoSeparatorImpl implements IoSeparator {

  private static final Logger logger = LoggerFactory.getLogger(IoSeparatorImpl.class);
  private Coordinator coordinator;
  private int maxNetworkFrameSizeForWrite;
  private int maxNetworkFrameSizeForRead;

  public IoSeparatorImpl(int maxNetworkFrameSizeForRead, int maxNetworkFrameSizeForWrite,
      Coordinator coordinator) {
    this.coordinator = coordinator;
    this.maxNetworkFrameSizeForRead = maxNetworkFrameSizeForRead;
    this.maxNetworkFrameSizeForWrite = maxNetworkFrameSizeForWrite;
  }

  public void messageReceived(List<IoUnitContext> contexts, NbdRequestType type) {
    if (type == NbdRequestType.Read) {
      splitRead(contexts);
    } else if (type == NbdRequestType.Write) {
      splitWrite(contexts);
    } else {
      throw new RuntimeException("type: " + type + " is not right for contexts: " + contexts);
    }
  }

  @Override
  public void splitRead(List<IoUnitContext> contexts) {
    processContext(contexts, IoRequestType.Read, maxNetworkFrameSizeForRead);
  }

  @Override
  public void splitWrite(List<IoUnitContext> contexts) {
    processContext(contexts, IoRequestType.Write, maxNetworkFrameSizeForWrite);
  }


  @Override
  public void processDiscard(List<IoUnitContext> contexts) {
    processContextWithDiscardLogMaxCountPerRequest(contexts, IoRequestType.Discard);
  }

  private void processContext(List<IoUnitContext> contexts, IoRequestType type,
      int maxNetworkFrameSize) {

    int currentSize = 0;
    List<IoUnitContext> sendContexts = new ArrayList<>();
    for (IoUnitContext context : contexts) {
      IoUnit requestUnit = context.getIoUnit();
      if (requestUnit.getLength() > maxNetworkFrameSize) {
        Validate.isTrue(false,
            "unit size: " + requestUnit.getLength() + ", expected size:" + maxNetworkFrameSize
                + ", type: "
                + type);
      }

      if (currentSize + requestUnit.getLength() > maxNetworkFrameSize) {
        logger.info("get an read request which size {} big enough with max frame {}, type: {}",
            currentSize,
            maxNetworkFrameSize, type);
        Long volumeId = context.getIoRequest().getVolumeId();
        IoUnitContextPacketImpl callback = new IoUnitContextPacketImpl(volumeId, sendContexts,
            context.getSegIndex(), type);
        try {
          coordinator.submit(callback);
        } catch (StorageException e) {
          logger.error("we meet an exception when IO", e);
          doneWithSubmitFailure(callback);
        } finally {
          currentSize = 0;
          sendContexts = new ArrayList<>();
        }
      }

      sendContexts.add(context);
      currentSize += requestUnit.getLength();
    }

    if (!sendContexts.isEmpty()) {
      IoUnitContextPacketImpl callback = new IoUnitContextPacketImpl(
          sendContexts.get(0).getIoRequest().getVolumeId(), sendContexts,
          sendContexts.get(0).getSegIndex(),
          type);
      try {
        coordinator.submit(callback);
      } catch (StorageException e) {
        logger.error("we meet an exception we " + type + " data", e);
        doneWithSubmitFailure(callback);
      }
    }
  }

  private void processContextWithDiscardLogMaxCountPerRequest(List<IoUnitContext> contexts,
      IoRequestType type) {
    if (contexts.isEmpty()) {
      logger.error("can not happen this way when process discard logs");
      return;
    }
    int discardLogMaxCountPerRequest = CoordinatorConfigSingleton.getInstance()
        .getDiscardLogMaxCountPerRequest();
    int currentCount = 0;
    List<IoUnitContext> sendContexts = new ArrayList<>();
    for (IoUnitContext context : contexts) {
      currentCount++;
      if (currentCount >= discardLogMaxCountPerRequest) {
        logger.info("got enough discard logs:{} for one request, should send them at once",
            currentCount);
        Long volumeId = context.getIoRequest().getVolumeId();
        IoUnitContextPacketImpl callback = new IoUnitContextPacketImpl(volumeId, sendContexts,
            context.getSegIndex(), type);
        try {
          coordinator.submit(callback);
        } catch (StorageException e) {
          logger.error("we meet an exception when process discard IO", e);
          doneWithSubmitFailure(callback);
        } finally {
          currentCount = 0;
          sendContexts = new ArrayList<>();
        }
      }

      sendContexts.add(context);
    }

    if (!sendContexts.isEmpty()) {
      IoUnitContextPacketImpl callback = new IoUnitContextPacketImpl(
          sendContexts.get(0).getIoRequest().getVolumeId(),
          sendContexts, sendContexts.get(0).getSegIndex(), type);
      try {
        coordinator.submit(callback);
      } catch (StorageException e) {
        logger.error("we meet an exception we " + type + " data", e);
        doneWithSubmitFailure(callback);
      }
    }

  }

  protected void doneWithSubmitFailure(IoUnitContextPacketImpl callback) {
    for (IoUnitContext ioContext : callback.getIoContext()) {
      ioContext.getIoUnit().setSuccess(false);
      ioContext.done();
    }
  }
}
