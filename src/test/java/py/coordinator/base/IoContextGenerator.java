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

package py.coordinator.base;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Semaphore;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.coordinator.calculator.LogicalToPhysicalCalculator;
import py.coordinator.calculator.StripeLogicalToPhysicalCalculator;
import py.coordinator.iorequest.iorequest.IoRequest;
import py.coordinator.iorequest.iorequest.IoRequestImpl;
import py.coordinator.iorequest.iorequest.IoRequestType;
import py.coordinator.iorequest.iounitcontext.IoUnitContext;
import py.coordinator.lib.Coordinator;
import py.coordinator.lib.IoConvertorImpl;
import py.coordinator.lib.IoSeparatorImpl;
import py.coordinator.lib.NbdAsyncIoCallBack;
import py.coordinator.lib.VolumeInfoHolder;
import py.coordinator.lib.VolumeInfoHolderImpl;
import py.coordinator.lib.VolumeInfoHolderManager;
import py.coordinator.lib.VolumeInfoHolderManagerImpl;
import py.coordinator.nbd.NbdResponseSender;
import py.coordinator.nbd.request.NbdRequestType;
import py.coordinator.nbd.request.PydNormalRequestHeader;
import py.coordinator.nbd.request.Request;
import py.coordinator.nbd.request.RequestHeader;
import py.coordinator.volumeinfo.VolumeInfoRetriever;
import py.performance.PerformanceManager;
import py.performance.PerformanceRecorder;

/**
 * xx.
 */
public class IoContextGenerator {

  private static final Logger logger = LoggerFactory.getLogger(IoContextGenerator.class);
  private static final Random random = new Random();
  private final int pageCount;

  private NbdResponseSender sender = mock(NbdResponseSender.class);
  private Channel channel = mock(Channel.class);
  private VolumeInfoHolderManager volumeInfoHolderManager;
  private VolumeInfoHolder volumeInfoHolder;

  private VolumeInfoRetriever volumeInfoRetriever = mock(VolumeInfoRetriever.class);
  private IoConvertorImpl ioConvertor;
  private Semaphore ioDepth;
  private int snapshotId = (int) RequestIdBuilder.get();

  public IoContextGenerator(long volumeId, long segmentSize, int pageSize) {
    this(volumeId, segmentSize, pageSize, 0);
  }

  public IoContextGenerator(long volumeId, long segmentSize, int pageSize, int pageCount) {
    this(volumeId, segmentSize, pageSize, pageCount, null, null);
  }

  public IoContextGenerator(long volumeId, long segmentSize, int pageSize, int pageCount,
      List<Long> volumeLayout) {
    this(volumeId, segmentSize, pageSize, pageCount, volumeLayout, null);
  }


  /**
   * xx.
   */
  public IoContextGenerator(long volumeId, long segmentSize, int pageSize, int pageCount,
      List<Long> volumeLayout,
      LogicalToPhysicalCalculator calculator) {
    when(channel.remoteAddress()).thenReturn(new InetSocketAddress("10.0.1.127", 1234));

    long firstSegmentCount = 3;
    long secondSegmentCount = 5;
    long thirdSegmentCount = 2;
    if (volumeLayout == null) {
      volumeLayout = new ArrayList<>();
      volumeLayout.add(firstSegmentCount);
      volumeLayout.add(secondSegmentCount);
      volumeLayout.add(thirdSegmentCount);
    }

    this.ioDepth = new Semaphore(128);
    if (pageCount <= 0) {
      this.pageCount = random.nextInt(5) + 1;
    } else {
      this.pageCount = pageCount;
    }

    if (calculator == null) {
      calculator = new StripeLogicalToPhysicalCalculator(volumeInfoRetriever, volumeId, segmentSize,
          pageSize);
      ((StripeLogicalToPhysicalCalculator) calculator).initPagePackageCount(this.pageCount);
      calculator.setVolumeLayout(volumeLayout);
    }

    final Coordinator coordinator = mock(Coordinator.class);
    volumeInfoHolderManager = new VolumeInfoHolderManagerImpl(volumeInfoRetriever);
    volumeInfoHolder = new VolumeInfoHolderImpl(volumeId, snapshotId, volumeInfoRetriever,
        volumeInfoHolderManager.getRootVolumeIdMap());
    volumeInfoHolder.setLogicalToPhysicalCalculator(calculator);

    volumeInfoHolderManager.addVolumeInfoHolder(volumeId, volumeInfoHolder);

    //add 01.08 for Performance
    PerformanceRecorder performanceRecorder = new PerformanceRecorder();
    PerformanceManager.getInstance().addPerformance(volumeId, performanceRecorder);

    long volumeSize = (firstSegmentCount + secondSegmentCount + thirdSegmentCount) * segmentSize;
    when(coordinator.size()).thenReturn(volumeSize);

    MyIoSeparator myIoSeparator = new MyIoSeparator(1024 * 1024, 1024 * 1024, coordinator);
    ioConvertor = new IoConvertorImpl(coordinator, volumeInfoHolderManager, myIoSeparator,
        segmentSize, pageSize);
    logger.info("segment size: {}, page size: {}, volume layout: {}, pageCount: {}", segmentSize,
        pageSize,
        volumeLayout, this.pageCount);
  }

  public void setSender(NbdResponseSender sender) {
    this.sender = sender;
  }


  /**
   * xx.
   */
  public List<IoUnitContext> generateReadIoContexts(long volumeId, long offset, int len)
      throws Exception {
    RequestHeader requestHeader = new PydNormalRequestHeader(NbdRequestType.Read, 0, offset, len,
        0);
    ByteBuf body = null;
    NbdAsyncIoCallBack nbdAsyncIoCallBack = new NbdAsyncIoCallBack(channel, requestHeader, sender);

    IoRequest ioRequest = new IoRequestImpl(offset, len, IoRequestType.Read, body,
        nbdAsyncIoCallBack, 1);
    ioRequest.getTicket(ioDepth);
    ioRequest.setVolumeId(volumeId);
    Long requestUuid = RequestIdBuilder.get();
    ioConvertor.processReadRequest(requestUuid, ioRequest, true);
    return ((MyIoSeparator) ioConvertor.getIoSeparator()).getResult();
  }


  /**
   * xx.
   */
  public List<IoUnitContext> generateDiscardIoContexts(long volumeId, long offset, int len)
      throws Exception {
    RequestHeader requestHeader = new PydNormalRequestHeader(NbdRequestType.Discard, 0, offset, len,
        0);
    ByteBuf body = null;
    Request request = new Request(requestHeader, body);
    NbdAsyncIoCallBack nbdAsyncIoCallBack = new NbdAsyncIoCallBack(channel, requestHeader, sender);

    IoRequest ioRequest = new IoRequestImpl(offset, len, IoRequestType.Discard, body,
        nbdAsyncIoCallBack, 1);
    ioRequest.getTicket(ioDepth);
    ioRequest.setVolumeId(volumeId);
    Long requestUuid = RequestIdBuilder.get();
    ioConvertor.processDiscardRequest(requestUuid, ioRequest, true);
    return ((MyIoSeparator) ioConvertor.getIoSeparator()).getResult();
  }

  public List<IoUnitContext> generateWriteIoContexts(long volumeId, long offset, int len)
      throws Exception {
    return generateWriteIoContexts(volumeId, offset, len, new byte[len]);
  }


  /**
   * xx.
   */
  public List<IoUnitContext> generateWriteIoContexts(long volumeId, long offset, int len,
      byte[] data)
      throws Exception {
    Validate.isTrue(data.length == len);
    RequestHeader requestHeader = new PydNormalRequestHeader(NbdRequestType.Write, 0, offset, len,
        0);
    ByteBuf body = Unpooled.wrappedBuffer(data);
    Request request = new Request(requestHeader, body);
    NbdAsyncIoCallBack nbdAsyncIoCallBack = new NbdAsyncIoCallBack(channel, requestHeader, sender);

    IoRequest ioRequest = new IoRequestImpl(offset, len, IoRequestType.Write, body,
        nbdAsyncIoCallBack, 1);
    ioRequest.getTicket(ioDepth);
    ioRequest.setVolumeId(volumeId);
    Long requestUuid = RequestIdBuilder.get();
    ioConvertor.processWriteRequest(requestUuid, ioRequest, true);
    return ((MyIoSeparator) ioConvertor.getIoSeparator()).getResult();
  }


  /**
   * xx.
   */
  public List<IoUnitContext> generateWriteIoContexts(long volumeId, ByteBuf request)
      throws Exception {

    RequestHeader requestHeader = new PydNormalRequestHeader(NbdRequestType.Write, 0, 0,
        request.readableBytes(),
        0);
    Request oriRequest = new Request(requestHeader, request);
    NbdAsyncIoCallBack nbdAsyncIoCallBack = new NbdAsyncIoCallBack(channel, requestHeader, sender);

    IoRequest ioRequest = new IoRequestImpl(0, request.readableBytes(), IoRequestType.Write,
        request,
        nbdAsyncIoCallBack, 1);
    ioRequest.getTicket(ioDepth);
    ioRequest.setVolumeId(volumeId);
    Long requestUuid = RequestIdBuilder.get();
    ioConvertor.processWriteRequest(requestUuid, ioRequest, true);
    return ((MyIoSeparator) ioConvertor.getIoSeparator()).getResult();
  }


  /**
   * xx.
   */
  public List<IoUnitContext> generateWriteIoContexts(long volumeId, Request request)
      throws Exception {

    NbdAsyncIoCallBack nbdAsyncIoCallBack = new NbdAsyncIoCallBack(channel, request.getHeader(),
        sender);

    IoRequest ioRequest = new IoRequestImpl(0, request.getHeader().getLength(), IoRequestType.Write,
        request.getBody(), nbdAsyncIoCallBack, 1);
    ioRequest.getTicket(ioDepth);
    ioRequest.setVolumeId(volumeId);
    Long requestUuid = RequestIdBuilder.get();
    volumeInfoHolderManager.addVolumeInfoHolder(volumeId, volumeInfoHolder);
    PerformanceRecorder performanceRecorder = new PerformanceRecorder();
    PerformanceManager.getInstance().addPerformance(volumeId, performanceRecorder);
    ioConvertor.processWriteRequest(requestUuid, ioRequest, true);
    return ((MyIoSeparator) ioConvertor.getIoSeparator()).getResult();
  }

  public IoConvertorImpl getIoConvertor() {
    return ioConvertor;
  }

  public class MyIoSeparator extends IoSeparatorImpl {

    private List<IoUnitContext> result = new ArrayList<IoUnitContext>();

    public MyIoSeparator(int maxNetworkFrameSizeForRead, int maxNetworkFrameSizeForWrite,
        Coordinator coordinator) {
      super(maxNetworkFrameSizeForRead, maxNetworkFrameSizeForWrite, coordinator);
    }


    /**
     * xx.
     */
    public List<IoUnitContext> getResult() {
      List<IoUnitContext> tmp = result;
      result = new ArrayList<>();
      return tmp;
    }

    public void splitRead(List<IoUnitContext> contexts) {
      this.result.addAll(contexts);
    }

    public void splitWrite(List<IoUnitContext> contexts) {
      this.result.addAll(contexts);
    }

    public void processDiscard(List<IoUnitContext> contexts) {
      this.result.addAll(contexts);
    }

  }
}
