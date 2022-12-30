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

package py.coordinator;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.client.thrift.GenericThriftClientFactory;
import py.common.NamedThreadFactory;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.coordinator.volumeinfo.VolumeInfoRetriever;
import py.datanode.client.DataNodeServiceAsyncClientWrapper;
import py.datanode.client.DataNodeServiceAsyncClientWrapper.BroadcastResult;
import py.datanode.client.ResponseCollector;
import py.drivercontainer.exception.InitializationException;
import py.exception.GenericThriftClientFactoryException;
import py.exception.StorageException;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.membership.SegmentMembership;
import py.storage.Storage;
import py.thrift.datanode.service.BroadcastRequest;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.datanode.service.ReadRequest;
import py.thrift.datanode.service.ReadResponse;
import py.thrift.datanode.service.SegmentNotFoundExceptionThrift;
import py.thrift.datanode.service.WriteRequest;
import py.thrift.datanode.service.WriteResponse;
import py.thrift.share.NotPrimaryExceptionThrift;
import py.volume.VolumeMetadata;



@Deprecated
public class CoordinatorAsync extends Storage {

  private static final Logger logger = LoggerFactory.getLogger(CoordinatorAsync.class);
  private static Random random = new Random();
  private final InstanceStore instanceStore;
  private VolumeMetadata volumeMetaData;
 
 
  private InstanceId myInstanceId;
 
 

 
  private DataNodeServiceAsyncClientWrapper dataNodeAsyncClient = null;
  private GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory = null;
  private VolumeInfoRetriever volumeInfoRetriever;

  private boolean openFlag = false;
  private ExecutorService readExecutor;
  private ExecutorService writeExecutor;
  private CoordinatorConfigSingleton cfg;


  
  public CoordinatorAsync(InstanceStore instanceStore,
      GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory,
      GenericThriftClientFactory<DataNodeService.AsyncIface> dataNodeAsyncClientFactory) {
    super("Coordinator for volume " + random.nextLong());

    this.dataNodeSyncClientFactory = dataNodeSyncClientFactory;
    this.dataNodeAsyncClient = new DataNodeServiceAsyncClientWrapper(dataNodeAsyncClientFactory);
    this.readExecutor = Executors.newCachedThreadPool(new NamedThreadFactory("Read Segment"));
    this.writeExecutor = Executors.newCachedThreadPool(new NamedThreadFactory("Write Segment"));
    this.instanceStore = instanceStore;
   
   
    this.myInstanceId = new InstanceId(RequestIdBuilder.get());
  }

  
  public void init() throws Exception {
   
    int maxAttempts = 3;
    int retry = 0;
    while (true) {
      try {
        this.volumeMetaData = null;
        if (volumeMetaData == null) {
          throw new InitializationException("Can't retrieve volume metadata ");
        } else {
          break;
        }
      } catch (Exception e) {
        logger.warn("can't retrieve volume information , retry:{} again", retry + 1);
        if (retry == maxAttempts) {
          throw e;
        } else {
          logger.warn("sleep 10 seconds");
          try {
            Thread.sleep(3000);
          } catch (InterruptedException e1) {
            logger.warn("caught an exception", e1);
          }
          retry++;
          continue;
        }
      }
    }

    this.volumeMetaData.setSegmentSize(cfg.getSegmentSize());
  }

  @Override
  public void open() throws StorageException {
    if (!openFlag) {
      try {
        init();
        this.openFlag = true;
        super.open();
      } catch (Exception e) {
        this.openFlag = false;
        logger.error("caught an exception", e);
        throw new StorageException(e);
      }
    }
  }

  @Override
  public void close() throws StorageException {
    try {
     
      super.close();
    } catch (Exception e) {
      logger.error("something wrong when closing the coordinator", e);
      throw new StorageException(e);
    }
  }

  @Override
  public void read(long pos, byte[] dstBuf, int off, int len) throws StorageException {
    ByteBuffer byteBuffer = null;
    logger.debug("read request: pos is {} offset is {} len is {}", pos, off, len);
    try {
      byteBuffer = ByteBuffer.wrap(dstBuf, off, len);
    } catch (IndexOutOfBoundsException e) {
      logger.error("offset is {} len is {} buffer size is {}", off, len, dstBuf.length, e);
      throw new StorageException(e);
    }
    read(pos, byteBuffer);
  }

  @Override
  public void read(long pos, ByteBuffer buffer) throws StorageException {
    processWriteReadSegments(ProcessType.READ, pos, buffer);
  }

  @Override
  public long size() {
    return volumeMetaData.getVolumeSize();
  }

  @Override
  public void write(long pos, byte[] buf, int off, int len) throws StorageException {
    ByteBuffer byteBuffer = null;
    logger.debug("write request: pos is {} offset is {} len is {}", pos, off, len);
    try {
      byteBuffer = ByteBuffer.wrap(buf, off, len);
    } catch (IndexOutOfBoundsException e) {
      logger.error("offset is {} len is {} buffer size is {}", off, len, buf.length, e);
      throw new StorageException(e);
    }
    write(pos, byteBuffer);
  }

  @Override
  public void write(long pos, ByteBuffer buffer) throws StorageException {
    processWriteReadSegments(ProcessType.WRITE, pos, buffer);
  }

  protected void processWriteReadSegments(ProcessType processType, long pos, ByteBuffer buffer)
      throws StorageException {
    int totalLen = buffer.remaining();
    if (totalLen == 0) {
      logger.warn("the read/write request asks for 0 length data. Do nothing");
      return;
    }

    long volumeSize = size();
    if (pos < 0 || (pos + totalLen) > volumeSize) {
     
      try {
        init();
      } catch (Exception e) {
        logger.error("caught an exception", e);
      }
      long newVolumeSize = size();
      if (volumeSize == newVolumeSize) {
        throw new StorageException("the position is out of range from 0 to " + volumeSize);
      }
    }

    int startSegIndex = segmentIndex(pos);
    int endSegIndex = segmentIndex(pos + totalLen - 1);
    CountDownLatch latch = new CountDownLatch(endSegIndex - startSegIndex + 1);

    ResponseCollector<Integer, ReadResponse> readResponseCollector = null;
    ResponseCollector<Integer, WriteResponse> writeResponseCollector = null;
    if (processType.equals(ProcessType.READ)) {
      readResponseCollector = new ResponseCollector<Integer, ReadResponse>();
    } else {
      writeResponseCollector = new ResponseCollector<Integer, WriteResponse>();
    }

    long processingLen = 0;
    long remainingLen = totalLen;
    while (remainingLen > 0) {
      int currentSegIndex = segmentIndex(pos);
      processingLen = (currentSegIndex + 1) * cfg.getSegmentSize() - pos;
      if (processingLen > remainingLen) {
       
        processingLen = remainingLen;
      }

      logger.debug("processing request data. Segment Index: {} Pos is {} length to read/write: {}",
          currentSegIndex, pos, processingLen);

      ByteBuffer tempBuffer = buffer.slice();
      tempBuffer.limit((int) processingLen);
      if (processType.equals(ProcessType.WRITE)) {
        WriteWorker worker = new WriteWorker(latch, writeResponseCollector);
        worker.setBuffer(tempBuffer);
        worker.setPos(pos);
        writeExecutor.execute(worker);
      } else {
        ReadWorker worker = new ReadWorker(latch, readResponseCollector);
        worker.setPos(pos);
        worker.setBuffer(tempBuffer);
        readExecutor.execute(worker);
      }
      pos += processingLen;
      remainingLen -= processingLen;
      int currentBufferPos = buffer.position();
      buffer.position(currentBufferPos + (int) processingLen);
    }

    try {
      latch.await();
    } catch (InterruptedException e) {
      logger.warn("Interrupted while waiting for read/writer work to complete execution", e);
    }

    String errString = null;
    Collection<Throwable> ts = null;
    if (processType.equals(ProcessType.WRITE)
        && (writeResponseCollector.getGoodResponses() == null
        || writeResponseCollector.getGoodResponses()
        .size() != (endSegIndex - startSegIndex + 1))) {
      errString = "Can't get a good response from writing to data node";
      ts = writeResponseCollector.getServerSideThrowables().values();
    }

    if (processType.equals(ProcessType.READ)
        && (readResponseCollector.getGoodResponses() == null
        || readResponseCollector.getGoodResponses().size() != (endSegIndex
        - startSegIndex + 1))) {
      errString = "Can't get a good response for read";
      ts = readResponseCollector.getServerSideThrowables().values();
    }

    if (errString != null) {
      logger.error(errString);
      if (ts != null) {
        for (Throwable t : ts) {
          logger.error("Got throwable : ", t);
        }
      }
      throw new StorageException(errString);
    } else {
      logger.debug("got good response");
    }
  }

  private int segmentIndex(long pos) {
    return (int) (pos / cfg.getSegmentSize());
  }

  private int pageIndex(long pos) {
    return (int) (pos / cfg.getPageSize());
  }

  private DataNodeService.Iface getClientToPrimary(SegmentMembership membership)
      throws GenericThriftClientFactoryException {

    Instance primary = instanceStore.get(membership.getPrimary());
    if (primary == null) {
      logger.warn("Can't get primary {} from instance store", membership.getPrimary());
      return null;
    }

    logger.debug("data node end point is to send the request {}", primary.getEndPoint());
    System.out.println("###########getClientToPrimary" + cfg.getThriftRequestTimeoutMs());
    return dataNodeSyncClientFactory
        .generateSyncClient(primary.getEndPointByServiceName(PortType.CONTROL),
            cfg.getThriftRequestTimeoutMs());
  }

  private SegmentMembership broadcastToMembersToGetLatestMembership(long requestId, long volumeId,
      int segIndex,
      SegmentMembership membership) {

   
    List<EndPoint> endpoints = RequestResponseHelper
        .buildEndPoints(instanceStore, membership, false,
            membership.getPrimary());
   
   
    int quorumSize = 1;
    try {
      BroadcastRequest request = RequestResponseHelper
          .buildGiveMeYourMembershipRequest(requestId, new SegId(
              volumeId, segIndex), membership, myInstanceId.getId());
      
      BroadcastResult response = dataNodeAsyncClient
          .broadcast(request, cfg.getThriftRequestTimeoutMs(), quorumSize,
              false, endpoints);
      logger.info("We have received a quorum to get membership");

      return response.getHighestMembership();
    } catch (Exception e) {
      logger.warn("caught an exception when broadcast a GiveMeYourMembership", e);
      return null;
    }
  }

  public InstanceId getMyInstanceId() {
    return myInstanceId;
  }

  public void setMyInstanceId(InstanceId myInstanceId) {
    this.myInstanceId = myInstanceId;
  }

  public VolumeInfoRetriever getVolumeInfoRetriever() {
    return volumeInfoRetriever;
  }

  public void setVolumeInfoRetriever(VolumeInfoRetriever volumeInfoRetriever) {
    this.volumeInfoRetriever = volumeInfoRetriever;
  }

 
  protected void setAsyncClient(DataNodeServiceAsyncClientWrapper asyncClient) {
    this.dataNodeAsyncClient = asyncClient;
  }


  
  public int getBackoffSleepTime(int sleepTimeUnit, int failureTimes, int maxBackoffTime) {
    long exponentialBackoff = (long) sleepTimeUnit * (long) (2 << (failureTimes - 1));
    if (exponentialBackoff < 0 || exponentialBackoff > maxBackoffTime) {
     
      return maxBackoffTime;
    } else {
      return (int) exponentialBackoff;
    }
  }


  
  public void retrieveVolumeMetadata() throws StorageException {
    try {
      init();
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw new StorageException(e);
    }
  }

  private enum ProcessType {
    WRITE, READ;
  }

  
  private class ReadWorker implements Runnable {

    private ResponseCollector<Integer, ReadResponse> readResponseCollector;

    private ByteBuffer buffer;

    private long pos;

    private CountDownLatch latch;
    private AtomicInteger key = new AtomicInteger(0);

    public ReadWorker(CountDownLatch latch,
        ResponseCollector<Integer, ReadResponse> readResponseCollector) {
      this.latch = latch;
      this.readResponseCollector = readResponseCollector;
    }

    public void setBuffer(ByteBuffer buffer) {
      this.buffer = buffer;
    }

    public void setPos(long pos) {
      this.pos = pos;
    }

    @Override
    public void run() {
      try {
        final int len = buffer.remaining();
        int segIndex = segmentIndex(pos);

        SegmentMembership membership = volumeMetaData.getMembership(segIndex);
        if (membership == null) {
          String errString = "Can't get segment membership for the " + segIndex
              + " segment. Let's get the latest volume metadata ";
          logger.warn(errString);
          retrieveVolumeMetadata();

          membership = volumeMetaData.getMembership(segIndex);
          if (membership == null) {
            errString =
                "After getting the latest volume metadata, still can't get segment membership "
                    + "for the " + segIndex + " segment. Give up";
            logger.error(errString);
           
           
            readResponseCollector
                .addServerSideThrowable(key.incrementAndGet(), new StorageException(errString));
            return;
          }
        }

        long beginningTime = System.currentTimeMillis();
        int failureTimes = 0;
        int sleepTimeUnit = 250;
        SegmentMembership latestMembershipFromResponses = null;
        ReadResponse response = null;
        String errString = null;
        Exception exception = null;
        long requestId = RequestIdBuilder.get();
        do {
          if (membership.compareTo(latestMembershipFromResponses) < 0) {
            logger.info("Get a newer membership: {}", latestMembershipFromResponses);
            membership = latestMembershipFromResponses;
           
            volumeMetaData.updateMembership(segIndex, latestMembershipFromResponses);
           
           
           
           
            failureTimes = 0;
          } else if (failureTimes > 0) {
           
           
            long sleepTime = 1000;
            logger.info("request {} has been {} failures to read. Let's sleep {} ms", requestId,
                failureTimes, sleepTime);
            try {
              Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
              logger.warn("Caught an exception while invoking sleep. It is fine to wake up", e);
            }
          }

          long posWithinSegment = pos - segIndex * cfg.getSegmentSize();

          SegmentMetadata absoluteSegment = volumeMetaData.getSegmentByIndex(segIndex);
          if (absoluteSegment == null) {
            logger.error(
                "Logic segment index {} is out of volume range:{} let's retrieve the volume "
                    + "metadata again",
                segIndex, volumeMetaData);
            retrieveVolumeMetadata();
            continue;
          }

         
          ReadRequest readRequest = null;
          
          try {
            logger.debug("sending a readRequest {}", readRequest);
            DataNodeService.Iface client = getClientToPrimary(membership);
           
           
            response = client.readData(readRequest);
            logger.debug("Got a good read response {}", response);
          } catch (GenericThriftClientFactoryException e) {
            errString = readRequest + "can't connect to the primary";
            exception = e;
            continue;
          } catch (SegmentNotFoundExceptionThrift | NotPrimaryExceptionThrift e) {
            errString = readRequest + "The primary within (" + membership
                + ") either doesn't have the segment or not the primary anymore. "
                + "ask other group members for the latest membership";
            exception = e;
            continue;
          } catch (Exception e) {
            errString = readRequest + "Something wrong with reading data from DN";
            exception = e;
            continue;
          } finally {
            if (errString != null) {
              logger.error(errString, exception);
             
             
              latestMembershipFromResponses = broadcastToMembersToGetLatestMembership(requestId,
                  volumeMetaData.getVolumeId(), segIndex, membership);
              failureTimes++;
            }
          }
        } while (response == null
            && System.currentTimeMillis() - beginningTime < cfg.getReadIoTimeoutMs());

        if (response == null) {
          errString = requestId + " Reading data timed out";
          logger.error(errString);
          readResponseCollector
              .addServerSideThrowable(key.incrementAndGet(), new StorageException(errString));
          return;
        }

       
       
        byte[] data = null;
        if (data.length != len) {
         
          errString = "No enough data is read. Ask for " + len + ", read " + data.length;
        } 

        if (errString != null) {
          logger.error(errString);
          readResponseCollector
              .addServerSideThrowable(key.incrementAndGet(), new StorageException(errString));
        }

        if (response.getMembership() != null) {
          latestMembershipFromResponses = RequestResponseHelper.buildSegmentMembershipFrom(
              response.getMembership()).getSecond();
          if (membership.compareTo(latestMembershipFromResponses) < 0) {
            volumeMetaData.updateMembership(segIndex, latestMembershipFromResponses);
          }
        }
       
        buffer.put(data);
        this.readResponseCollector.addGoodResponse(key.incrementAndGet(), response);
      } catch (Throwable t) {
        logger.error("caught a throwable when read the data", t);
      } finally {
        this.latch.countDown();
      }
    }
  }

  
  private class WriteWorker implements Runnable {

    private ResponseCollector<Integer, WriteResponse> writeResponseCollector;

    private ByteBuffer buffer;

    private long posInVolume;

    private CountDownLatch latch;
    private AtomicInteger key = new AtomicInteger();

    public WriteWorker(CountDownLatch latch,
        ResponseCollector<Integer, WriteResponse> writeResponseCollector) {
      this.writeResponseCollector = writeResponseCollector;
      this.latch = latch;
    }

    public void setBuffer(ByteBuffer buffer) {
      this.buffer = buffer;
    }

    public void setPos(long pos) {
      this.posInVolume = pos;
    }

    @Override
    public void run() {
      try {
        int currentSegIndex = segmentIndex(posInVolume);

        SegmentMembership membership = volumeMetaData.getMembership(currentSegIndex);
        if (membership == null) {
          String errString = "Can't get segment membership for the " + currentSegIndex
              + " segment. Let's get the latest volume metadata ";
          logger.warn(errString);
          retrieveVolumeMetadata();

          membership = volumeMetaData.getMembership(currentSegIndex);
          if (membership == null) {
            errString =
                "After getting the latest volume metadata, still can't get segment membership "
                    + "for the " + currentSegIndex + " segment. Give up";
            logger.error(errString);
            writeResponseCollector
                .addServerSideThrowable(key.incrementAndGet(), new StorageException(errString));
            return;
          }
        }

        long processingLen = 0;
        long remainingLen = buffer.remaining();
        long posWithinSegment = posInVolume - currentSegIndex * cfg.getSegmentSize();
        Validate.isTrue(
            posWithinSegment + remainingLen <= (currentSegIndex + 1) * cfg.getSegmentSize());

        SegmentMembership latestMembershipFromResponses = null;
        WriteResponse response = null;
        long requestId = RequestIdBuilder.get();
        while (remainingLen > 0) {
          long currentPageIndex = pageIndex(posWithinSegment);
          processingLen = (currentPageIndex + 1) * cfg.getPageSize() - posWithinSegment;
          if (processingLen > remainingLen) {
           
            processingLen = remainingLen;
          }
          Validate.isTrue(processingLen > 0);

          logger.debug(
              "processing request data. Page Index : {} in the segment {} Pos is {} length to "
                  + "read/write: {}",
              currentPageIndex, currentSegIndex, posWithinSegment, processingLen);

          ByteBuffer processingBuffer = buffer.slice();
          processingBuffer.limit((int) processingLen);

          processingBuffer.mark();
          processingBuffer.reset();
          SegmentMetadata absoluteSegment = volumeMetaData.getSegmentByIndex(currentSegIndex);
          if (absoluteSegment == null) {
            logger.error(
                "Logic segment index {} is out of volume range:{} let's retrieve the volume "
                    + "metadata again",
                currentSegIndex, volumeMetaData);
            retrieveVolumeMetadata();
            continue;
          }

          WriteRequest writeRequest = null;

          int failureTimes = 0;
          int sleepTimeUnit = 250;
          long beginningTime = System.currentTimeMillis();
          do {
            if (membership.compareTo(latestMembershipFromResponses) < 0) {
              logger.debug(
                  "the current membership {} is less than the latest membereship from responses {}",
                  membership, latestMembershipFromResponses);
              membership = latestMembershipFromResponses;
             
              volumeMetaData.updateMembership(currentSegIndex, latestMembershipFromResponses);
             
             
             
             
             
             
              failureTimes = 0;
            } else if (failureTimes > 0) {
              long sleepTime = 1000;
              logger.info("It has been {} failures to read. Let's sleep {} ms", failureTimes,
                  sleepTime);
              try {
                Thread.sleep(sleepTime);
              } catch (InterruptedException e) {
                logger.warn("Caught an exception while invoking sleep. It is fine to wake up", e);
              }
            }

            String errString = null;
            Exception exception = null;
            try {
              logger.debug("sending a write request {}", writeRequest);
              DataNodeService.Iface client = getClientToPrimary(membership);
              response = client.writeData(writeRequest);
              logger.debug("Got a good write response{}", response);
            } catch (GenericThriftClientFactoryException e) {
              errString = writeRequest + "can't connect to the primary";
              exception = e;
              continue;
            } catch (SegmentNotFoundExceptionThrift | NotPrimaryExceptionThrift e) {
              errString = writeRequest + "The primary within (" + membership
                  + ") either doesn't have the segment or not the primary anymore. "
                  + "ask other group members for the latest membership";
              exception = e;
              continue;
            } catch (Exception e) {
              errString = writeRequest + "Something wrong with writing data to DN";
              exception = e;
              continue;
            } finally {
              if (errString != null) {
                logger.error(errString, exception);
               
               
                latestMembershipFromResponses = broadcastToMembersToGetLatestMembership(requestId,
                    volumeMetaData.getVolumeId(), currentSegIndex, membership);
                failureTimes++;
              }
            }
          } while (response == null
              && System.currentTimeMillis() - beginningTime < cfg.getWriteIoTimeoutMs());
          if (response == null) {
            String errString = writeRequest + "Writing data at page " + currentPageIndex
                + " got timed out. Request:" + writeRequest;
            logger.error(errString);
            writeResponseCollector
                .addServerSideThrowable(key.incrementAndGet(), new StorageException(errString));
            return;
          }
          remainingLen -= processingLen;
          posWithinSegment += processingLen;
          int currentBufferPos = buffer.position();
          buffer.position(currentBufferPos + (int) processingLen);
        }

        if (response.getMembership() != null) {
          latestMembershipFromResponses = RequestResponseHelper.buildSegmentMembershipFrom(
              response.getMembership()).getSecond();
          if (membership.compareTo(latestMembershipFromResponses) < 0) {
            volumeMetaData.updateMembership(currentSegIndex, latestMembershipFromResponses);
          }
        }
        writeResponseCollector.addGoodResponse(key.incrementAndGet(), response);
      } catch (Throwable t) {
        logger.error("Caught a throwable while sending write request", t);
      } finally {
        this.latch.countDown();
      }
    }
  }
}
