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

package py.integ.coordinator;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static py.common.Constants.SUPERADMIN_ACCOUNT_ID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.commons.lang.Validate;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.client.thrift.GenericThriftClientFactory;
import py.common.PyService;
import py.common.struct.EndPoint;
import py.common.struct.EndPointParser;
import py.coordinator.CoordinatorBuilder;
import py.coordinator.base.CoordinatorBaseTest;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.coordinator.configuration.NbdConfiguration;
import py.coordinator.lib.Coordinator;
import py.coordinator.lib.StorageDriver;
import py.coordinator.nbd.request.Request;
import py.coordinator.volumeinfo.SpaceSavingVolumeMetadata;
import py.coordinator.worker.ExtendingVolumeUpdater;
import py.dih.client.DihClientFactory;
import py.dih.client.DihInstanceStore;
import py.drivercontainer.driver.LaunchDriverParameters;
import py.exception.StorageException;
import py.infocenter.client.InformationCenterClientFactory;
import py.infocenter.client.InformationCenterClientWrapper;
import py.infocenter.client.VolumeMetadataAndDrivers;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.membership.SegmentMembership;
import py.test.TestBase;
import py.test.TestUtils;
import py.thrift.datanode.service.DataNodeService;
import py.volume.VolumeMetadata;

public class CoordinatorReadWriteTest extends TestBase {

  protected final long smallLength = 512;
  protected StorageDriver storage;
  protected NbdConfiguration nbdConfiguration;
  protected CoordinatorConfigSingleton cfg;
  protected long volumeSize = 1 * 1024 * 1024 * 1024L; // 1Gb
  private int localDihPort = 10000;
  private LaunchDriverParameters launchDriverParameters = mock(LaunchDriverParameters.class);
  private long accountId = SUPERADMIN_ACCOUNT_ID;
  private long segmentSize = 1 * 1024 * 1024 * 1024L;  // 1Gb
  //    private long segmentSize = 8 * 1024 * 1024L;  // 1Gb
  private int pageSize = 8 * 1024;  //8Kb

  /**
   * modify fields.
   */
  // fill with one of dih host name

  private String hostName = "10.0.0.80";
  private long volumeId = 3098469543225837556L;


  /**
   * xx.
   */
  public CoordinatorReadWriteTest() {
    cfg = CoordinatorConfigSingleton.getInstance();
    cfg.setMaxWriteDataSizePerRequest(16 * 1024 * 1024);
    cfg.setMaxReadDataSizePerRequest(16 * 1024 * 1024);
    cfg.setSegmentSize(segmentSize);
    cfg.setPageSize(pageSize);
    logger
        .info("the segment size is {} the pageSize is {}", cfg.getSegmentSize(), cfg.getPageSize());
  }

  protected static void writeAddressData(StorageDriver coordinator, long pos, int length)
      throws StorageException {
    byte[] src = TestUtils.generateDataAssociatedWithAddress(pos, length);
    long startTime = System.currentTimeMillis();
    coordinator.write(pos, src, 0, length);
    long endTime = System.currentTimeMillis();
    long requestTime = endTime - startTime;
    if (requestTime > 1000) {
      // logger.debug("It took " + requestTime + " to process the write request");
    }
  }

  protected static long readDataWithRequestListDirectly(StorageDriver coordinator,
      List<Request> readRequestList)
      throws StorageException {
    long readSize = 0;
    for (Request request : readRequestList) {
      readSize += request.getHeader().getLength();
    }

    for (Request request : readRequestList) {
      byte[] dst = new byte[(int) request.getHeader().getLength()];
      long pos = request.getHeader().getOffset();
      coordinator.read(pos, dst, 0, (int) request.getHeader().getLength());
      TestUtils.checkDataAssociatedWithAddress(pos, dst, (int) request.getHeader().getLength());
    }
    return readSize;
  }

  protected static void verifyAddressData(StorageDriver coordinator, long pos, int len, byte[] dst)
      throws StorageException {
    long startTime = System.currentTimeMillis();
    coordinator.read(pos, dst, 0, len);
    long endTime = System.currentTimeMillis();
    long requestTime = endTime - startTime;
    if (requestTime > 1000) {
      // logger.debug("It took " + requestTime + " to process the read request");
    }

    TestUtils.checkDataAssociatedWithAddress(pos, dst, len);
  }

  public EndPoint localDihEp() {
    return EndPointParser.parseLocalEndPoint(localDihPort, hostName);
  }

  public DihClientFactory dihClientFactory() {
    DihClientFactory dihClientFactory = new DihClientFactory(1);
    return dihClientFactory;
  }


  /**
   * xx.
   */
  public InstanceStore instanceStore() throws Exception {
    Object instanceStore = DihInstanceStore.getSingleton();
    ((DihInstanceStore) instanceStore).setDihClientFactory(dihClientFactory());
    ((DihInstanceStore) instanceStore).setDihEndPoint(localDihEp());
    ((DihInstanceStore) instanceStore).init();
    return (InstanceStore) instanceStore;
  }


  /**
   * xx.
   */
  public InformationCenterClientFactory informationCenterClientFactory() throws Exception {
    InformationCenterClientFactory factory = new InformationCenterClientFactory(1);
    factory.setInstanceName(PyService.INFOCENTER.getServiceName());
    factory.setInstanceStore(instanceStore());
    return factory;
  }


  /**
   * xx.
   */
  public GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory() {
    return GenericThriftClientFactory.create(DataNodeService.Iface.class)
        .withMaxChannelPendingSizeMb(
            CoordinatorConfigSingleton.getInstance().getMaxChannelPendingSize());
  }


  /**
   * xx.
   */
  public GenericThriftClientFactory<DataNodeService.AsyncIface> dataNodeAsyncClientFactory() {
    return GenericThriftClientFactory.create(DataNodeService.AsyncIface.class)
        .withMaxChannelPendingSizeMb(
            CoordinatorConfigSingleton.getInstance().getMaxChannelPendingSize());
  }

  @Override
  @Before
  public void init() throws Exception {
    super.init();
    CoordinatorBuilder builder = new CoordinatorBuilder();

    builder.setInformationCenterClientFactory(informationCenterClientFactory());
    builder.setDataNodeAsyncClientFactory(dataNodeAsyncClientFactory());
    builder.setDataNodeSyncClientFactory(dataNodeSyncClientFactory());
    builder.setInstanceStore(instanceStore());

    ExtendingVolumeUpdater extendingVolumeUpdater = new ExtendingVolumeUpdater(
        informationCenterClientFactory(), launchDriverParameters);
    extendingVolumeUpdater.start();
    builder.setExtendingVolumeUpdater(extendingVolumeUpdater);

    builder.setAccountId(accountId);
    builder.setVolumeId(volumeId);
    builder.setSnapshotId(0);

    CoordinatorConfigSingleton.getInstance().setSegmentSize(segmentSize);
    CoordinatorConfigSingleton.getInstance().setPageSize(pageSize);

    Coordinator coordinator = builder.build();
    // segment id and account id are fixed and test specific.
    nbdConfiguration = new NbdConfiguration();
    Thread.sleep(1000);
    coordinator.open(volumeId, 0);
    SpaceSavingVolumeMetadata volumeMetadata = coordinator.getVolumeMetaData(volumeId);
    if (volumeMetadata != null) {
      volumeSize = volumeMetadata.getVolumeSize();
    }
    logger.info("volume size is {} ", volumeSize);
    // configures the root logger. Please don't move the log configuration to the beginning
    // of this function, because there is another log configuration within Coordinator by spring
    // initialization
    Logger rootLogger = Logger.getRootLogger();
    // rootLogger.setLevel(Level.DEBUG);
    rootLogger.setLevel(Level.INFO);

    storage = new CoordinatorBaseTest(nbdConfiguration, coordinator);
  }

  @Test
  public void testGetVolumeByPagination() {
    try {
      InformationCenterClientWrapper client = informationCenterClientFactory().build(6000);
      logger.warn("try to get info center client in time: {}");
      VolumeMetadata volumeMetadataGetVolume = client.getVolume(volumeId, accountId);

      VolumeMetadataAndDrivers volumeMetadataAndDrivers = client
          .getVolumeByPagination(volumeId, accountId);
      VolumeMetadata volumeMetadataGetVolumeByPagination = volumeMetadataAndDrivers
          .getVolumeMetadata();

      /* the creatingBeginTime is different **/

      //check the driver

      for (int i = 0; i < 128; i++) {
        SegmentMetadata beforeSegment = volumeMetadataGetVolume.getSegmentByIndex(i);
        SegmentMetadata afterSegment = volumeMetadataGetVolumeByPagination.getSegmentByIndex(i);
        compareTwoSegment(beforeSegment, afterSegment);
      }

    } catch (Exception e) {
      logger.error("catch an exception when get volume: {}, accountId: {}", volumeId, accountId, e);
    }
  }

  @Test
  public void testWriteData1() throws StorageException, InterruptedException {
    long startPosition = 0;
    long stepLength = 1024 * 1024; // 1M
    byte[] writeData = new byte[(int) stepLength];

    long volumeSize = storage.size();
    while (startPosition < volumeSize) {
      long writeLength;
      if (volumeSize - startPosition >= stepLength) {
        writeLength = stepLength;
      } else {
        writeLength = volumeSize - startPosition;
      }
      try {
        storage.write(startPosition, writeData, 0, (int) writeLength);
      } catch (Throwable t) {
        logger.error("caught an exception", t);
        fail();
      }
      startPosition += writeLength;
    }
  }

  @Test
  public void testReadData1() throws StorageException, InterruptedException {
    long startPosition = 0;
    long stepLength = 1024 * 1024; // 1M
    byte[] writeData = new byte[(int) stepLength];

    long volumeSize = storage.size();
    while (startPosition < volumeSize) {
      long writeLength;
      if (volumeSize - startPosition >= stepLength) {
        writeLength = stepLength;
      } else {
        writeLength = volumeSize - startPosition;
      }
      try {
        storage.read(startPosition, writeData, 0, (int) writeLength);
      } catch (Throwable t) {
        logger.error("caught an exception", t);
        fail();
      }
      startPosition += writeLength;
    }
  }

  @Test
  public void testWriteAndReadData() throws StorageException, InterruptedException {
    long offset = 0;
    int length = 128 * 1024;
    // first we write some data to datanode
    byte[] src = TestUtils.generateDataAssociatedWithAddress(offset, length);
    storage.write(offset, src, 0, length);
    logger.warn("1, write data:{}", src);

    Thread.sleep(200);
    // second write request
    byte[] secSrc = TestUtils.generateDataAssociatedWithAddress(offset + length, length);
    storage.write(offset + length, secSrc, 0, length);
    logger.warn("2, write data:{}", secSrc);

    // then we read from datanode at the same position, and the same length, check data is same
    // or not
    byte[] readData1 = new byte[length];
    storage.read(offset, readData1, 0, length);
    logger.warn("1, read data:{}", readData1);
    assertTrue(Arrays.equals(src, readData1));

    byte[] readData2 = new byte[length];
    storage.read(offset + length, readData2, 0, length);
    logger.warn("2, read data:{}", readData2);
    assertTrue(Arrays.equals(secSrc, readData2));
  }

  @Test
  public void testWriteData() throws StorageException, InterruptedException {
    long offset = 0;
    int length = 1024;
    // first we write some data to datanode

    byte[] src = TestUtils.generateDataAssociatedWithAddress(offset, length);
    storage.write(offset, src, 0, length);
    logger.warn("1, write data:{}", src);
  }

  @Test
  public void testReadData() throws StorageException, InterruptedException {
    long offset = 0;
    int length = 1024;
    // then we read from datanode at the same position, and the same length, check data is same?
    byte[] readData1 = new byte[length];
    storage.read(offset, readData1, 0, length);
    logger.warn("1, read data:{}", readData1);
  }

  @Test
  public void testCrossWrite() throws StorageException, InterruptedException {
    long offset = 0;
    int length = 1024;
    // first we write some data to datanode

    byte[] src1 = TestUtils.generateDataAssociatedWithAddress(offset, length);
    storage.write(offset, src1, 0, length);
    logger.warn("write data1:{}", src1);

    offset = 16 * 1024 * 1024 - 512;
    length = 1024;

    byte[] src2 = TestUtils.generateDataAssociatedWithAddress(offset, length);

    storage.write(offset, src2, 0, length);
    logger.warn("write data2:{}", src2);
  }

  @Test
  public void testCrossRead() throws StorageException, InterruptedException {
    long offset = 0;
    int length = 1024;
    // first we write some data to datanode
    byte[] readData1 = new byte[length];
    storage.read(offset, readData1, 0, length);

    offset = 16 * 1024 * 1024 - 512;
    length = 1024;
    byte[] readData2 = new byte[length];
    storage.read(offset, readData2, 0, length);
  }

  @Test
  public void testCrossWrite1M() throws StorageException, InterruptedException {
    long offset = 0;
    int length = 1024 * 1024;
    // first we write some data to datanode
    byte[] src1 = TestUtils.generateDataAssociatedWithAddress(offset, length);
    storage.write(offset, src1, 0, length);
    logger.warn("write data1:{}", src1);

    offset = 16 * 1024 * 1024 - 512;
    length = 1024 * 1024;
    byte[] src2 = TestUtils.generateDataAssociatedWithAddress(offset, length);

    storage.write(offset, src2, 0, length);
    logger.warn("write data2:{}", src2);
  }

  @Test
  public void testCrossRead1M() throws StorageException, InterruptedException {
    long offset = 0;
    int length = 1024 * 1024;
    // first we write some data to datanode
    byte[] readData1 = new byte[length];
    storage.read(offset, readData1, 0, length);

    offset = 16 * 1024 * 1024 - 512;
    length = 1024 * 1024;
    byte[] readData2 = new byte[length];
    storage.read(offset, readData2, 0, length);
  }

  @Test
  public void testWriteManyData() throws StorageException, InterruptedException {
    long offset = 0;
    int length = 1024;
    int count = 500;
    for (int i = 0; i < count; i++) {
      // first we write some data to datanode
      byte[] src = TestUtils.generateDataAssociatedWithAddress(offset, length);
      storage.write(offset, src, 0, length);
      logger.warn("[{}] write data:{}", i, src);
      offset += 1024L;
    }
  }

  @Test
  public void testReadManyData() throws StorageException, InterruptedException {
    long offset = 0;
    int length = 1024;
    int count = 500;
    for (int i = 0; i < count; i++) {
      // first we write some data to datanode
      byte[] readData = new byte[length];
      storage.read(offset, readData, 0, length);
      logger.warn("[{}] read data:{}", i, readData);
      offset += 1024L;
    }
  }

  // end for new coordinator test
  @Test
  public void testReadWritePageRandom() throws StorageException {
    int size = cfg.getPageSize();
    byte[] data = TestUtils.generateRandomData(size);
    storage.write(0, data, 0, size);
    byte[] dst = new byte[size];
    storage.read(0, dst, 0, size);

    assertArrayEquals(data, dst);
  }

  /**
   * just for the small write length,the length is 512.
   */
  @Test
  public void justTestReadWriteSmallLength() throws StorageException {
    byte[] src = TestUtils.generateDataAssociatedWithAddress(0, 512);
    storage.write(0, src, 0, 512);
  }

  @Test
  public void testReadWritePage() throws StorageException {
    long size = cfg.getPageSize();
    testReadWrite(size);
  }

  @Test
  public void testReadWriteMultiplePages() throws StorageException {
    long size = cfg.getPageSize() * 2000;
    if (size > volumeSize) {
      // if the size is larger than volumeSize, then read/write all data in volume.
      // the reason to substract a page from volumeSize is readWrite() function might
      // try to access the offset right starting from volumeSize
      size = volumeSize - cfg.getPageSize();
      Validate.isTrue(size > 0);
    }

    testReadWrite(size);
  }

  @Test
  public void testReadWriteSegment() throws StorageException {
    long size = cfg.getSegmentSize();
    testReadWrite(size);
  }

  @Test
  public void testReadWriteVolume() throws StorageException {
    // the same reason as above why we substract a page from the size
    long size = volumeSize - cfg.getPageSize();
    testReadWrite(size);
  }

  @Test
  public void testGenerateData() {
    long offset = 0;
    long segmentSize = cfg.getSegmentSize();
    int pageSize = cfg.getPageSize();
    for (int i = 0; i < segmentSize / pageSize; i++) {
      byte[] src = TestUtils.generateDataAssociatedWithAddress(offset, pageSize);
      TestUtils.checkDataAssociatedWithAddress(offset, src, src.length);
      offset += pageSize;
    }
  }

  protected void testReadWrite(long length) throws StorageException {
    // write first
    Random random = new Random(System.currentTimeMillis());
    long tempSmallLen = 0;
    int pageSize = cfg.getPageSize();
    final long writeTimeBegin = System.currentTimeMillis();
    long offset = 0;
    assertTrue(" length has to be multiple times of PAGE SIZE", length % pageSize == 0);
    logger.warn("begin length is :{}", length);
    while (offset < length) {
      int smallLen = random.nextInt(pageSize * 10) + (int) smallLength;
      logger.warn("begin smallLen is:{}", smallLen);
      smallLen -= smallLen % smallLength;

      logger.warn("begin smallLen % SMALL_LENGTH is:{},total is {}", smallLen, offset + smallLen);
      if (offset + smallLen > length) {
        tempSmallLen = (int) (length - offset);
        logger.warn("begin in the smalllen is:{}", smallLen);
        if (tempSmallLen % smallLength == 0) {
          smallLen = (int) tempSmallLen;
        }
      }

      logger.warn("offset is :{}  smallLen is :{}", offset, smallLen);
      writeAddressData(storage, offset, smallLen);
      offset += smallLen;
    }
    long writeTimeEnd = System.currentTimeMillis();
    offset = 0;

    // read
    byte[] dst = new byte[pageSize * 10];
    while (offset < length) {
      int smallLen = random.nextInt(pageSize * 10) + (int) smallLength;
      smallLen -= smallLen % smallLength;

      if (offset + smallLen > length) {
        tempSmallLen = (int) (length - offset);
        if (tempSmallLen % smallLength == 0) {
          smallLen = (int) tempSmallLen;
        }
      }
      verifyAddressData(storage, offset, smallLen, dst);
      offset += smallLen;
    }
    logger
        .debug("Write " + length / (1024 * 1024) + " MB. Time: " + (writeTimeEnd - writeTimeBegin));
    logger.debug("Read " + length / (1024 * 1024) + " MB. Time: " + (System.currentTimeMillis()
        - writeTimeEnd));
  }

  /* the old function,because the small write length is 512,**/
  @Deprecated
  protected void testReadWriteOld(long length) throws StorageException {
    // write first
    Random random = new Random(System.currentTimeMillis());
    int pageSize = cfg.getPageSize();
    final long writeTimeBegin = System.currentTimeMillis();
    long offset = 0;
    assertTrue(" length has to be multiple times of PAGE SIZE", length % pageSize == 0);
    while (offset < length) {
      int smallLen = random.nextInt(pageSize * 10);
      smallLen -= smallLen % 8;
      if (offset + smallLen > length) {
        smallLen = (int) (length - offset);
      }

      logger.warn("offset is :{}  smallLen is :{}", offset, smallLen);
      writeAddressData(storage, offset, smallLen);
      offset += smallLen;
    }
    long writeTimeEnd = System.currentTimeMillis();
    offset = 0;

    // read
    byte[] dst = new byte[pageSize * 10];
    while (offset < length) {
      int smallLen = random.nextInt(pageSize * 10);
      smallLen -= smallLen % 8;
      if (offset + smallLen > length) {
        smallLen = (int) (length - offset);
      }
      verifyAddressData(storage, offset, smallLen, dst);
      offset += smallLen;
    }
    logger
        .debug("Write " + length / (1024 * 1024) + " MB. Time: " + (writeTimeEnd - writeTimeBegin));
    logger.debug("Read " + length / (1024 * 1024) + " MB. Time: " + (System.currentTimeMillis()
        - writeTimeEnd));
  }

  private void compareTwoSegment(SegmentMetadata beforeSegment, SegmentMetadata afterSegment) {
    assertEquals(beforeSegment.getSegId().getVolumeId().getId(),
        afterSegment.getSegId().getVolumeId().getId());
    for (InstanceId beforeInstanceId : beforeSegment.getSegmentUnitMetadataTable().keySet()) {
      assertTrue(afterSegment.getSegmentUnitMetadataTable().containsKey(beforeInstanceId));
    }

    List<SegmentMembership> beforeMemberships = new ArrayList<>();
    List<SegmentMembership> afterMemberships = new ArrayList<>();

    for (SegmentUnitMetadata beforeUnit : beforeSegment.getSegmentUnitMetadataTable().values()) {
      beforeMemberships.add(beforeUnit.getMembership());
    }
    for (SegmentUnitMetadata afterUnit : afterSegment.getSegmentUnitMetadataTable().values()) {
      afterMemberships.add(afterUnit.getMembership());
    }

    for (SegmentMembership before : beforeMemberships) {
      boolean found = false;
      for (SegmentMembership after : afterMemberships) {
        if (after.equals(before)) {
          found = true;
          break;
        }
      }
      assertTrue(found);
    }
  }
}
