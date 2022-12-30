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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.HashMultimap;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.common.RequestIdBuilder;
import py.common.struct.Pair;
import py.coordinator.calculator.StripeLogicalToPhysicalCalculator;
import py.coordinator.nbd.Util;
import py.coordinator.volumeinfo.SpaceSavingVolumeMetadata;
import py.coordinator.volumeinfo.VolumeInfoRetriever;
import py.drivercontainer.exception.ProcessPositionException;
import py.exception.StorageException;
import py.membership.SegmentMembership;
import py.test.TestBase;
import py.volume.VolumeMetadata;

/**
 * xx.
 */
public class LogicalToPhyiscalCalculatorTest extends TestBase {

  private static final Logger logger = LoggerFactory
      .getLogger(LogicalToPhyiscalCalculatorTest.class);

  private static final long One_MB = 1024 * 1024; // 1 MB
  private static final int PAGE_SIZE = 1024 * 128; // 128k

  private static final long SEGMENT_SIZE = One_MB * 16; // 16Mb
  private static final long VOLUME_SIZE = One_MB * 16 * 4; // 64Mb
  private static final long PAGE_PACKAGE_COUNT = SEGMENT_SIZE / (PAGE_SIZE * 8);
  @Mock
  VolumeInfoRetriever volumeInfoRetriever;
  private List<Long> countOfSegments;
  private long wholeVolumeSize;
  private StripeLogicalToPhysicalCalculator calculator;
  private Set<Long> segmentIndex;
  private Long volumeId = RequestIdBuilder.get();

  public LogicalToPhyiscalCalculatorTest() {
  }


  /**
   * xx.
   */
  @Before
  public void initList() {
    countOfSegments = new ArrayList<>();
    long firstSegmentCount = 800L;
    wholeVolumeSize += (firstSegmentCount * SEGMENT_SIZE);
    countOfSegments.add(firstSegmentCount);
    long secondSegmentCount = 50L;
    wholeVolumeSize += (secondSegmentCount * SEGMENT_SIZE);
    countOfSegments.add(secondSegmentCount);
    long thirdSegmentCount = 150L;
    wholeVolumeSize += (thirdSegmentCount * SEGMENT_SIZE);
    countOfSegments.add(thirdSegmentCount);
    segmentIndex = new HashSet<>();
    for (long i = 0; i < wholeVolumeSize / SEGMENT_SIZE; i++) {
      segmentIndex.add(i);
    }
    calculator = new StripeLogicalToPhysicalCalculator(volumeInfoRetriever, volumeId, SEGMENT_SIZE,
        PAGE_SIZE);
    calculator.initPagePackageCount((int) PAGE_PACKAGE_COUNT);
    calculator.setVolumeLayout(countOfSegments);
  }

  @Test
  public void testConvertPos2() {
    int pageSize = 8;
    long segmentSize = 131072;

    Random random = new Random(System.currentTimeMillis());
    for (int segmentCount = 1; segmentCount < 200; segmentCount += random.nextInt(100)) {
      for (int pagePackageCount = 1; pagePackageCount < 20; pagePackageCount++) {
        logger.warn("segment count {}, page package count {}", segmentCount, pagePackageCount);
        Set<Long> posSet = new HashSet<>();
        for (long pos = 0; pos < segmentCount * segmentSize; pos += pageSize) {
          // logger.warn("segment count {}, page package count {}, pos {}", segmentCount,
          // pagePackageCount, pos);
          long offsetInPage = random.nextInt(pageSize);
          long value = Util
              .convertPosition(pos + offsetInPage, pageSize, segmentSize, segmentCount,
                  pagePackageCount);
          assertTrue(value < (segmentSize * segmentCount));
          long pageIndex = value / pageSize;
          assertTrue(posSet.add(pageIndex));
        }
        assertEquals(segmentSize * segmentCount / pageSize, posSet.size());
      }
    }
  }

  @Test
  public void testConvertPos() {
    List<Integer> segmentIndexList = new ArrayList<Integer>();
    int compareValue = 0;
    long lastConvertPos = -1;
    long convertedPos = -1;
    int pageCount = (int) (SEGMENT_SIZE / PAGE_SIZE);
    int pagePackageCount = pageCount / 8;
    logger.debug("page package count:{}", pagePackageCount);
    int segmentCount = (int) (VOLUME_SIZE / SEGMENT_SIZE);
    logger.debug("segmentCount: {}", segmentCount);
    for (int pageIndex = 0; pageIndex < (VOLUME_SIZE / PAGE_SIZE); pageIndex++) {
      int basicPos = (new Random()).nextInt(PAGE_SIZE);
      assertTrue(basicPos < PAGE_SIZE);
      lastConvertPos = convertedPos;
      int segmentIndex = -1;
      int pos = -1;

      pos = pageIndex * PAGE_SIZE + basicPos;
      segmentIndex = (int) (pos / SEGMENT_SIZE);
      assertTrue(segmentIndex == pageIndex / pageCount);
      convertedPos = Util
          .convertPosition(pos, PAGE_SIZE, SEGMENT_SIZE, segmentCount, pagePackageCount);

      assertTrue(lastConvertPos != convertedPos);
      int convertSegmentIndex = -1;
      convertSegmentIndex = (int) (convertedPos / SEGMENT_SIZE);
      logger.debug("index: {} convert pos:{} on segment index:{}", pageIndex, convertedPos,
          convertSegmentIndex);
      assertTrue(compareValue == convertSegmentIndex);
      if ((pageIndex + 1) % pagePackageCount == 0) {
        compareValue++;
      }
      if (compareValue == VOLUME_SIZE / SEGMENT_SIZE) {
        compareValue = 0;
      }
      segmentIndexList.add(convertSegmentIndex);
    }
    logger.debug("finally get segmentIndex list: {}", segmentIndexList);
  }

  @Test
  public void testOriPosAndConvertBackPosEqual() {
    int originPos;
    long convertPos;
    long converBackPos;
    int pageCount = (int) (SEGMENT_SIZE / PAGE_SIZE);
    int pagePackageCount = pageCount / 8;
    int segmentCount = (int) (VOLUME_SIZE / SEGMENT_SIZE);
    for (int count = 0; count < 10000; count++) {
      originPos = 0;
      convertPos = 0;
      converBackPos = 0;
      originPos = (new Random()).nextInt((int) VOLUME_SIZE);
      assertTrue(0 <= originPos && originPos < (int) VOLUME_SIZE);
      convertPos = Util
          .convertPosition(originPos, PAGE_SIZE, SEGMENT_SIZE, segmentCount, pagePackageCount);
      converBackPos = Util
          .convertBackPosition(convertPos, PAGE_SIZE, SEGMENT_SIZE, segmentCount, pagePackageCount);
      logger.warn("origin Pos:{}, convert back pos:{}", originPos, converBackPos);
      assertTrue(originPos == converBackPos);
    }
  }

  @Test
  public void testProcessPosition() throws ProcessPositionException, StorageException {
    long originPos;
    Pair<Integer, Long> convertPos = new Pair<Integer, Long>(0, 0L);
    Pair<Integer, Long> convertBackPos = new Pair<Integer, Long>(0, 0L);
    long originalSegmentIndex = -1;
    long convertSegmentIndex = -1;
    for (int count = 0; count < 100000; count++) {
      originPos = Math.abs((new Random()).nextLong()) % wholeVolumeSize;
      assertTrue(0 <= originPos && originPos < wholeVolumeSize);
      convertPos = calculator.convertLogicalPositionToPhysical(originPos);
      originalSegmentIndex = convertPos.getFirst();

      long trueConvertPos = convertPos.getSecond() + (convertPos.getFirst() * SEGMENT_SIZE);
      convertBackPos = calculator.convertPhysicalPositionToLogical(trueConvertPos);
      convertSegmentIndex = convertBackPos.getFirst();
      segmentIndex.remove(convertSegmentIndex);
      logger.info("segment index:[{}] ==> [{}], position:[{}] ==> [{}]", originalSegmentIndex,
          convertSegmentIndex, originPos, convertBackPos.getSecond());
      long trueConvertBackPos =
          convertBackPos.getSecond() + (convertBackPos.getFirst() * SEGMENT_SIZE);
      assertTrue(originPos == trueConvertBackPos);
    }
    assertTrue(segmentIndex.isEmpty());
  }

  @Test
  public void testProcessPositionWithException() {
    long pos = wholeVolumeSize + 1;
    int pageCount = (int) (SEGMENT_SIZE / PAGE_SIZE);
    int pagePackageCount = pageCount / 8;
    boolean caughtException = false;
    try {
      Util.processConvertPos(pos, PAGE_SIZE, SEGMENT_SIZE, pagePackageCount, countOfSegments,
          false);
    } catch (ProcessPositionException e) {
      caughtException = true;
    }
    assertTrue(caughtException);
  }

  @Test
  public void testMultiMap() {
    Long test1 = 1L;
    Long test2 = 2L;
    Long test3 = 3L;
    HashMultimap<Long, Long> testMap = HashMultimap.<Long, Long>create();
    testMap.put(test1, test2);
    testMap.put(test1, test3);
    assertTrue(testMap.size() == 2);

    testMap.put(test1, test3);
    assertTrue(testMap.size() == 2);
    testMap.put(test1, test2);
    assertTrue(testMap.size() == 2);
    boolean firstLoop = true;
    for (Long value : testMap.get(test1)) {
      if (firstLoop) {
        assertTrue(value.equals(test2));
        firstLoop = false;
      } else {
        assertTrue(value.equals(test3));
      }
    }
  }

  @Test
  public void testChangeValueByKeyInConfigFile() throws FileNotFoundException, IOException {
    String rootPath = "/tmp/";
    String configFile = "testfile";
    final String key = "testKey1";
    final String key2 = "testKey2";
    final String key3 = "testKey3";
    Path path = Paths.get(rootPath, configFile);
    Files.deleteIfExists(path);
    Files.createFile(path);

    // prepare env
    logger.info("path:{}", path.toString());
    File file = path.toFile();
    String originalValue = TestBase.getRandomString(20);
    String originalValue2 = TestBase.getRandomString(30);
    Properties prop = new Properties();
    prop.setProperty(key, originalValue);
    prop.setProperty(key2, originalValue2);
    FileOutputStream fos = new FileOutputStream(file);
    prop.store(fos, null);
    fos.close();

    FileInputStream fis = new FileInputStream(file);

    // load again, use a brand new Properties
    Properties prop2 = new Properties();
    // get all keys and values from config file
    prop2.load(fis);
    fis.close();
    String newValue = TestBase.getRandomString(10);
    prop2.setProperty(key3, newValue);
    FileOutputStream fos2 = new FileOutputStream(file);
    prop2.store(fos2, null);
    fos2.close();

    // load again to check, use a brand new Properties
    Properties prop3 = new Properties();
    FileInputStream fis2 = new FileInputStream(file);
    prop3.load(fis2);

    String getValue = prop3.getProperty(key);
    String getValue2 = prop3.getProperty(key2);
    final String getValue3 = prop3.getProperty(key3);
    fis2.close();
    assertTrue(getValue.equals(originalValue));
    assertTrue(getValue2.equals(originalValue2));
    assertTrue(getValue3.equals(newValue));

    // after test, delete this file
    Files.deleteIfExists(path);
  }

  @Test
  public void testUpdateVolumeInformation() {
    int segmentCountOfFirstVolume = 18;
    int segmentCountOfSecondVolume = 21;
    int segmentCountOfThirdVolume = 30;
    final int segmentLayout = 5;

    long firstVolumeId = RequestIdBuilder.get();
    long secondVolumeId = RequestIdBuilder.get();
    long thirdVolumeId = RequestIdBuilder.get();

    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setSegmentSize(SEGMENT_SIZE);
    long allSegmentCount =
        segmentCountOfFirstVolume + segmentCountOfSecondVolume + segmentCountOfThirdVolume;
    final long volumeSize = allSegmentCount * SEGMENT_SIZE;

    //the extend volume, make each
    List<Integer> values = new ArrayList<>();
    values.add(segmentCountOfFirstVolume);
    values.add(segmentCountOfSecondVolume);
    values.add(segmentCountOfThirdVolume);
    String eachTimeExtendVolumeSize = makeEachTimeExtendVolumeSize(values);

    volumeMetadata.setEachTimeExtendVolumeSize(eachTimeExtendVolumeSize);
    volumeMetadata.setVolumeSize(volumeSize);
    volumeMetadata.setSegmentNumToCreateEachTime(segmentLayout);

    //volumeMetadata.setSegmentNumToCreateEachTime(segmentEachTime);
    //volumeMetadata.setNotCreateAllSegmentAtBeginning(true);
    volumeMetadata.setSegmentWrappCount(segmentLayout);
    //volumeMetadata.setVolumeId(firstVolumeId);
    //volumeMetadata.setRootVolumeId(volumeMetadata.getVolumeId());

    SegmentMembership membership = mock(SegmentMembership.class);
    int first = 0;
    int second = 0;
    int third = 0;
    for (int i = 0; i < allSegmentCount; i++) {
      SegId segId;
      if (i < segmentCountOfFirstVolume) {
        segId = new SegId(firstVolumeId, i);
        first++;
      } else if (i < segmentCountOfFirstVolume + segmentCountOfSecondVolume) {
        segId = new SegId(secondVolumeId, i - segmentCountOfFirstVolume);
        second++;
      } else {
        segId = new SegId(thirdVolumeId,
            i - segmentCountOfFirstVolume - segmentCountOfSecondVolume);
        third++;
      }
      SegmentMetadata segmentMetadata = new SegmentMetadata(segId, i);
      volumeMetadata.addSegmentMetadata(segmentMetadata, membership);
    }

    assertEquals(first, segmentCountOfFirstVolume);
    assertEquals(second, segmentCountOfSecondVolume);
    assertEquals(third, segmentCountOfThirdVolume);

    try {
      when(volumeInfoRetriever.getVolume(anyLong()))
          .thenReturn(new SpaceSavingVolumeMetadata(volumeMetadata));
      calculator.updateVolumeInformation();
    } catch (Exception e) {
      assertTrue(false);
    }

    List<Long> layout = calculator.getVolumeLayout();

    for (long tmpSegmentCount : layout) {
      if (segmentCountOfFirstVolume > 0) {
        if (segmentCountOfFirstVolume >= segmentLayout) {
          segmentCountOfFirstVolume -= segmentLayout;
          assertEquals(tmpSegmentCount, segmentLayout);
        } else {
          assertEquals(tmpSegmentCount, segmentCountOfFirstVolume);
          segmentCountOfFirstVolume = 0;
        }
        continue;
      } else if (segmentCountOfSecondVolume > 0) {
        if (segmentCountOfSecondVolume >= segmentLayout) {
          segmentCountOfSecondVolume -= segmentLayout;
          assertEquals(tmpSegmentCount, segmentLayout);
        } else {
          assertEquals(tmpSegmentCount, segmentCountOfSecondVolume);
          segmentCountOfSecondVolume = 0;
        }
        continue;
      } else if (segmentCountOfThirdVolume > 0) {
        if (segmentCountOfThirdVolume >= segmentLayout) {
          segmentCountOfThirdVolume -= segmentLayout;
          assertEquals(tmpSegmentCount, segmentLayout);
        } else {
          assertEquals(tmpSegmentCount, segmentCountOfThirdVolume);
          segmentCountOfThirdVolume = 0;
        }
        continue;
      }
    }
  }

  @Test
  public void testUpdateVolumeInformation1() {
    int segmentCountOfFirstVolume = 18;
    int segmentCountOfSecondVolume = 21;
    int segmentCountOfThirdVolume = 30;
    /*
     * this segmentLayout = 0 is test purpose
     */
    final int segmentLayout = 0;

    long firstVolumeId = RequestIdBuilder.get();
    long secondVolumeId = RequestIdBuilder.get();
    long thirdVolumeId = RequestIdBuilder.get();

    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setSegmentSize(SEGMENT_SIZE);
    long allSegmentCount =
        segmentCountOfFirstVolume + segmentCountOfSecondVolume + segmentCountOfThirdVolume;
    final long volumeSize = allSegmentCount * SEGMENT_SIZE;

    //the extend volume, make each
    List<Integer> values = new ArrayList<>();
    values.add(segmentCountOfFirstVolume);
    values.add(segmentCountOfSecondVolume);
    values.add(segmentCountOfThirdVolume);
    String eachTimeExtendVolumeSize = makeEachTimeExtendVolumeSize(values);

    volumeMetadata.setEachTimeExtendVolumeSize(eachTimeExtendVolumeSize);
    volumeMetadata.setVolumeSize(volumeSize);
    volumeMetadata.setSegmentNumToCreateEachTime(segmentLayout);

    SegmentMembership membership = mock(SegmentMembership.class);
    int first = 0;
    int second = 0;
    int third = 0;
    for (int i = 0; i < allSegmentCount; i++) {
      SegId segId;
      if (i < segmentCountOfFirstVolume) {
        segId = new SegId(firstVolumeId, i);
        first++;
      } else if (i < segmentCountOfFirstVolume + segmentCountOfSecondVolume) {
        segId = new SegId(secondVolumeId, i - segmentCountOfFirstVolume);
        second++;
      } else {
        segId = new SegId(thirdVolumeId,
            i - segmentCountOfFirstVolume - segmentCountOfSecondVolume);
        third++;
      }
      SegmentMetadata segmentMetadata = new SegmentMetadata(segId, i);
      volumeMetadata.addSegmentMetadata(segmentMetadata, membership);
    }

    assertEquals(first, segmentCountOfFirstVolume);
    assertEquals(second, segmentCountOfSecondVolume);
    assertEquals(third, segmentCountOfThirdVolume);

    try {
      when(volumeInfoRetriever.getVolume(anyLong()))
          .thenReturn(new SpaceSavingVolumeMetadata(volumeMetadata));
      calculator.updateVolumeInformation();
    } catch (Exception e) {
      assertTrue(false);
    }

    List<Long> layout = calculator.getVolumeLayout();

    for (long tmpSegmentCount : layout) {
      if (segmentCountOfFirstVolume > 0) {
        assertEquals(tmpSegmentCount, segmentCountOfFirstVolume);
        segmentCountOfFirstVolume = 0;
      } else if (segmentCountOfSecondVolume > 0) {
        assertEquals(tmpSegmentCount, segmentCountOfSecondVolume);
        segmentCountOfSecondVolume = 0;
      } else if (segmentCountOfThirdVolume > 0) {
        assertEquals(tmpSegmentCount, segmentCountOfThirdVolume);
        segmentCountOfThirdVolume = 0;
      }
    }
  }

  /*
   * Test for simple volume
   * rootVolume segments don't be create completely, but extend volume segment be create
   *  Return Error
   */
  @Test(expected = StorageException.class)
  public void testUpdateVolumeInformation2() throws Exception {
    int segmentCountOfFirstVolume = 18;
    int segmentCountOfSecondVolume = 21;
    int segmentCountOfThirdVolume = 30;
    final int segmentEachTime = 5;
    final int segmentWappedCount = 3;

    long firstVolumeId = RequestIdBuilder.get();
    long secondVolumeId = RequestIdBuilder.get();
    long thirdVolumeId = RequestIdBuilder.get();

    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setSegmentSize(SEGMENT_SIZE);
    long allSegmentCount =
        segmentCountOfFirstVolume + segmentCountOfSecondVolume + segmentCountOfThirdVolume;
    final long volumeSize = allSegmentCount * SEGMENT_SIZE;

    //the extend volume, make each
    List<Integer> values = new ArrayList<>();
    values.add(segmentCountOfFirstVolume);
    values.add(segmentCountOfSecondVolume);
    values.add(segmentCountOfThirdVolume);
    String eachTimeExtendVolumeSize = makeEachTimeExtendVolumeSize(values);

    volumeMetadata.setEachTimeExtendVolumeSize(eachTimeExtendVolumeSize);
    volumeMetadata.setVolumeSize(volumeSize);
    volumeMetadata.setSegmentNumToCreateEachTime(segmentEachTime);
    volumeMetadata.setSegmentWrappCount(segmentWappedCount);

    SegmentMembership membership = mock(SegmentMembership.class);
    int first = 0;
    int second = 0;
    int third = 0;

    for (int i = 0; i < (segmentCountOfFirstVolume - 1); i++) {
      SegId segId;
      first++;
      segId = new SegId(firstVolumeId, i);
      SegmentMetadata segmentMetadata = new SegmentMetadata(segId, i);
      volumeMetadata.addSegmentMetadata(segmentMetadata, membership);
    }

    for (int i = 0; i < segmentCountOfSecondVolume; i++) {
      SegId segId;
      second++;
      segId = new SegId(secondVolumeId, i);
      SegmentMetadata segmentMetadata = new SegmentMetadata(segId, i);
      volumeMetadata.addSegmentMetadata(segmentMetadata, membership);
    }

    for (int i = 0; i < segmentCountOfThirdVolume; i++) {
      SegId segId;
      third++;
      segId = new SegId(thirdVolumeId, i);
      SegmentMetadata segmentMetadata = new SegmentMetadata(segId, i);
      volumeMetadata.addSegmentMetadata(segmentMetadata, membership);
    }

    assertEquals(first, segmentCountOfFirstVolume - 1);
    assertEquals(second, segmentCountOfSecondVolume);
    assertEquals(third, segmentCountOfThirdVolume);

    //boolean exceptionCaught = false;

    when(volumeInfoRetriever.getVolume(anyLong()))
        .thenReturn(new SpaceSavingVolumeMetadata(volumeMetadata));
    calculator.updateVolumeInformation();

    //assertTrue(exceptionCaught);

  }

  /*
   * Test for simple volume
   * rootVolume segments and extend volume segment were create completely
   *  Return layout <3,1,3,1,1,3,3,2,3,3,1>
   */
  @Test
  public void testUpdateVolumeInformation3() {
    int segmentCountOfFirstVolume = 9;
    int segmentCountOfSecondVolume = 8;
    int segmentCountOfThirdVolume = 7;
    final int segmentEachTime = 4;
    final int segmentWappedCount = 3;

    final long firstVolumeId = RequestIdBuilder.get();
    long secondVolumeId = RequestIdBuilder.get();
    long thirdVolumeId = RequestIdBuilder.get();

    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setSegmentSize(SEGMENT_SIZE);
    long allSegmentCount =
        segmentCountOfFirstVolume + segmentCountOfSecondVolume + segmentCountOfThirdVolume;
    final long volumeSize = allSegmentCount * SEGMENT_SIZE;

    //the extend volume, make each
    List<Integer> values = new ArrayList<>();
    values.add(segmentCountOfFirstVolume);
    values.add(segmentCountOfSecondVolume);
    values.add(segmentCountOfThirdVolume);
    String eachTimeExtendVolumeSize = makeEachTimeExtendVolumeSize(values);

    volumeMetadata.setVolumeSize(volumeSize);
    volumeMetadata.setSegmentNumToCreateEachTime(segmentEachTime);
    volumeMetadata.setSegmentWrappCount(segmentWappedCount);
    volumeMetadata.setRootVolumeId(firstVolumeId);
    volumeMetadata.setEachTimeExtendVolumeSize(eachTimeExtendVolumeSize);
    SegmentMembership membership = mock(SegmentMembership.class);
    int first = 0;
    int second = 0;
    int third = 0;

    for (int i = 0; i < allSegmentCount; i++) {
      SegId segId;
      if (i < segmentCountOfFirstVolume) {
        segId = new SegId(firstVolumeId, i);
        first++;
      } else if (i < segmentCountOfFirstVolume + segmentCountOfSecondVolume) {
        segId = new SegId(secondVolumeId, i - segmentCountOfFirstVolume);
        second++;
      } else {
        segId = new SegId(thirdVolumeId,
            i - segmentCountOfFirstVolume - segmentCountOfSecondVolume);
        third++;
      }
      SegmentMetadata segmentMetadata = new SegmentMetadata(segId, i);
      volumeMetadata.addSegmentMetadata(segmentMetadata, membership);
    }

    assertEquals(first, segmentCountOfFirstVolume);
    assertEquals(second, segmentCountOfSecondVolume);
    assertEquals(third, segmentCountOfThirdVolume);

    try {
      when(volumeInfoRetriever.getVolume(anyLong()))
          .thenReturn(new SpaceSavingVolumeMetadata(volumeMetadata));
      calculator.updateVolumeInformation();
    } catch (Exception e) {
      assertTrue(false);
    }

    //<3,1,3,1,1,3,3,2,3,3,1>
    List<Long> layout = calculator.getVolumeLayout();

    List<Long> volumeListInOrder = new ArrayList<Long>(
        Arrays.asList(3L, 1L, 3L, 1L, 1L, 3L, 3L, 2L, 3L, 3L, 1L));

    assertEquals(layout.size(), volumeListInOrder.size());

    int listSize = layout.size();
    for (int i = 0; i < listSize; i++) {
      assertEquals(layout.get(i), volumeListInOrder.get(i));
    }
  }

  /*
   * Test for simple volume
   * rootVolume segments were create completely. not has extend volume
   *  Return layout <3,1,3,1,1>
   */
  @Test
  public void testUpdateVolumeInformation4() {
    int segmentCountOfFirstVolume = 9;

    final int segmentEachTime = 4;
    final int segmentWappedCount = 3;

    final long firstVolumeId = RequestIdBuilder.get();
    long secondVolumeId = RequestIdBuilder.get();
    long thirdVolumeId = RequestIdBuilder.get();

    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setSegmentSize(SEGMENT_SIZE);
    long allSegmentCount = segmentCountOfFirstVolume;
    long volumeSize = allSegmentCount * SEGMENT_SIZE;

    //the extend volume, make each
    List<Integer> values = new ArrayList<>();
    values.add(segmentCountOfFirstVolume);
    String eachTimeExtendVolumeSize = makeEachTimeExtendVolumeSize(values);

    volumeMetadata.setEachTimeExtendVolumeSize(eachTimeExtendVolumeSize);
    volumeMetadata.setVolumeSize(volumeSize);
    volumeMetadata.setSegmentNumToCreateEachTime(segmentEachTime);
    volumeMetadata.setSegmentWrappCount(segmentWappedCount);
    volumeMetadata.setRootVolumeId(firstVolumeId);

    SegmentMembership membership = mock(SegmentMembership.class);
    int first = 0;
    int second = 0;
    int third = 0;

    for (int i = 0; i < segmentCountOfFirstVolume; i++) {
      SegId segId;
      first++;
      segId = new SegId(firstVolumeId, i);
      SegmentMetadata segmentMetadata = new SegmentMetadata(segId, i);
      volumeMetadata.addSegmentMetadata(segmentMetadata, membership);
    }
    assertEquals(first, segmentCountOfFirstVolume);

    try {
      when(volumeInfoRetriever.getVolume(anyLong()))
          .thenReturn(new SpaceSavingVolumeMetadata(volumeMetadata));
      calculator.updateVolumeInformation();
    } catch (Exception e) {
      assertTrue(false);
    }

    //<3,1,3,1,1>
    List<Long> layout = calculator.getVolumeLayout();

    List<Long> volumeListInOrder = new ArrayList<Long>(Arrays.asList(3L, 1L, 3L, 1L, 1L));

    assertEquals(layout.size(), volumeListInOrder.size());

    int listSize = layout.size();
    for (int i = 0; i < listSize; i++) {
      assertEquals(layout.get(i), volumeListInOrder.get(i));
    }
  }

  /*
   * Test for simple volume
   * rootVolume segments were not create completely, AND the count that had been creatd segment
   * more than EachTimeCount .
   *  Return layout <3,1,3,1>
   */
  @Test
  public void testUpdateVolumeInformation5() {
    int segmentCountOfFirstVolume = 9;

    final int segmentEachTime = 4;
    final int segmentWappedCount = 3;

    final long firstVolumeId = RequestIdBuilder.get();

    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setSegmentSize(SEGMENT_SIZE);
    long allSegmentCount = segmentCountOfFirstVolume;
    long volumeSize = allSegmentCount * SEGMENT_SIZE;

    //the extend volume, make each
    List<Integer> values = new ArrayList<>();
    values.add(segmentCountOfFirstVolume);
    String eachTimeExtendVolumeSize = makeEachTimeExtendVolumeSize(values);

    volumeMetadata.setEachTimeExtendVolumeSize(eachTimeExtendVolumeSize);
    volumeMetadata.setVolumeSize(volumeSize);
    volumeMetadata.setSegmentNumToCreateEachTime(segmentEachTime);
    volumeMetadata.setSegmentWrappCount(segmentWappedCount);
    volumeMetadata.setRootVolumeId(firstVolumeId);
    SegmentMembership membership = mock(SegmentMembership.class);
    int first = 0;
    int second = 0;
    int third = 0;

    int hasCreatedCount = 8;
    for (int i = 0; i < hasCreatedCount; i++) {
      SegId segId;
      first++;
      segId = new SegId(firstVolumeId, i);
      SegmentMetadata segmentMetadata = new SegmentMetadata(segId, i);
      volumeMetadata.addSegmentMetadata(segmentMetadata, membership);
    }
    assertEquals(first, hasCreatedCount);

    try {
      when(volumeInfoRetriever.getVolume(anyLong()))
          .thenReturn(new SpaceSavingVolumeMetadata(volumeMetadata));
      calculator.updateVolumeInformation();
    } catch (Exception e) {
      assertTrue(false);
    }

    //<3,1,3,1,>
    List<Long> layout = calculator.getVolumeLayout();

    List<Long> volumeListInOrder = new ArrayList<Long>(Arrays.asList(3L, 1L, 3L, 1L));

    assertEquals(layout.size(), volumeListInOrder.size());

    int listSize = layout.size();
    for (int i = 0; i < listSize; i++) {
      assertEquals(layout.get(i), volumeListInOrder.get(i));
    }
  }

  /*
   * Test for simple volume
   * rootVolume segments were not create completely, And  less than  EachTimeCount segments had
   * been created.
   *  throw
   */
  @Test(expected = StorageException.class)
  public void testUpdateVolumeInformation6() throws Exception {
    int segmentCountOfFirstVolume = 9;

    final int segmentEachTime = 4;
    final int segmentWappedCount = 3;

    final long firstVolumeId = RequestIdBuilder.get();

    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setSegmentSize(SEGMENT_SIZE);
    long allSegmentCount = segmentCountOfFirstVolume;
    long volumeSize = allSegmentCount * SEGMENT_SIZE;

    //the extend volume, make each
    List<Integer> values = new ArrayList<>();
    values.add(segmentCountOfFirstVolume);
    String eachTimeExtendVolumeSize = makeEachTimeExtendVolumeSize(values);

    volumeMetadata.setEachTimeExtendVolumeSize(eachTimeExtendVolumeSize);
    volumeMetadata.setVolumeSize(volumeSize);
    volumeMetadata.setSegmentNumToCreateEachTime(segmentEachTime);
    volumeMetadata.setSegmentWrappCount(segmentWappedCount);
    volumeMetadata.setRootVolumeId(firstVolumeId);
    SegmentMembership membership = mock(SegmentMembership.class);
    int first = 0;
    int second = 0;
    int third = 0;

    int hasCreatedCount = 2;
    for (int i = 0; i < hasCreatedCount; i++) {
      SegId segId;
      first++;
      segId = new SegId(firstVolumeId, i);
      SegmentMetadata segmentMetadata = new SegmentMetadata(segId, i);
      volumeMetadata.addSegmentMetadata(segmentMetadata, membership);
    }
    assertEquals(first, hasCreatedCount);
    when(volumeInfoRetriever.getVolume(anyLong()))
        .thenReturn(new SpaceSavingVolumeMetadata(volumeMetadata));
    calculator.updateVolumeInformation();

  }

  /*
   * Test for simple volume
   * rootVolume segments were not create completely, but  More than EachTimeCount segments had
   * been created.
   *  Return layout <3,1>
   */
  @Test
  public void testUpdateVolumeInformation7() {
    int segmentCountOfFirstVolume = 9;

    final int segmentEachTime = 4;
    final int segmentWappedCount = 3;

    final long firstVolumeId = RequestIdBuilder.get();

    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setSegmentSize(SEGMENT_SIZE);
    long allSegmentCount = segmentCountOfFirstVolume;
    long volumeSize = allSegmentCount * SEGMENT_SIZE;

    //the extend volume, make each
    List<Integer> values = new ArrayList<>();
    values.add(segmentCountOfFirstVolume);
    String eachTimeExtendVolumeSize = makeEachTimeExtendVolumeSize(values);

    volumeMetadata.setEachTimeExtendVolumeSize(eachTimeExtendVolumeSize);
    volumeMetadata.setVolumeSize(volumeSize);
    volumeMetadata.setSegmentNumToCreateEachTime(segmentEachTime);
    volumeMetadata.setSegmentWrappCount(segmentWappedCount);
    volumeMetadata.setRootVolumeId(firstVolumeId);
    SegmentMembership membership = mock(SegmentMembership.class);
    int first = 0;
    int second = 0;
    int third = 0;

    int hasCreatedCount = 6;
    for (int i = 0; i < hasCreatedCount; i++) {
      SegId segId;
      first++;
      segId = new SegId(firstVolumeId, i);
      SegmentMetadata segmentMetadata = new SegmentMetadata(segId, i);
      volumeMetadata.addSegmentMetadata(segmentMetadata, membership);
    }
    assertEquals(first, hasCreatedCount);

    try {
      when(volumeInfoRetriever.getVolume(anyLong()))
          .thenReturn(new SpaceSavingVolumeMetadata(volumeMetadata));
      calculator.updateVolumeInformation();
    } catch (Exception e) {
      assertTrue(false);
    }

    //<3,1>
    List<Long> layout = calculator.getVolumeLayout();

    List<Long> volumeListInOrder = new ArrayList<Long>(Arrays.asList(3L, 1L));

    assertEquals(layout.size(), volumeListInOrder.size());

    int listSize = layout.size();
    for (int i = 0; i < listSize; i++) {
      assertEquals(layout.get(i), volumeListInOrder.get(i));
    }
  }

  /*
   * Test for simple volume
   * rootVolume segments less than  EachTimeCount And WrappedCount.
   *  Return layout <2>
   */
  @Test
  public void testUpdateVolumeInformation8() {
    int segmentCountOfFirstVolume = 2;

    final int segmentEachTime = 4;
    final int segmentWappedCount = 3;

    final long firstVolumeId = RequestIdBuilder.get();

    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setSegmentSize(SEGMENT_SIZE);
    long allSegmentCount = segmentCountOfFirstVolume;
    long volumeSize = allSegmentCount * SEGMENT_SIZE;

    //the extend volume, make each
    List<Integer> values = new ArrayList<>();
    values.add(segmentCountOfFirstVolume);
    String eachTimeExtendVolumeSize = makeEachTimeExtendVolumeSize(values);

    volumeMetadata.setEachTimeExtendVolumeSize(eachTimeExtendVolumeSize);
    volumeMetadata.setVolumeSize(volumeSize);
    volumeMetadata.setSegmentNumToCreateEachTime(segmentEachTime);
    volumeMetadata.setSegmentWrappCount(segmentWappedCount);
    volumeMetadata.setVolumeId(firstVolumeId);
    volumeMetadata.setRootVolumeId(volumeMetadata.getVolumeId());

    SegmentMembership membership = mock(SegmentMembership.class);
    int first = 0;
    int second = 0;
    int third = 0;

    int hasCreatedCount = 2;
    for (int i = 0; i < hasCreatedCount; i++) {
      SegId segId;
      first++;
      segId = new SegId(firstVolumeId, i);
      SegmentMetadata segmentMetadata = new SegmentMetadata(segId, i);
      volumeMetadata.addSegmentMetadata(segmentMetadata, membership);
    }
    assertEquals(first, hasCreatedCount);

    try {
      when(volumeInfoRetriever.getVolume(anyLong()))
          .thenReturn(new SpaceSavingVolumeMetadata(volumeMetadata));
      calculator.updateVolumeInformation();
    } catch (Exception e) {
      assertTrue(false);
    }

    //<2,>
    List<Long> layout = calculator.getVolumeLayout();

    List<Long> volumeListInOrder = new ArrayList<Long>(Arrays.asList(2L));

    assertEquals(layout.size(), volumeListInOrder.size());

    int listSize = layout.size();
    for (int i = 0; i < listSize; i++) {
      assertEquals(layout.get(i), volumeListInOrder.get(i));
    }
  }

  /*
   * Test for simple volume
   * rootVolume segments between  EachTimeCount And WrappedCount.
   *  Return layout <3,2>
   */
  @Test
  public void testUpdateVolumeInformation9() {
    int segmentCountOfFirstVolume = 5;

    final int segmentEachTime = 7;
    final int segmentWappedCount = 3;

    final long firstVolumeId = RequestIdBuilder.get();

    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setSegmentSize(SEGMENT_SIZE);
    long allSegmentCount = segmentCountOfFirstVolume;
    long volumeSize = allSegmentCount * SEGMENT_SIZE;

    //the extend volume, make each
    List<Integer> values = new ArrayList<>();
    values.add(segmentCountOfFirstVolume);
    String eachTimeExtendVolumeSize = makeEachTimeExtendVolumeSize(values);

    volumeMetadata.setEachTimeExtendVolumeSize(eachTimeExtendVolumeSize);
    volumeMetadata.setVolumeSize(volumeSize);
    volumeMetadata.setSegmentNumToCreateEachTime(segmentEachTime);
    volumeMetadata.setSegmentWrappCount(segmentWappedCount);
    volumeMetadata.setVolumeId(firstVolumeId);
    volumeMetadata.setRootVolumeId(volumeMetadata.getVolumeId());

    SegmentMembership membership = mock(SegmentMembership.class);
    int first = 0;
    int second = 0;
    int third = 0;

    int hasCreatedCount = segmentCountOfFirstVolume;
    for (int i = 0; i < hasCreatedCount; i++) {
      SegId segId;
      first++;
      segId = new SegId(firstVolumeId, i);
      SegmentMetadata segmentMetadata = new SegmentMetadata(segId, i);
      volumeMetadata.addSegmentMetadata(segmentMetadata, membership);
    }
    assertEquals(first, hasCreatedCount);

    try {
      when(volumeInfoRetriever.getVolume(anyLong()))
          .thenReturn(new SpaceSavingVolumeMetadata(volumeMetadata));
      calculator.updateVolumeInformation();
    } catch (Exception e) {
      assertTrue(false);
    }

    //<3,2,>
    List<Long> layout = calculator.getVolumeLayout();

    List<Long> volumeListInOrder = new ArrayList<Long>(Arrays.asList(3L, 2L));

    assertEquals(layout.size(), volumeListInOrder.size());

    int listSize = layout.size();
    for (int i = 0; i < listSize; i++) {
      assertEquals(layout.get(i), volumeListInOrder.get(i));
    }
  }

  /*
   * Test for simple volume
   * rootVolume segments had been create completely, And  extend volume not.
   *  throw
   */
  @Test(expected = StorageException.class)
  public void testUpdateVolumeInformation10() throws Exception {
    int segmentCountOfFirstVolume = 9;
    int segmentCountOfSecondtVolume = 9;
    final int segmentEachTime = 4;
    final int segmentWappedCount = 3;

    final long firstVolumeId = RequestIdBuilder.get();
    long secondVolumeId = RequestIdBuilder.get();
    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setSegmentSize(SEGMENT_SIZE);
    long allSegmentCount = segmentCountOfFirstVolume + segmentCountOfSecondtVolume;
    final long volumeSize = allSegmentCount * SEGMENT_SIZE;

    //the extend volume, make each
    List<Integer> values = new ArrayList<>();
    values.add(segmentCountOfFirstVolume);
    values.add(segmentCountOfSecondtVolume);
    String eachTimeExtendVolumeSize = makeEachTimeExtendVolumeSize(values);

    volumeMetadata.setEachTimeExtendVolumeSize(eachTimeExtendVolumeSize);
    volumeMetadata.setVolumeSize(volumeSize);
    volumeMetadata.setSegmentNumToCreateEachTime(segmentEachTime);
    volumeMetadata.setSegmentWrappCount(segmentWappedCount);
    volumeMetadata.setRootVolumeId(firstVolumeId);
    SegmentMembership membership = mock(SegmentMembership.class);
    int first = 0;
    int second = 0;
    int third = 0;

    int hasCreatedCount = segmentCountOfFirstVolume;
    for (int i = 0; i < hasCreatedCount; i++) {
      SegId segId;
      first++;
      segId = new SegId(firstVolumeId, i);
      SegmentMetadata segmentMetadata = new SegmentMetadata(segId, i);
      volumeMetadata.addSegmentMetadata(segmentMetadata, membership);
    }
    assertEquals(first, hasCreatedCount);

    hasCreatedCount = segmentCountOfSecondtVolume - 1;
    for (int i = 0; i < hasCreatedCount; i++) {
      SegId segId;
      first++;
      segId = new SegId(secondVolumeId, i);
      SegmentMetadata segmentMetadata = new SegmentMetadata(segId, i + segmentCountOfFirstVolume);
      volumeMetadata.addSegmentMetadata(segmentMetadata, membership);
    }

    when(volumeInfoRetriever.getVolume(anyLong()))
        .thenReturn(new SpaceSavingVolumeMetadata(volumeMetadata));
    calculator.updateVolumeInformation();

  }

  /*
   * Test for simple volume
   * EachTimeCount less than  WrappedCount.
   *  Return layout <3,3,3,2>
   */
  @Test
  public void testUpdateVolumeInformation11() {
    int segmentCountOfFirstVolume = 11;

    final int segmentEachTime = 3;
    final int segmentWappedCount = 5;

    final long firstVolumeId = RequestIdBuilder.get();

    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setSegmentSize(SEGMENT_SIZE);
    long allSegmentCount = segmentCountOfFirstVolume;
    long volumeSize = allSegmentCount * SEGMENT_SIZE;

    //the extend volume, make each
    List<Integer> values = new ArrayList<>();
    values.add(segmentCountOfFirstVolume);
    String eachTimeExtendVolumeSize = makeEachTimeExtendVolumeSize(values);

    volumeMetadata.setEachTimeExtendVolumeSize(eachTimeExtendVolumeSize);
    volumeMetadata.setVolumeSize(volumeSize);
    volumeMetadata.setSegmentNumToCreateEachTime(segmentEachTime);
    volumeMetadata.setSegmentWrappCount(segmentWappedCount);
    volumeMetadata.setVolumeId(firstVolumeId);
    volumeMetadata.setRootVolumeId(volumeMetadata.getVolumeId());

    SegmentMembership membership = mock(SegmentMembership.class);
    int first = 0;
    int second = 0;
    int third = 0;

    int hasCreatedCount = segmentCountOfFirstVolume;
    for (int i = 0; i < hasCreatedCount; i++) {
      SegId segId;
      first++;
      segId = new SegId(firstVolumeId, i);
      SegmentMetadata segmentMetadata = new SegmentMetadata(segId, i);
      volumeMetadata.addSegmentMetadata(segmentMetadata, membership);
    }
    assertEquals(first, hasCreatedCount);

    try {
      when(volumeInfoRetriever.getVolume(anyLong()))
          .thenReturn(new SpaceSavingVolumeMetadata(volumeMetadata));
      calculator.updateVolumeInformation();
    } catch (Exception e) {
      assertTrue(false);
    }

    //<3,3,3,2,>
    List<Long> layout = calculator.getVolumeLayout();

    List<Long> volumeListInOrder = new ArrayList<Long>(Arrays.asList(3L, 3L, 3L, 2L));

    assertEquals(layout.size(), volumeListInOrder.size());

    int listSize = layout.size();
    for (int i = 0; i < listSize; i++) {
      assertEquals(layout.get(i), volumeListInOrder.get(i));
    }
  }

  /*
   * Test for simple volume
   * EachTimeCount equal  WrappedCount.
   *  Return layout <3,3,3,2>
   */
  @Test
  public void testUpdateVolumeInformation12() {
    int segmentCountOfFirstVolume = 11;

    final int segmentEachTime = 3;
    final int segmentWappedCount = 3;

    final long firstVolumeId = RequestIdBuilder.get();

    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setSegmentSize(SEGMENT_SIZE);
    long allSegmentCount = segmentCountOfFirstVolume;
    long volumeSize = allSegmentCount * SEGMENT_SIZE;

    //the extend volume, make each
    List<Integer> values = new ArrayList<>();
    values.add(segmentCountOfFirstVolume);
    String eachTimeExtendVolumeSize = makeEachTimeExtendVolumeSize(values);

    volumeMetadata.setEachTimeExtendVolumeSize(eachTimeExtendVolumeSize);
    volumeMetadata.setVolumeSize(volumeSize);
    volumeMetadata.setSegmentNumToCreateEachTime(segmentEachTime);
    volumeMetadata.setSegmentWrappCount(segmentWappedCount);
    volumeMetadata.setVolumeId(firstVolumeId);
    volumeMetadata.setRootVolumeId(volumeMetadata.getVolumeId());

    SegmentMembership membership = mock(SegmentMembership.class);
    int first = 0;
    int second = 0;
    int third = 0;

    int hasCreatedCount = segmentCountOfFirstVolume;
    for (int i = 0; i < hasCreatedCount; i++) {
      SegId segId;
      first++;
      segId = new SegId(firstVolumeId, i);
      SegmentMetadata segmentMetadata = new SegmentMetadata(segId, i);
      volumeMetadata.addSegmentMetadata(segmentMetadata, membership);
    }
    assertEquals(first, hasCreatedCount);

    try {
      when(volumeInfoRetriever.getVolume(anyLong()))
          .thenReturn(new SpaceSavingVolumeMetadata(volumeMetadata));
      calculator.updateVolumeInformation();
    } catch (Exception e) {
      assertTrue(false);
    }

    //<3,3,3,2,>
    List<Long> layout = calculator.getVolumeLayout();

    List<Long> volumeListInOrder = new ArrayList<Long>(Arrays.asList(3L, 3L, 3L, 2L));

    assertEquals(layout.size(), volumeListInOrder.size());

    int listSize = layout.size();
    for (int i = 0; i < listSize; i++) {
      assertEquals(layout.get(i), volumeListInOrder.get(i));
    }
  }

  /*
   * Test for simple volume
   * EachTimeCount less than  WrappedCount, AND segment not been create completely
   *  Return layout <3,3,3>
   */
  @Test
  public void testUpdateVolumeInformation13() {
    int segmentCountOfFirstVolume = 11;

    final int segmentEachTime = 3;
    final int segmentWappedCount = 5;

    final long firstVolumeId = RequestIdBuilder.get();

    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setSegmentSize(SEGMENT_SIZE);
    long allSegmentCount = segmentCountOfFirstVolume;
    long volumeSize = allSegmentCount * SEGMENT_SIZE;

    //the extend volume, make each
    List<Integer> values = new ArrayList<>();
    values.add(segmentCountOfFirstVolume);
    String eachTimeExtendVolumeSize = makeEachTimeExtendVolumeSize(values);

    volumeMetadata.setEachTimeExtendVolumeSize(eachTimeExtendVolumeSize);
    volumeMetadata.setVolumeSize(volumeSize);
    volumeMetadata.setSegmentNumToCreateEachTime(segmentEachTime);
    volumeMetadata.setSegmentWrappCount(segmentWappedCount);
    volumeMetadata.setVolumeId(firstVolumeId);
    volumeMetadata.setRootVolumeId(volumeMetadata.getVolumeId());

    SegmentMembership membership = mock(SegmentMembership.class);
    int first = 0;
    int second = 0;
    int third = 0;

    int hasCreatedCount = segmentCountOfFirstVolume - 1;
    for (int i = 0; i < hasCreatedCount; i++) {
      SegId segId;
      first++;
      segId = new SegId(firstVolumeId, i);
      SegmentMetadata segmentMetadata = new SegmentMetadata(segId, i);
      volumeMetadata.addSegmentMetadata(segmentMetadata, membership);
    }
    assertEquals(first, hasCreatedCount);

    try {
      when(volumeInfoRetriever.getVolume(anyLong()))
          .thenReturn(new SpaceSavingVolumeMetadata(volumeMetadata));
      calculator.updateVolumeInformation();
    } catch (Exception e) {
      assertTrue(false);
    }

    //<3,3,3,>
    List<Long> layout = calculator.getVolumeLayout();

    List<Long> volumeListInOrder = new ArrayList<Long>(Arrays.asList(3L, 3L, 3L));

    assertEquals(layout.size(), volumeListInOrder.size());

    int listSize = layout.size();
    for (int i = 0; i < listSize; i++) {
      assertEquals(layout.get(i), volumeListInOrder.get(i));
    }
  }

  /*
   * Test for simple volume
   * EachTimeCount equal  WrappedCount. AND segment has not been create completely
   *  Return layout <3,3,3>
   */
  @Test
  public void testUpdateVolumeInformation14() {
    int segmentCountOfFirstVolume = 11;

    final int segmentEachTime = 3;
    final int segmentWappedCount = 3;

    final long firstVolumeId = RequestIdBuilder.get();

    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setSegmentSize(SEGMENT_SIZE);
    long allSegmentCount = segmentCountOfFirstVolume;
    long volumeSize = allSegmentCount * SEGMENT_SIZE;

    //the extend volume, make each
    List<Integer> values = new ArrayList<>();
    values.add(segmentCountOfFirstVolume);
    String eachTimeExtendVolumeSize = makeEachTimeExtendVolumeSize(values);

    volumeMetadata.setEachTimeExtendVolumeSize(eachTimeExtendVolumeSize);
    volumeMetadata.setVolumeSize(volumeSize);
    volumeMetadata.setSegmentNumToCreateEachTime(segmentEachTime);
    volumeMetadata.setSegmentWrappCount(segmentWappedCount);
    volumeMetadata.setVolumeId(firstVolumeId);
    volumeMetadata.setRootVolumeId(volumeMetadata.getVolumeId());

    SegmentMembership membership = mock(SegmentMembership.class);
    int first = 0;
    int second = 0;
    int third = 0;

    int hasCreatedCount = segmentCountOfFirstVolume - 1;
    for (int i = 0; i < hasCreatedCount; i++) {
      SegId segId;
      first++;
      segId = new SegId(firstVolumeId, i);
      SegmentMetadata segmentMetadata = new SegmentMetadata(segId, i);
      volumeMetadata.addSegmentMetadata(segmentMetadata, membership);
    }
    assertEquals(first, hasCreatedCount);

    try {
      when(volumeInfoRetriever.getVolume(anyLong()))
          .thenReturn(new SpaceSavingVolumeMetadata(volumeMetadata));
      calculator.updateVolumeInformation();
    } catch (Exception e) {
      assertTrue(false);
    }

    //<3,3,3,>
    List<Long> layout = calculator.getVolumeLayout();

    List<Long> volumeListInOrder = new ArrayList<Long>(Arrays.asList(3L, 3L, 3L));

    assertEquals(layout.size(), volumeListInOrder.size());

    int listSize = layout.size();
    for (int i = 0; i < listSize; i++) {
      assertEquals(layout.get(i), volumeListInOrder.get(i));
    }
  }

  /*
   * Test for simple volume
   * EachTimeCount more than  WrappedCount. AND segment has not been create completely
   *  Return layout <3,>
   */
  @Test
  public void testUpdateVolumeInformation15() {
    int segmentCountOfFirstVolume = 11;

    final int segmentEachTime = 6;
    final int segmentWappedCount = 3;

    final long firstVolumeId = RequestIdBuilder.get();

    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setSegmentSize(SEGMENT_SIZE);
    long allSegmentCount = segmentCountOfFirstVolume;
    long volumeSize = allSegmentCount * SEGMENT_SIZE;

    //the extend volume, make each
    List<Integer> values = new ArrayList<>();
    values.add(segmentCountOfFirstVolume);
    String eachTimeExtendVolumeSize = makeEachTimeExtendVolumeSize(values);

    volumeMetadata.setEachTimeExtendVolumeSize(eachTimeExtendVolumeSize);
    volumeMetadata.setVolumeSize(volumeSize);
    volumeMetadata.setSegmentNumToCreateEachTime(segmentEachTime);
    volumeMetadata.setSegmentWrappCount(segmentWappedCount);
    volumeMetadata.setVolumeId(firstVolumeId);
    volumeMetadata.setRootVolumeId(volumeMetadata.getVolumeId());

    SegmentMembership membership = mock(SegmentMembership.class);
    int first = 0;
    int second = 0;
    int third = 0;

    int hasCreatedCount = 5;
    for (int i = 0; i < hasCreatedCount; i++) {
      SegId segId;
      first++;
      segId = new SegId(firstVolumeId, i);
      SegmentMetadata segmentMetadata = new SegmentMetadata(segId, i);
      volumeMetadata.addSegmentMetadata(segmentMetadata, membership);
    }
    assertEquals(first, hasCreatedCount);

    try {
      when(volumeInfoRetriever.getVolume(anyLong()))
          .thenReturn(new SpaceSavingVolumeMetadata(volumeMetadata));
      calculator.updateVolumeInformation();
    } catch (Exception e) {
      assertTrue(false);
    }

    //<3,>
    List<Long> layout = calculator.getVolumeLayout();

    List<Long> volumeListInOrder = new ArrayList<Long>(Arrays.asList(3L));

    assertEquals(layout.size(), volumeListInOrder.size());

    int listSize = layout.size();
    for (int i = 0; i < listSize; i++) {
      assertEquals(layout.get(i), volumeListInOrder.get(i));
    }
  }

  /*
   * Test for simple volume
   * EachTimeCount less than  WrappedCount, AND segment not been create completely
   *  Return layout
   */
  @Test
  public void testUpdateVolumeInformation16() {
    int segmentCountOfFirstVolume = 20;

    final int segmentEachTime = 5;
    final int segmentWappedCount = 10;

    final long firstVolumeId = RequestIdBuilder.get();

    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setSegmentSize(SEGMENT_SIZE);
    long allSegmentCount = segmentCountOfFirstVolume;
    long volumeSize = allSegmentCount * SEGMENT_SIZE;

    //the extend volume, make each
    List<Integer> values = new ArrayList<>();
    values.add(segmentCountOfFirstVolume);
    String eachTimeExtendVolumeSize = makeEachTimeExtendVolumeSize(values);

    volumeMetadata.setEachTimeExtendVolumeSize(eachTimeExtendVolumeSize);
    volumeMetadata.setVolumeSize(volumeSize);
    volumeMetadata.setSegmentNumToCreateEachTime(segmentEachTime);
    volumeMetadata.setSegmentWrappCount(segmentWappedCount);
    volumeMetadata.setVolumeId(firstVolumeId);
    volumeMetadata.setRootVolumeId(volumeMetadata.getVolumeId());

    SegmentMembership membership = mock(SegmentMembership.class);
    int first = 0;
    int second = 0;
    int third = 0;

    int hasCreatedCount = 5;
    for (int i = 0; i < hasCreatedCount; i++) {
      SegId segId;
      first++;
      segId = new SegId(firstVolumeId, i);
      SegmentMetadata segmentMetadata = new SegmentMetadata(segId, i);
      volumeMetadata.addSegmentMetadata(segmentMetadata, membership);
    }
    assertEquals(first, hasCreatedCount);

    try {
      when(volumeInfoRetriever.getVolume(anyLong()))
          .thenReturn(new SpaceSavingVolumeMetadata(volumeMetadata));
      calculator.updateVolumeInformation();
    } catch (Exception e) {
      assertTrue(false);
    }

    //<3>
    List<Long> layout = calculator.getVolumeLayout();

    List<Long> volumeListInOrder = new ArrayList<Long>(Arrays.asList(5L));

    assertEquals(layout.size(), volumeListInOrder.size());

    int listSize = layout.size();
    for (int i = 0; i < listSize; i++) {
      assertEquals(layout.get(i), volumeListInOrder.get(i));
    }
  }


  /**
   * xx.
   */
  public String makeEachTimeExtendVolumeSize(List<Integer> values) {
    String result = null;
    for (Integer i : values) {
      if (result == null) {
        result = String.valueOf(i) + ",";
      } else {
        result = result + String.valueOf(i) + ",";
      }
    }
    logger.warn("get the result is :{}", result);
    return result;
  }

}
