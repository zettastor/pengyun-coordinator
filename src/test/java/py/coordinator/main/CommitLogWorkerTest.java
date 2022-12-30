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

package py.coordinator.main;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import py.archive.segment.SegId;
import py.common.RequestIdBuilder;
import py.connection.pool.PyConnection;
import py.coordinator.iofactory.WriteFactory;
import py.coordinator.lib.Coordinator;
import py.coordinator.log.BroadcastLog;
import py.coordinator.logmanager.LogRecorder;
import py.coordinator.logmanager.LogRecorderImpl;
import py.coordinator.logmanager.UncommittedLogManagerImpl;
import py.coordinator.logmanager.WriteIoContextManager;
import py.coordinator.worker.CommitLogWorker;
import py.coordinator.worker.CommitLogWorkerImpl;
import py.coordinator.worker.CommitWorkProgress;
import py.exception.GenericThriftClientFactoryException;
import py.icshare.BroadcastLogStatus;
import py.instance.InstanceId;
import py.membership.IoActionContext;
import py.membership.IoMember;
import py.membership.SegmentMembership;
import py.netty.core.MethodCallback;
import py.netty.core.Protocol;
import py.netty.datanode.AsyncDataNode;
import py.proto.Commitlog;
import py.test.TestBase;
import py.test.TestUtils;
import py.volume.VolumeType;

/**
 * xx.
 */
public class CommitLogWorkerTest extends TestBase {

  private static Semaphore keepCommitting = new Semaphore(1);
  private static Commitlog.PbCommitlogResponse response = mock(Commitlog.PbCommitlogResponse.class);
  private AtomicInteger responseCount = new AtomicInteger(0);
  private Long volumeId = RequestIdBuilder.get();
  private Coordinator coordinator = mock(Coordinator.class);
  @Mock
  private PyConnection connection;
  @Mock
  private Protocol protocol;
  private AsyncDataNode.AsyncIface primaryClient = new AsyncDataNodeAsyncIfaceTest();

  private AsyncDataNode.AsyncIface primaryKeepCommittingClient =
      new AsyncDataNodeAsyncIfaceKeepCommittingTest();

  private LogRecorder logRecorder = mock(LogRecorderImpl.class);

  private UncommittedLogManagerImpl uncommittedLogManager = mock(UncommittedLogManagerImpl.class);

  private WriteIoContextManager manager = mock(WriteIoContextManager.class);


  /**
   * xx.
   */
  @Before
  public void init() throws GenericThriftClientFactoryException {
    when(coordinator.getDatanodeNettyClient(any(InstanceId.class))).thenReturn(primaryClient);
    when(coordinator.getLogRecorder()).thenReturn(logRecorder);
    when(coordinator.getVolumeType(anyLong())).thenReturn(VolumeType.REGULAR);
    SegmentMembership membership = TestUtils.generateMembership();
    when(coordinator.getSegmentMembership(anyLong(), any(SegId.class))).thenReturn(membership);
    WriteFactory writeFactory = mock(WriteFactory.class);
    when(coordinator.getWriteFactory()).thenReturn(writeFactory);
    IoActionContext ioActionContext = mock(IoActionContext.class);
    IoMember ioMember = mock(IoMember.class);
    Set<IoMember> ioMembers = new HashSet<>();
    InstanceId instanceId = mock(InstanceId.class);
    when(ioMember.getInstanceId()).thenReturn(instanceId);
    ioMembers.add(ioMember);

    when(ioActionContext.getIoMembers()).thenReturn(ioMembers);
    when(ioActionContext.isResendDirectly()).thenReturn(false);
    when(writeFactory
        .generateIoMembers(any(SegmentMembership.class), any(VolumeType.class), any(SegId.class),
            anyLong()))
        .thenReturn(ioActionContext);
    // when(logRecorder.getCount(any(SegId.class))).thenReturn(2);
    List<WriteIoContextManager> managerList = new ArrayList<>();
    managerList.add(manager);

    when(manager.getLogsToCreate()).thenReturn(buildLogList());
    when(manager.getRequestId()).thenReturn(RequestIdBuilder.get());
    when(manager.isAllFinalStatus()).thenReturn(false);
    when(coordinator.getUncommittedLogManager()).thenReturn(uncommittedLogManager);
    when(uncommittedLogManager.pollLogManagerToCommit(anyLong(), any(SegId.class)))
        .thenReturn(managerList);
  }

  @Test
  public void testPutManagerWhileCommitting() throws InterruptedException {
    try {
      keepCommitting.acquire();
    } catch (Exception e) {
      assertTrue(false);
    }

    // when(manager.isAllFinalStatus()).thenReturn(true);
    when(coordinator.getDatanodeNettyClient(any(InstanceId.class)))
        .thenReturn(primaryKeepCommittingClient);

    CommitLogWorker commitLogWorker = new CommitLogWorkerImpl(coordinator);
    when(coordinator.getCommitLogWorker()).thenReturn(commitLogWorker);
    commitLogWorker.start();
    SegId segId1 = new SegId(10000L, 0);
    boolean result = commitLogWorker.put(volumeId, segId1, false);
    assertTrue(result);
    assertEquals(0, responseCount.get());

    result = commitLogWorker.put(volumeId, segId1, false);
    assertTrue(!result);

    keepCommitting.release();

    // wait for work processing
    Thread.sleep(2000);
    // List<WriteIoContextManager> managerList = new ArrayList<>();
    // when(uncommittedLogManager.pollLogManagerToCommit(anyLong(), any(SegId.class))).thenReturn
    // (managerList);
    assertTrue("response count: " + responseCount.get(), responseCount.get() == 1);

    // wait for work processing, because when pull empty managers, mark as commitable
    Thread.sleep(500);
    result = commitLogWorker.put(volumeId, segId1, false);
    assertTrue(result);
  }

  @Test
  public void testCompareAndSet() {
    AtomicReference<CommitWorkProgress> testReference = new AtomicReference<>(
        CommitWorkProgress.Commitable);

    assertEquals(testReference.get(), CommitWorkProgress.Commitable);
    boolean result = testReference
        .compareAndSet(CommitWorkProgress.Commitable, CommitWorkProgress.CommitWaiting);
    assertTrue(result);
    assertEquals(testReference.get(), CommitWorkProgress.CommitWaiting);

    result = testReference
        .compareAndSet(CommitWorkProgress.Commitable, CommitWorkProgress.Committing);
    assertTrue(!result);
    assertEquals(testReference.get(), CommitWorkProgress.CommitWaiting);

    CommitWorkProgress progress = testReference.getAndSet(CommitWorkProgress.Committing);
    assertEquals(progress, CommitWorkProgress.CommitWaiting);

    progress = testReference.getAndSet(CommitWorkProgress.Commitable);
    assertEquals(progress, CommitWorkProgress.Committing);
  }

  @Test
  public void testPutFilter() {
    CommitLogWorker commitLogWorker = new CommitLogWorkerImpl(coordinator);
    SegId segId1 = new SegId(10000L, 0);
    boolean result = commitLogWorker.put(volumeId, segId1, false);
    assertTrue(result);
    // put segId1 again
    result = commitLogWorker.put(volumeId, segId1, false);
    assertTrue(!result);

    SegId segId2 = new SegId(10000L, 1);
    result = commitLogWorker.put(volumeId, segId2, false);
    assertTrue(result);
  }

  @Test
  public void testStop() throws InterruptedException {
    CommitLogWorker commitLogWorker = new CommitLogWorkerImpl(coordinator);
    commitLogWorker.start();
    int segTaskCount = 1000;
    for (int i = 0; i < segTaskCount; i++) {
      SegId segId = new SegId(10000L, i);
      boolean result = commitLogWorker.put(volumeId, segId, false);
      assertTrue(result);
    }
    Thread.sleep(1000);
    commitLogWorker.stop();
    assertEquals(segTaskCount, responseCount.get());
  }

  @Test
  public void testRunning() throws GenericThriftClientFactoryException, InterruptedException {
    CommitLogWorker commitLogWorker = new CommitLogWorkerImpl(coordinator);
    commitLogWorker.start();
    int segTaskCount = 50;
    Thread.sleep(1000);
    for (int i = 0; i < segTaskCount; i++) {
      SegId segId = new SegId(10000L, i);
      commitLogWorker.put(volumeId, segId, false);
    }

    Thread.sleep(2000);
    assertEquals(segTaskCount, responseCount.get());
  }

  @Test
  public void testMapRefer() {
    Map<Long, List<BroadcastLog>> testMap = new HashMap<Long, List<BroadcastLog>>();
    List<BroadcastLog> logList = new ArrayList<BroadcastLog>();
    for (int i = 0; i < 3; i++) {
      BroadcastLog broadcastLog = new BroadcastLog(i);
      logList.add(broadcastLog);
    }
    testMap.put(10000L, logList);

    for (Entry<Long, List<BroadcastLog>> entry : testMap.entrySet()) {
      for (BroadcastLog broadcastLog : entry.getValue()) {
        assertEquals(BroadcastLogStatus.Creating, broadcastLog.getStatus());
      }
    }

    for (Entry<Long, List<BroadcastLog>> entry : testMap.entrySet()) {
      setBroadcastLosStatus(entry.getValue());
    }

    for (Entry<Long, List<BroadcastLog>> entry : testMap.entrySet()) {
      for (BroadcastLog broadcastLog : entry.getValue()) {
        assertEquals(BroadcastLogStatus.Abort, broadcastLog.getStatus());
      }
    }
  }

  @Test
  public void testSubList() {
    List<Long> testList = new ArrayList<>();
    int wholeCount = (int) (RequestIdBuilder.get() % 1000);
    int perCount = 100;
    for (Long i = 0L; i < wholeCount; i++) {
      testList.add(i);
    }

    int loopCount = 0;
    int beginPosition = 0;
    while (wholeCount > 0) {
      int oneLoopIncCount = perCount > wholeCount ? wholeCount : perCount;
      final List<Long> subList = testList.subList(beginPosition, beginPosition + oneLoopIncCount);
      beginPosition += oneLoopIncCount;
      wholeCount -= oneLoopIncCount;
      loopCount++;
      assertTrue(subList.size() == oneLoopIncCount);
      assertTrue(oneLoopIncCount <= perCount);
    }

    int expectLoopCount = testList.size() / perCount;
    if (testList.size() % perCount != 0) {
      expectLoopCount++;
    }
    assertEquals(0, wholeCount);
    assertEquals(expectLoopCount, loopCount);
  }

  @Test
  public void testConCurrentHashMapPutIfAbsent() {
    final Map<Long, Long> testMap = new ConcurrentHashMap<>();
    Long preValue = testMap.putIfAbsent(RequestIdBuilder.get(), RequestIdBuilder.get());
    assertNull(preValue);
    testMap.clear();

    AtomicInteger putCount = new AtomicInteger(0);
    int baseCount = 100;
    int threadCount = RandomUtils.nextInt(baseCount);

    // 100 <= threadCount < 200
    threadCount += baseCount;

    CountDownLatch oneLatch = new CountDownLatch(1);
    CountDownLatch threadLatch = new CountDownLatch(threadCount);

    final int loopCount = threadCount;

    for (int i = 0; i < loopCount; i++) {
      Thread thread = new Thread() {
        @Override
        public void run() {
          try {
            oneLatch.await();
            for (long j = 0; j < loopCount; j++) {
              Long preValue = testMap.putIfAbsent(j, j);
              if (preValue == null) {
                putCount.incrementAndGet();
              }
            }
          } catch (Exception e) {
            fail();
          } finally {
            threadLatch.countDown();
          }
        }
      };
      thread.start();
    }

    oneLatch.countDown();

    try {
      threadLatch.await();
    } catch (Exception e) {
      fail();
    }

    assertEquals(threadCount, putCount.get());
  }

  private void setBroadcastLosStatus(List<BroadcastLog> logs) {
    for (BroadcastLog broadcastLog : logs) {
      broadcastLog.setStatus(BroadcastLogStatus.Abort);
    }
  }

  private List<BroadcastLog> buildLogList() {
    List<BroadcastLog> logList = new ArrayList<BroadcastLog>();
    for (int k = 0; k < 5; k++) {
      BroadcastLog log = new BroadcastLog((long) k);
      logList.add(log);
    }
    return logList;
  }

  private class AsyncDataNodeAsyncIfaceTest extends AsyncDataNode.Client {

    public AsyncDataNodeAsyncIfaceTest() {
      super(connection, protocol);
    }

    @Override
    public void addOrCommitLogs(Commitlog.PbCommitlogRequest request,
        MethodCallback<Commitlog.PbCommitlogResponse> callback) {
      responseCount.incrementAndGet();
    }
  }

  private class AsyncDataNodeAsyncIfaceKeepCommittingTest extends AsyncDataNode.Client {

    public AsyncDataNodeAsyncIfaceKeepCommittingTest() {
      super(connection, protocol);
    }

    @Override
    public void addOrCommitLogs(Commitlog.PbCommitlogRequest request,
        MethodCallback<Commitlog.PbCommitlogResponse> callback) {
      try {
        keepCommitting.acquire();
      } catch (Exception e) {
        assertTrue(false);
      } finally {
        keepCommitting.release();
      }
      responseCount.incrementAndGet();
      callback.complete(response);
    }
  }
}
