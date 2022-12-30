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

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.junit.Test;
import org.mockito.Mock;
import py.common.RequestIdBuilder;
import py.common.struct.Pair;
import py.coordinator.log.LogResult;
import py.coordinator.logmanager.WriteIoContextManager;
import py.coordinator.logmanager.WriteMethodCallback;
import py.icshare.BroadcastLogStatus;
import py.membership.IoActionContext;
import py.membership.MemberIoStatus;
import py.membership.SegmentForm;
import py.proto.Broadcastlog.PbBroadcastLog;
import py.proto.Broadcastlog.PbBroadcastLogManager;
import py.proto.Broadcastlog.PbBroadcastLogStatus;
import py.proto.Broadcastlog.PbIoUnitResult;
import py.proto.Broadcastlog.PbWriteResponse;
import py.proto.Broadcastlog.PbWriteResponseUnit;
import py.test.TestBase;
import py.volume.VolumeType;

/**
 * xx.
 */
public class WriteLogResultPssaaTest extends TestBase {

  private static final long REQUEST_ID = RequestIdBuilder.get();
  private static final long LOG_ID = RequestIdBuilder.get();
  @Mock
  private WriteMethodCallback primaryCallBack;
  @Mock
  private WriteMethodCallback secondary1CallBack;
  @Mock
  private WriteMethodCallback secondary2CallBack;
  @Mock
  private IoActionContext ioActionContext;
  @Mock
  private WriteIoContextManager contextManager;

  private void asignReturnValueToWriteMethodCallback(WriteMethodCallback callBack,
      PbIoUnitResult result,
      MemberIoStatus memberIoStatus) {
    PbWriteResponse response = mock(PbWriteResponse.class);
    when(callBack.getResponse()).thenReturn(response);
    when(callBack.getMemberIoStatus()).thenReturn(memberIoStatus);

    PbWriteResponseUnit responseUnit = mock(PbWriteResponseUnit.class);
    when(response.getResponseUnits(anyInt())).thenReturn(responseUnit);
    when(responseUnit.getLogId()).thenReturn(LOG_ID);
    when(responseUnit.getLogResult()).thenReturn(result);
  }

  private void asignReturnValueToWriteMethodCallback(WriteMethodCallback callBack,
      PbBroadcastLogStatus result,
      MemberIoStatus memberIoStatus) {
    PbWriteResponse response = mock(PbWriteResponse.class);
    when(callBack.getResponse()).thenReturn(response);
    when(callBack.getMemberIoStatus()).thenReturn(memberIoStatus);

    List logList = mock(List.class);
    when(logList.size()).thenReturn(1);
    when(contextManager.getLogsToCommit()).thenReturn(logList);
    when(contextManager.getRequestId()).thenReturn(REQUEST_ID);
    when(contextManager.getIoActionContext()).thenReturn(ioActionContext);

    PbBroadcastLogManager pbManager = mock(PbBroadcastLogManager.class);
    PbBroadcastLog pbLog = mock(PbBroadcastLog.class);

    when(response.getLogManagersToCommit(anyInt())).thenReturn(pbManager);
    when(pbManager.getBroadcastLogs(anyInt())).thenReturn(pbLog);
    when(pbManager.getRequestId()).thenReturn(REQUEST_ID);
    when(pbLog.getLogStatus()).thenReturn(result);
  }

  private void asignReturnValueToIoActionContext(SegmentForm segmentForm) {
    when(ioActionContext.getSegmentForm()).thenReturn(segmentForm);
    mockAllMemberAlive();
  }

  private void mockPrimaryStatus(boolean isDown) {
    when(ioActionContext.isPrimaryDown()).thenReturn(isDown);
  }

  private void mockSecondaryStatus(boolean isDown) {
    when(ioActionContext.isSecondaryDown()).thenReturn(isDown);
  }

  private void mockSetIoCountOne() {
    when(ioActionContext.getTotalWriteCount()).thenReturn(1);
  }

  private void mockSetIoCountTwo() {
    when(ioActionContext.getTotalWriteCount()).thenReturn(2);
  }

  private void mockSetIoCountThree() {
    when(ioActionContext.getTotalWriteCount()).thenReturn(3);
  }

  private void mockJoiningSecondaryStatus(boolean isDown) {
    when(ioActionContext.isJoiningSecondaryDown()).thenReturn(isDown);
  }

  private void mockOnlyPrimaryDown() {
    mockPrimaryStatus(true);
    mockSecondaryStatus(false);
    mockJoiningSecondaryStatus(false);
  }

  private void mockOnlySecondaryDown() {
    mockPrimaryStatus(false);
    mockSecondaryStatus(true);
    mockJoiningSecondaryStatus(false);
  }

  private void mockOnlyJoiningSecondaryDown() {
    mockPrimaryStatus(false);
    mockSecondaryStatus(false);
    mockJoiningSecondaryStatus(true);
  }

  private void mockAllMemberAlive() {
    mockPrimaryStatus(false);
    mockSecondaryStatus(false);
    mockJoiningSecondaryStatus(false);
  }

  @Test
  public void testMergeCreateResultOfPssaa() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[3];

    callBacks[0] = primaryCallBack;
    callBacks[1] = secondary1CallBack;
    callBacks[2] = secondary2CallBack;

    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.PSSAA);
    // primary is OK, but we need one primary committed at least
    // P,S1,S2 OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.SECONDARY_NOT_STABLE,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    mockSetIoCountThree();
    Pair<BroadcastLogStatus, Long> logStatusAndId;
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    //Three P committed ,S1,S2 OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.SECONDARY_NOT_STABLE,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    mockSetIoCountThree();
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    //Two PS
    WriteMethodCallback[] callBacks2 = new WriteMethodCallback[2];
    callBacks2[0] = primaryCallBack;
    callBacks2[1] = secondary1CallBack;
    mockSetIoCountTwo();
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.SECONDARY_NOT_STABLE,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks2, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    //Two SS
    WriteMethodCallback[] callBacks3 = new WriteMethodCallback[2];
    callBacks3[0] = secondary1CallBack;
    callBacks3[1] = secondary2CallBack;
    mockSetIoCountTwo();
    mockPrimaryStatus(true);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks3, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    //One P
    WriteMethodCallback[] callBacks4 = new WriteMethodCallback[1];
    callBacks4[0] = primaryCallBack;
    mockSetIoCountOne();
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks4, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    //One S
    WriteMethodCallback[] callBacks5 = new WriteMethodCallback[1];
    callBacks5[0] = secondary1CallBack;
    mockSetIoCountOne();
    mockPrimaryStatus(true);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks5, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P committed,S1 OK, S2 not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.SECONDARY_NOT_STABLE,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    mockSetIoCountThree();
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed, S1 & S2 not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    mockSetIoCountThree();
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P OutOfRange
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OUT_OF_RANGE,
        MemberIoStatus.Primary);
    mockSetIoCountOne();
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Abort);

    // P InputHasNoData
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.INPUT_HAS_NO_DATA,
        MemberIoStatus.Primary);
    mockSetIoCountOne();
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Abort);

    // primary is down
    // P not OK, S1, S2 OK
    mockOnlyPrimaryDown();
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    mockSetIoCountThree();
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // given primary committed to secondary 2
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Secondary);
    mockSetIoCountThree();
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P not OK, S1 not OK, S2 OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Secondary);
    mockSetIoCountThree();
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // secondary is down
    mockOnlySecondaryDown();
    // P primary committed, S2 OK, S1 not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    mockSetIoCountThree();
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P primary committed, S2 not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    mockSetIoCountThree();
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // callbacks == null
    mockSetIoCountThree();
    logStatusAndId = LogResult
        .mergeCreateResult((WriteMethodCallback[]) null, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // callbacks is empty
    callBacks = new WriteMethodCallback[0];
    mockSetIoCountThree();
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);
  }

  @Test
  public void testMergeCreateResultOfPssai() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[3];

    callBacks[0] = primaryCallBack;
    callBacks[1] = secondary1CallBack;
    callBacks[2] = secondary2CallBack;

    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.PSSAI);
    // primary is OK, but we need one primary committed at least
    // P,S1,S2 OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.SECONDARY_NOT_STABLE,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    mockSetIoCountThree();
    Pair<BroadcastLogStatus, Long> logStatusAndId;
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    //Three P committed ,S1,S2 OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.SECONDARY_NOT_STABLE,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    mockSetIoCountThree();
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    //Two PS
    WriteMethodCallback[] callBacks2 = new WriteMethodCallback[2];
    callBacks2[0] = primaryCallBack;
    callBacks2[1] = secondary1CallBack;
    mockSetIoCountTwo();
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.SECONDARY_NOT_STABLE,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks2, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    //Two SS
    WriteMethodCallback[] callBacks3 = new WriteMethodCallback[2];
    callBacks3[0] = secondary1CallBack;
    callBacks3[1] = secondary2CallBack;
    mockSetIoCountTwo();
    mockPrimaryStatus(true);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks3, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P committed,S1 OK, S2 not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.SECONDARY_NOT_STABLE,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    mockSetIoCountThree();
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed, S1 & S2 not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    mockSetIoCountThree();
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P OutOfRange
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OUT_OF_RANGE,
        MemberIoStatus.Primary);
    mockSetIoCountOne();
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Abort);

    // P InputHasNoData
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.INPUT_HAS_NO_DATA,
        MemberIoStatus.Primary);
    mockSetIoCountOne();
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Abort);

    // primary is down
    // P not OK, S1, S2 OK
    mockOnlyPrimaryDown();
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    mockSetIoCountThree();
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // given primary committed to secondary 2
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Secondary);
    mockSetIoCountThree();
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P not OK, S1 not OK, S2 OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Secondary);
    mockSetIoCountThree();
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // secondary is down
    mockOnlySecondaryDown();
    // P primary committed, S2 OK, S1 not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    mockSetIoCountThree();
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P primary committed, S2 not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    mockSetIoCountThree();
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // callbacks == null
    mockSetIoCountThree();
    logStatusAndId = LogResult
        .mergeCreateResult((WriteMethodCallback[]) null, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // callbacks is empty
    callBacks = new WriteMethodCallback[0];
    mockSetIoCountThree();
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.LARGE);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);
  }
}

