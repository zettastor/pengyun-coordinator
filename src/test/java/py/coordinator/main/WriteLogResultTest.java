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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
public class WriteLogResultTest extends TestBase {

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
  public void testMergeCreateResultOfPss() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[3];

    callBacks[0] = primaryCallBack;
    callBacks[1] = secondary1CallBack;
    callBacks[2] = secondary2CallBack;

    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.PSS);
    // primary is OK, but we need one primary committed at least
    // P,S1,S2 OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.SECONDARY_NOT_STABLE,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    Pair<BroadcastLogStatus, Long> logStatusAndId;
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed ,S1,S2 OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.SECONDARY_NOT_STABLE,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P committed,S1 OK, S2 not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.SECONDARY_NOT_STABLE,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P committed, S1 & S2 not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P OutOfRange
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OUT_OF_RANGE,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Abort);

    // P InputHasNoData
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.INPUT_HAS_NO_DATA,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
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

    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // given primary committed to secondary 2
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Secondary);

    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P not OK, S1 not OK, S2 OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
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

    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P primary committed, S2 not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);

    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // callbacks == null
    logStatusAndId = LogResult
        .mergeCreateResult((WriteMethodCallback[]) null, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // callbacks is empty
    callBacks = new WriteMethodCallback[0];
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);
  }

  @Test
  public void testMergeCreateResultOfPsj() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[3];

    callBacks[0] = primaryCallBack;
    callBacks[1] = secondary1CallBack;
    callBacks[2] = secondary2CallBack;

    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.PSJ);

    // P & S & J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.OK,
        MemberIoStatus.JoiningSecondary);

    Pair<BroadcastLogStatus, Long> logStatusAndId;
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed, S & J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.OK,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // J committed, P & S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // S committed, P & J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.OK,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed, S OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P committed, S & J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed, S1 not OK, J1 is OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.OK,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // S is down
    mockOnlySecondaryDown();

    // P committed, S not OK, J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.OK,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P committed, S & J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // primary is down
    mockOnlyPrimaryDown();

    // P not OK, S committed, J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.OK,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P not OK, S OK, J committed
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P not OK, S not OK, J committed
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P not, S OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P not OK, S1 committed, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // J is down
    mockOnlyJoiningSecondaryDown();

    // P committed, S OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P committed, S not OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);
  }

  @Test
  public void testMergeCreateResultOfPsi() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[2];

    callBacks[0] = primaryCallBack;
    callBacks[1] = secondary1CallBack;

    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.PSI);

    // P & S1 OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    Pair<BroadcastLogStatus, Long> logStatusAndId;
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed,S1 OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P committed, S1 not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // primary is down
    mockOnlyPrimaryDown();

    // P not OK, S1 is OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // S is committed
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P not OK, S1 not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // secondary is down
    mockOnlySecondaryDown();

    // P OK, S1 not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed, S1 not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P not OK, S1 not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

  }

  @Test
  public void testMergeCreateResultOfPji() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[2];

    callBacks[0] = primaryCallBack;
    callBacks[1] = secondary1CallBack;

    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.PJI);

    // P & J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.JoiningSecondary);
    Pair<BroadcastLogStatus, Long> logStatusAndId;
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P PrimaryCommitted, J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P OK, J PrimaryCommitted
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed,J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // primary is down
    mockOnlyPrimaryDown();

    // P committed, J OK, still create false
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P not OK, J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P not OK, J committed
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P not OK, S1 not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // J is down
    mockOnlyJoiningSecondaryDown();

    // P committed, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);
  }

  @Test
  public void testMergeCreateResultOfPjj() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[3];

    callBacks[0] = primaryCallBack;
    callBacks[1] = secondary1CallBack;
    callBacks[2] = secondary2CallBack;

    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.PJJ);

    // P & J1 & J2 OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.JoiningSecondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.OK,
        MemberIoStatus.JoiningSecondary);
    Pair<BroadcastLogStatus, Long> logStatusAndId;
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed, J1 & J2 OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.JoiningSecondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.OK,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // primary down
    mockOnlyPrimaryDown();

    // P not OK, J1 committed, J2 OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.JoiningSecondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.OK,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // J down
    mockOnlyJoiningSecondaryDown();
    // P committed, J1 OK,  J2 not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.JoiningSecondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

  }

  @Test
  public void testMergeCreateResultOfPii() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[3];

    callBacks[0] = primaryCallBack;
    callBacks[1] = secondary1CallBack;
    callBacks[2] = secondary1CallBack;

    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.PII);

    // P OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);

    Pair<BroadcastLogStatus, Long> logStatusAndId;
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P down
    mockOnlyPrimaryDown();

    // P committed
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

  }

  @Test
  public void testMergeCreateResultOfPs() {

    WriteMethodCallback[] callBacks = new WriteMethodCallback[2];

    callBacks[0] = primaryCallBack;
    callBacks[1] = secondary1CallBack;

    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.PS);

    // P & S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    Pair<BroadcastLogStatus, Long> logStatusAndId;
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P OK, S committed
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed, S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // Primary down
    mockOnlyPrimaryDown();

    // P committed, S committed
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed, S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // Secondary down
    mockOnlySecondaryDown();

    // P committed, S not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P OK, S not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

  }

  @Test
  public void testMergeCreateResultOfPj() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[2];

    callBacks[0] = primaryCallBack;
    callBacks[1] = secondary1CallBack;

    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.PJ);

    // P & J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.JoiningSecondary);

    Pair<BroadcastLogStatus, Long> logStatusAndId;
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed, J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P OK, J committed
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P not OK, J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // primary is down
    mockOnlyPrimaryDown();

    // P committed, J committed
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P not OK, J committed
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // J is down
    mockOnlyJoiningSecondaryDown();

    // P committed, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);
  }

  @Test
  public void testMergeCreateResultOfPi() {

    WriteMethodCallback[] callBacks = new WriteMethodCallback[1];

    callBacks[0] = primaryCallBack;

    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.PI);

    // P OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    Pair<BroadcastLogStatus, Long> logStatusAndId;
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // Primary down
    mockOnlyPrimaryDown();

    // P committed
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);
  }

  @Test
  public void testMergeCreateResultOfPsa() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[2];

    callBacks[0] = primaryCallBack;
    callBacks[1] = secondary1CallBack;

    // primary is alive
    asignReturnValueToIoActionContext(SegmentForm.PSA);

    // P & S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    Pair<BroadcastLogStatus, Long> logStatusAndId;
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed, S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P OK, S committed
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed, S not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P OutOfRange
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OUT_OF_RANGE,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Abort);

    // P InputHasNoData
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.INPUT_HAS_NO_DATA,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Abort);

    // primary is down
    mockOnlyPrimaryDown();

    // P not OK, S committed
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P not OK, S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P not OK, S1 not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // Secondary is down
    mockOnlySecondaryDown();

    // P committed, S not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P OK, S not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P not OK, S1 not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

  }

  @Test
  public void testMergeCreateResultOfPja() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[2];

    callBacks[0] = primaryCallBack;
    callBacks[1] = secondary1CallBack;

    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.PJA);

    // P & J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.JoiningSecondary);
    Pair<BroadcastLogStatus, Long> logStatusAndId;
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed, J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P OK,J committed
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P OK,J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // primary is down
    mockOnlyPrimaryDown();

    // P not OK, J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P not OK, J committed
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P not OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // Joining Secondary is down
    mockOnlyJoiningSecondaryDown();

    // P OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P not OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

  }

  @Test
  public void testMergeCreateResultOfPia() {

    WriteMethodCallback[] callBacks = new WriteMethodCallback[1];

    callBacks[0] = primaryCallBack;

    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.PIA);

    // P OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    Pair<BroadcastLogStatus, Long> logStatusAndId;
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // Primary is down
    mockOnlyPrimaryDown();

    // P OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.REGULAR);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

  }

  @Test
  public void testMergeCreateResultOfPa() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[1];

    callBacks[0] = primaryCallBack;

    // primary is alive
    asignReturnValueToIoActionContext(SegmentForm.PA);

    // P OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    Pair<BroadcastLogStatus, Long> logStatusAndId;
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // primary is down
    mockOnlyPrimaryDown();

    // P OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);
  }

  @Test
  public void testMergeCreateResultOfTps() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[2];

    callBacks[0] = primaryCallBack;
    callBacks[1] = secondary1CallBack;

    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.TPS);

    // P & S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    Pair<BroadcastLogStatus, Long> logStatusAndId;
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed, S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P OK,S committed
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed,S not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P OK,S not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // primary is down
    mockOnlyPrimaryDown();

    // P not OK, S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P not OK, S committed
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P not OK, S1 not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // secondary is down
    mockOnlySecondaryDown();

    // P OK, S not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed, S not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P not OK, S not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Secondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);
  }

  @Test
  public void testMergeCreateResultOfTpj() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[2];

    callBacks[0] = primaryCallBack;
    callBacks[1] = secondary1CallBack;

    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.TPJ);

    // P & J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.JoiningSecondary);
    Pair<BroadcastLogStatus, Long> logStatusAndId;
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed, J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P OK, J committed
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // J down
    mockOnlyJoiningSecondaryDown();

    // P committed, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P not OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // primary is down
    mockOnlyPrimaryDown();

    // P not OK, S1 OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.OK,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P not OK, J committed
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P not OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.JoiningSecondary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);
  }

  @Test
  public void testMergeCreateResultOfTpi() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[1];

    callBacks[0] = primaryCallBack;

    // primary is alive
    asignReturnValueToIoActionContext(SegmentForm.TPI);

    // P OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    Pair<BroadcastLogStatus, Long> logStatusAndId;
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Created);

    // P not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.EXHAUSTED,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // primary is down
    mockOnlyPrimaryDown();

    // P not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.BROADCAST_FAILED,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.OK,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);

    // P committed
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbIoUnitResult.PRIMARY_COMMITTED,
        MemberIoStatus.Primary);
    logStatusAndId = LogResult.mergeCreateResult(callBacks, 0, ioActionContext, VolumeType.SMALL);
    assertTrue(logStatusAndId.getFirst() == BroadcastLogStatus.Creating);
  }

  // commit result sorts
  @Test
  public void testCommitResultOfPss() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[3];

    callBacks[0] = primaryCallBack;
    callBacks[1] = secondary1CallBack;
    callBacks[2] = secondary2CallBack;

    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.PSS);

    // P & S1 & S2 OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    int successTimes = 0;
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P & S1 OK, S2 not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P is OK, S1, S2 not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // primary is down
    mockOnlyPrimaryDown();

    // P down, S1, J1 OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P down, S1 not OK, S2 is OK
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P down, S1 & S2 not OK
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));
  }

  @Test
  public void testCommitResultOfPsj() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[3];

    callBacks[0] = primaryCallBack;
    callBacks[1] = secondary1CallBack;
    callBacks[2] = secondary2CallBack;

    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.PSJ);

    // P & S & J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    int successTimes = 0;
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P & S OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P OK, S not OK, J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // secondary down
    mockSecondaryStatus(true);
    // P OK, S not OK, J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    mockSecondaryStatus(false);

    // P OK, S & J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // primary is down
    mockPrimaryStatus(true);

    // P not OK, S & J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK, S not OK, J is OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK, S OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P & S & J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));
  }

  @Test
  public void testCommitResultOfPsi() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[2];

    callBacks[0] = primaryCallBack;
    callBacks[1] = secondary1CallBack;

    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.PSI);

    // P & S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    int successTimes = 0;
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P OK, S not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // primary is down
    mockPrimaryStatus(true);

    // P OK, S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK, S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK, S not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // secondary is down
    mockOnlySecondaryDown();

    // P OK, S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    // did not check secondary down, still success
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P OK, S not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK, S not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));
  }

  @Test
  public void testCommitResultOfPji() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[2];

    callBacks[0] = primaryCallBack;
    callBacks[1] = secondary1CallBack;

    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.PJI);

    // P & J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    int successTimes = 0;
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // primary is down
    mockOnlyPrimaryDown();

    // P not OK, J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P OK, J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // J is down
    mockOnlyJoiningSecondaryDown();

    // P OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P OK, J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK, J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));
  }

  @Test
  public void testcommitresultofpjj() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[3];

    callBacks[0] = primaryCallBack;
    callBacks[1] = secondary1CallBack;
    callBacks[2] = secondary2CallBack;
    final int successTimes = 0;

    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.PJJ);

    // P & J1 & J2 OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // primary is down
    mockOnlyPrimaryDown();

    // P & J1 & J2 OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // joining secondary is down
    mockOnlyJoiningSecondaryDown();

    // P & J1 & J2 OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    asignReturnValueToWriteMethodCallback(secondary2CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));
  }

  @Test
  public void testCommitResultOfPii() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[1];

    callBacks[0] = primaryCallBack;

    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.PII);

    // P OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    int successTimes = 0;
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // primary is down
    mockOnlyPrimaryDown();

    // P OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));
  }

  @Test
  public void testCommitResultOfPa() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[2];

    callBacks[0] = primaryCallBack;
    callBacks[1] = secondary1CallBack;

    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.PS);

    // P & S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    int successTimes = 0;
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P OK, S not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK, S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // primary is down
    mockOnlyPrimaryDown();

    // P not OK, S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P & S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // secondary is down
    mockOnlySecondaryDown();
    // P OK, S not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P & S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));
  }

  @Test
  public void testCommitResultOfPj() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[2];

    callBacks[0] = primaryCallBack;
    callBacks[1] = secondary1CallBack;

    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.PJ);

    // P & J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    int successTimes = 0;
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK, J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P & J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // primary down
    mockOnlyPrimaryDown();

    // P & J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK, J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P & J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // joining secondary down
    mockOnlyJoiningSecondaryDown();

    // P & J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK, J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P & J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));
  }

  @Test
  public void testCommitResultOfPi() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[1];

    callBacks[0] = primaryCallBack;

    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.PI);

    // P OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    int successTimes = 0;
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // primary is down
    mockOnlyPrimaryDown();

    // P OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.REGULAR);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));
  }

  @Test
  public void testCommitResultOfPsa() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[2];

    callBacks[0] = primaryCallBack;
    callBacks[1] = secondary1CallBack;

    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.PSA);

    // P & S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    int successTimes = 0;
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P OK, S not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // primary is down
    mockOnlyPrimaryDown();

    // P not OK, S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK, S not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P OK, S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P OK, S not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // secondary is down
    mockOnlySecondaryDown();

    // P not OK, S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK, S not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P OK, S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P OK, S not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

  }

  @Test
  public void testcommitresultofpja() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[2];

    callBacks[0] = primaryCallBack;
    callBacks[1] = secondary1CallBack;

    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.PJA);

    // P & J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    int successTimes = 0;
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK, J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P & J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // primary is down
    mockOnlyPrimaryDown();

    // P & J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK, J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P & J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // J is down
    mockOnlyJoiningSecondaryDown();

    // P & J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK, J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P & J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));
  }

  @Test
  public void testCommitResultOfPia() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[1];

    callBacks[0] = primaryCallBack;

    // primary is alive
    asignReturnValueToIoActionContext(SegmentForm.PIA);

    // P OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    int successTimes = 0;
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // primary is down
    mockPrimaryStatus(true);

    // P OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));
  }

  @Test
  public void testCommitResultOfPa1() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[1];

    callBacks[0] = primaryCallBack;

    // primary is alive
    asignReturnValueToIoActionContext(SegmentForm.PA);

    // P OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    int successTimes = 0;
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // primary is down
    mockPrimaryStatus(true);

    // P OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));
  }

  @Test
  public void testCommitResultOfTps() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[2];

    callBacks[0] = primaryCallBack;
    callBacks[1] = secondary1CallBack;
    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.TPS);

    // P & S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    int successTimes = 0;

    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P OK, S not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK, S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P & S not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // primary is down
    mockOnlyPrimaryDown();

    // P & S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P OK, S not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK, S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P & S not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // secondary is down
    mockOnlySecondaryDown();

    // P & S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P OK, S not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK, S OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P & S not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Secondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));
  }

  @Test
  public void testCommitResultOfTpj() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[2];

    callBacks[0] = primaryCallBack;
    callBacks[1] = secondary1CallBack;

    // all member are alive
    asignReturnValueToIoActionContext(SegmentForm.TPJ);

    // P & J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    int successTimes = 0;
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK, J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P & J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // primary is down
    mockOnlyPrimaryDown();

    // P & J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK, J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P & J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // J is down
    mockOnlyJoiningSecondaryDown();

    // P & J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P OK, J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK, J OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P & J not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    asignReturnValueToWriteMethodCallback(secondary1CallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.JoiningSecondary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));
  }

  @Test
  public void testCommitResultOfTpi() {
    WriteMethodCallback[] callBacks = new WriteMethodCallback[1];

    callBacks[0] = primaryCallBack;

    // primary is alive
    asignReturnValueToIoActionContext(SegmentForm.TPI);

    // P OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    int successTimes = 0;
    successTimes++;
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // primary is down
    mockPrimaryStatus(true);

    // P OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.COMMITTED,
        MemberIoStatus.Primary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));

    // P not OK
    asignReturnValueToWriteMethodCallback(primaryCallBack, PbBroadcastLogStatus.CREATED,
        MemberIoStatus.Primary);
    LogResult.mergeCommitResult(callBacks, 0, contextManager, ioActionContext, VolumeType.SMALL);
    verify(contextManager, times(successTimes)).commitPbLog(anyInt(), any(PbBroadcastLog.class));
  }

}
