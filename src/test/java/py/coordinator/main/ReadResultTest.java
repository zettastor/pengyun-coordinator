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
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import py.coordinator.logmanager.ReadIoContextManager;
import py.coordinator.logmanager.ReadMethodCallback;
import py.membership.IoActionContext;
import py.membership.IoMember;
import py.membership.MemberIoStatus;
import py.membership.SegmentForm;
import py.test.TestBase;
import py.volume.VolumeType;

public class ReadResultTest extends TestBase {

  @Mock
  ReadMethodCallback primaryCallback;

  @Mock
  ReadMethodCallback callback1;

  @Mock
  ReadMethodCallback callback2;

  @Mock
  IoActionContext ioActionContext;

  @Mock
  IoMember checkIoMember;

  @Mock
  IoMember mergeIoMember;

  @Mock
  IoMember fetchIoMember;

  @Mock
  IoMember primaryIoMember;

  private Set<IoMember> checkReaders;
  private Set<IoMember> mergeReaders;
  private Set<IoMember> fetchReaders;


  /**
   * xx.
   */
  public ReadResultTest() {
    checkReaders = new HashSet<>();
    mergeReaders = new HashSet<>();
    fetchReaders = new HashSet<>();
    addMergeReaders();
    addCheckReaders();
    addFetchReaders();
  }

  private void addMergeReaders() {
    mergeReaders.add(mergeIoMember);
    mergeReaders.add(mergeIoMember);
  }

  private void addFetchReaders() {
    fetchReaders.add(fetchIoMember);
  }

  private void addCheckReaders() {
    checkReaders.add(checkIoMember);
    checkReaders.add(checkIoMember);
  }

  private void mockPrimaryStatus(boolean isDown) {
    when(ioActionContext.isPrimaryDown()).thenReturn(isDown);
  }

  private void mockIoMethodCallbackGoodResponse(ReadMethodCallback callback,
      MemberIoStatus memberIoStatus, ReadType type) {
    when(callback.weakGoodResponse()).thenReturn(true);
    when(callback.getMemberIoStatus()).thenReturn(memberIoStatus);
    if (callback.equals(primaryCallback)) {
      when(callback.getIoMember()).thenReturn(primaryIoMember);
      when(ioActionContext.isPrimaryDown()).thenReturn(false);
    } else {
      if (type == ReadType.Check) {
        when(callback.getIoMember()).thenReturn(checkIoMember);
      } else if (type == ReadType.Fetch) {
        when(callback.getIoMember()).thenReturn(fetchIoMember);
      } else if (type == ReadType.Merge) {
        when(callback.getIoMember()).thenReturn(mergeIoMember);
      } else {
        logger.error("the ReadType is error :{}", type);
        return;
      }
    }
  }

  private void mockIoMethodCallbackGoodResponse(ReadMethodCallback callback,
      MemberIoStatus memberIoStatus) {
    when(callback.weakGoodResponse()).thenReturn(true);
    when(callback.getMemberIoStatus()).thenReturn(memberIoStatus);
    if (callback.equals(primaryCallback)) {
      when(callback.getIoMember()).thenReturn(primaryIoMember);
      when(ioActionContext.isPrimaryDown()).thenReturn(false);
    }
  }

  private void mockIoMethodCallbackBadResponse(ReadMethodCallback callback) {
    when(callback.weakGoodResponse()).thenReturn(false);
    //if it is the P, make it down
    if (callback.equals(primaryCallback)) {
      when(ioActionContext.isPrimaryDown()).thenReturn(true);
    }
  }

  private void mockSegmentForm(SegmentForm segmentForm) {
    when(ioActionContext.getSegmentForm()).thenReturn(segmentForm);
  }


  /**
   * xx.
   */
  @Before
  public void initIoActionContext() {
    when(ioActionContext.isPrimaryDown()).thenReturn(false);
    when(ioActionContext.isSecondaryDown()).thenReturn(false);
    when(ioActionContext.isJoiningSecondaryDown()).thenReturn(false);

    when(ioActionContext.getCheckReaders()).thenReturn(checkReaders);
    when(ioActionContext.getFetchReader()).thenReturn(fetchReaders);
  }

  @Test
  public void testPss() {
    mockSegmentForm(SegmentForm.PSS);

    // P is alive
    // PS1S2 response, success
    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[3];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;
    readMethodCallbacks[2] = callback2;
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Check);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // PS1 response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // PS2 response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackBadResponse(callback1);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // S1S2 response, fail merge
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Merge);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Merge);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // S1S2 response, check
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Check);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // P response, fail
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // S1 response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // S2 response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // no response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // S1 response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // S2 response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Check);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // no response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);
  }

  @Test
  public void testPsss1ReadDown() {
    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[3];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;
    readMethodCallbacks[2] = callback2;

    mockSegmentForm(SegmentForm.PSS);
    // mock S1 is read down
    when(ioActionContext.isMetReadDownSecondary()).thenReturn(true);

    // S1S2 response, success
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Check);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // S1 response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Fetch);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // S2 response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);
  }

  @Test
  public void testpsj() {
    mockSegmentForm(SegmentForm.PSJ);
    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[3];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;
    readMethodCallbacks[2] = callback2;

    // P is alive
    // PSJ response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // PS response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // PJ response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // SJ response, success
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // P response, fail
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // S response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // J response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // SJ response, success
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // S response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // J response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

  }

  @Test
  public void testpjj() {
    mockSegmentForm(SegmentForm.PJJ);
    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[3];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;
    readMethodCallbacks[2] = callback2;

    // P is alive
    // PJ1J2 response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // PJ1 response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // PJ2 response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // J1J2 response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // P response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // J1 response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // J2 response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // J1J2 response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // J1 response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // J2 response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);
  }

  @Test
  public void testpji() {
    mockSegmentForm(SegmentForm.PJI);
    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[2];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;

    // P is alive
    // PJ response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // P response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // J response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // J response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);
  }

  @Test
  public void testpii() {
    mockSegmentForm(SegmentForm.PII);
    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[1];
    readMethodCallbacks[0] = primaryCallback;

    // P is alive
    // P response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // P is down
    mockIoMethodCallbackBadResponse(primaryCallback);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);
  }

  @Test
  public void testpsi() {
    mockSegmentForm(SegmentForm.PSI);
    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[2];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;

    // P is alive
    // PS response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // P response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // S response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // S response, fail for now
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);
  }

  @Test
  public void testPs() {
    mockSegmentForm(SegmentForm.PS);
    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[2];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;

    // P is alive
    // PS response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // P response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // S response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // S response, fail for now
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);
  }

  @Test
  public void testPj() {
    mockSegmentForm(SegmentForm.PJ);
    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[2];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;

    // P is alive
    // PJ response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // P response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // J response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // J response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);
  }

  @Test
  public void testPi() {
    mockSegmentForm(SegmentForm.PI);
    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[1];
    readMethodCallbacks[0] = primaryCallback;

    // P is alive
    // P response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // P is down
    mockIoMethodCallbackBadResponse(primaryCallback);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);
  }

  @Test
  public void testpsa() {
    mockSegmentForm(SegmentForm.PSA);
    // P is alive
    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[3];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;
    readMethodCallbacks[2] = callback2;

    // PSA response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // PS response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // PA response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // P response, fail
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // S response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // A response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // SA response, success
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Fetch);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // S response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // A response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);
  }

  @Test
  public void testpja() {
    mockSegmentForm(SegmentForm.PJA);
    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[3];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;
    readMethodCallbacks[2] = callback2;

    // P is alive
    // PJA response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // PJ response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // PA response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // JA response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // P response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // J response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // A response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // JA response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // J response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // A response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);
  }

  @Test
  public void testpia() {
    mockSegmentForm(SegmentForm.PIA);
    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[2];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;

    // P is alive
    // PA response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Arbiter);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // P response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // A response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // A response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);
  }

  @Test
  public void testPa() {
    mockSegmentForm(SegmentForm.PA);
    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[2];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;

    // P is alive
    // PA response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Arbiter);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // P response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // A response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // A response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);
  }

  @Test
  public void testtps() {
    mockSegmentForm(SegmentForm.TPS);
    // P is alive
    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[2];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;

    // PS response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // P response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // S response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);
  }

  @Test
  public void testtpj() {
    mockSegmentForm(SegmentForm.TPJ);
    // P is alive
    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[2];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;

    // PS response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // P response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);

    // S response, fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);

    // S response,
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(!mergeResult);
  }

  @Test
  public void testtpi() {
    final boolean mergeResult;
    mockSegmentForm(SegmentForm.TPI);
    // P is alive
    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[1];
    readMethodCallbacks[0] = primaryCallback;

    // P response, success
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.REGULAR);
    assertTrue(mergeResult);
    // P is down, no one response
  }

  public enum ReadType {
    Fetch(0),
    Check(1),
    Merge(2);

    int type;

    ReadType(int type) {
      this.type = type;
    }
  }
}
