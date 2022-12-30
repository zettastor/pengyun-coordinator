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

public class ReadResultPssaaTest extends TestBase {

  @Mock
  ReadMethodCallback primaryCallback;

  @Mock
  ReadMethodCallback callback1;

  @Mock
  ReadMethodCallback callback2;

  @Mock
  ReadMethodCallback callback3;

  @Mock
  ReadMethodCallback callback4;

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
  public ReadResultPssaaTest() {
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
  public void testPssaa() {
    mockSegmentForm(SegmentForm.PSSAA);

    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[5];

    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;
    readMethodCallbacks[2] = callback2;
    readMethodCallbacks[3] = callback3;
    readMethodCallbacks[4] = callback4;

    // PSSAA OK
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    // P not OK SSAA, must have one fetch read
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    // P not OK SSAA, must have one fetch read
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Fetch);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //PSAA
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //PSSA
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackBadResponse(callback3);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //SAA
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //SAA
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Fetch);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    // AA
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //PAA
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //SSA P Adown
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackBadResponse(callback3);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //SSA P A down
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Fetch);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackBadResponse(callback3);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //PSA SAdown
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackBadResponse(callback3);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //PSS AAdown
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackBadResponse(callback3);
    mockIoMethodCallbackBadResponse(callback4);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //PS
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackBadResponse(callback3);
    mockIoMethodCallbackBadResponse(callback4);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    // ALL not OK
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackBadResponse(callback3);
    mockIoMethodCallbackBadResponse(callback4);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

  }

  @Test
  public void testPssai() {
    mockSegmentForm(SegmentForm.PSSAI);

    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[4];

    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;
    readMethodCallbacks[2] = callback2;
    readMethodCallbacks[3] = callback3;

    //PSSAI OK
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //SSAI
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //SSAI
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Fetch);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //PSAI
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //PSSI
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackBadResponse(callback3);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //SSA
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //SSA
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Fetch);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //SAI
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    // ALL not OK
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackBadResponse(callback3);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

  }

  @Test
  public void testPssii() {
    mockSegmentForm(SegmentForm.PSSII);

    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[3];
    // PSSII OK
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;
    readMethodCallbacks[2] = callback2;

    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Check);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //SSII
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Check);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //SSII
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Fetch);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Secondary, ReadType.Check);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //PSII
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    // ALL not OK
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

  }

  @Test
  public void testPsiaa() {
    mockSegmentForm(SegmentForm.PSJAA);

    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[5];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;
    readMethodCallbacks[2] = callback2;
    readMethodCallbacks[3] = callback3;
    readMethodCallbacks[4] = callback4;

    // PSJAA OK
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //SJAA check
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //SJAA fetch
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Fetch);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //PJAA
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //PSAA
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //PSJA
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackBadResponse(callback3);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //JAA
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //SAA check
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //SAA fetch
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Fetch);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //SJA check
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackBadResponse(callback4);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //SJA fetch
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Fetch);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackBadResponse(callback4);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //PAA
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //PJA
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackBadResponse(callback4);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //PSA
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackBadResponse(callback4);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    // ALL not OK
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackBadResponse(callback3);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

  }

  @Test
  public void testPsjai() {
    mockSegmentForm(SegmentForm.PSJAI);

    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[4];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;
    readMethodCallbacks[2] = callback2;
    readMethodCallbacks[3] = callback3;

    // PSJAI OK
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //SJAI check
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //SJAI fetch
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Fetch);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //PJAI
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //PSAI
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //PSJI
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackBadResponse(callback3);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //JAI
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //PSI
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackBadResponse(callback3);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    // ALL not OK
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackBadResponse(callback3);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

  }

  @Test
  public void testPsjii() {
    mockSegmentForm(SegmentForm.PSJII);

    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[3];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;
    readMethodCallbacks[2] = callback2;

    // PSJII OK
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //SJII fail
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    // PSII fail
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    // PJII fail
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    // ALL not OK
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

  }

  @Test
  public void testPsiaa1() {
    mockSegmentForm(SegmentForm.PSIAA);

    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[4];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;
    readMethodCallbacks[2] = callback2;
    readMethodCallbacks[3] = callback3;

    // PSIAA OK
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //SIAA check
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //SIAA
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Fetch);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //PIAA
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //PSIA
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mockIoMethodCallbackBadResponse(callback3);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //PSI AA
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackBadResponse(callback3);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //AAI PS
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    // ALL not OK
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackBadResponse(callback3);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);
  }

  @Test
  public void testpsiai() {
    mockSegmentForm(SegmentForm.PSIAI);

    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[3];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;
    readMethodCallbacks[2] = callback2;

    // PSIAI OK
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //SIAI
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //PIAI
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    // ALL not OK
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);
  }

  @Test
  public void testpsiii() {

  }

  @Test
  public void testpjjaa() {
    mockSegmentForm(SegmentForm.PJJAA);

    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[5];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;
    readMethodCallbacks[2] = callback2;
    readMethodCallbacks[3] = callback3;
    readMethodCallbacks[4] = callback4;

    // PJJAA OK
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //JJAA
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //PJAA
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //PJJA
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackBadResponse(callback4);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //JAA PJ
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //JJA
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackBadResponse(callback4);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //PJA
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackBadResponse(callback4);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //PJJ
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackBadResponse(callback3);
    mockIoMethodCallbackBadResponse(callback4);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    // ALL not OK
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackBadResponse(callback3);
    mockIoMethodCallbackBadResponse(callback4);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);
  }

  @Test
  public void testPjjai() {
    mockSegmentForm(SegmentForm.PJJAI);

    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[4];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;
    readMethodCallbacks[2] = callback2;
    readMethodCallbacks[3] = callback3;

    // PJJAI OK
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //JJAI
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //PJAI
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //PJJI
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackBadResponse(callback3);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //JAI
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    // ALL not OK
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackBadResponse(callback3);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

  }

  @Test
  public void testPjjii() {
    mockSegmentForm(SegmentForm.PJJII);

    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[3];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;
    readMethodCallbacks[2] = callback2;

    // PJJII OK
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //JJII
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //PJII
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.JoiningSecondary);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    // ALL not OK
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

  }

  @Test
  public void testPjiaa() {
    mockSegmentForm(SegmentForm.PJIAA);

    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[4];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;
    readMethodCallbacks[2] = callback2;
    readMethodCallbacks[3] = callback3;

    // PJIAA OK
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //JIAA
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //PIAA
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //PJAI
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //AAI PJ
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //PAI JA
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    // ALL not OK
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackBadResponse(callback3);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

  }

  @Test
  public void testPjiai() {
    mockSegmentForm(SegmentForm.PJIAI);

    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[3];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;
    readMethodCallbacks[2] = callback2;

    // PJIAI OK
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //JIAI
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //PIAI
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    // ALL not OK
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

  }

  @Test
  public void testpjiii() {

  }

  @Test
  public void testpiiaa() {

  }

  @Test
  public void testpiiai() {

  }

  @Test
  public void testpiiii() {

  }

  @Test
  public void testpsaa() {
    mockSegmentForm(SegmentForm.PSAA);

    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[4];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;
    readMethodCallbacks[2] = callback2;
    readMethodCallbacks[3] = callback3;

    // PSAA OK
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback4, MemberIoStatus.Arbiter);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //SAA check
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //SAA fetch
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Fetch);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //PAA
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //PSA
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //PA
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    // ALL not OK
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackBadResponse(callback3);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

  }

  @Test
  public void testpsai() {
    mockSegmentForm(SegmentForm.PSAI);

    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[3];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;
    readMethodCallbacks[2] = callback2;

    // PSAI OK
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //SAI
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Secondary, ReadType.Check);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //PAI
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    // ALL not OK
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

  }

  @Test
  public void testpsii() {

  }

  @Test
  public void testpiaa() {
    mockSegmentForm(SegmentForm.PIAA);

    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[3];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;
    readMethodCallbacks[2] = callback2;

    // PIAA OK
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //AAI
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //PAI
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Arbiter);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    // ALL not OK
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);
  }

  @Test
  public void testpiai() {

  }

  @Test
  public void testpiii() {

  }

  @Test
  public void testpjaa() {
    mockSegmentForm(SegmentForm.PJAA);

    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[4];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;
    readMethodCallbacks[2] = callback2;
    readMethodCallbacks[3] = callback3;

    // PJAA OK
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //JAA
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //PAA
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //PJA
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackGoodResponse(callback3, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    // ALL not OK
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mockIoMethodCallbackBadResponse(callback3);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

  }

  @Test
  public void testpjai() {
    mockSegmentForm(SegmentForm.PJAA);

    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[3];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;
    readMethodCallbacks[2] = callback2;

    // PJAI OK
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //JAI
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.JoiningSecondary);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    //PAI
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    // ALL not OK
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);
  }

  @Test
  public void testpjii() {

  }

  @Test
  public void testpaa() {
    mockSegmentForm(SegmentForm.PAA);

    ReadMethodCallback[] readMethodCallbacks = new ReadMethodCallback[3];
    readMethodCallbacks[0] = primaryCallback;
    readMethodCallbacks[1] = callback1;
    readMethodCallbacks[2] = callback2;
    // PAA OK
    mockIoMethodCallbackGoodResponse(primaryCallback, MemberIoStatus.Primary);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    boolean mergeResult;
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(mergeResult);

    //AA
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackGoodResponse(callback1, MemberIoStatus.Arbiter);
    mockIoMethodCallbackGoodResponse(callback2, MemberIoStatus.Arbiter);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

    // ALL not OK
    mockIoMethodCallbackBadResponse(primaryCallback);
    mockIoMethodCallbackBadResponse(callback1);
    mockIoMethodCallbackBadResponse(callback2);
    mergeResult = ReadIoContextManager
        .mergeReadResponses(readMethodCallbacks, ioActionContext, VolumeType.LARGE);
    assertTrue(!mergeResult);

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
