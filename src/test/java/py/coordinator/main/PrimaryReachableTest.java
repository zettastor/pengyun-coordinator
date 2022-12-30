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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import py.archive.segment.SegmentUnitStatus;
import py.common.struct.EndPoint;
import py.coordinator.response.CheckPrimaryReachableCallbackCollector;
import py.coordinator.response.CheckRequestCallback;
import py.coordinator.response.CheckRequestCallbackCollector;
import py.instance.InstanceId;
import py.instance.SimpleInstance;
import py.membership.MemberIoStatus;
import py.netty.datanode.NettyExceptionHelper;
import py.netty.exception.MembershipVersionLowerException;
import py.proto.Broadcastlog;
import py.proto.Broadcastlog.RequestOption;
import py.volume.VolumeType;

public class PrimaryReachableTest extends ReachableTestBase {


  /**
   * xx.
   */
  @Before
  public void myInit() {
    checkSuccess.set(true);
    when(response.hasPbMembership()).thenReturn(false);
    when(response.hasPcl()).thenReturn(true);
    when(response.hasMyInstanceId()).thenReturn(true);

    checkInstance = new SimpleInstance(new InstanceId(0), new EndPoint("localhost", 0));
  }

  protected void checkPrimaryreachableindn() {

  }

  protected void buildCheckThroughList() {

    for (InstanceId instanceId : segmentMembership.getSecondaries()) {
      checkThroughList
          .add(new SimpleInstance(instanceId, new EndPoint("localhost", (int) instanceId.getId())));
    }

    for (InstanceId instanceId : segmentMembership.getArbiters()) {
      checkThroughList
          .add(new SimpleInstance(instanceId, new EndPoint("localhost", (int) instanceId.getId())));
    }

    for (InstanceId instanceId : segmentMembership.getJoiningSecondaries()) {
      checkThroughList
          .add(new SimpleInstance(instanceId, new EndPoint("localhost", (int) instanceId.getId())));
    }
    logger.warn("checkThroughList {}", checkThroughList);
  }


  /**
   * the two S response the P unreachable this case cover psj psa.
   */
  @Test
  public void testBothSecondariesCompleteFalseAtPss() {
    buildMembership(VolumeType.REGULAR, false);
    // check primary reachable
    CheckRequestCallbackCollector collector = new CheckPrimaryReachableCallbackCollector(
        coordinator, checkThroughList,
        checkInstance, segId, triggerByCheckCallback, segmentMembership, new Exception());
    request = buildCheckRequest(RequestOption.CHECK_PRIMARY, -1, primary);

    checkSuccess.set(true);
    when(response.getReachable()).thenReturn(false);
    for (InstanceId secondary : segmentMembership.getSecondaries()) {
      CheckRequestCallback callback = new CheckRequestCallback(collector, secondary);

      when(response.getPcl()).thenReturn(secondary.getId());
      when(response.getMyInstanceId()).thenReturn(secondary.getId());

      asyncIface.check(request, callback);
    }
    assertTrue(collector.getGoodCount() == 2);
    assertTrue(collector.getLeftCount() == 0);
    assertTrue(collector.getReachableCount() == 0);
    assertTrue(collector.getUnReachableCount() == 2);
    assertTrue(collector.primaryUnreachable());
    assertTrue(tmpPrimary == collector.getTempPrimaryId());
    assertTrue(segmentMembership.getMemberIoStatus(segmentMembership.getPrimary())
        == MemberIoStatus.PrimaryDown);
  }

  /**
   * one two S response the P unreachable while another response reachable this case cover psj psa.
   */
  @Test
  public void testOneSecondaryCompleteFalseButAnotherTrueAtPss() {
    buildMembership(VolumeType.REGULAR, false);
    // check primary reachable
    CheckRequestCallbackCollector collector = new CheckPrimaryReachableCallbackCollector(
        coordinator, checkThroughList,
        checkInstance, segId, triggerByCheckCallback, segmentMembership, new Exception());
    request = buildCheckRequest(RequestOption.CHECK_PRIMARY, -1, primary);

    int okSecondary = 0;
    for (InstanceId secondary : segmentMembership.getSecondaries()) {
      final CheckRequestCallback callback = new CheckRequestCallback(collector, secondary);

      when(response.getPcl()).thenReturn(secondary.getId());
      when(response.getMyInstanceId()).thenReturn(secondary.getId());

      okSecondary++;
      if (okSecondary == 2) {
        when(response.getReachable()).thenReturn(true);
      } else {
        when(response.getReachable()).thenReturn(false);
      }
      checkSuccess.set(true);
      asyncIface.check(request, callback);

    }

    assertTrue(collector.getGoodCount() == 2);
    assertTrue(collector.getLeftCount() == 0);
    assertTrue(collector.getReachableCount() == 1);
    assertTrue(collector.getUnReachableCount() == 1);
    assertTrue(!collector.primaryUnreachable());
    assertTrue(tmpPrimary == -1);
    assertTrue(
        segmentMembership.getMemberIoStatus(segmentMembership.getPrimary()).isUnstablePrimary());
    //this case cover psj
  }

  /**
   * one two S response the P unreachable while another response reachable this case cover psj psa.
   */
  @Test
  public void testOneSecondaryCompleteFalseButAnotherFailAtPss() {
    buildMembership(VolumeType.REGULAR, false);
    // check primary reachable
    CheckRequestCallbackCollector collector = new CheckPrimaryReachableCallbackCollector(
        coordinator, checkThroughList,
        checkInstance, segId, triggerByCheckCallback, segmentMembership, new Exception());
    request = buildCheckRequest(Broadcastlog.RequestOption.CHECK_PRIMARY, -1, primary);

    int okSecondary = 0;
    for (InstanceId secondary : segmentMembership.getSecondaries()) {
      final CheckRequestCallback callback = new CheckRequestCallback(collector, secondary);

      when(response.getPcl()).thenReturn(secondary.getId());
      when(response.getMyInstanceId()).thenReturn(secondary.getId());

      okSecondary++;
      if (okSecondary == 2) {
        checkSuccess.set(true);
        when(response.getReachable()).thenReturn(true);
      } else {
        checkSuccess.set(false);
        checkException = new MembershipVersionLowerException(segId.getVolumeId().getId(),
            segId.getIndex());
      }
      asyncIface.check(request, callback);
    }

    assertTrue(collector.getGoodCount() == 1);
    assertTrue(collector.getLeftCount() == 0);
    assertTrue(collector.getReachableCount() == 1);
    assertTrue(collector.getUnReachableCount() == 0);
    assertTrue(!collector.primaryUnreachable());
    assertTrue(tmpPrimary == -1);
    assertTrue(
        segmentMembership.getMemberIoStatus(segmentMembership.getPrimary()).isUnstablePrimary());
  }

  /**
   * the server side callback fail of not primary exception this case cover psj psa.
   */
  @Test
  public void testNotPrimaryExceptionAtPss() {
    buildMembership(VolumeType.REGULAR, false);

    checkSuccess.set(false);
    datanodeGenerateHigherMembership();
    checkException = NettyExceptionHelper
        .buildNotPrimaryException(segId, SegmentUnitStatus.Primary, responseMembership);
    // check primary reachable
    CheckRequestCallbackCollector collector = new CheckPrimaryReachableCallbackCollector(
        coordinator, checkThroughList,
        checkInstance, segId, triggerByCheckCallback, segmentMembership, new Exception());
    request = buildCheckRequest(Broadcastlog.RequestOption.CHECK_PRIMARY, -1, primary);

    for (InstanceId secondary : segmentMembership.getSecondaries()) {
      CheckRequestCallback callback = new CheckRequestCallback(collector, secondary);

      when(response.getPcl()).thenReturn(secondary.getId());
      when(response.getMyInstanceId()).thenReturn(secondary.getId());
      asyncIface.check(request, callback);
    }
    assertTrue(collector.getGoodCount() == 0);
    assertTrue(!collector.primaryReachable());
    assertTrue(!collector.primaryUnreachable());
    assertTrue(tmpPrimary == -1);
    assertTrue(collector.getResponseMembership().compareTo(responseMembership) == 0);
  }

  /**
   * the server side callback fail of not membership lower exception this case cover psj psa.
   */
  @Test
  public void testMembershipLowerExceptionAtPss() {
    buildMembership(VolumeType.REGULAR, false);

    checkSuccess.set(false);
    responseMembership = segmentMembership
        .aliveSecondaryBecomeInactive(segmentMembership.getSecondaries().iterator().next());
    checkException = NettyExceptionHelper
        .buildMembershipVersionLowerException(segId, responseMembership);
    // check primary reachable
    CheckRequestCallbackCollector collector = new CheckPrimaryReachableCallbackCollector(
        coordinator, checkThroughList,
        checkInstance, segId, triggerByCheckCallback, segmentMembership, new Exception());
    request = buildCheckRequest(Broadcastlog.RequestOption.CHECK_PRIMARY, -1, primary);

    for (InstanceId secondary : segmentMembership.getSecondaries()) {
      CheckRequestCallback callback = new CheckRequestCallback(collector, secondary);

      when(response.getPcl()).thenReturn(secondary.getId());
      when(response.getMyInstanceId()).thenReturn(secondary.getId());
      asyncIface.check(request, callback);

    }
    assertTrue(collector.getGoodCount() == 0);
    assertTrue(!collector.primaryReachable());
    assertTrue(!collector.primaryUnreachable());
    assertTrue(tmpPrimary == -1);
    assertTrue(collector.getResponseMembership().compareTo(responseMembership) == 0);
  }

  /**
   * one arbiter and two secondary response unreachable, another arbiter says reachable we can
   * select a tp.
   */
  @Test
  public void testQuromCompleteFalseIncludeBothSecondaryAtPssaa() {
    buildMembership(VolumeType.LARGE, false);
    // check primary reachable
    CheckRequestCallbackCollector collector = new CheckPrimaryReachableCallbackCollector(
        coordinator, checkThroughList,
        checkInstance, segId, triggerByCheckCallback, segmentMembership, new Exception());
    request = buildCheckRequest(Broadcastlog.RequestOption.CHECK_PRIMARY, -1, primary);

    int okArbiter = 0;
    for (InstanceId arbiter : segmentMembership.getArbiters()) {
      final CheckRequestCallback callback = new CheckRequestCallback(collector, arbiter);

      when(response.getPcl()).thenReturn(-1L);
      when(response.getMyInstanceId()).thenReturn(arbiter.getId());
      okArbiter++;
      if (okArbiter == 2) {
        when(response.getReachable()).thenReturn(false);
      } else {
        when(response.getReachable()).thenReturn(true);
      }
      asyncIface.check(request, callback);
      assertTrue(!collector.primaryUnreachable());
    }

    when(response.getReachable()).thenReturn(false);
    for (InstanceId secondary : segmentMembership.getSecondaries()) {
      CheckRequestCallback callback = new CheckRequestCallback(collector, secondary);

      when(response.getPcl()).thenReturn(secondary.getId());
      when(response.getMyInstanceId()).thenReturn(secondary.getId());
      asyncIface.check(request, callback);

    }
    assertTrue(collector.getGoodCount() == 4);
    assertTrue(collector.getLeftCount() == 0);
    assertTrue(collector.getReachableCount() == 1);
    assertTrue(collector.getUnReachableCount() == 3);
    assertTrue(!collector.primaryUnreachable());
    assertEquals(-1, tmpPrimary);
    assertTrue(segmentMembership.getMemberIoStatus(segmentMembership.getPrimary())
        == MemberIoStatus.UnstablePrimary);

  }

  /**
   * two arbiter and one secondary response unreachable, another secondary says reachable we can not
   * select tp.
   */
  @Test
  public void testQuromCompleteFalseIncludeOneSecondaryAtPssaa() {
    buildMembership(VolumeType.LARGE, false);
    // check primary reachable
    CheckRequestCallbackCollector collector = new CheckPrimaryReachableCallbackCollector(
        coordinator, checkThroughList,
        checkInstance, segId, triggerByCheckCallback, segmentMembership, new Exception());
    request = buildCheckRequest(Broadcastlog.RequestOption.CHECK_PRIMARY, -1, primary);

    when(response.getReachable()).thenReturn(false);
    for (InstanceId arbiter : segmentMembership.getArbiters()) {
      CheckRequestCallback callback = new CheckRequestCallback(collector, arbiter);

      when(response.getPcl()).thenReturn(-1L);
      when(response.getMyInstanceId()).thenReturn(arbiter.getId());
      asyncIface.check(request, callback);
    }

    int okSecondary = 0;
    for (InstanceId secondary : segmentMembership.getSecondaries()) {
      final CheckRequestCallback callback = new CheckRequestCallback(collector, secondary);

      when(response.getPcl()).thenReturn(secondary.getId());
      when(response.getMyInstanceId()).thenReturn(secondary.getId());

      okSecondary++;
      if (okSecondary == 2) {
        when(response.getReachable()).thenReturn(true);
      } else {
        //this test cover the case when qurom ok but only include one s
        when(response.getReachable()).thenReturn(false);
      }
      asyncIface.check(request, callback);

    }
    assertTrue(collector.getGoodCount() == 4);
    assertTrue(collector.getLeftCount() == 0);
    assertTrue(collector.getReachableCount() == 1);
    assertTrue(collector.getUnReachableCount() == 3);
    assertTrue(!collector.primaryUnreachable());
    assertEquals(tmpPrimary, -1);
    assertTrue(segmentMembership.getMemberIoStatus(segmentMembership.getPrimary())
        == MemberIoStatus.UnstablePrimary);

  }

  /**
   * two arbiter and one secondary response unreachable, another joining secondary says reachable we
   * can select tp.
   */
  @Test
  public void testQuromCompleteFalseIncludeOneSecondaryAtPsjaa() {
    buildMembership(VolumeType.LARGE, true);
    // check primary reachable
    CheckRequestCallbackCollector collector = new CheckPrimaryReachableCallbackCollector(
        coordinator, checkThroughList,
        checkInstance, segId, triggerByCheckCallback, segmentMembership, new Exception());
    request = buildCheckRequest(Broadcastlog.RequestOption.CHECK_PRIMARY, -1, primary);

    //two arbiter response false
    when(response.getReachable()).thenReturn(false);
    for (InstanceId arbiter : segmentMembership.getArbiters()) {
      CheckRequestCallback callback = new CheckRequestCallback(collector, arbiter);

      when(response.getPcl()).thenReturn(-1L);
      when(response.getMyInstanceId()).thenReturn(arbiter.getId());
      asyncIface.check(request, callback);
    }
    //only one s response false
    for (InstanceId secondary : segmentMembership.getSecondaries()) {
      CheckRequestCallback callback = new CheckRequestCallback(collector, secondary);

      when(response.getPcl()).thenReturn(secondary.getId());
      when(response.getMyInstanceId()).thenReturn(secondary.getId());
      asyncIface.check(request, callback);
    }

    //only one js response true
    when(response.getReachable()).thenReturn(true);
    for (InstanceId secondary : segmentMembership.getJoiningSecondaries()) {
      CheckRequestCallback callback = new CheckRequestCallback(collector, secondary);

      when(response.getPcl()).thenReturn(-1L);
      when(response.getMyInstanceId()).thenReturn(secondary.getId());
      asyncIface.check(request, callback);
    }

    assertTrue(collector.getGoodCount() == 4);
    assertTrue(collector.getLeftCount() == 0);
    assertTrue(collector.getReachableCount() == 1);
    assertTrue(collector.getUnReachableCount() == 3);
    assertTrue(!collector.primaryUnreachable());
    assertEquals(-1, tmpPrimary);
    assertTrue(segmentMembership.getMemberIoStatus(segmentMembership.getPrimary())
        == MemberIoStatus.UnstablePrimary);
  }

  /**
   * if both js and S have max pcl, select the S.
   */
  @Test
  public void testJsHasMaxPclAtPsjaa() {
    buildMembership(VolumeType.LARGE, true);
    // check primary reachable
    CheckRequestCallbackCollector collector = new CheckPrimaryReachableCallbackCollector(
        coordinator, checkThroughList,
        checkInstance, segId, triggerByCheckCallback, segmentMembership, new Exception());
    request = buildCheckRequest(Broadcastlog.RequestOption.CHECK_PRIMARY, -1, primary);

    //two arbiter response false
    when(response.getReachable()).thenReturn(false);
    for (InstanceId arbiter : segmentMembership.getArbiters()) {
      CheckRequestCallback callback = new CheckRequestCallback(collector, arbiter);

      when(response.getPcl()).thenReturn(-1L);
      when(response.getMyInstanceId()).thenReturn(arbiter.getId());
      asyncIface.check(request, callback);
    }
    //only one s response false
    for (InstanceId secondary : segmentMembership.getSecondaries()) {
      CheckRequestCallback callback = new CheckRequestCallback(collector, secondary);

      when(response.getPcl()).thenReturn(secondary.getId());
      when(response.getMyInstanceId()).thenReturn(secondary.getId());
      asyncIface.check(request, callback);
    }

    //only one js response false
    when(response.getReachable()).thenReturn(false);
    for (InstanceId secondary : segmentMembership.getJoiningSecondaries()) {
      final CheckRequestCallback callback = new CheckRequestCallback(collector, secondary);

      when(response.hasMigrating()).thenReturn(true);
      when(response.getMigrating()).thenReturn(true);
      when(response.getPcl()).thenReturn(Long.MAX_VALUE);
      when(response.getMyInstanceId()).thenReturn(secondary.getId());
      asyncIface.check(request, callback);
    }

    assertTrue(collector.getGoodCount() == 4);
    assertTrue(collector.getLeftCount() == 0);
    assertTrue(collector.getReachableCount() == 0);
    assertTrue(collector.getUnReachableCount() == 4);
    assertTrue(collector.primaryUnreachable());
    assertTrue(-1 == tmpPrimary);
  }

  /**
   * if both js and S have max pcl, select the S.
   */
  @Test
  public void testBothJsAndsHasMaxPclAtPsjaa() {
    buildMembership(VolumeType.LARGE, true);
    // check primary reachable
    CheckRequestCallbackCollector collector = new CheckPrimaryReachableCallbackCollector(
        coordinator, checkThroughList,
        checkInstance, segId, triggerByCheckCallback, segmentMembership, new Exception());
    request = buildCheckRequest(Broadcastlog.RequestOption.CHECK_PRIMARY, -1, primary);

    //two arbiter response false
    when(response.getReachable()).thenReturn(false);
    for (InstanceId arbiter : segmentMembership.getArbiters()) {
      CheckRequestCallback callback = new CheckRequestCallback(collector, arbiter);

      when(response.getPcl()).thenReturn(-1L);
      when(response.getMyInstanceId()).thenReturn(arbiter.getId());
      asyncIface.check(request, callback);
    }
    //only one s response false
    for (InstanceId secondary : segmentMembership.getSecondaries()) {
      CheckRequestCallback callback = new CheckRequestCallback(collector, secondary);

      when(response.getPcl()).thenReturn(Long.MAX_VALUE);
      when(response.getMyInstanceId()).thenReturn(secondary.getId());
      asyncIface.check(request, callback);
    }

    //only one js response false
    when(response.getReachable()).thenReturn(false);
    for (InstanceId secondary : segmentMembership.getJoiningSecondaries()) {
      final CheckRequestCallback callback = new CheckRequestCallback(collector, secondary);

      when(response.hasMigrating()).thenReturn(true);
      when(response.getMigrating()).thenReturn(true);
      when(response.getPcl()).thenReturn(Long.MAX_VALUE);
      when(response.getMyInstanceId()).thenReturn(100L);
      asyncIface.check(request, callback);
    }

    assertTrue(collector.getGoodCount() == 4);
    assertTrue(collector.getLeftCount() == 0);
    assertTrue(collector.getReachableCount() == 0);
    assertTrue(collector.getUnReachableCount() == 4);
    assertTrue(collector.primaryUnreachable());
    assertTrue(collector.getTempPrimaryId() == tmpPrimary);
    assertTrue(segmentMembership.getMemberIoStatus(segmentMembership.getPrimary())
        == MemberIoStatus.PrimaryDown);
  }

  /**
   * one arbiter and one secondary response unreachable, another joining secondary says reachable we
   * can select tp the case also cover psaai pssai.
   */
  @Test
  public void testQuromCompleteFalseIncludeOneSecondaryAtPsjai() {
    buildMembership(VolumeType.LARGE, true);
    InstanceId kickarbiter = new InstanceId(4);
    segmentMembership.getArbiters().remove(kickarbiter);
    checkThroughList.remove(new SimpleInstance(kickarbiter, null));
    // check primary reachable
    CheckRequestCallbackCollector collector = new CheckPrimaryReachableCallbackCollector(
        coordinator, checkThroughList,
        checkInstance, segId, triggerByCheckCallback, segmentMembership, new Exception());
    request = buildCheckRequest(Broadcastlog.RequestOption.CHECK_PRIMARY, -1, primary);

    when(response.getReachable()).thenReturn(false);
    for (InstanceId arbiter : segmentMembership.getArbiters()) {
      CheckRequestCallback callback = new CheckRequestCallback(collector, arbiter);

      when(response.getPcl()).thenReturn(-1L);
      when(response.getMyInstanceId()).thenReturn(arbiter.getId());
      asyncIface.check(request, callback);
    }
    //only one s
    for (InstanceId secondary : segmentMembership.getSecondaries()) {
      CheckRequestCallback callback = new CheckRequestCallback(collector, secondary);

      when(response.getPcl()).thenReturn(secondary.getId());
      when(response.getMyInstanceId()).thenReturn(secondary.getId());
      asyncIface.check(request, callback);
    }

    //only one js
    for (InstanceId secondary : segmentMembership.getJoiningSecondaries()) {
      CheckRequestCallback callback = new CheckRequestCallback(collector, secondary);

      when(response.getPcl()).thenReturn(-1L);
      when(response.getMyInstanceId()).thenReturn(secondary.getId());
      asyncIface.check(request, callback);
    }

    assertTrue(collector.getGoodCount() == 3);
    assertTrue(collector.getLeftCount() == 0);
    assertTrue(collector.getReachableCount() == 0);
    assertTrue(collector.getUnReachableCount() == 3);
    assertTrue(collector.primaryUnreachable());
    assertTrue(tmpPrimary == collector.getTempPrimaryId());
    assertTrue(segmentMembership.getMemberIoStatus(segmentMembership.getPrimary())
        == MemberIoStatus.PrimaryDown);
  }

  @Test
  public void test() {
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 5; j++) {
        if (j == 2) {
          break;
        }
        logger.warn("this is j {}", j);
      }
      logger.warn("this is i {}", i);
    }
  }
}
