/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/ 

package py.coordinator.main;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import org.junit.Before;
import org.junit.Test;
import py.common.struct.EndPoint;
import py.coordinator.response.CheckRequestCallback;
import py.coordinator.response.CheckSecondaryReachableCallbackCollector;
import py.coordinator.response.TempPrimaryCheckSecondaryReachableCallbackCollector;
import py.instance.InstanceId;
import py.instance.SimpleInstance;
import py.proto.Broadcastlog;
import py.volume.VolumeType;

public class SecondaryReachableTest extends ReachableTestBase {

  InstanceId killedSecondary;
  InstanceId anotherSecondary;


  /**
   * xx.
   */
  @Before
  public void myInit() {
    checkSuccess.set(true);
    when(response.hasPbMembership()).thenReturn(false);
    when(response.hasPcl()).thenReturn(false);
    when(response.hasMyInstanceId()).thenReturn(true);
    when(response.getReachable()).thenReturn(false);

  }

  protected void buildCheckThroughList() {
    Iterator<InstanceId> iterator = segmentMembership.getSecondaries().iterator();
    killedSecondary = iterator.next();
    anotherSecondary = iterator.next();

    checkInstance = new SimpleInstance(killedSecondary,
        new EndPoint("localhost", (int) killedSecondary.getId()));

    logger.warn("checkThroughList {}", checkThroughList);
  }

  @Test
  public void testPrimaryCheckSecondaryCompleteTrueAtPss() {
    buildMembership(VolumeType.REGULAR, false);
    // primary check S reachable
    checkThroughList
        .add(new SimpleInstance(segmentMembership.getPrimary(), new EndPoint("localhost", 0)));

    CheckSecondaryReachableCallbackCollector collector =
        new CheckSecondaryReachableCallbackCollector(
            coordinator,
            checkThroughList, checkInstance, segId, triggerByCheckCallback, segmentMembership);
    request = buildCheckRequest(Broadcastlog.RequestOption.CHECK_SECONDARY, -1,
        killedSecondary.getId());

    CheckRequestCallback callback = new CheckRequestCallback(collector,
        segmentMembership.getPrimary());
    asyncIface.check(request, callback);
    assertTrue(collector.secondaryUnreachable());
    assertTrue(segmentMembership.getMemberIoStatus(killedSecondary).isDown());
  }

  @Test
  public void testPrimaryCheckSecondaryCompleteTrueAtPssaa() {
    buildMembership(VolumeType.LARGE, false);
    // primary check S reachable
    checkThroughList
        .add(new SimpleInstance(segmentMembership.getPrimary(), new EndPoint("localhost", 0)));

    CheckSecondaryReachableCallbackCollector collector =
        new CheckSecondaryReachableCallbackCollector(
            coordinator,
            checkThroughList, checkInstance, segId, triggerByCheckCallback, segmentMembership);
    request = buildCheckRequest(Broadcastlog.RequestOption.CHECK_SECONDARY, -1,
        killedSecondary.getId());

    CheckRequestCallback callback = new CheckRequestCallback(collector,
        segmentMembership.getPrimary());
    asyncIface.check(request, callback);
    assertTrue(collector.secondaryUnreachable());
    assertTrue(segmentMembership.getMemberIoStatus(killedSecondary).isDown());
  }

  @Test
  public void testTempPrimaryCheckSecondaryCompleteTrueAtPssaa() {
    buildMembership(VolumeType.REGULAR, false);
    segmentMembership = segmentMembership.secondaryBecomeTempPrimary(anotherSecondary);
    for (InstanceId member : segmentMembership.getAllSecondaries()) {
      segmentMembership.getMemberIoStatus(member);
    }
    // primary check S reachable
    checkThroughList
        .add(new SimpleInstance(segmentMembership.getTempPrimary(), new EndPoint("localhost", 0)));

    TempPrimaryCheckSecondaryReachableCallbackCollector collector =
        new TempPrimaryCheckSecondaryReachableCallbackCollector(
            coordinator,
            checkThroughList, checkInstance, segId, triggerByCheckCallback, segmentMembership);
    request = buildCheckRequest(Broadcastlog.RequestOption.CHECK_SECONDARY, -1,
        killedSecondary.getId());

    CheckRequestCallback callback = new CheckRequestCallback(collector,
        segmentMembership.getPrimary());
    asyncIface.check(request, callback);
    assertTrue(collector.secondaryUnreachable());
    assertTrue(segmentMembership.getMemberIoStatus(killedSecondary).isDown());
  }
}
