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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import org.junit.Before;
import org.junit.Test;
import py.common.struct.EndPoint;
import py.coordinator.response.CheckRequestCallback;
import py.coordinator.response.CheckRequestCallbackCollector;
import py.coordinator.response.CheckTempPrimaryReachableCallbackCollector;
import py.instance.InstanceId;
import py.instance.SimpleInstance;
import py.membership.MemberIoStatus;
import py.proto.Broadcastlog;
import py.volume.VolumeType;

public class TempPrimaryReachableTest extends ReachableTestBase {

  InstanceId oneSecondary;
  InstanceId anotherSecondary;

  @Before
  public void myInit() {
    tmpPrimary = -1;
    checkSuccess.set(true);
    when(response.hasPbMembership()).thenReturn(false);
    when(response.hasPcl()).thenReturn(true);
    when(response.hasMyInstanceId()).thenReturn(true);
    when(response.getReachable()).thenReturn(false);
  }

  protected void buildCheckThroughList() {
    Iterator<InstanceId> iterator = segmentMembership.getSecondaries().iterator();
    oneSecondary = iterator.next();
    anotherSecondary = iterator.next();

    segmentMembership = segmentMembership.secondaryBecomeTempPrimary(oneSecondary);
    for (InstanceId member : segmentMembership.getAllSecondaries()) {
      segmentMembership.getMemberIoStatus(member);
    }

    checkThroughList.add(new SimpleInstance(anotherSecondary,
        new EndPoint("localhost", (int) anotherSecondary.getId())));
    checkInstance = new SimpleInstance(oneSecondary,
        new EndPoint("localhost", (int) oneSecondary.getId()));

    for (InstanceId instanceId : segmentMembership.getArbiters()) {
      checkThroughList
          .add(new SimpleInstance(instanceId, new EndPoint("localhost", (int) instanceId.getId())));
    }

    logger.warn("checkThroughList {}", checkThroughList);
  }

  @Test
  public void testQuromCompleteFalseIncludeAllsecondaryAtTpsaa() {
    buildMembership(VolumeType.LARGE, false);
    // check temp primary reachable
    CheckRequestCallbackCollector collector = new CheckTempPrimaryReachableCallbackCollector(
        coordinator,
        checkThroughList, checkInstance, segId, triggerByCheckCallback, segmentMembership,
        new Exception());
    request = buildCheckRequest(Broadcastlog.RequestOption.CHECK_TEMP_PRIMARY, -1,
        checkInstance.getInstanceId().getId());

    when(response.getReachable()).thenReturn(false);
    for (InstanceId arbiter : segmentMembership.getArbiters()) {
      CheckRequestCallback callback = new CheckRequestCallback(collector, arbiter);

      when(response.getPcl()).thenReturn(-1L);
      when(response.getMyInstanceId()).thenReturn(arbiter.getId());
      asyncIface.check(request, callback);
    }

    CheckRequestCallback callback = new CheckRequestCallback(collector, anotherSecondary);
    when(response.getPcl()).thenReturn(anotherSecondary.getId());
    when(response.getMyInstanceId()).thenReturn(anotherSecondary.getId());
    asyncIface.check(request, callback);
    assertTrue(collector.getGoodCount() == 3);
    assertTrue(collector.getLeftCount() == 0);
    assertTrue(collector.getReachableCount() == 0);
    assertTrue(collector.getUnReachableCount() == 3);
    assertTrue(collector.tempPrimaryUnreachable());
    assertEquals(tmpPrimary, (long) collector.getTempPrimaryId());
    assertTrue(tmpPrimary != segmentMembership.getTempPrimary().getId());
    assertEquals(segmentMembership.getMemberIoStatus(segmentMembership.getTempPrimary()),
        MemberIoStatus.SecondaryDown);

  }


}
