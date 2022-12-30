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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Before;
import org.mockito.Mock;
import org.testng.collections.Sets;
import py.PbRequestResponseHelper;
import py.archive.segment.SegId;
import py.archive.segment.SegmentVersion;
import py.common.RequestIdBuilder;
import py.coordinator.lib.Coordinator;
import py.coordinator.response.TriggerByCheckCallback;
import py.instance.InstanceId;
import py.instance.SimpleInstance;
import py.membership.MemberIoStatus;
import py.membership.SegmentMembership;
import py.netty.client.GenericAsyncClientFactory;
import py.netty.core.MethodCallback;
import py.netty.datanode.AsyncDataNode;
import py.proto.Broadcastlog;
import py.test.TestBase;
import py.volume.VolumeType;

public class ReachableTestBase extends TestBase {

  Broadcastlog.PbCheckRequest request;

  @Mock
  Broadcastlog.PbCheckResponse response;

  @Mock
  AsyncDataNode.AsyncIface asyncIface;

  @Mock
  Coordinator coordinator;

  @Mock
  TriggerByCheckCallback triggerByCheckCallback;


  SimpleInstance checkInstance;

  SegId segId;

  List<SimpleInstance> checkThroughList;

  SegmentMembership segmentMembership;

  SegmentMembership responseMembership;
  Broadcastlog.PbMembership pbResponseMembership;

  long tmpPrimary = -1;

  long primary = 0;

  AtomicBoolean checkSuccess = new AtomicBoolean(true);

  Exception checkException;

  ReachableTestBase() {
    //mock mark P
    doAnswer(invocationOnMock -> {
      InstanceId instanceId = invocationOnMock.getArgument(1);
      logger.warn("enter mock function of markPrimaryAsUnstablePrimary, instanceId is {}",
          instanceId);

      segmentMembership.markMemberIoStatus(instanceId, MemberIoStatus.UnstablePrimary);
      return null;
    }).when(coordinator)
        .markPrimaryAsUnstablePrimary(any(SegmentMembership.class), any(InstanceId.class));

    doAnswer(invocationOnMock -> {
      InstanceId instanceId = invocationOnMock.getArgument(1);
      logger.warn("enter mock function of markPrimaryDown, instanceId is {}", instanceId);

      segmentMembership.markMemberIoStatus(instanceId, MemberIoStatus.PrimaryDown);
      return null;
    }).when(coordinator).markPrimaryDown(any(SegmentMembership.class), any(InstanceId.class));

    doAnswer(invocationOnMock -> {
      InstanceId instanceId = invocationOnMock.getArgument(1);
      logger.warn("enter mock function of markTempPrimaryDown, instanceId is {}", instanceId);

      segmentMembership.markMemberIoStatus(instanceId, MemberIoStatus.SecondaryDown);
      return null;
    }).when(coordinator).markTempPrimaryDown(any(SegmentMembership.class), any(InstanceId.class));

    //mock mark S
    doAnswer(invocationOnMock -> {
      InstanceId instanceId = invocationOnMock.getArgument(1);
      logger.warn("enter mock function of markSecondaryDown, instanceId is {}", instanceId);

      segmentMembership.markMemberIoStatus(instanceId, MemberIoStatus.SecondaryDown);
      return null;
    }).when(coordinator).markSecondaryDown(any(SegmentMembership.class), any(InstanceId.class));

    //mock check
    GenericAsyncClientFactory clientFactory = mock(GenericAsyncClientFactory.class);
    when(coordinator.getClientFactory()).thenReturn(clientFactory);
    when(clientFactory.generate(any())).thenReturn(asyncIface);
    doAnswer(invocationOnMock -> {
      Broadcastlog.PbCheckRequest pbCheckRequest = invocationOnMock.getArgument(0);
      MethodCallback callback = invocationOnMock.getArgument(1);
      logger.warn("enter mock function of check, request is {}", pbCheckRequest);
      switch (pbCheckRequest.getRequestOption()) {
        case CHECK_PRIMARY:
          checkPrimaryReachableInDn();
          break;
        case CONFIRM_UNREACHABLE:
          tmpPrimary = pbCheckRequest.getTempPrimary();
          responseMembership = segmentMembership
              .secondaryBecomeTempPrimary(new InstanceId(tmpPrimary));
          when(response.hasPbMembership()).thenReturn(true);
          when(response.getPbMembership())
              .thenReturn(PbRequestResponseHelper.buildPbMembershipFrom(responseMembership));
          break;
        case CONFIRM_TP_UNREACHABLE:
          tmpPrimary = pbCheckRequest.getTempPrimary();
          break;
        default:
          break;
      }
      if (checkSuccess.get()) {
        callback.complete(response);
      } else {
        callback.fail(checkException);
      }

      return null;
    }).when(asyncIface).check(any(), any());

    checkThroughList = new LinkedList<>();
    segId = new SegId(1L, 0);
  }

  @Before
  public void init() {
    checkThroughList.clear();
  }

  protected void checkPrimaryReachableInDn() {

  }

  protected void buildMembership(VolumeType volumeType, boolean hasJoinSecondary) {
    when(coordinator.getVolumeType(any(Long.class))).thenReturn(volumeType);

    Long id = 0L;
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();

    switch (volumeType) {
      case LARGE:
        secondaries.add(new InstanceId(id++));
        secondaries.add(new InstanceId(id++));
        arbiters.add(new InstanceId(id++));
        arbiters.add(new InstanceId(id++));
        break;
      case SMALL:
        secondaries.add(new InstanceId(id++));
        arbiters.add(new InstanceId(id++));
        break;
      case REGULAR:
        secondaries.add(new InstanceId(id++));
        secondaries.add(new InstanceId(id++));
        break;
      default:
        break;
    }

    if (hasJoinSecondary) {
      joiningSecondaries.add(secondaries.remove(0));
    }

    segmentMembership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries,
        arbiters,
        inactiveSecondaries, joiningSecondaries);
    segmentMembership.getMemberIoStatus(primary);
    logger.warn("membership is {}", segmentMembership);

    for (InstanceId member : segmentMembership.getAllSecondaries()) {
      segmentMembership.getMemberIoStatus(member);
    }
    buildCheckThroughList();
  }

  protected void datanodeGenerateHigherMembership() {
    Set<InstanceId> secondarySet = Sets.newHashSet(segmentMembership.getSecondaries());
    for (InstanceId firstSecondary : secondarySet) {
      InstanceId primary = firstSecondary;
      secondarySet.remove(firstSecondary);
      //one secondary become p
      responseMembership = new SegmentMembership(segmentMembership.getSegmentVersion().incEpoch(),
          primary, secondarySet);
      //while old p become inactive S
      responseMembership = responseMembership.addSecondaries(segmentMembership.getPrimary());
      responseMembership = responseMembership
          .aliveSecondaryBecomeInactive(segmentMembership.getPrimary());
      break;
    }
  }

  protected void buildCheckThroughList() {
    throw new RuntimeException("not impelemntion");
  }

  protected Broadcastlog.PbCheckRequest buildCheckRequest(Broadcastlog.RequestOption requestOption,
      long tempPrimary, long checkInstanceId) {
    Broadcastlog.PbCheckRequest.Builder requestBuilder = Broadcastlog.PbCheckRequest.newBuilder();
    requestBuilder.setRequestId(RequestIdBuilder.get());
    requestBuilder.setCheckInstanceId(checkInstanceId);
    requestBuilder.setRequestOption(requestOption);
    requestBuilder.setVolumeId(segId.getVolumeId().getId());
    requestBuilder.setSegIndex(segId.getIndex());
    requestBuilder.setTempPrimary(tempPrimary);
    requestBuilder.setRequestPbMembership(PbRequestResponseHelper.buildPbMembershipFrom(
        segmentMembership));
    return requestBuilder.build();
  }
}
