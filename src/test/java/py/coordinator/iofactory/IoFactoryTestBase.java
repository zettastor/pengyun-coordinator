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

package py.coordinator.iofactory;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;
import org.junit.Before;
import org.mockito.Mock;
import py.archive.segment.SegId;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.membership.MemberIoStatus;
import py.membership.SegmentForm;
import py.membership.SegmentMembership;
import py.test.TestBase;
import py.volume.VolumeMetadata;
import py.volume.VolumeType;

public class IoFactoryTestBase extends TestBase {

  @Mock
  public VolumeMetadata volumeMetadata;

  @Mock
  public SegmentMembership membership;

  @Mock
  public InstanceStore instanceStore;

  @Mock
  public SegId segId;


  /**
   * xx.
   */
  @Before
  public void init() {
    Instance instance = mock(Instance.class);
    EndPoint endPoint = mock(EndPoint.class);
    when(instanceStore.get(any(InstanceId.class))).thenReturn(instance);
    when(instance.getEndPointByServiceName(any(PortType.class))).thenReturn(endPoint);
    mockVolumeTyep(VolumeType.REGULAR);
  }

  public void mockVolumeTyep(VolumeType volumeType) {
    when(volumeMetadata.getVolumeType()).thenReturn(volumeType);
  }


  /**
   * xx.
   */
  public void mockSegmentMembership(SegmentForm segmentForm) {
    Long startInstanceId = RequestIdBuilder.get();
    InstanceId primary = null;
    Set<InstanceId> secondaries = new HashSet<>();
    Set<InstanceId> joiningSecondaries = new HashSet<>();
    Set<InstanceId> arbiters = new HashSet<>();
    Set<InstanceId> inactiveSecondaries = new HashSet<>();

    String name = segmentForm.name();
    for (int i = 0; i < name.length(); i++) {
      switch (name.charAt(i)) {
        case 'P':
          primary = new InstanceId(startInstanceId++);
          when(membership.getMemberIoStatus(primary)).thenReturn(MemberIoStatus.Primary);
          break;
        case 'S':
          InstanceId secondary = new InstanceId(startInstanceId++);
          secondaries.add(secondary);
          when(membership.getMemberIoStatus(secondary)).thenReturn(MemberIoStatus.Secondary);
          break;
        case 'J':
          InstanceId joiningSecondary = new InstanceId(startInstanceId++);
          joiningSecondaries.add(joiningSecondary);
          when(membership.getMemberIoStatus(joiningSecondary))
              .thenReturn(MemberIoStatus.JoiningSecondary);
          break;
        case 'A':
          InstanceId arbiter = new InstanceId(startInstanceId++);
          arbiters.add(arbiter);
          when(membership.getMemberIoStatus(arbiter)).thenReturn(MemberIoStatus.Arbiter);
          break;
        case 'I':
          InstanceId inactiveSecondarie = new InstanceId(startInstanceId++);
          inactiveSecondaries.add(inactiveSecondarie);
          when(membership.getMemberIoStatus(inactiveSecondarie))
              .thenReturn(MemberIoStatus.InactiveSecondary);
          break;
        default:
          break;

      }
    }
    when(membership.getPrimary()).thenReturn(primary);
    when(membership.getSecondaries()).thenReturn(secondaries);
    when(membership.getJoiningSecondaries()).thenReturn(joiningSecondaries);
    when(membership.getArbiters()).thenReturn(arbiters);
    when(membership.getInactiveSecondaries()).thenReturn(inactiveSecondaries);
    //TODO getSegmentForm not in membership now

  }


  /**
   * xx.
   */
  public void mockPrimaryDown(SegmentForm segmentForm) {
    mockSegmentMembership(segmentForm);
    InstanceId primary = membership.getPrimary();
    when(membership.getMemberIoStatus(primary)).thenReturn(MemberIoStatus.PrimaryDown);
  }


  /**
   * xx.
   */
  public void mockPrimaryAsUnstablePrimary(SegmentForm segmentForm) {
    mockSegmentMembership(segmentForm);
    InstanceId primary = membership.getPrimary();
    when(membership.getMemberIoStatus(primary)).thenReturn(MemberIoStatus.UnstablePrimary);
  }


  /**
   * xx.
   */
  public void mockSecondaryDown(SegmentForm segmentForm) {
    mockSegmentMembership(segmentForm);
    InstanceId secondary = membership.getSecondaries().iterator().next();
    when(membership.getMemberIoStatus(secondary)).thenReturn(MemberIoStatus.SecondaryDown);
  }


  /**
   * xx.
   */
  public void mockAllArbiterDown(SegmentForm segmentForm) {
    mockSegmentMembership(segmentForm);
    for (InstanceId arbiter : membership.getArbiters()) {
      when(membership.getMemberIoStatus(arbiter)).thenReturn(MemberIoStatus.ArbiterDown);
    }
  }


  /**
   * xx.
   */
  public void mockPrimaryDownAndOneSecondaryReadDown(SegmentForm segmentForm) {
    mockSegmentMembership(segmentForm);
    InstanceId secondary = membership.getSecondaries().iterator().next();
    InstanceId primary = membership.getPrimary();
    when(membership.getMemberIoStatus(primary)).thenReturn(MemberIoStatus.PrimaryDown);
    when(membership.getMemberIoStatus(secondary)).thenReturn(MemberIoStatus.SecondaryReadDown);
  }


  /**
   * xx.
   */
  public void mockPrimaryDownAndAllSecondariesReadDown(SegmentForm segmentForm) {
    mockSegmentMembership(segmentForm);
    InstanceId primary = membership.getPrimary();
    when(membership.getMemberIoStatus(primary)).thenReturn(MemberIoStatus.PrimaryDown);
    for (InstanceId secondary : membership.getSecondaries()) {
      when(membership.getMemberIoStatus(secondary)).thenReturn(MemberIoStatus.SecondaryReadDown);
    }
  }


  /**
   * xx.
   */
  public void mockPrimaryDownAndAllSecondariesDown(SegmentForm segmentForm) {
    mockSegmentMembership(segmentForm);
    InstanceId primary = membership.getPrimary();
    when(membership.getMemberIoStatus(primary)).thenReturn(MemberIoStatus.PrimaryDown);
    for (InstanceId secondary : membership.getSecondaries()) {
      when(membership.getMemberIoStatus(secondary)).thenReturn(MemberIoStatus.SecondaryDown);
    }
  }


  /**
   * xx.
   */
  public void mockPrimaryAndSecondaryDown(SegmentForm segmentForm) {
    mockSegmentMembership(segmentForm);
    InstanceId primary = membership.getPrimary();
    InstanceId secondary = membership.getSecondaries().iterator().next();
    when(membership.getMemberIoStatus(primary)).thenReturn(MemberIoStatus.PrimaryDown);
    when(membership.getMemberIoStatus(secondary)).thenReturn(MemberIoStatus.SecondaryDown);
  }


  /**
   * xx.
   */
  public void mockJoiningSecondaryDown(SegmentForm segmentForm) {
    mockSegmentMembership(segmentForm);
    InstanceId joiningSecondary = membership.getJoiningSecondaries().iterator().next();
    when(membership.getMemberIoStatus(joiningSecondary))
        .thenReturn(MemberIoStatus.JoiningSecondaryDown);
  }


  /**
   * xx.
   */
  public void mockPrimaryAndJoiningSecondaryDown(SegmentForm segmentForm) {
    mockSegmentMembership(segmentForm);
    InstanceId primary = membership.getPrimary();
    InstanceId joiningSecondary = membership.getJoiningSecondaries().iterator().next();
    when(membership.getMemberIoStatus(primary)).thenReturn(MemberIoStatus.PrimaryDown);
    when(membership.getMemberIoStatus(joiningSecondary))
        .thenReturn(MemberIoStatus.JoiningSecondaryDown);
  }


  /**
   * xx.
   */
  public void mockArbiterDown(SegmentForm segmentForm) {
    mockSegmentMembership(segmentForm);
    InstanceId arbiter = membership.getArbiters().iterator().next();
    when(membership.getMemberIoStatus(arbiter)).thenReturn(MemberIoStatus.ArbiterDown);
  }


  /**
   * xx.
   */
  public void mockPrimaryAndArbiterDown(SegmentForm segmentForm) {
    mockSegmentMembership(segmentForm);
    InstanceId primary = membership.getPrimary();
    InstanceId arbiter = membership.getArbiters().iterator().next();
    when(membership.getMemberIoStatus(primary)).thenReturn(MemberIoStatus.PrimaryDown);
    when(membership.getMemberIoStatus(arbiter)).thenReturn(MemberIoStatus.ArbiterDown);
  }


  /**
   * xx.
   */
  public void mockSecondarAndJoiningSecondaryDown(SegmentForm segmentForm) {
    mockSegmentMembership(segmentForm);
    InstanceId secondary = membership.getSecondaries().iterator().next();
    InstanceId joiningSecondary = membership.getJoiningSecondaries().iterator().next();
    when(membership.getMemberIoStatus(secondary)).thenReturn(MemberIoStatus.SecondaryDown);
    when(membership.getMemberIoStatus(joiningSecondary))
        .thenReturn(MemberIoStatus.JoiningSecondaryDown);

  }


  /**
   * xx.
   */
  public void mockSecondaryAndArbiterDown(SegmentForm segmentForm) {
    mockSegmentMembership(segmentForm);
    InstanceId secondary = membership.getSecondaries().iterator().next();
    InstanceId arbiter = membership.getArbiters().iterator().next();
    when(membership.getMemberIoStatus(secondary)).thenReturn(MemberIoStatus.SecondaryDown);
    when(membership.getMemberIoStatus(arbiter)).thenReturn(MemberIoStatus.ArbiterDown);
  }


  /**
   * xx.
   */
  public void mockJoingingSeconaryAndArbiterDown(SegmentForm segmentForm) {
    mockSegmentMembership(segmentForm);
    InstanceId joiningSecondary = membership.getJoiningSecondaries().iterator().next();
    InstanceId arbiter = membership.getArbiters().iterator().next();
    when(membership.getMemberIoStatus(joiningSecondary))
        .thenReturn(MemberIoStatus.JoiningSecondaryDown);
    when(membership.getMemberIoStatus(arbiter)).thenReturn(MemberIoStatus.ArbiterDown);
  }


  /**
   * xx.
   */
  public void mockAllSecondaryDown(SegmentForm segmentForm) {
    mockSegmentMembership(segmentForm);
    for (InstanceId secondary : membership.getSecondaries()) {
      when(membership.getMemberIoStatus(secondary)).thenReturn(MemberIoStatus.SecondaryDown);
    }
  }


  /**
   * xx.
   */
  public void mockAllJoiningSecondaryDown(SegmentForm segmentForm) {
    mockSegmentMembership(segmentForm);
    for (InstanceId joiningSecondary : membership.getJoiningSecondaries()) {
      when(membership.getMemberIoStatus(joiningSecondary))
          .thenReturn(MemberIoStatus.JoiningSecondaryDown);
    }
  }


  /**
   * xx.
   */
  public void mockAllMemberDown(SegmentForm segmentForm) {
    mockSegmentMembership(segmentForm);
    InstanceId primary = membership.getPrimary();
    when(membership.getMemberIoStatus(primary)).thenReturn(MemberIoStatus.PrimaryDown);

    for (InstanceId secondary : membership.getSecondaries()) {
      when(membership.getMemberIoStatus(secondary)).thenReturn(MemberIoStatus.SecondaryDown);
    }

    for (InstanceId joiningSecondary : membership.getJoiningSecondaries()) {
      when(membership.getMemberIoStatus(joiningSecondary))
          .thenReturn(MemberIoStatus.JoiningSecondaryDown);
    }

    for (InstanceId arbiter : membership.getArbiters()) {
      when(membership.getMemberIoStatus(arbiter)).thenReturn(MemberIoStatus.ArbiterDown);
    }

  }


  /**
   * xx.
   */
  public void mockPandAllaMemberDown(SegmentForm segmentForm) {
    mockSegmentMembership(segmentForm);
    InstanceId primary = membership.getPrimary();
    when(membership.getMemberIoStatus(primary)).thenReturn(MemberIoStatus.PrimaryDown);

    for (InstanceId arbiter : membership.getArbiters()) {
      when(membership.getMemberIoStatus(arbiter)).thenReturn(MemberIoStatus.ArbiterDown);
    }

  }
}
