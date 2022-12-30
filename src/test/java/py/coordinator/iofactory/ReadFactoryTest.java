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

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.membership.IoActionContext;
import py.membership.IoMember;
import py.membership.SegmentForm;
import py.volume.VolumeType;

public class ReadFactoryTest extends IoFactoryTestBase {

  private static final Logger logger = LoggerFactory.getLogger(ReadFactoryTest.class);
  private ReadFactory readFactory = new ReadFactory(instanceStore);

  private int getCheckReadNumber(IoActionContext ioActionContext) {
    int checkReadNum = 0;
    for (IoMember ioMember : ioActionContext.getIoMembers()) {
      if (ioMember.isCheckRead()) {
        checkReadNum++;
      }
    }
    return checkReadNum;
  }

  private int getRealReadNumber(IoActionContext ioActionContext) {
    int realReadNum = 0;
    for (IoMember ioMember : ioActionContext.getIoMembers()) {
      if (!ioMember.isCheckRead()) {
        realReadNum++;
      }
    }
    return realReadNum;
  }

  @Test
  public void testpss() {
    // PSS OK
    mockSegmentMembership(SegmentForm.PSS);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 2);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.PSS);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 2);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());

    // mock P not OK and one secondary is read down
    mockPrimaryDownAndOneSecondaryReadDown(SegmentForm.PSS);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());

    // mock P not OK and all secondaries are read down
    mockPrimaryDownAndAllSecondariesReadDown(SegmentForm.PSS);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());

    // S not OK
    mockSecondaryDown(SegmentForm.PSS);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 1);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());

    // SS not OK
    mockAllSecondaryDown(SegmentForm.PSS);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());

    // P and S not OK
    mockPrimaryAndSecondaryDown(SegmentForm.PSS);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());

    // ALL not OK
    mockAllMemberDown(SegmentForm.PSS);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());

  }

  @Test
  public void testPsj() {
    // PSJ OK
    mockSegmentMembership(SegmentForm.PSJ);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 2);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());
    assertTrue(!ioActionContext.isJoiningSecondaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.PSJ);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 1);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());
    assertTrue(!ioActionContext.isJoiningSecondaryDown());

    // S not OK
    mockSecondaryDown(SegmentForm.PSJ);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 1);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());
    assertTrue(!ioActionContext.isJoiningSecondaryDown());

    // J not OK
    mockJoiningSecondaryDown(SegmentForm.PSJ);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 1);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());

    // P and S not OK
    mockPrimaryAndSecondaryDown(SegmentForm.PSJ);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 1);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());
    assertTrue(!ioActionContext.isJoiningSecondaryDown());

    // S and J not OK
    mockSecondarAndJoiningSecondaryDown(SegmentForm.PSJ);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());

    // All member not OK
    mockAllMemberDown(SegmentForm.PSJ);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());
  }

  @Test
  public void testPjj() {
    // PJJ OK
    mockSegmentMembership(SegmentForm.PJJ);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.PJJ);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 2);
    assertTrue(ioActionContext.isPrimaryDown());

    // J not OK
    mockJoiningSecondaryDown(SegmentForm.PJJ);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());

    // P and J not OK
    mockPrimaryAndJoiningSecondaryDown(SegmentForm.PJJ);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 1);
    assertTrue(ioActionContext.isPrimaryDown());

    // all J not OK
    mockAllJoiningSecondaryDown(SegmentForm.PJJ);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());

    // all member not OK
    mockAllMemberDown(SegmentForm.PJJ);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(ioActionContext.isPrimaryDown());
  }

  @Test
  public void testpji() {
    // PJ OK
    mockSegmentMembership(SegmentForm.PJI);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.PJI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 1);
    assertTrue(ioActionContext.isPrimaryDown());

    // J not OK
    mockJoiningSecondaryDown(SegmentForm.PJI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());

    // P and J not OK
    mockPrimaryAndJoiningSecondaryDown(SegmentForm.PJI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(ioActionContext.isPrimaryDown());

  }

  @Test
  public void testpii() {
    // P OK
    mockSegmentMembership(SegmentForm.PII);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.PII);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(ioActionContext.isPrimaryDown());

  }

  @Test
  public void testPsi() {
    // PS OK
    mockSegmentMembership(SegmentForm.PSI);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());

    // P not OK, can not read from S for now
    mockPrimaryDown(SegmentForm.PSI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isSecondaryDown());

    // S not OK
    mockSecondaryDown(SegmentForm.PSI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());

    // P and S not OK
    mockPrimaryAndSecondaryDown(SegmentForm.PSI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());

  }

  @Test
  public void testPs() {
    // PS OK
    mockSegmentMembership(SegmentForm.PS);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());

    // P not OK, can not read from S for now
    mockPrimaryDown(SegmentForm.PS);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());

    // S not OK
    mockSecondaryDown(SegmentForm.PS);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());

    // P and S not OK
    mockPrimaryAndSecondaryDown(SegmentForm.PS);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());
  }

  @Test
  public void testPj() {
    // PJ OK
    mockSegmentMembership(SegmentForm.PJ);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.PJ);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 1);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isJoiningSecondaryDown());

    // J not OK
    mockJoiningSecondaryDown(SegmentForm.PJ);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());

    // P and J not OK
    mockPrimaryAndJoiningSecondaryDown(SegmentForm.PJ);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());
  }

  @Test
  public void testPi() {
    // P OK
    mockSegmentMembership(SegmentForm.PI);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.PI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(ioActionContext.isPrimaryDown());
  }

  @Test
  public void testPsa() {
    // PSA OK
    mockVolumeTyep(VolumeType.SMALL);
    mockSegmentMembership(SegmentForm.PSA);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 2);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.PSA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 1);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());

    // S not OK
    mockSecondaryDown(SegmentForm.PSA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 1);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());

    // A not OK
    mockArbiterDown(SegmentForm.PSA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 1);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());

    // P and S not OK
    mockPrimaryAndSecondaryDown(SegmentForm.PSA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 1);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());

    // P and A not OK
    mockPrimaryAndArbiterDown(SegmentForm.PSA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());

    // S and A not OK
    mockSecondaryAndArbiterDown(SegmentForm.PSA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());

    // All member not OK
    mockAllMemberDown(SegmentForm.PSA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());
  }

  @Test
  public void testPja() {
    // PJA OK
    mockVolumeTyep(VolumeType.SMALL);
    mockSegmentMembership(SegmentForm.PJA);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.PJA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 2);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isJoiningSecondaryDown());

    // J not OK
    mockJoiningSecondaryDown(SegmentForm.PJA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());

    // A not OK
    mockArbiterDown(SegmentForm.PJA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());

    // P and J not OK
    mockPrimaryAndJoiningSecondaryDown(SegmentForm.PJA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 1);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());

    // P and A not OK
    mockPrimaryAndArbiterDown(SegmentForm.PJA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 1);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isJoiningSecondaryDown());

    // J and A not OK
    mockJoingingSeconaryAndArbiterDown(SegmentForm.PJA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());

    // All member not OK
    mockAllMemberDown(SegmentForm.PJA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());
  }

  @Test
  public void testPia() {
    // PA OK
    mockVolumeTyep(VolumeType.SMALL);
    mockSegmentMembership(SegmentForm.PIA);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.PIA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 1);
    assertTrue(ioActionContext.isPrimaryDown());

    // A not OK
    mockArbiterDown(SegmentForm.PIA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());

    // P and A not OK
    mockPrimaryAndArbiterDown(SegmentForm.PIA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(ioActionContext.isPrimaryDown());
  }

  @Test
  public void testPa() {
    // PA OK
    mockVolumeTyep(VolumeType.SMALL);
    mockSegmentMembership(SegmentForm.PA);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.PA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 1);
    assertTrue(ioActionContext.isPrimaryDown());

    // A not OK
    mockArbiterDown(SegmentForm.PA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());

    // P and A not OK
    mockPrimaryAndArbiterDown(SegmentForm.PA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(ioActionContext.isPrimaryDown());
  }

  @Test
  public void testTps() {
    // PS OK
    mockVolumeTyep(VolumeType.SMALL);
    mockSegmentMembership(SegmentForm.TPS);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());

    // P not OK, can not read from S for now
    mockPrimaryDown(SegmentForm.TPS);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());

    // S not OK
    mockSecondaryDown(SegmentForm.TPS);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());

    // P and S not OK
    mockPrimaryAndSecondaryDown(SegmentForm.TPS);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());
  }

  @Test
  public void testTpj() {
    // PJ OK
    mockVolumeTyep(VolumeType.SMALL);
    mockSegmentMembership(SegmentForm.TPJ);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.TPJ);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 1);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isJoiningSecondaryDown());

    // J not OK
    mockJoiningSecondaryDown(SegmentForm.TPJ);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());

    // P and J not OK
    mockPrimaryAndJoiningSecondaryDown(SegmentForm.TPJ);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());
  }

  @Test
  public void testTpi() {
    // P OK
    mockVolumeTyep(VolumeType.SMALL);
    mockSegmentMembership(SegmentForm.TPI);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 1);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(!ioActionContext.isPrimaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.TPI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(getRealReadNumber(ioActionContext) == 0);
    assertTrue(getCheckReadNumber(ioActionContext) == 0);
    assertTrue(ioActionContext.isPrimaryDown());

  }

}
