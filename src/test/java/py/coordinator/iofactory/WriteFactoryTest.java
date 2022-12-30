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

import org.junit.Ignore;
import org.junit.Test;
import py.common.struct.EndPoint;
import py.instance.InstanceId;
import py.membership.IoActionContext;
import py.membership.IoMember;
import py.membership.MemberIoStatus;
import py.membership.SegmentForm;
import py.volume.VolumeType;

@Ignore
public class WriteFactoryTest extends IoFactoryTestBase {

  private WriteFactory writeFactory = new WriteFactory(instanceStore);

  @Test
  public void testCanGenerateNewPrimary() {
    assertTrue(SegmentForm.PSS.canGenerateNewPrimary());
    assertTrue(SegmentForm.PSJ.canGenerateNewPrimary());
    assertTrue(SegmentForm.PSI.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PJI.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PJJ.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PII.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PS.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PJ.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PI.canGenerateNewPrimary());
    assertTrue(SegmentForm.PSA.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PJA.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PIA.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PA.canGenerateNewPrimary());
    assertTrue(!SegmentForm.TPS.canGenerateNewPrimary());
    assertTrue(!SegmentForm.TPJ.canGenerateNewPrimary());
    assertTrue(!SegmentForm.TPI.canGenerateNewPrimary());
  }

  @Test
  public void testOnlyPrimary() {
    assertTrue(!SegmentForm.PSS.onlyPrimary());
    assertTrue(!SegmentForm.PSJ.onlyPrimary());
    assertTrue(!SegmentForm.PSI.onlyPrimary());
    assertTrue(!SegmentForm.PJI.onlyPrimary());
    assertTrue(!SegmentForm.PJJ.onlyPrimary());
    assertTrue(SegmentForm.PII.onlyPrimary());
    assertTrue(!SegmentForm.PS.onlyPrimary());
    assertTrue(!SegmentForm.PJ.onlyPrimary());
    assertTrue(SegmentForm.PI.onlyPrimary());
    assertTrue(!SegmentForm.PSA.onlyPrimary());
    assertTrue(!SegmentForm.PJA.onlyPrimary());
    assertTrue(SegmentForm.PIA.onlyPrimary());
    assertTrue(SegmentForm.PA.onlyPrimary());
    assertTrue(!SegmentForm.TPS.onlyPrimary());
    assertTrue(!SegmentForm.TPJ.onlyPrimary());
    assertTrue(SegmentForm.TPI.onlyPrimary());
  }

  @Test
  public void testWriteRequestDoneDirectly() {
    assertTrue(!SegmentForm.PSS.writeDoneDirectly(1, 0, 0, 0, VolumeType.REGULAR));
    assertTrue(!SegmentForm.PSS.writeDoneDirectly(0, 1, 0, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PSS.writeDoneDirectly(1, 1, 0, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PSS.writeDoneDirectly(1, 2, 0, 0, VolumeType.REGULAR));

    assertTrue(!SegmentForm.PSJ.writeDoneDirectly(0, 1, 0, 0, VolumeType.REGULAR));
    assertTrue(!SegmentForm.PSJ.writeDoneDirectly(1, 0, 0, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PSJ.writeDoneDirectly(1, 1, 0, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PSJ.writeDoneDirectly(1, 0, 1, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PSJ.writeDoneDirectly(1, 1, 1, 0, VolumeType.REGULAR));

    assertTrue(SegmentForm.PSI.writeDoneDirectly(0, 1, 0, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PSI.writeDoneDirectly(1, 0, 0, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PSI.writeDoneDirectly(1, 1, 0, 0, VolumeType.REGULAR));

    assertTrue(!SegmentForm.PJI.writeDoneDirectly(0, 0, 1, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PJI.writeDoneDirectly(1, 0, 0, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PJI.writeDoneDirectly(1, 0, 1, 0, VolumeType.REGULAR));

    assertTrue(SegmentForm.PJJ.writeDoneDirectly(0, 0, 0, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PJJ.writeDoneDirectly(0, 0, 1, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PJJ.writeDoneDirectly(1, 0, 1, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PJJ.writeDoneDirectly(1, 0, 2, 0, VolumeType.REGULAR));

    assertTrue(!SegmentForm.PII.writeDoneDirectly(0, 0, 0, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PII.writeDoneDirectly(1, 0, 0, 0, VolumeType.REGULAR));

    assertTrue(!SegmentForm.PS.writeDoneDirectly(0, 1, 0, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PS.writeDoneDirectly(1, 0, 0, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PS.writeDoneDirectly(1, 1, 0, 0, VolumeType.REGULAR));

    assertTrue(!SegmentForm.PJ.writeDoneDirectly(0, 0, 1, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PJ.writeDoneDirectly(1, 0, 0, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PJ.writeDoneDirectly(1, 0, 1, 0, VolumeType.REGULAR));

    assertTrue(!SegmentForm.PI.writeDoneDirectly(0, 0, 0, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PI.writeDoneDirectly(1, 0, 0, 0, VolumeType.REGULAR));

    assertTrue(!SegmentForm.PSA.writeDoneDirectly(0, 1, 0, 0, VolumeType.SMALL));
    assertTrue(!SegmentForm.PSA.writeDoneDirectly(1, 0, 0, 0, VolumeType.SMALL));
    assertTrue(SegmentForm.PSA.writeDoneDirectly(1, 1, 0, 0, VolumeType.SMALL));

    assertTrue(!SegmentForm.PJA.writeDoneDirectly(0, 0, 1, 0, VolumeType.SMALL));
    assertTrue(SegmentForm.PJA.writeDoneDirectly(1, 0, 0, 0, VolumeType.SMALL));
    assertTrue(SegmentForm.PJA.writeDoneDirectly(1, 0, 1, 0, VolumeType.SMALL));

    assertTrue(SegmentForm.PIA.writeDoneDirectly(1, 0, 0, 0, VolumeType.SMALL));

    assertTrue(SegmentForm.PA.writeDoneDirectly(1, 0, 0, 0, VolumeType.SMALL));

    assertTrue(!SegmentForm.TPS.writeDoneDirectly(0, 1, 0, 0, VolumeType.SMALL));
    assertTrue(SegmentForm.TPS.writeDoneDirectly(1, 0, 0, 0, VolumeType.SMALL));
    assertTrue(SegmentForm.TPS.writeDoneDirectly(1, 1, 0, 0, VolumeType.SMALL));

    assertTrue(!SegmentForm.TPJ.writeDoneDirectly(0, 0, 1, 0, VolumeType.SMALL));
    assertTrue(SegmentForm.TPJ.writeDoneDirectly(1, 0, 0, 0, VolumeType.SMALL));
    assertTrue(SegmentForm.TPJ.writeDoneDirectly(1, 0, 1, 0, VolumeType.SMALL));

    assertTrue(SegmentForm.TPI.writeDoneDirectly(1, 0, 0, 0, VolumeType.SMALL));
  }

  @Test
  public void testReadRequestDoneDirectly() {
    assertTrue(!SegmentForm.PSS.readDoneDirectly(1, 0, 0, 0, VolumeType.REGULAR));
    assertTrue(!SegmentForm.PSS.readDoneDirectly(0, 1, 0, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PSS.readDoneDirectly(1, 1, 0, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PSS.readDoneDirectly(1, 2, 0, 0, VolumeType.REGULAR));

    assertTrue(!SegmentForm.PSJ.readDoneDirectly(0, 1, 0, 0, VolumeType.REGULAR));
    assertTrue(!SegmentForm.PSJ.readDoneDirectly(1, 0, 0, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PSJ.readDoneDirectly(1, 1, 0, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PSJ.readDoneDirectly(1, 0, 1, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PSJ.readDoneDirectly(1, 1, 1, 0, VolumeType.REGULAR));

    assertTrue(!SegmentForm.PSI.readDoneDirectly(0, 1, 0, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PSI.readDoneDirectly(1, 0, 0, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PSI.readDoneDirectly(1, 1, 0, 0, VolumeType.REGULAR));

    assertTrue(!SegmentForm.PJI.readDoneDirectly(0, 0, 1, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PJI.readDoneDirectly(1, 0, 0, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PJI.readDoneDirectly(1, 0, 1, 0, VolumeType.REGULAR));

    assertTrue(!SegmentForm.PJJ.readDoneDirectly(0, 0, 0, 0, VolumeType.REGULAR));
    assertTrue(!SegmentForm.PJJ.readDoneDirectly(0, 0, 1, 0, VolumeType.REGULAR));
    assertTrue(!SegmentForm.PJJ.readDoneDirectly(0, 0, 2, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PJJ.readDoneDirectly(1, 0, 1, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PJJ.readDoneDirectly(1, 0, 2, 0, VolumeType.REGULAR));

    assertTrue(!SegmentForm.PII.readDoneDirectly(0, 0, 0, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PII.readDoneDirectly(1, 0, 0, 0, VolumeType.REGULAR));

    assertTrue(!SegmentForm.PS.readDoneDirectly(0, 1, 0, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PS.readDoneDirectly(1, 0, 0, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PS.readDoneDirectly(1, 1, 0, 0, VolumeType.REGULAR));

    assertTrue(!SegmentForm.PJ.readDoneDirectly(0, 0, 1, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PJ.readDoneDirectly(1, 0, 0, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PJ.readDoneDirectly(1, 0, 1, 0, VolumeType.REGULAR));

    assertTrue(!SegmentForm.PI.readDoneDirectly(0, 0, 0, 0, VolumeType.REGULAR));
    assertTrue(SegmentForm.PI.readDoneDirectly(1, 0, 0, 0, VolumeType.REGULAR));

    assertTrue(!SegmentForm.PSA.readDoneDirectly(1, 0, 0, 0, VolumeType.SMALL));
    assertTrue(!SegmentForm.PSA.readDoneDirectly(0, 1, 0, 0, VolumeType.SMALL));
    assertTrue(!SegmentForm.PSA.readDoneDirectly(0, 0, 0, 1, VolumeType.SMALL));
    assertTrue(SegmentForm.PSA.readDoneDirectly(1, 1, 0, 0, VolumeType.SMALL));
    assertTrue(SegmentForm.PSA.readDoneDirectly(1, 0, 0, 1, VolumeType.SMALL));
    assertTrue(SegmentForm.PSA.readDoneDirectly(0, 1, 0, 1, VolumeType.SMALL));
    assertTrue(SegmentForm.PSA.readDoneDirectly(1, 1, 0, 1, VolumeType.SMALL));

    assertTrue(!SegmentForm.PJA.readDoneDirectly(0, 0, 1, 0, VolumeType.SMALL));
    assertTrue(SegmentForm.PJA.readDoneDirectly(1, 0, 0, 0, VolumeType.SMALL));
    assertTrue(SegmentForm.PJA.readDoneDirectly(1, 0, 1, 0, VolumeType.SMALL));

    assertTrue(SegmentForm.PIA.readDoneDirectly(1, 0, 0, 0, VolumeType.SMALL));

    assertTrue(SegmentForm.PA.readDoneDirectly(1, 0, 0, 0, VolumeType.SMALL));

    assertTrue(!SegmentForm.TPS.readDoneDirectly(0, 1, 0, 0, VolumeType.SMALL));
    assertTrue(SegmentForm.TPS.readDoneDirectly(1, 0, 0, 0, VolumeType.SMALL));
    assertTrue(SegmentForm.TPS.readDoneDirectly(1, 1, 0, 0, VolumeType.SMALL));

    assertTrue(!SegmentForm.TPJ.readDoneDirectly(0, 0, 1, 0, VolumeType.SMALL));
    assertTrue(SegmentForm.TPJ.readDoneDirectly(1, 0, 0, 0, VolumeType.SMALL));
    assertTrue(SegmentForm.TPJ.readDoneDirectly(1, 0, 1, 0, VolumeType.SMALL));

    assertTrue(SegmentForm.TPI.readDoneDirectly(1, 0, 0, 0, VolumeType.SMALL));
  }

  @Test
  public void testGotTempPrimary() {
    IoActionContext ioActionContext = new IoActionContext();
    ioActionContext.addIoMember(
        new IoMember(new InstanceId(0L), new EndPoint("127.0.0.1", 1234),
            MemberIoStatus.Secondary));
    assertTrue(!ioActionContext.gotTempPrimary());
    ioActionContext.addIoMember(
        new IoMember(new InstanceId(1L), new EndPoint("127.0.0.2", 1234),
            MemberIoStatus.TempPrimary));
    assertTrue(ioActionContext.gotTempPrimary());
  }

  @Test
  public void testUnstablePrimary() {
    // PSS OK
    mockSegmentMembership(SegmentForm.PSS);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isUnstablePrimaryWrite());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 3);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());

    // P not OK
    mockPrimaryAsUnstablePrimary(SegmentForm.PSS);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);
    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isUnstablePrimaryWrite());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 3);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());
  }

  @Test
  public void testpss() {
    // PSS OK
    mockSegmentMembership(SegmentForm.PSS);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 3);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.PSS);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 2);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());

    // S not OK
    mockSecondaryDown(SegmentForm.PSS);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 2);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());

    // SS not OK
    mockAllSecondaryDown(SegmentForm.PSS);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());

    // P and S not OK
    mockPrimaryAndSecondaryDown(SegmentForm.PSS);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());

    // ALL not OK
    mockAllMemberDown(SegmentForm.PSS);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());

  }

  @Test
  public void testpsj() {
    // PSJ OK
    mockSegmentMembership(SegmentForm.PSJ);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 3);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());
    assertTrue(!ioActionContext.isJoiningSecondaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.PSJ);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 2);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());
    assertTrue(!ioActionContext.isJoiningSecondaryDown());

    // S not OK
    mockSecondaryDown(SegmentForm.PSJ);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 2);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());
    assertTrue(!ioActionContext.isJoiningSecondaryDown());

    // J not OK
    mockJoiningSecondaryDown(SegmentForm.PSJ);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 2);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());

    // P and S not OK
    mockPrimaryAndSecondaryDown(SegmentForm.PSJ);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());
    assertTrue(!ioActionContext.isJoiningSecondaryDown());

    // S and J not OK
    mockSecondarAndJoiningSecondaryDown(SegmentForm.PSJ);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());

    // All member not OK
    mockAllMemberDown(SegmentForm.PSJ);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());
  }

  @Test
  public void testpjj() {
    // PJJ OK
    mockSegmentMembership(SegmentForm.PJJ);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.getIoMembers().size() == 3);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isJoiningSecondaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.PJJ);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 2);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isJoiningSecondaryDown());

    // J not OK
    mockJoiningSecondaryDown(SegmentForm.PJJ);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 2);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isJoiningSecondaryDown());

    // P and J not OK
    mockPrimaryAndJoiningSecondaryDown(SegmentForm.PJJ);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isJoiningSecondaryDown());

    // all J not OK
    mockAllJoiningSecondaryDown(SegmentForm.PJJ);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());

    // all member not OK
    mockAllMemberDown(SegmentForm.PJJ);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());
  }

  @Test
  public void testpji() {
    // PJ OK
    mockSegmentMembership(SegmentForm.PJI);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 2);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isJoiningSecondaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.PJI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isJoiningSecondaryDown());

    // J not OK
    mockJoiningSecondaryDown(SegmentForm.PJI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());

    // P and J not OK
    mockPrimaryAndJoiningSecondaryDown(SegmentForm.PJI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());

  }

  @Test
  public void testpii() {
    // P OK
    mockSegmentMembership(SegmentForm.PII);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(!ioActionContext.isPrimaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.PII);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 0);
    assertTrue(ioActionContext.isPrimaryDown());

  }

  @Test
  public void testpsi() {
    // PS OK
    mockSegmentMembership(SegmentForm.PSI);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 2);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.PSI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());

    // S not OK
    mockSecondaryDown(SegmentForm.PSI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());

    // P and S not OK
    mockPrimaryAndSecondaryDown(SegmentForm.PSI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());

  }

  @Test
  public void testPs() {
    // PS OK
    mockSegmentMembership(SegmentForm.PS);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 2);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.PS);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());

    // S not OK
    mockSecondaryDown(SegmentForm.PS);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());

    // P and S not OK
    mockPrimaryAndSecondaryDown(SegmentForm.PS);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());
  }

  @Test
  public void testPj() {
    // PJ OK
    mockSegmentMembership(SegmentForm.PJ);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 2);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isJoiningSecondaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.PJ);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isJoiningSecondaryDown());

    // J not OK
    mockJoiningSecondaryDown(SegmentForm.PJ);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());

    // P and J not OK
    mockPrimaryAndJoiningSecondaryDown(SegmentForm.PJ);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());
  }

  @Test
  public void testPi() {
    // P OK
    mockSegmentMembership(SegmentForm.PI);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(!ioActionContext.isPrimaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.PI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.REGULAR, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 0);
    assertTrue(ioActionContext.isPrimaryDown());
  }

  @Test
  public void testpsa() {
    // PSA OK
    mockVolumeTyep(VolumeType.SMALL);
    mockSegmentMembership(SegmentForm.PSA);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 2);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.PSA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());

    // S not OK
    mockSecondaryDown(SegmentForm.PSA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());

    // A not OK
    mockArbiterDown(SegmentForm.PSA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 2);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());

    // P and S not OK
    mockPrimaryAndSecondaryDown(SegmentForm.PSA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());

    // P and A not OK
    mockPrimaryAndArbiterDown(SegmentForm.PSA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());

    // S and A not OK
    mockSecondaryAndArbiterDown(SegmentForm.PSA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());

    // All member not OK
    mockAllMemberDown(SegmentForm.PSA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());
  }

  @Test
  public void testpja() {
    // PJA OK
    mockVolumeTyep(VolumeType.SMALL);
    mockSegmentMembership(SegmentForm.PJA);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 2);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isJoiningSecondaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.PJA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isJoiningSecondaryDown());

    // J not OK
    mockJoiningSecondaryDown(SegmentForm.PJA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());

    // A not OK
    mockArbiterDown(SegmentForm.PJA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 2);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isJoiningSecondaryDown());

    // P and J not OK
    mockPrimaryAndJoiningSecondaryDown(SegmentForm.PJA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());

    // P and A not OK
    mockPrimaryAndArbiterDown(SegmentForm.PJA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isJoiningSecondaryDown());

    // J and A not OK
    mockJoingingSeconaryAndArbiterDown(SegmentForm.PJA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());

    // All member not OK
    mockAllMemberDown(SegmentForm.PJA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());
  }

  @Test
  public void testpia() {
    // PA OK
    mockVolumeTyep(VolumeType.SMALL);
    mockSegmentMembership(SegmentForm.PIA);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(!ioActionContext.isPrimaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.PIA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 0);
    assertTrue(ioActionContext.isPrimaryDown());

    // A not OK
    mockArbiterDown(SegmentForm.PIA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(!ioActionContext.isPrimaryDown());

    // P and A not OK
    mockPrimaryAndArbiterDown(SegmentForm.PIA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 0);
    assertTrue(ioActionContext.isPrimaryDown());
  }

  @Test
  public void testPa() {
    // PA OK
    mockVolumeTyep(VolumeType.SMALL);
    mockSegmentMembership(SegmentForm.PA);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(!ioActionContext.isPrimaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.PA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 0);
    assertTrue(ioActionContext.isPrimaryDown());

    // A not OK
    mockArbiterDown(SegmentForm.PA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(!ioActionContext.isPrimaryDown());

    // P and A not OK
    mockPrimaryAndArbiterDown(SegmentForm.PA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 0);
    assertTrue(ioActionContext.isPrimaryDown());
  }

  @Test
  public void testtps() {
    // PS OK
    mockVolumeTyep(VolumeType.SMALL);
    mockSegmentMembership(SegmentForm.TPS);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 2);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.TPS);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());

    // S not OK
    mockSecondaryDown(SegmentForm.TPS);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());

    // P and S not OK
    mockPrimaryAndSecondaryDown(SegmentForm.TPS);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isSecondaryDown());
  }

  @Test
  public void testtpj() {
    // PJ OK
    mockVolumeTyep(VolumeType.SMALL);
    mockSegmentMembership(SegmentForm.TPJ);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 2);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isJoiningSecondaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.TPJ);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isJoiningSecondaryDown());

    // J not OK
    mockJoiningSecondaryDown(SegmentForm.TPJ);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());

    // P and J not OK
    mockPrimaryAndJoiningSecondaryDown(SegmentForm.TPJ);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 0);
    assertTrue(ioActionContext.isPrimaryDown());
    assertTrue(ioActionContext.isJoiningSecondaryDown());
  }

  @Test
  public void testtpi() {
    // P OK
    mockVolumeTyep(VolumeType.SMALL);
    mockSegmentMembership(SegmentForm.TPI);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 1);
    assertTrue(!ioActionContext.isPrimaryDown());

    // P not OK
    mockPrimaryDown(SegmentForm.TPI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.SMALL, segId, 0L);

    assertTrue(ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 0);
    assertTrue(ioActionContext.isPrimaryDown());

  }
}
