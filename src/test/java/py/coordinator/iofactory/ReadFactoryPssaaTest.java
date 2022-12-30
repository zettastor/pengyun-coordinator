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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.membership.IoActionContext;
import py.membership.IoMember;
import py.membership.SegmentForm;
import py.volume.VolumeType;

@Ignore
public class ReadFactoryPssaaTest extends IoFactoryTestBase {

  private static final Logger logger = LoggerFactory.getLogger(ReadFactoryPssaaTest.class);
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

  //true is down or there is no this action

  /**
   * xx.
   */
  public void checkValuePrimaryAndSecondary(IoActionContext ioActionContext, boolean isZombie,
      boolean isResend, int realNumber,
      int checkNumber, boolean isPrimaryDown, boolean isSecondaryDown) {
    assertTrue(ioActionContext.isZombieRequest() == isZombie);
    assertTrue(ioActionContext.isResendDirectly() == isResend);
    assertTrue(getRealReadNumber(ioActionContext) == realNumber);
    assertTrue(getCheckReadNumber(ioActionContext) == checkNumber);
    assertTrue(ioActionContext.isPrimaryDown() == isPrimaryDown);
    assertTrue(ioActionContext.isSecondaryDown() == isSecondaryDown);
  }


  /**
   * xx.
   */
  public void checkValueAll(IoActionContext ioActionContext, boolean isZombie, boolean isResend,
      int realNumber,
      int checkNumber, boolean isPrimaryDown, boolean isSecondaryDown,
      boolean isJoiningSecondaryDown) {
    assertTrue(ioActionContext.isZombieRequest() == isZombie);
    assertTrue(ioActionContext.isResendDirectly() == isResend);
    assertTrue(getRealReadNumber(ioActionContext) == realNumber);
    assertTrue(getCheckReadNumber(ioActionContext) == checkNumber);
    assertTrue(ioActionContext.isPrimaryDown() == isPrimaryDown);
    assertTrue(ioActionContext.isSecondaryDown() == isSecondaryDown);
    assertTrue(ioActionContext.isJoiningSecondaryDown() == isJoiningSecondaryDown);
  }


  @Test
  public void testPssaaAndCheckFetchorCheckRead() {
    mockVolumeTyep(VolumeType.LARGE);
    // PSSAA OK
    mockSegmentMembership(SegmentForm.PSSAA);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 1, 4, false,
        false);
    assertTrue(!ioActionContext.getFetchReader().isEmpty());

    // P not OK SSAA
    mockPrimaryDown(SegmentForm.PSSAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, false, 1, 3, true,
        false);
    assertTrue(!ioActionContext.getFetchReader().isEmpty());

    //PSAA
    mockSecondaryDown(SegmentForm.PSSAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 1, 3, false,
        false);
    assertTrue(!ioActionContext.getFetchReader().isEmpty());

    //PSSA
    mockArbiterDown(SegmentForm.PSSAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 1, 3, false,
        false);
    assertTrue(!ioActionContext.getFetchReader().isEmpty());

    // mock P not OK and one secondary is read down SAA
    mockPrimaryDownAndOneSecondaryReadDown(SegmentForm.PSSAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, false, 1, 2, true,
        false);
    assertTrue(!ioActionContext.getFetchReader().isEmpty());

    // mock P not OK and all secondaries are read down  AA
    mockPrimaryDownAndAllSecondariesReadDown(SegmentForm.PSSAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 0, 2, true,
        true);
    assertTrue(ioActionContext.getFetchReader().isEmpty());

    // SS not OK PAA
    mockAllSecondaryDown(SegmentForm.PSSAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 1, 2, false,
        true);
    assertTrue(!ioActionContext.getFetchReader().isEmpty());

    //SSA P Adown
    mockPrimaryAndArbiterDown(SegmentForm.PSSAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, false, 1, 2, true,
        false);
    assertTrue(!ioActionContext.getFetchReader().isEmpty());

    //PSA SAdown
    mockSecondaryAndArbiterDown(SegmentForm.PSSAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 1, 2, false,
        false);
    assertTrue(!ioActionContext.getFetchReader().isEmpty());

    //PSS AAdown
    mockAllArbiterDown(SegmentForm.PSSAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 1, 2, false,
        false);
    assertTrue(!ioActionContext.getFetchReader().isEmpty());

    //SS
    mockPandAllaMemberDown(SegmentForm.PSSAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 1, 1, true,
        false);
    assertTrue(!ioActionContext.getFetchReader().isEmpty());

    // ALL not OK
    mockAllMemberDown(SegmentForm.PSSAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 0, 0, true,
        true);
    assertTrue(ioActionContext.getFetchReader().isEmpty());

  }

  @Test
  public void testpssai() {
    mockVolumeTyep(VolumeType.LARGE);
    // PSSAI OK
    mockSegmentMembership(SegmentForm.PSSAI);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 1, 3, false,
        false);

    //SSAI
    mockPrimaryDown(SegmentForm.PSSAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, false, 1, 2, true,
        false);

    //PSAI
    mockSecondaryDown(SegmentForm.PSSAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 1, 2, false,
        false);

    //PSSI
    mockArbiterDown(SegmentForm.PSSAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 1, 2, false,
        false);

    //SSA
    mockPrimaryDown(SegmentForm.PSSAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, false, 1, 2, true,
        false);

    //SAI
    mockPrimaryAndSecondaryDown(SegmentForm.PSSAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 1, 1, true,
        false);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PSSAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 0, 0, true,
        true);

  }

  @Test
  public void testpssii() {
    mockVolumeTyep(VolumeType.LARGE);
    // PSSII OK
    mockSegmentMembership(SegmentForm.PSSII);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 1, 2, false,
        false);

    //SSII
    mockPrimaryDown(SegmentForm.PSSII);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 1, 1, true,
        false);

    //PSII
    mockSecondaryDown(SegmentForm.PSSII);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, true, 1, 1, false,
        false);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PSSII);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 0, 0, true,
        true);

  }

  @Test
  public void testpsjaa() {
    mockVolumeTyep(VolumeType.LARGE);
    // PSJAA OK
    mockSegmentMembership(SegmentForm.PSJAA);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 4, false,
        false, false);

    //SJAA
    mockPrimaryDown(SegmentForm.PSJAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, false, 1, 3, true,
        false, false);

    //PJAA
    mockSecondaryDown(SegmentForm.PSJAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 3, false,
        true, false);

    //PSAA
    mockJoiningSecondaryDown(SegmentForm.PSJAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 3, false,
        false, true);

    //PSJA
    mockArbiterDown(SegmentForm.PSJAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 3, false,
        false, false);

    //JAA
    mockPrimaryAndSecondaryDown(SegmentForm.PSJAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 3, true,
        true, false);

    //SAA
    mockPrimaryAndJoiningSecondaryDown(SegmentForm.PSJAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, false, 1, 2, true,
        false, true);

    //SJA
    mockPrimaryAndArbiterDown(SegmentForm.PSJAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, false, 1, 2, true,
        false, false);

    //PAA
    mockSecondarAndJoiningSecondaryDown(SegmentForm.PSJAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 2, false,
        true, true);

    //PJA
    mockSecondaryAndArbiterDown(SegmentForm.PSJAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 2, false,
        true, false);

    //PSA
    mockJoingingSeconaryAndArbiterDown(SegmentForm.PSJAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 2, false,
        false, true);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PSJAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 0, true,
        true, true);

  }

  @Test
  public void testpsjai() {
    mockVolumeTyep(VolumeType.LARGE);
    // PSJAI OK
    mockSegmentMembership(SegmentForm.PSJAI);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 3, false,
        false, false);

    //SJAI
    mockPrimaryDown(SegmentForm.PSJAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, false, 1, 2, true,
        false, false);

    //PJAI
    mockSecondaryDown(SegmentForm.PSJAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 2, false,
        true, false);

    //PSAI
    mockJoiningSecondaryDown(SegmentForm.PSJAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 2, false,
        false, true);

    //PSJI
    mockArbiterDown(SegmentForm.PSJAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 2, false,
        false, false);

    //JAI
    mockPrimaryAndSecondaryDown(SegmentForm.PSJAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 2, true,
        true, false);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PSJAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 0, true,
        true, true);

  }

  @Test
  public void testpsjii() {
    mockVolumeTyep(VolumeType.LARGE);
    // PSJII OK
    mockSegmentMembership(SegmentForm.PSJII);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 2, false,
        false, false);

    //SJII
    mockPrimaryDown(SegmentForm.PSJII);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 1, 1, true,
        false, false);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PSJII);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 0, true,
        true, true);

  }

  @Test
  public void testpsiaa() {
    mockVolumeTyep(VolumeType.LARGE);
    // PSIAA OK
    mockSegmentMembership(SegmentForm.PSIAA);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 1, 3, false,
        false);

    //SIAA
    mockPrimaryDown(SegmentForm.PSIAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, false, 1, 2, true,
        false);

    //PIAA
    mockSecondaryDown(SegmentForm.PSIAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 1, 2, false,
        true);

    //PSIA
    mockArbiterDown(SegmentForm.PSIAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 1, 2, false,
        false);

    //PSI AA
    mockAllArbiterDown(SegmentForm.PSIAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, true, 1, 1, false,
        false);

    //AAI PS
    mockPrimaryAndSecondaryDown(SegmentForm.PSIAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 0, 2, true,
        true);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PSIAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 0, 0, true,
        true);

  }

  @Test
  public void testpsiai() {
    mockVolumeTyep(VolumeType.LARGE);
    // PSIAI OK
    mockSegmentMembership(SegmentForm.PSIAI);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 1, 2, false,
        false);

    //SIAI
    mockPrimaryDown(SegmentForm.PSIAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 1, 1, true,
        false);

    //PIAI
    mockSecondaryDown(SegmentForm.PSIAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, true, 1, 1, false,
        true);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PSIAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 0, 0, true,
        true);
  }

  @Test
  public void testpsiii() {
    mockVolumeTyep(VolumeType.LARGE);
    // PSIII OK
    mockSegmentMembership(SegmentForm.PSIII);

    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, true, 1, 1, false,
        false);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PSIII);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 0, 0, true,
        true);

  }

  @Test
  public void testpjjaa() {
    mockVolumeTyep(VolumeType.LARGE);
    // PJJAA OK
    mockSegmentMembership(SegmentForm.PJJAA);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 4, false,
        true, false);

    //JJAA
    mockPrimaryDown(SegmentForm.PJJAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 4, true,
        true, false);

    //PJAA
    mockJoiningSecondaryDown(SegmentForm.PJJAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 3, false,
        true, false);

    //PJJA
    mockArbiterDown(SegmentForm.PJJAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 3, false,
        true, false);

    //JAA PJ
    mockPrimaryAndJoiningSecondaryDown(SegmentForm.PJJAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 3, true,
        true, false);

    //JJA
    mockPrimaryAndArbiterDown(SegmentForm.PJJAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 3, true,
        true, false);

    //PJA
    mockJoingingSeconaryAndArbiterDown(SegmentForm.PJJAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 2, false,
        true, false);

    //PJJ
    mockAllArbiterDown(SegmentForm.PJJAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 2, false,
        true, false);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PJJAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 0, true,
        true, true);

  }

  @Test
  public void testpjjai() {
    mockVolumeTyep(VolumeType.LARGE);
    // PJJAI OK
    mockSegmentMembership(SegmentForm.PJJAI);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 3, false,
        true, false);

    //JJAI
    mockPrimaryDown(SegmentForm.PJJAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 3, true,
        true, false);

    //PJAI
    mockJoiningSecondaryDown(SegmentForm.PJJAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 2, false,
        true, false);

    //PJJI
    mockArbiterDown(SegmentForm.PJJAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 2, false,
        true, false);

    //JAI
    mockPrimaryAndJoiningSecondaryDown(SegmentForm.PJJAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 2, true,
        true, false);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PJJAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 0, true,
        true, true);

  }

  @Test
  public void testpjjii() {
    mockVolumeTyep(VolumeType.LARGE);
    // PJJII OK
    mockSegmentMembership(SegmentForm.PJJII);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 2, false,
        true, false);

    //JJII
    mockPrimaryDown(SegmentForm.PJJII);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 2, true,
        true, false);

    //PJII
    mockJoiningSecondaryDown(SegmentForm.PJJII);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, true, 1, 1, false,
        true, false);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PJJII);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 0, true,
        true, true);

  }

  @Test
  public void testpjiaa() {
    mockVolumeTyep(VolumeType.LARGE);
    // PJIAA OK
    mockSegmentMembership(SegmentForm.PJIAA);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 3, false,
        true, false);

    //JIAA
    mockPrimaryDown(SegmentForm.PJIAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 3, true,
        true, false);

    //PIAA
    mockJoiningSecondaryDown(SegmentForm.PJIAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 2, false,
        true, true);

    //PJAI
    mockArbiterDown(SegmentForm.PJIAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 2, false,
        true, false);

    //AAI PJ
    mockPrimaryAndJoiningSecondaryDown(SegmentForm.PJIAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 2, true,
        true, true);

    //PAI JA
    mockJoingingSeconaryAndArbiterDown(SegmentForm.PJIAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, true, 1, 1, false,
        true, true);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PJIAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 0, true,
        true, true);

  }

  @Test
  public void testpjiai() {
    mockVolumeTyep(VolumeType.LARGE);
    // PJIAI OK
    mockSegmentMembership(SegmentForm.PJIAI);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 2, false,
        true, false);

    //JIAI
    mockPrimaryDown(SegmentForm.PJIAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 2, true,
        true, false);

    //PIAI
    mockJoiningSecondaryDown(SegmentForm.PJIAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, true, 1, 1, false,
        true, true);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PJIAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 0, true,
        true, true);

  }

  @Test
  public void testpjiii() {

    mockVolumeTyep(VolumeType.LARGE);
    // PJIAI OK
    mockSegmentMembership(SegmentForm.PJIII);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, true, 1, 1, false,
        true, false);

  }

  @Test
  public void testpiiaa() {

    mockVolumeTyep(VolumeType.LARGE);
    // PJIAI OK
    mockSegmentMembership(SegmentForm.PIIAA);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 2, false,
        true, true);

  }

  @Test
  public void testpiiai() {

    mockVolumeTyep(VolumeType.LARGE);
    // PJIAI OK
    mockSegmentMembership(SegmentForm.PIIAI);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, true, 1, 1, false,
        true, true);

  }

  @Test
  public void testpiiii() {

    mockVolumeTyep(VolumeType.LARGE);
    // PJIAI OK
    mockSegmentMembership(SegmentForm.PIIII);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, true, 1, 0, false,
        true, true);

  }

  @Test
  public void testpsaa() {
    mockVolumeTyep(VolumeType.LARGE);
    // PSAA OK
    mockSegmentMembership(SegmentForm.PSAA);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 1, 3, false,
        false);

    //SAA
    mockPrimaryDown(SegmentForm.PSAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, false, 1, 2, true,
        false);

    //PAA
    mockSecondaryDown(SegmentForm.PSAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 1, 2, false,
        true);

    //PSA
    mockArbiterDown(SegmentForm.PSAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 1, 2, false,
        false);
    //PA
    mockSecondaryAndArbiterDown(SegmentForm.PSAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, true, 1, 1, false,
        true);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PSAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 0, 0, true,
        true);

  }

  @Test
  public void testpsai() {
    mockVolumeTyep(VolumeType.LARGE);
    // PSAI OK
    mockSegmentMembership(SegmentForm.PSAI);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 1, 2, false,
        false);

    //SAI
    mockPrimaryDown(SegmentForm.PSAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 1, 1, true,
        false);

    //PAI
    mockSecondaryDown(SegmentForm.PSAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, true, 1, 1, false,
        true);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PSAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 0, 0, true,
        true);

  }

  @Test
  public void testpsii() {

  }

  @Test
  public void testpiaa() {
    mockVolumeTyep(VolumeType.LARGE);
    // PIAA OK
    mockSegmentMembership(SegmentForm.PIAA);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 1, 2, false,
        true);

    //AAI
    mockPrimaryDown(SegmentForm.PIAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 0, 2, true,
        true);

    //PAI
    mockArbiterDown(SegmentForm.PIAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, true, 1, 1, false,
        true);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PIAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 0, 0, true,
        true);

  }

  @Test
  public void testpiai() {

  }

  @Test
  public void testpiii() {

  }

  @Test
  public void testpjaa() {
    mockVolumeTyep(VolumeType.LARGE);
    // PJAA OK
    mockSegmentMembership(SegmentForm.PJAA);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 3, false,
        true, false);

    //JAA
    mockPrimaryDown(SegmentForm.PJAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 3, true,
        true, false);

    //PAA
    mockJoiningSecondaryDown(SegmentForm.PJAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 1, 2, false,
        true);

    //PJA
    mockArbiterDown(SegmentForm.PJAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 2, false,
        true, false);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PJAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 0, 0, true,
        true);

  }

  @Test
  public void testpjai() {
    mockVolumeTyep(VolumeType.LARGE);
    // PJAI OK
    mockSegmentMembership(SegmentForm.PJAI);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 2, false,
        true, false);

    //JAI
    mockPrimaryDown(SegmentForm.PJAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 2, true,
        true, false);

    //PAI
    mockJoiningSecondaryDown(SegmentForm.PJAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, true, 1, 1, false,
        true);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PJAI);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 0, 0, true,
        true);

  }

  @Test
  public void testpjii() {

  }

  @Test
  public void testpaa() {
    mockVolumeTyep(VolumeType.LARGE);
    // PAA OK
    mockSegmentMembership(SegmentForm.PAA);
    IoActionContext ioActionContext = readFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 2, false,
        true, true);

    //AA
    mockPrimaryDown(SegmentForm.PAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 2, true,
        true, true);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PAA);
    ioActionContext = readFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 0, true,
        true, true);

  }

}
