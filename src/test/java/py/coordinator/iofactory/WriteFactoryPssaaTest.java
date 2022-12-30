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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
public class WriteFactoryPssaaTest extends IoFactoryTestBase {

  private WriteFactory writeFactory = new WriteFactory(instanceStore);

  //true is down or there is no this action, "checkNumber" unused, all set to  0

  /**
   * xx.
   */
  public void checkValuePrimaryAndSecondary(IoActionContext ioActionContext, boolean isZombie,
      boolean isResend, int ioMembers,
      int checkNumber, boolean isPrimaryDown, boolean isSecondaryDown) {
    assertTrue(ioActionContext.isZombieRequest() == isZombie);
    assertTrue(ioActionContext.isResendDirectly() == isResend);
    assertTrue(ioActionContext.getIoMembers().size() == ioMembers);
    assertTrue(ioActionContext.isPrimaryDown() == isPrimaryDown);
    assertTrue(ioActionContext.isSecondaryDown() == isSecondaryDown);
  }


  /**
   * xx.
   */
  public void checkValueAll(IoActionContext ioActionContext, boolean isZombie, boolean isResend,
      int ioMembers,
      int checkNumber, boolean isPrimaryDown, boolean isSecondaryDown,
      boolean isJoiningSecondaryDown) {
    assertTrue(ioActionContext.isZombieRequest() == isZombie);
    assertTrue(ioActionContext.isResendDirectly() == isResend);
    assertTrue(ioActionContext.getIoMembers().size() == ioMembers);
    assertTrue(ioActionContext.isPrimaryDown() == isPrimaryDown);
    assertTrue(ioActionContext.isSecondaryDown() == isSecondaryDown);
    assertTrue(ioActionContext.isJoiningSecondaryDown() == isJoiningSecondaryDown);
  }


  /**
   * xx.
   */
  public void makePssaa(SegmentForm segmentForm) {
    int p = 0;
    int s = 0;
    int j = 0;
    int a = 0;
    String name = segmentForm.name();
    for (int i = 0; i < name.length(); i++) {
      switch (name.charAt(i)) {
        case 'P':
          p++;
          break;
        case 'S':
          s++;
          break;
        case 'J':
          j++;
          break;
        case 'A':
          a++;
          break;
        default:
          break;

      }
    }
    for (int i = 0; i <= p; i++) {
      for (int m = 0; m <= s; m++) {
        for (int k = 0; k <= j; k++) {
          for (int l = 0; l <= a; l++) {

          }
        }
      }
    }

  }

  @Test
  public void testGet() {
    makePssaa(SegmentForm.PSSAI);
  }

  @Test
  public void testGetPssaa() {
    int m = 0;
    int end = 0;
    String temA = "";
    String temS = "";
    String temJ = "";
    String temI = "";
    Map<Integer, String> mapForPssaa = new ConcurrentHashMap<>();
    //value.put(4000,"PI");
    List<Integer> list = new ArrayList<>();
    System.out.println("S " + " J " + " I " + " A ");
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 3; j++) {
        for (int k = 0; k < 3; k++) {
          for (int l = 0; l < 5; l++) {
            int minTotal = 3;
            int maxTotal = 5;
            int total = i + j + k + l;
            if (total > 3 && total < 5) {
              end = i * 2 + j * 200 + k * 3000 + l * 40000;
              mapForPssaa.put(end, "");
              if (i == 1) {
                temS = "S";
              }

              if (i == 2) {
                temS = "SS";
              }

              if (j == 1) {
                temA = "A";
              }

              if (j == 2) {
                temA = "AA";
              }

              if (k == 1) {
                temJ = "J";
              }

              if (k == 2) {
                temJ = "JJ";
              }

              if (l == 1) {
                temI = "I";
              }

              if (l == 2) {
                temI = "II";
              }

              if (l == 3) {
                temI = "III";
              }

              if (l == 4) {
                temI = "IIII";
              }

              String tmpValue = "P" + temS + temJ + temI + temA;
              mapForPssaa.put(end, tmpValue);
              //for easy
              //get value
              temA = "";
              temI = "";
              temJ = "";
              temS = "";
              m++;
            }
          }
        }
      }
    }

    System.out.println("m  === " + m);

    mapForPssaa.put(160000, "PIIII");
    mapForPssaa.put(123000, "PJIII");
    mapForPssaa.put(86000, "PJJII");
    mapForPssaa.put(120200, "PIIIA");
    mapForPssaa.put(83200, "PJIIA");
    mapForPssaa.put(46200, "PJJIA");
    mapForPssaa.put(80400, "PIIAA");
    mapForPssaa.put(43400, "PJIAA");
    mapForPssaa.put(6400, "PJJAA");
    mapForPssaa.put(120002, "PSIII");
    mapForPssaa.put(83002, "PSJII");
    // mapForPSSAA.put(46002,"PSJJI");
    mapForPssaa.put(80202, "PSIIA");
    mapForPssaa.put(43202, "PSJIA");
    //       mapForPSSAA.put(6202,"PSJJA");
    mapForPssaa.put(40402, "PSIAA");
    mapForPssaa.put(3402, "PSJAA");
    mapForPssaa.put(80004, "PSSII");
    mapForPssaa.put(40204, "PSSIA");
    mapForPssaa.put(404, "PSSAA");


  }

  @Test
  public void testgetpssaaTwo() {
    int m = 0;
    int end = 0;
    String temA = "";
    String temS = "";
    String temJ = "";
    String temI = "";
    Map<Integer, String> mapForPssaa = new ConcurrentHashMap<>();
    List<Integer> list = new ArrayList<>();
    System.out.println("S " + " I " + " J " + " A ");
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 3; j++) {
        for (int k = 0; k < 2; k++) {
          for (int l = 0; l < 4; l++) {
            int minTotal = 2;
            int maxTotal = 4;
            int total = i + j + k + l;
            if (total > 2 && total < 4) {
              end = i * 2 + j * 200 + k * 3000 + l * 40000;
              mapForPssaa.put(end, "");
              if (i == 1) {
                temS = "S";
              }

              if (j == 1) {
                temA = "A";
              }

              if (j == 2) {
                temA = "AA";
              }

              if (k == 1) {
                temJ = "J";
              }

              if (l == 1) {
                temI = "I";
              }

              if (l == 2) {
                temI = "II";
              }

              if (l == 3) {
                temI = "III";
              }

              String tmpValue = "P" + temS + temJ + temI + temA;
              mapForPssaa.put(end, tmpValue);
              //for easy
              //get value
              temA = "";
              temI = "";
              temJ = "";
              temS = "";
              m++;
            }
          }
        }
      }
    }

    mapForPssaa.put(120000, "PIII");
    mapForPssaa.put(83000, "PJII");
    mapForPssaa.put(80200, "PIIA");
    mapForPssaa.put(43200, "PJIA");
    mapForPssaa.put(40400, "PIAA");
    mapForPssaa.put(3400, "PJAA");
    mapForPssaa.put(80002, "PSII");
    //  mapForPSSAA.put(43002,"PSJI");
    mapForPssaa.put(40202, "PSIA");
    //   mapForPSSAA.put(3202,"PSJA");
    mapForPssaa.put(402, "PSAA");

    System.out.println("m  === " + m);

  }

  @Test
  public void testgetpssaaThree() {
    int m = 0;
    int end = 0;

    String temS = "PSS";
    List<Integer> list = new ArrayList<>();
    System.out.println("A " + " A ");

    end = 2 * 200;

  }

  @Test
  public void testgetpss() {
    int m = 0;
    int end = 0;
    List<Integer> list = new ArrayList<>();
    final Map<Integer, String> value = new ConcurrentHashMap<>();
    System.out.println("S " + " J " + " I ");
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 3; j++) {
        for (int k = 0; k < 3; k++) {
          end = i * 2 + j * 200 + k * 3000;
          if ((i + j + k) < 3) {
            list.add(end);
            m++;
          }

        }
      }
    }

    System.out.println("m  === " + m);

    value.put(3000, "PI");
    value.put(6000, "PII");
    value.put(200, "PJ");
    value.put(3200, "PJI");
    value.put(400, "PJJ");
    value.put(2, "PS");
    value.put(3002, "PSI");
    value.put(202, "PSJ");
    value.put(4, "PSS");
    for (int i = 0; i < list.size(); i++) {

    }
  }

  @Test
  public void testGetPsa() {
    int m = 0;
    int end = 0;
    List<Integer> list = new ArrayList<>();
    final Map<Integer, String> value = new ConcurrentHashMap<>();
    System.out.println("S " + " J " + " I " + " A ");
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 2; j++) {
        for (int k = 0; k < 3; k++) {
          for (int l = 0; l < 2; l++) {
            end = i * 2 + j * 201 + k * 3000 + l * 40000;
            int total = i + j + k + l;
            if (total < 3 && total > 0) {
              list.add(end);
              m++;
            }
          }

        }
      }
    }

    System.out.println("m  === " + m);

    value.put(40000, "PA");
    value.put(3000, "TPI");
    value.put(43000, "PIA");
    value.put(201, "TPJ");
    value.put(40201, "PJA");
    value.put(3201, "TPJ");
    value.put(2, "TPS");
    value.put(40002, "PSA");
    value.put(3002, "TPS");
    //PII
    value.put(6000, "TPI");
    for (int i = 0; i < list.size(); i++) {

    }
  }

  @Test
  public void testCanGenerateNewPrimary() {
    assertTrue(SegmentForm.PSSAA.canGenerateNewPrimary());
    assertTrue(SegmentForm.PSSAI.canGenerateNewPrimary());
    assertTrue(SegmentForm.PSSII.canGenerateNewPrimary());
    assertTrue(SegmentForm.PSIAA.canGenerateNewPrimary());
    assertTrue(SegmentForm.PSIAI.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PSIII.canGenerateNewPrimary());
    assertTrue(SegmentForm.PSJAA.canGenerateNewPrimary());
    assertTrue(SegmentForm.PSJAI.canGenerateNewPrimary());
    assertTrue(SegmentForm.PSJII.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PJJAA.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PJJAI.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PJJII.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PJIAA.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PJIAI.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PIIAI.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PJIAI.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PIIAA.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PIIAI.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PIIII.canGenerateNewPrimary());
    assertTrue(SegmentForm.PSAA.canGenerateNewPrimary());
    assertTrue(SegmentForm.PSAI.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PSII.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PIAA.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PIAI.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PIII.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PJAA.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PJAI.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PJII.canGenerateNewPrimary());
    assertTrue(!SegmentForm.PAA.canGenerateNewPrimary());
  }

  @Test
  public void testOnlyPrimary() {
    assertTrue(!SegmentForm.PSSAA.onlyPrimary());
    assertTrue(!SegmentForm.PSSAI.onlyPrimary());
    assertTrue(!SegmentForm.PSSII.onlyPrimary());
    assertTrue(!SegmentForm.PSIAA.onlyPrimary());
    assertTrue(!SegmentForm.PSIAI.onlyPrimary());
    assertTrue(!SegmentForm.PSIII.onlyPrimary());
    assertTrue(!SegmentForm.PSJAA.onlyPrimary());
    assertTrue(!SegmentForm.PSJAI.onlyPrimary());
    assertTrue(!SegmentForm.PSJII.onlyPrimary());
    assertTrue(!SegmentForm.PJJAA.onlyPrimary());
    assertTrue(!SegmentForm.PJJAI.onlyPrimary());
    assertTrue(!SegmentForm.PJJII.onlyPrimary());
    assertTrue(!SegmentForm.PJIAA.onlyPrimary());
    assertTrue(!SegmentForm.PJIAI.onlyPrimary());
    assertTrue(!SegmentForm.PIIAI.onlyPrimary()); //
    assertTrue(!SegmentForm.PJIAI.onlyPrimary());
    assertTrue(SegmentForm.PIIAA.onlyPrimary()); //
    assertTrue(!SegmentForm.PIIAI.onlyPrimary()); //
    assertTrue(!SegmentForm.PIIII.onlyPrimary()); //
    assertTrue(!SegmentForm.PSAA.onlyPrimary());
    assertTrue(!SegmentForm.PSAI.onlyPrimary());
    assertTrue(!SegmentForm.PSII.onlyPrimary());
    assertTrue(SegmentForm.PIAA.onlyPrimary());
    assertTrue(!SegmentForm.PIAI.onlyPrimary()); //
    assertTrue(!SegmentForm.PIII.onlyPrimary()); //
    assertTrue(!SegmentForm.PJAA.onlyPrimary());
    assertTrue(!SegmentForm.PJAI.onlyPrimary());
    assertTrue(!SegmentForm.PJII.onlyPrimary());
    assertTrue(SegmentForm.PAA.onlyPrimary());
  }


  /**
   * xx.
   */
  public void checkReadDoneDirectly(SegmentForm segmentForm, int pdisCount, int sdisCount,
      int jdisCount, int adisCount, boolean isDone) {
    boolean down = false;
    switch (segmentForm.name()) {
      case "PSSAA":
        down = SegmentForm.PSSAA
            .readDoneDirectly(pdisCount, sdisCount, jdisCount, adisCount, VolumeType.LARGE);
        break;
      case "PSSAI":
        down = SegmentForm.PSSAI
            .readDoneDirectly(pdisCount, sdisCount, jdisCount, adisCount, VolumeType.LARGE);
        break;
      case "PSSII":
        down = SegmentForm.PSSII
            .readDoneDirectly(pdisCount, sdisCount, jdisCount, adisCount, VolumeType.LARGE);
        break;
      case "PSJAA":
        down = SegmentForm.PSJAA
            .readDoneDirectly(pdisCount, sdisCount, jdisCount, adisCount, VolumeType.LARGE);
        break;
      case "PSJAI":
        down = SegmentForm.PSJAI
            .readDoneDirectly(pdisCount, sdisCount, jdisCount, adisCount, VolumeType.LARGE);
        break;
      case "PSJII":
        down = SegmentForm.PSJII
            .readDoneDirectly(pdisCount, sdisCount, jdisCount, adisCount, VolumeType.LARGE);
        break;
      case "PSIAA":
        down = SegmentForm.PSIAA
            .readDoneDirectly(pdisCount, sdisCount, jdisCount, adisCount, VolumeType.LARGE);
        break;
      case "PSIAI":
        down = SegmentForm.PSIAI
            .readDoneDirectly(pdisCount, sdisCount, jdisCount, adisCount, VolumeType.LARGE);
        break;
      case "PSIII":
        //  down = SegmentForm.PSIII.readDoneDirectly(pdisCount, sdisCount, jdisCount, adisCount,
        //  VolumeType.LARGE);
        break;
      case "PJJAA":
        down = SegmentForm.PJJAA
            .readDoneDirectly(pdisCount, sdisCount, jdisCount, adisCount, VolumeType.LARGE);
        break;
      case "PJJAI":
        down = SegmentForm.PJJAI
            .readDoneDirectly(pdisCount, sdisCount, jdisCount, adisCount, VolumeType.LARGE);
        break;
      case "PJJII":
        down = SegmentForm.PJJII
            .readDoneDirectly(pdisCount, sdisCount, jdisCount, adisCount, VolumeType.LARGE);
        break;
      case "PJIAA":
        down = SegmentForm.PJIAA
            .readDoneDirectly(pdisCount, sdisCount, jdisCount, adisCount, VolumeType.LARGE);
        break;
      case "PJIAI":
        down = SegmentForm.PJIAI
            .readDoneDirectly(pdisCount, sdisCount, jdisCount, adisCount, VolumeType.LARGE);
        break;
      case "PJIII":
        //  down = SegmentForm.PJIII.readDoneDirectly(pdisCount, sdisCount, jdisCount, adisCount,
        //  VolumeType.LARGE);
        break;
      case "PIIAA":
        down = SegmentForm.PIIAA
            .readDoneDirectly(pdisCount, sdisCount, jdisCount, adisCount, VolumeType.LARGE);
        break;
      case "PIIAI":
        break;
      case "PIIII":
        break;
      case "PSAA":
        down = SegmentForm.PSAA
            .readDoneDirectly(pdisCount, sdisCount, jdisCount, adisCount, VolumeType.LARGE);
        break;
      case "PSAI":
        down = SegmentForm.PSAI
            .readDoneDirectly(pdisCount, sdisCount, jdisCount, adisCount, VolumeType.LARGE);
        break;
      case "PSII":
        break;
      case "PIAA":
        down = SegmentForm.PIAA
            .readDoneDirectly(pdisCount, sdisCount, jdisCount, adisCount, VolumeType.LARGE);
        break;
      case "PIAI":
        break;
      case "PIII":
        break;
      case "PJAA":
        down = SegmentForm.PJAA
            .readDoneDirectly(pdisCount, sdisCount, jdisCount, adisCount, VolumeType.LARGE);
        break;
      case "PJAI":
        down = SegmentForm.PJAI
            .readDoneDirectly(pdisCount, sdisCount, jdisCount, adisCount, VolumeType.LARGE);
        break;
      case "PJII":
        break;
      case "PAA":
        down = SegmentForm.PAA
            .readDoneDirectly(pdisCount, sdisCount, jdisCount, adisCount, VolumeType.LARGE);
        break;
      default:
        break;
    }

    if (down == isDone) {
      assertTrue(true);
    } else {
      assertTrue(false);
    }

  }


  /**
   * xx.
   */
  public void checkWriteDoneDirectly(SegmentForm segmentForm, int pdiscount, int sdiscount,
      int jdiscount, int adiscount, boolean isdone) {
    boolean down = false;
    switch (segmentForm.name()) {
      case "PSSAA":
        down = SegmentForm.PSSAA
            .writeDoneDirectly(pdiscount, sdiscount, jdiscount, adiscount, VolumeType.LARGE);
        break;
      case "PSSAI":
        down = SegmentForm.PSSAI
            .writeDoneDirectly(pdiscount, sdiscount, jdiscount, adiscount, VolumeType.LARGE);
        break;
      case "PSSII":
        down = SegmentForm.PSSII
            .writeDoneDirectly(pdiscount, sdiscount, jdiscount, adiscount, VolumeType.LARGE);
        break;
      case "PSJAA":
        down = SegmentForm.PSJAA
            .writeDoneDirectly(pdiscount, sdiscount, jdiscount, adiscount, VolumeType.LARGE);
        break;
      case "PSJAI":
        down = SegmentForm.PSJAI
            .writeDoneDirectly(pdiscount, sdiscount, jdiscount, adiscount, VolumeType.LARGE);
        break;
      case "PSJII":
        down = SegmentForm.PSJII
            .writeDoneDirectly(pdiscount, sdiscount, jdiscount, adiscount, VolumeType.LARGE);
        break;
      case "PSIAA":
        down = SegmentForm.PSIAA
            .writeDoneDirectly(pdiscount, sdiscount, jdiscount, adiscount, VolumeType.LARGE);
        break;
      case "PSIAI":
        down = SegmentForm.PSIAI
            .writeDoneDirectly(pdiscount, sdiscount, jdiscount, adiscount, VolumeType.LARGE);
        break;
      case "PSIII":
        //  down = SegmentForm.PSIII.writeDoneDirectly(pdiscount, sdiscount, jdiscount,
        //  adiscount, VolumeType.LARGE);
        break;
      case "PJJAA":
        down = SegmentForm.PJJAA
            .writeDoneDirectly(pdiscount, sdiscount, jdiscount, adiscount, VolumeType.LARGE);
        break;
      case "PJJAI":
        down = SegmentForm.PJJAI
            .writeDoneDirectly(pdiscount, sdiscount, jdiscount, adiscount, VolumeType.LARGE);
        break;
      case "PJJII":
        down = SegmentForm.PJJII
            .writeDoneDirectly(pdiscount, sdiscount, jdiscount, adiscount, VolumeType.LARGE);
        break;
      case "PJIAA":
        down = SegmentForm.PJIAA
            .writeDoneDirectly(pdiscount, sdiscount, jdiscount, adiscount, VolumeType.LARGE);
        break;
      case "PJIAI":
        down = SegmentForm.PJIAI
            .writeDoneDirectly(pdiscount, sdiscount, jdiscount, adiscount, VolumeType.LARGE);
        break;
      case "PJIII":
        //  down = SegmentForm.PSSAA.writeDoneDirectly(pdiscount, sdiscount, jdiscount,
        //  adiscount, VolumeType.LARGE);
        break;
      case "PIIAA":
        down = SegmentForm.PIIAA
            .writeDoneDirectly(pdiscount, sdiscount, jdiscount, adiscount, VolumeType.LARGE);
        break;
      case "PIIAI":
        break;
      case "PIIII":
        break;
      case "PSAA":
        down = SegmentForm.PSAA
            .writeDoneDirectly(pdiscount, sdiscount, jdiscount, adiscount, VolumeType.LARGE);
        break;
      case "PSAI":
        down = SegmentForm.PSAI
            .writeDoneDirectly(pdiscount, sdiscount, jdiscount, adiscount, VolumeType.LARGE);
        break;
      case "PSII":
        break;
      case "PIAA":
        down = SegmentForm.PIAA
            .writeDoneDirectly(pdiscount, sdiscount, jdiscount, adiscount, VolumeType.LARGE);
        break;
      case "PIAI":
        break;
      case "PIII":
        break;
      case "PJAA":
        down = SegmentForm.PJAA
            .writeDoneDirectly(pdiscount, sdiscount, jdiscount, adiscount, VolumeType.LARGE);
        break;
      case "PJAI":
        down = SegmentForm.PJAI
            .writeDoneDirectly(pdiscount, sdiscount, jdiscount, adiscount, VolumeType.LARGE);
        break;
      case "PJII":
        break;
      case "PAA":
        down = SegmentForm.PAA
            .writeDoneDirectly(pdiscount, sdiscount, jdiscount, adiscount, VolumeType.LARGE);
        break;
      default:
        break;

    }

    if (down == isdone) {
      assertTrue(true);
    } else {
      assertTrue(false);
    }

  }

  @Test
  public void testReadRequestDoneDirectly() {

    //PSSAA
    checkReadDoneDirectly(SegmentForm.PSSAA, 0, 0, 0, 0, false); //PSSAA
    checkReadDoneDirectly(SegmentForm.PSSAA, 1, 0, 0, 0, false); //SSAA
    checkReadDoneDirectly(SegmentForm.PSSAA, 0, 1, 0, 0, false); //PSAA
    checkReadDoneDirectly(SegmentForm.PSSAA, 0, 0, 0, 1, false); //PSSA
    checkReadDoneDirectly(SegmentForm.PSSAA, 1, 1, 0, 0, false); //SAA
    checkReadDoneDirectly(SegmentForm.PSSAA, 0, 2, 0, 0, false); //PAA
    checkReadDoneDirectly(SegmentForm.PSSAA, 1, 0, 0, 1, false); //SSA
    checkReadDoneDirectly(SegmentForm.PSSAA, 0, 1, 0, 1, false); //PSA
    checkReadDoneDirectly(SegmentForm.PSSAA, 0, 0, 0, 2, false); //PSS
    checkReadDoneDirectly(SegmentForm.PSSAA, 1, 0, 0, 2, true); //SS
    checkReadDoneDirectly(SegmentForm.PSSAA, 1, 2, 0, 0, true); //AA
    checkReadDoneDirectly(SegmentForm.PSSAA, 0, 1, 0, 2, true); //PS
    checkReadDoneDirectly(SegmentForm.PSSAA, 0, 2, 0, 1, true); //PA
    checkReadDoneDirectly(SegmentForm.PSSAA, 1, 1, 0, 1, true); //SA

    //PSSAI
    checkReadDoneDirectly(SegmentForm.PSSAI, 0, 0, 0, 0, false); //PSSAI
    checkReadDoneDirectly(SegmentForm.PSSAI, 1, 0, 0, 0, false); //SSAI
    checkReadDoneDirectly(SegmentForm.PSSAI, 0, 1, 0, 0, false); //PSAI
    checkReadDoneDirectly(SegmentForm.PSSAI, 0, 0, 0, 1, false); //PSSI
    checkReadDoneDirectly(SegmentForm.PSSAI, 1, 1, 0, 0, true); //SAI
    checkReadDoneDirectly(SegmentForm.PSSAI, 1, 2, 0, 1, true); //SSI
    checkReadDoneDirectly(SegmentForm.PSSAI, 0, 2, 0, 0, true); //PAI
    checkReadDoneDirectly(SegmentForm.PSSAI, 0, 1, 0, 1, true); //PSI

    //PSSII
    checkReadDoneDirectly(SegmentForm.PSSII, 0, 0, 0, 0, false); //PSSII
    checkReadDoneDirectly(SegmentForm.PSSII, 1, 0, 0, 0, true); //SSII
    checkReadDoneDirectly(SegmentForm.PSSII, 0, 1, 0, 0, true); //PSII
    checkReadDoneDirectly(SegmentForm.PSSII, 1, 1, 0, 0, true); //SII

    //PSJAA
    checkReadDoneDirectly(SegmentForm.PSJAA, 0, 0, 0, 0, false); //PSJAA
    checkReadDoneDirectly(SegmentForm.PSJAA, 1, 0, 0, 0, false); //SJAA
    checkReadDoneDirectly(SegmentForm.PSJAA, 0, 1, 0, 0, false); //PJAA
    checkReadDoneDirectly(SegmentForm.PSJAA, 0, 0, 1, 0, false); //PSAA
    checkReadDoneDirectly(SegmentForm.PSJAA, 0, 0, 0, 1, false); //PSJA
    checkReadDoneDirectly(SegmentForm.PSJAA, 1, 1, 0, 0, true); //JAA
    checkReadDoneDirectly(SegmentForm.PSJAA, 1, 0, 1, 0, false); //SAA
    checkReadDoneDirectly(SegmentForm.PSJAA, 1, 0, 0, 1, false); //SJA
    checkReadDoneDirectly(SegmentForm.PSJAA, 0, 1, 1, 0, false); //PAA
    checkReadDoneDirectly(SegmentForm.PSJAA, 0, 1, 0, 1, false); //PJA
    checkReadDoneDirectly(SegmentForm.PSJAA, 0, 0, 1, 1, false); //PSA
    checkReadDoneDirectly(SegmentForm.PSJAA, 0, 0, 0, 2, false); //PJA
    checkReadDoneDirectly(SegmentForm.PSJAA, 1, 0, 0, 2, true); //SJ
    checkReadDoneDirectly(SegmentForm.PSJAA, 1, 1, 1, 0, true); //AA
    checkReadDoneDirectly(SegmentForm.PSJAA, 0, 0, 1, 2, true); //PS
    checkReadDoneDirectly(SegmentForm.PSJAA, 0, 1, 1, 1, true); //PA
    checkReadDoneDirectly(SegmentForm.PSJAA, 1, 1, 1, 1, true); //SA

    //PSJAI
    checkReadDoneDirectly(SegmentForm.PSJAI, 0, 0, 0, 0, false); //PSJAI
    checkReadDoneDirectly(SegmentForm.PSJAI, 1, 0, 0, 0, false); //SJAI
    checkReadDoneDirectly(SegmentForm.PSJAI, 0, 1, 0, 0, false); //PJAI
    checkReadDoneDirectly(SegmentForm.PSJAI, 0, 0, 1, 0, false); //PSAI
    checkReadDoneDirectly(SegmentForm.PSJAI, 0, 0, 0, 1, false); //PSJI
    checkReadDoneDirectly(SegmentForm.PSJAI, 1, 1, 0, 0, true); //JAI
    checkReadDoneDirectly(SegmentForm.PSJAI, 1, 0, 1, 0, true); //SAI
    checkReadDoneDirectly(SegmentForm.PSJAI, 1, 0, 0, 1, true); //SJI
    checkReadDoneDirectly(SegmentForm.PSJAI, 0, 1, 1, 0, true); //PAI
    checkReadDoneDirectly(SegmentForm.PSJAI, 1, 0, 0, 1, true); //SJ
    checkReadDoneDirectly(SegmentForm.PSJAI, 0, 0, 1, 1, true); //PS

    //PSJII
    checkReadDoneDirectly(SegmentForm.PSJII, 0, 0, 0, 0, false); //PSJII
    checkReadDoneDirectly(SegmentForm.PSJII, 1, 0, 0, 0, true); //SJII
    checkReadDoneDirectly(SegmentForm.PSJII, 0, 1, 0, 0, true); //PJII
    checkReadDoneDirectly(SegmentForm.PSJII, 0, 0, 1, 0, true); //PSII
    checkReadDoneDirectly(SegmentForm.PSJII, 1, 0, 0, 1, true); //SJ
    checkReadDoneDirectly(SegmentForm.PSJII, 0, 0, 1, 1, true); //PS

    //PSIAA
    checkReadDoneDirectly(SegmentForm.PSIAA, 0, 0, 0, 0, false); //PSIAA
    checkReadDoneDirectly(SegmentForm.PSIAA, 1, 0, 0, 0, false); //SIAA
    checkReadDoneDirectly(SegmentForm.PSIAA, 0, 1, 0, 0, false); //PIAA
    checkReadDoneDirectly(SegmentForm.PSIAA, 0, 0, 0, 1, false); //PSAI
    checkReadDoneDirectly(SegmentForm.PSIAA, 1, 1, 0, 1, true); //AAI
    checkReadDoneDirectly(SegmentForm.PSIAA, 0, 0, 0, 2, true); //PSI
    checkReadDoneDirectly(SegmentForm.PSIAA, 0, 1, 0, 1, true); //PAI

    //PSIAI
    checkReadDoneDirectly(SegmentForm.PSIAI, 0, 0, 0, 0, false); //PSIAI
    checkReadDoneDirectly(SegmentForm.PSIAI, 1, 0, 0, 0, true); //SIAI
    checkReadDoneDirectly(SegmentForm.PSIAI, 0, 1, 0, 0, true); //PIAI
    checkReadDoneDirectly(SegmentForm.PSIAI, 0, 0, 0, 1, true); //PSI
    checkReadDoneDirectly(SegmentForm.PSIAI, 1, 1, 0, 0, true); //AII

    //PSIII
    checkReadDoneDirectly(SegmentForm.PSIAI, 0, 0, 0, 0, false); //PSIAI

    //PJJAA
    checkReadDoneDirectly(SegmentForm.PJJAA, 0, 0, 0, 0, false); //PJJAA
    checkReadDoneDirectly(SegmentForm.PJJAA, 1, 0, 0, 0, true); //JJAA
    checkReadDoneDirectly(SegmentForm.PJJAA, 0, 0, 1, 0, false); //PJAA
    checkReadDoneDirectly(SegmentForm.PJJAA, 0, 0, 0, 1, false); //PJJA
    checkReadDoneDirectly(SegmentForm.PJJAA, 1, 0, 1, 0, true); //JAA
    checkReadDoneDirectly(SegmentForm.PJJAA, 1, 0, 1, 1, true); //JJA
    checkReadDoneDirectly(SegmentForm.PJJAA, 0, 0, 1, 1, false); //PJA
    checkReadDoneDirectly(SegmentForm.PJJAA, 0, 0, 0, 2, false); //PJJ
    checkReadDoneDirectly(SegmentForm.PJJAA, 1, 0, 0, 2, true); //JJ
    checkReadDoneDirectly(SegmentForm.PJJAA, 1, 0, 2, 0, true); //AA
    checkReadDoneDirectly(SegmentForm.PJJAA, 0, 0, 1, 2, true); //PJ
    checkReadDoneDirectly(SegmentForm.PJJAA, 0, 0, 2, 1, true); //PA

    //PJJAI
    checkReadDoneDirectly(SegmentForm.PJJAI, 0, 0, 0, 0, false); //PJJAI
    checkReadDoneDirectly(SegmentForm.PJJAI, 1, 0, 0, 0, true); //JJAI
    checkReadDoneDirectly(SegmentForm.PJJAI, 0, 0, 1, 0, false); //PJAI
    checkReadDoneDirectly(SegmentForm.PJJAI, 0, 0, 0, 1, false); //PJJI
    checkReadDoneDirectly(SegmentForm.PJJAI, 1, 0, 1, 0, true); //JAI
    checkReadDoneDirectly(SegmentForm.PJJAI, 1, 1, 0, 0, true); //JJI
    checkReadDoneDirectly(SegmentForm.PJJAI, 0, 0, 2, 0, true); //PAI

    //PJJII
    checkReadDoneDirectly(SegmentForm.PJJII, 0, 0, 0, 0, false); //PJJII
    checkReadDoneDirectly(SegmentForm.PJJII, 1, 0, 0, 0, true); //JJII
    checkReadDoneDirectly(SegmentForm.PJJII, 0, 0, 1, 0, true); //PJII
    checkReadDoneDirectly(SegmentForm.PJJII, 1, 0, 1, 0, true); //JII

    //PJIAA
    checkReadDoneDirectly(SegmentForm.PJIAA, 0, 0, 0, 0, false); //PJIAA
    checkReadDoneDirectly(SegmentForm.PJIAA, 1, 0, 0, 0, true); //JIAA
    checkReadDoneDirectly(SegmentForm.PJIAA, 0, 0, 1, 0, false); //PIAA
    checkReadDoneDirectly(SegmentForm.PJIAA, 0, 0, 0, 1, false); //PJIA
    checkReadDoneDirectly(SegmentForm.PJIAA, 0, 0, 0, 2, true); //AAI
    checkReadDoneDirectly(SegmentForm.PJIAA, 0, 0, 1, 1, true); //PIA
    checkReadDoneDirectly(SegmentForm.PJIAA, 0, 0, 0, 2, true); //PJI

    //PJIAI
    checkReadDoneDirectly(SegmentForm.PJIAI, 0, 0, 0, 0, false); //PJIAI
    checkReadDoneDirectly(SegmentForm.PJIAI, 1, 0, 0, 0, true); //JIAI
    checkReadDoneDirectly(SegmentForm.PJIAI, 0, 0, 1, 0, true); //PIAI
    checkReadDoneDirectly(SegmentForm.PJIAI, 0, 0, 0, 1, true); //PJII
    checkReadDoneDirectly(SegmentForm.PJIAI, 1, 0, 1, 0, true); //IAI

    //PJIII
    checkReadDoneDirectly(SegmentForm.PJIII, 0, 0, 0, 0, false);

    //PIIAA
    checkReadDoneDirectly(SegmentForm.PIIAA, 0, 0, 0, 0, false);
    checkReadDoneDirectly(SegmentForm.PIIAA, 1, 0, 0, 0, true); //IIAA
    checkReadDoneDirectly(SegmentForm.PIIAA, 0, 0, 0, 1, true); //PIIA

    //PIIAI
    checkReadDoneDirectly(SegmentForm.PIIAI, 0, 0, 0, 0, false);

    //PIIII
    checkReadDoneDirectly(SegmentForm.PIIII, 0, 0, 0, 0, false);

    //PSAA
    checkReadDoneDirectly(SegmentForm.PSAA, 0, 0, 0, 0, false); //PSAA
    checkReadDoneDirectly(SegmentForm.PSAA, 1, 0, 0, 0, false); //SAA
    checkReadDoneDirectly(SegmentForm.PSAA, 0, 1, 0, 0, false); //PAA
    checkReadDoneDirectly(SegmentForm.PSAA, 0, 0, 0, 1, false); //PSA
    checkReadDoneDirectly(SegmentForm.PSAA, 0, 0, 0, 2, true); //PS
    checkReadDoneDirectly(SegmentForm.PSAA, 0, 1, 0, 1, true); //PA
    checkReadDoneDirectly(SegmentForm.PSAA, 1, 0, 0, 1, true); //SA

    //PSAI
    checkReadDoneDirectly(SegmentForm.PSAI, 0, 0, 0, 0, false); //PSAI
    checkReadDoneDirectly(SegmentForm.PSAI, 1, 0, 0, 0, true); //SAI
    checkReadDoneDirectly(SegmentForm.PSAI, 0, 1, 0, 0, true); //PAI
    checkReadDoneDirectly(SegmentForm.PSAI, 0, 0, 0, 1, true); //PSI

    //PSII
    checkReadDoneDirectly(SegmentForm.PSII, 0, 0, 0, 0, false);

    //PIAA
    checkReadDoneDirectly(SegmentForm.PIAA, 0, 0, 0, 0, false); //PAAI
    checkReadDoneDirectly(SegmentForm.PIAA, 1, 0, 0, 0, true); //AAI
    checkReadDoneDirectly(SegmentForm.PIAA, 0, 0, 0, 1, true); //PAI

    //PIAI
    checkReadDoneDirectly(SegmentForm.PIAI, 0, 0, 0, 0, false);

    //PIII
    checkReadDoneDirectly(SegmentForm.PIII, 0, 0, 0, 0, false);

    //PJAA
    checkReadDoneDirectly(SegmentForm.PJAA, 0, 0, 0, 0, false); //PJAA
    checkReadDoneDirectly(SegmentForm.PJAA, 1, 0, 0, 0, true); //JAA
    checkReadDoneDirectly(SegmentForm.PJAA, 0, 0, 1, 0, false); //PAA
    checkReadDoneDirectly(SegmentForm.PJAA, 0, 0, 0, 1, false); //PJA
    checkReadDoneDirectly(SegmentForm.PJAA, 0, 0, 0, 2, true); //PJ
    checkReadDoneDirectly(SegmentForm.PJAA, 0, 0, 1, 1, true); //PA
    checkReadDoneDirectly(SegmentForm.PJAA, 1, 0, 0, 1, true); //JA

    //PJAI
    checkReadDoneDirectly(SegmentForm.PJAI, 0, 0, 0, 0, false); //PJAI
    checkReadDoneDirectly(SegmentForm.PJAI, 1, 0, 0, 0, true); //JAI
    checkReadDoneDirectly(SegmentForm.PJAI, 0, 0, 1, 0, true); //PAI
    checkReadDoneDirectly(SegmentForm.PJAI, 0, 0, 0, 1, true); //PJI
    checkReadDoneDirectly(SegmentForm.PJAI, 0, 0, 0, 1, true); //PJ

    //PJII
    checkReadDoneDirectly(SegmentForm.PJII, 1, 0, 0, 0, false);

    //PAA
    checkReadDoneDirectly(SegmentForm.PAA, 0, 0, 0, 0, false); //PAA
    checkReadDoneDirectly(SegmentForm.PAA, 1, 0, 0, 0, true); //AA
    checkReadDoneDirectly(SegmentForm.PAA, 0, 1, 0, 1, true); //PA

  }

  @Test
  public void testWriteRequestDoneDirectly() {

    //PSSAA
    checkWriteDoneDirectly(SegmentForm.PSSAA, 0, 0, 0, 0, false); //PSSAA
    checkWriteDoneDirectly(SegmentForm.PSSAA, 1, 0, 0, 0, false); //SSAA
    checkWriteDoneDirectly(SegmentForm.PSSAA, 0, 1, 0, 0, false); //PSAA
    checkWriteDoneDirectly(SegmentForm.PSSAA, 0, 0, 0, 1, false); //PSSA
    checkWriteDoneDirectly(SegmentForm.PSSAA, 1, 1, 0, 0, false); //SAA
    checkWriteDoneDirectly(SegmentForm.PSSAA, 0, 2, 0, 0, false); //PAA
    checkWriteDoneDirectly(SegmentForm.PSSAA, 1, 0, 0, 1, false); //SSA
    checkWriteDoneDirectly(SegmentForm.PSSAA, 0, 1, 0, 1, false); //PSA
    checkWriteDoneDirectly(SegmentForm.PSSAA, 0, 0, 0, 2, false); //PSS
    checkWriteDoneDirectly(SegmentForm.PSSAA, 1, 0, 0, 2, true); //SS
    checkWriteDoneDirectly(SegmentForm.PSSAA, 1, 2, 0, 0, true); //AA
    checkWriteDoneDirectly(SegmentForm.PSSAA, 0, 1, 0, 2, true); //PS
    checkWriteDoneDirectly(SegmentForm.PSSAA, 0, 2, 0, 1, true); //PA
    checkWriteDoneDirectly(SegmentForm.PSSAA, 1, 1, 0, 1, true); //SA

    //PSSAI
    checkWriteDoneDirectly(SegmentForm.PSSAI, 0, 0, 0, 0, false); //PSSAI
    checkWriteDoneDirectly(SegmentForm.PSSAI, 1, 0, 0, 0, false); //SSAI
    checkWriteDoneDirectly(SegmentForm.PSSAI, 0, 1, 0, 0, false); //PSAI
    checkWriteDoneDirectly(SegmentForm.PSSAI, 0, 0, 0, 1, false); //PSSI
    checkWriteDoneDirectly(SegmentForm.PSSAI, 1, 1, 0, 0, true); //SAI
    checkWriteDoneDirectly(SegmentForm.PSSAI, 1, 2, 0, 1, true); //SSI
    checkWriteDoneDirectly(SegmentForm.PSSAI, 0, 2, 0, 0, true); //PAI
    checkWriteDoneDirectly(SegmentForm.PSSAI, 0, 1, 0, 1, true); //PSI

    //PSSII
    checkWriteDoneDirectly(SegmentForm.PSSII, 0, 0, 0, 0, false); //PSSII
    checkWriteDoneDirectly(SegmentForm.PSSII, 1, 0, 0, 0, true); //SSII
    checkWriteDoneDirectly(SegmentForm.PSSII, 0, 1, 0, 0, true); //PSII
    checkWriteDoneDirectly(SegmentForm.PSSII, 1, 1, 0, 0, true); //SII

    //PSJAA
    checkWriteDoneDirectly(SegmentForm.PSJAA, 0, 0, 0, 0, false); //PSJAA
    checkWriteDoneDirectly(SegmentForm.PSJAA, 1, 0, 0, 0, false); //SJAA
    checkWriteDoneDirectly(SegmentForm.PSJAA, 0, 1, 0, 0, false); //PJAA
    checkWriteDoneDirectly(SegmentForm.PSJAA, 0, 0, 1, 0, false); //PSAA
    checkWriteDoneDirectly(SegmentForm.PSJAA, 0, 0, 0, 1, false); //PSJA
    checkWriteDoneDirectly(SegmentForm.PSJAA, 1, 1, 0, 0, true); //JAA
    checkWriteDoneDirectly(SegmentForm.PSJAA, 1, 0, 1, 0, false); //SAA
    checkWriteDoneDirectly(SegmentForm.PSJAA, 1, 0, 0, 1, false); //SJA
    checkWriteDoneDirectly(SegmentForm.PSJAA, 0, 1, 1, 0, false); //PAA
    checkWriteDoneDirectly(SegmentForm.PSJAA, 0, 1, 0, 1, false); //PJA
    checkWriteDoneDirectly(SegmentForm.PSJAA, 0, 0, 1, 1, false); //PSA
    checkWriteDoneDirectly(SegmentForm.PSJAA, 0, 0, 0, 2, false); //PJA
    checkWriteDoneDirectly(SegmentForm.PSJAA, 1, 0, 0, 2, true); //SJ
    checkWriteDoneDirectly(SegmentForm.PSJAA, 1, 1, 1, 0, true); //AA
    checkWriteDoneDirectly(SegmentForm.PSJAA, 0, 0, 1, 2, true); //PS
    checkWriteDoneDirectly(SegmentForm.PSJAA, 0, 1, 1, 1, true); //PA
    checkWriteDoneDirectly(SegmentForm.PSJAA, 1, 1, 1, 1, true); //SA

    //PSJAI
    checkWriteDoneDirectly(SegmentForm.PSJAI, 0, 0, 0, 0, false); //PSJAI
    checkWriteDoneDirectly(SegmentForm.PSJAI, 1, 0, 0, 0, false); //SJAI
    checkWriteDoneDirectly(SegmentForm.PSJAI, 0, 1, 0, 0, false); //PJAI
    checkWriteDoneDirectly(SegmentForm.PSJAI, 0, 0, 1, 0, false); //PSAI
    checkWriteDoneDirectly(SegmentForm.PSJAI, 0, 0, 0, 1, false); //PSJI
    checkWriteDoneDirectly(SegmentForm.PSJAI, 1, 1, 0, 0, true); //JAI
    checkWriteDoneDirectly(SegmentForm.PSJAI, 1, 0, 1, 0, true); //SAI
    checkWriteDoneDirectly(SegmentForm.PSJAI, 1, 0, 0, 1, true); //SJI
    checkWriteDoneDirectly(SegmentForm.PSJAI, 0, 1, 1, 0, true); //PAI
    checkWriteDoneDirectly(SegmentForm.PSJAI, 1, 0, 0, 1, true); //SJ
    checkWriteDoneDirectly(SegmentForm.PSJAI, 0, 0, 1, 1, true); //PS

    //PSJII
    checkWriteDoneDirectly(SegmentForm.PSJII, 0, 0, 0, 0, false); //PSJII
    checkWriteDoneDirectly(SegmentForm.PSJII, 1, 0, 0, 0, true); //SJII
    checkWriteDoneDirectly(SegmentForm.PSJII, 0, 1, 0, 0, true); //PJII
    checkWriteDoneDirectly(SegmentForm.PSJII, 0, 0, 1, 0, true); //PSII
    checkWriteDoneDirectly(SegmentForm.PSJII, 1, 0, 0, 1, true); //SJ
    checkWriteDoneDirectly(SegmentForm.PSJII, 0, 0, 1, 1, true); //PS

    //PSIAA
    checkWriteDoneDirectly(SegmentForm.PSIAA, 0, 0, 0, 0, false); //PSIAA
    checkWriteDoneDirectly(SegmentForm.PSIAA, 1, 0, 0, 0, false); //SIAA
    checkWriteDoneDirectly(SegmentForm.PSIAA, 0, 1, 0, 0, false); //PIAA
    checkWriteDoneDirectly(SegmentForm.PSIAA, 0, 0, 0, 1, false); //PSAI
    checkWriteDoneDirectly(SegmentForm.PSIAA, 1, 1, 0, 1, true); //AAI
    checkWriteDoneDirectly(SegmentForm.PSIAA, 0, 0, 0, 2, true); //PSI
    checkWriteDoneDirectly(SegmentForm.PSIAA, 0, 1, 0, 1, true); //PAI

    //PSIAI
    checkWriteDoneDirectly(SegmentForm.PSIAI, 0, 0, 0, 0, false); //PSIAI
    checkWriteDoneDirectly(SegmentForm.PSIAI, 1, 0, 0, 0, true); //SIAI
    checkWriteDoneDirectly(SegmentForm.PSIAI, 0, 1, 0, 0, true); //PIAI
    checkWriteDoneDirectly(SegmentForm.PSIAI, 0, 0, 0, 1, true); //PSI
    checkWriteDoneDirectly(SegmentForm.PSIAI, 1, 1, 0, 0, true); //AII

    //PSIII
    checkWriteDoneDirectly(SegmentForm.PSIAI, 0, 0, 0, 0, false); //PSIAI

    //PJJAA
    checkWriteDoneDirectly(SegmentForm.PJJAA, 0, 0, 0, 0, false); //PJJAA
    checkWriteDoneDirectly(SegmentForm.PJJAA, 1, 0, 0, 0, true); //JJAA
    checkWriteDoneDirectly(SegmentForm.PJJAA, 0, 0, 1, 0, false); //PJAA
    checkWriteDoneDirectly(SegmentForm.PJJAA, 0, 0, 0, 1, false); //PJJA
    checkWriteDoneDirectly(SegmentForm.PJJAA, 1, 0, 1, 0, true); //JAA
    checkWriteDoneDirectly(SegmentForm.PJJAA, 1, 0, 1, 1, true); //JJA
    checkWriteDoneDirectly(SegmentForm.PJJAA, 0, 0, 1, 1, false); //PJA
    checkWriteDoneDirectly(SegmentForm.PJJAA, 0, 0, 0, 2, false); //PJJ
    checkWriteDoneDirectly(SegmentForm.PJJAA, 1, 0, 0, 2, true); //JJ
    checkWriteDoneDirectly(SegmentForm.PJJAA, 1, 0, 2, 0, true); //AA
    checkWriteDoneDirectly(SegmentForm.PJJAA, 0, 0, 1, 2, true); //PJ
    checkWriteDoneDirectly(SegmentForm.PJJAA, 0, 0, 2, 1, true); //PA

    //PJJAI
    checkWriteDoneDirectly(SegmentForm.PJJAI, 0, 0, 0, 0, false); //PJJAI
    checkWriteDoneDirectly(SegmentForm.PJJAI, 1, 0, 0, 0, true); //JJAI
    checkWriteDoneDirectly(SegmentForm.PJJAI, 0, 0, 1, 0, false); //PJAI
    checkWriteDoneDirectly(SegmentForm.PJJAI, 0, 0, 0, 1, false); //PJJI
    checkWriteDoneDirectly(SegmentForm.PJJAI, 1, 0, 1, 0, true); //JAI
    checkWriteDoneDirectly(SegmentForm.PJJAI, 1, 1, 0, 0, true); //JJI
    checkWriteDoneDirectly(SegmentForm.PJJAI, 0, 0, 2, 0, true); //PAI

    //PJJII
    checkWriteDoneDirectly(SegmentForm.PJJII, 0, 0, 0, 0, false); //PJJII
    checkWriteDoneDirectly(SegmentForm.PJJII, 1, 0, 0, 0, true); //JJII
    checkWriteDoneDirectly(SegmentForm.PJJII, 0, 0, 1, 0, true); //PJII
    checkWriteDoneDirectly(SegmentForm.PJJII, 1, 0, 1, 0, true); //JII

    //PJIAA
    checkWriteDoneDirectly(SegmentForm.PJIAA, 0, 0, 0, 0, false); //PJIAA
    checkWriteDoneDirectly(SegmentForm.PJIAA, 1, 0, 0, 0, true); //JIAA
    checkWriteDoneDirectly(SegmentForm.PJIAA, 0, 0, 1, 0, false); //PIAA
    checkWriteDoneDirectly(SegmentForm.PJIAA, 0, 0, 0, 1, false); //PJIA
    checkWriteDoneDirectly(SegmentForm.PJIAA, 0, 0, 0, 2, true); //AAI
    checkWriteDoneDirectly(SegmentForm.PJIAA, 0, 0, 1, 1, true); //PIA
    checkWriteDoneDirectly(SegmentForm.PJIAA, 0, 0, 0, 2, true); //PJI

    //PJIAI
    checkWriteDoneDirectly(SegmentForm.PJIAI, 0, 0, 0, 0, false); //PJIAI
    checkWriteDoneDirectly(SegmentForm.PJIAI, 1, 0, 0, 0, true); //JIAI
    checkWriteDoneDirectly(SegmentForm.PJIAI, 0, 0, 1, 0, true); //PIAI
    checkWriteDoneDirectly(SegmentForm.PJIAI, 0, 0, 0, 1, true); //PJII
    checkWriteDoneDirectly(SegmentForm.PJIAI, 1, 0, 1, 0, true); //IAI

    //PJIII
    checkWriteDoneDirectly(SegmentForm.PJIII, 0, 0, 0, 0, false);

    //PIIAA
    checkWriteDoneDirectly(SegmentForm.PIIAA, 0, 0, 0, 0, false); //PIIAA
    checkWriteDoneDirectly(SegmentForm.PIIAA, 1, 0, 0, 0, true); //IIAA
    checkWriteDoneDirectly(SegmentForm.PIIAA, 0, 0, 0, 1, true); //PIIA

    //PIIAI
    checkWriteDoneDirectly(SegmentForm.PIIAI, 0, 0, 0, 0, false);

    //PIIII
    checkWriteDoneDirectly(SegmentForm.PIIII, 0, 0, 0, 0, false);

    //PSAA
    checkWriteDoneDirectly(SegmentForm.PSAA, 0, 0, 0, 0, false); //PSAA
    checkWriteDoneDirectly(SegmentForm.PSAA, 1, 0, 0, 0, false); //SAA
    checkWriteDoneDirectly(SegmentForm.PSAA, 0, 1, 0, 0, false); //PAA
    checkWriteDoneDirectly(SegmentForm.PSAA, 0, 0, 0, 1, false); //PSA
    checkWriteDoneDirectly(SegmentForm.PSAA, 0, 0, 0, 2, true); //PS
    checkWriteDoneDirectly(SegmentForm.PSAA, 0, 1, 0, 1, true); //PA
    checkWriteDoneDirectly(SegmentForm.PSAA, 1, 0, 0, 1, true); //SA

    //PSAI
    checkWriteDoneDirectly(SegmentForm.PSAI, 0, 0, 0, 0, false); //PSAI
    checkWriteDoneDirectly(SegmentForm.PSAI, 1, 0, 0, 0, true); //SAI
    checkWriteDoneDirectly(SegmentForm.PSAI, 0, 1, 0, 0, true); //PAI
    checkWriteDoneDirectly(SegmentForm.PSAI, 0, 0, 0, 1, true); //PSI

    //PSII
    checkWriteDoneDirectly(SegmentForm.PSII, 0, 0, 0, 0, false);

    //PIAA
    checkWriteDoneDirectly(SegmentForm.PIAA, 0, 0, 0, 0, false); //PAAI
    checkWriteDoneDirectly(SegmentForm.PIAA, 1, 0, 0, 0, true); //AAI
    checkWriteDoneDirectly(SegmentForm.PIAA, 0, 0, 0, 1, true); //PAI

    //PIAI
    checkWriteDoneDirectly(SegmentForm.PIAI, 0, 0, 0, 0, false);

    //PIII
    checkWriteDoneDirectly(SegmentForm.PIII, 0, 0, 0, 0, false);

    //PJAA
    checkWriteDoneDirectly(SegmentForm.PJAA, 0, 0, 0, 0, false); //PJAA
    checkWriteDoneDirectly(SegmentForm.PJAA, 1, 0, 0, 0, true); //JAA
    checkWriteDoneDirectly(SegmentForm.PJAA, 0, 0, 1, 0, false); //PAA
    checkWriteDoneDirectly(SegmentForm.PJAA, 0, 0, 0, 1, false); //PJA
    checkWriteDoneDirectly(SegmentForm.PJAA, 0, 0, 0, 2, true); //PJ
    checkWriteDoneDirectly(SegmentForm.PJAA, 0, 0, 1, 1, true); //PA
    checkWriteDoneDirectly(SegmentForm.PJAA, 1, 0, 0, 1, true); //JA

    //PJAI
    checkWriteDoneDirectly(SegmentForm.PJAI, 0, 0, 0, 0, false); //PJAI
    checkWriteDoneDirectly(SegmentForm.PJAI, 1, 0, 0, 0, true); //JAI
    checkWriteDoneDirectly(SegmentForm.PJAI, 0, 0, 1, 0, true); //PAI
    checkWriteDoneDirectly(SegmentForm.PJAI, 0, 0, 0, 1, true); //PJI
    checkWriteDoneDirectly(SegmentForm.PJAI, 0, 0, 0, 1, true); //PJ

    //PJII
    checkWriteDoneDirectly(SegmentForm.PJII, 1, 0, 0, 0, false);

    //PAA
    checkWriteDoneDirectly(SegmentForm.PAA, 0, 0, 0, 0, false); //PAA
    checkWriteDoneDirectly(SegmentForm.PAA, 1, 0, 0, 0, true); //AA
    checkWriteDoneDirectly(SegmentForm.PAA, 0, 1, 0, 1, true); //PA

  }

  @Test
  public void testGotTempPrimary() {
    IoActionContext ioActionContext = new IoActionContext();
    ioActionContext.addIoMember(new IoMember(new InstanceId(0L), new EndPoint("127.0.0.1", 1234),
        MemberIoStatus.Secondary));
    assertTrue(!ioActionContext.gotTempPrimary());
    ioActionContext.addIoMember(new IoMember(new InstanceId(1L), new EndPoint("127.0.0.2", 1234),
        MemberIoStatus.TempPrimary));
    assertTrue(ioActionContext.gotTempPrimary());
  }

  @Test
  public void testUnstablePrimary() {
    // PSS OK
    mockSegmentMembership(SegmentForm.PSS);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);

    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(!ioActionContext.isUnstablePrimaryWrite());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 3);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());

    // P not OK
    mockPrimaryAsUnstablePrimary(SegmentForm.PSS);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    assertTrue(!ioActionContext.isZombieRequest());
    assertTrue(ioActionContext.isUnstablePrimaryWrite());
    assertTrue(!ioActionContext.isResendDirectly());
    assertTrue(ioActionContext.getIoMembers().size() == 3);
    assertTrue(!ioActionContext.isPrimaryDown());
    assertTrue(!ioActionContext.isSecondaryDown());
  }

  @Test
  public void testpssaa() {
    mockVolumeTyep(VolumeType.LARGE);
    // PSSAA OK
    mockSegmentMembership(SegmentForm.PSSAA);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 3, 0, false,
        false);

    // P not OK SSAA
    mockPrimaryDown(SegmentForm.PSSAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, false, 2, 0, true,
        false);

    //PSAA
    mockSecondaryDown(SegmentForm.PSSAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 2, 0, false,
        false);

    //PSSA
    mockArbiterDown(SegmentForm.PSSAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 3, 0, false,
        false);

    // SAA
    mockPrimaryAndSecondaryDown(SegmentForm.PSSAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, false, 1, 0, true,
        false);

    // mock P not OK and all secondaries are read down  AA
    mockPrimaryDownAndAllSecondariesDown(SegmentForm.PSSAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 0, 0, true,
        true);

    // PAA
    mockAllSecondaryDown(SegmentForm.PSSAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 1, 0, false,
        true);

    //SSA P Adown
    mockPrimaryAndArbiterDown(SegmentForm.PSSAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, false, 2, 0, true,
        false);

    //PSA SAdown
    mockSecondaryAndArbiterDown(SegmentForm.PSSAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 2, 0, false,
        false);

    //PSS AA down
    mockAllArbiterDown(SegmentForm.PSSAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 3, 0, false,
        false);

    //SS
    mockPandAllaMemberDown(SegmentForm.PSSAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, false, 2, 0, true,
        false);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PSSAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 0, 0, true,
        true);

  }

  @Test
  public void testpssai() {
    mockVolumeTyep(VolumeType.LARGE);
    // PSSAI OK
    mockSegmentMembership(SegmentForm.PSSAI);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 3, 0, false,
        false);

    //SSAI
    mockPrimaryDown(SegmentForm.PSSAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, false, 2, 0, true,
        false);

    //PSAI
    mockSecondaryDown(SegmentForm.PSSAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 2, 0, false,
        false);

    //PSSI
    mockArbiterDown(SegmentForm.PSSAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 3, 0, false,
        false);

    //SSA
    mockPrimaryDown(SegmentForm.PSSAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, false, 2, 0, true,
        false);

    //SAI
    mockPrimaryAndSecondaryDown(SegmentForm.PSSAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 1, 0, true,
        false);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PSSAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 0, 0, true,
        true);

  }

  @Test
  public void testpssii() {
    mockVolumeTyep(VolumeType.LARGE);
    // PSSII OK
    mockSegmentMembership(SegmentForm.PSSII);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 3, 0, false,
        false);

    //SSII
    mockPrimaryDown(SegmentForm.PSSII);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 2, 0, true,
        false);

    //PSII
    mockSecondaryDown(SegmentForm.PSSII);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, true, 2, 0, false,
        false);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PSSII);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 0, 0, true,
        true);

  }

  @Test
  public void testpsjaa() {
    mockVolumeTyep(VolumeType.LARGE);
    // PSJAA OK
    mockSegmentMembership(SegmentForm.PSJAA);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 3, 0, false,
        false, false);

    //SJAA
    mockPrimaryDown(SegmentForm.PSJAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, false, 2, 0, true,
        false, false);

    //PJAA
    mockSecondaryDown(SegmentForm.PSJAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 2, 0, false,
        true, false);

    //PSAA
    mockJoiningSecondaryDown(SegmentForm.PSJAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 2, 0, false,
        false, true);

    //PSJA
    mockArbiterDown(SegmentForm.PSJAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 3, 0, false,
        false, false);

    //JAA
    mockPrimaryAndSecondaryDown(SegmentForm.PSJAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 1, 0, true,
        true, false);

    //SAA
    mockPrimaryAndJoiningSecondaryDown(SegmentForm.PSJAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, false, 1, 0, true,
        false, true);

    //SJA
    mockPrimaryAndArbiterDown(SegmentForm.PSJAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, false, 2, 0, true,
        false, false);

    //PAA
    mockSecondarAndJoiningSecondaryDown(SegmentForm.PSJAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 0, false,
        true, true);

    //PJA
    mockSecondaryAndArbiterDown(SegmentForm.PSJAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 2, 0, false,
        true, false);

    //PSA
    mockJoingingSeconaryAndArbiterDown(SegmentForm.PSJAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 2, 0, false,
        false, true);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PSJAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 0, true,
        true, true);

  }

  @Test
  public void testpsjai() {
    mockVolumeTyep(VolumeType.LARGE);
    // PSJAI OK
    mockSegmentMembership(SegmentForm.PSJAI);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 3, 0, false,
        false, false);

    //SJAI
    mockPrimaryDown(SegmentForm.PSJAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, false, 2, 0, true,
        false, false);

    //PJAI
    mockSecondaryDown(SegmentForm.PSJAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 2, 0, false,
        true, false);

    //PSAI
    mockJoiningSecondaryDown(SegmentForm.PSJAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 2, 0, false,
        false, true);

    //PSJI
    mockArbiterDown(SegmentForm.PSJAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 3, 0, false,
        false, false);

    //JAI
    mockPrimaryAndSecondaryDown(SegmentForm.PSJAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 1, 0, true,
        true, false);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PSJAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 0, true,
        true, true);
  }

  @Test
  public void testpsjii() {
    mockVolumeTyep(VolumeType.LARGE);
    // PSJII OK
    mockSegmentMembership(SegmentForm.PSJII);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 3, 0, false,
        false, false);

    //SJII
    mockPrimaryDown(SegmentForm.PSJII);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 2, 0, true,
        false, false);

    //PII
    mockSecondarAndJoiningSecondaryDown(SegmentForm.PSJII);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, true, 1, 0, false,
        true, true);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PSJII);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 0, true,
        true, true);

  }

  @Test
  public void testpsiaa() {
    mockVolumeTyep(VolumeType.LARGE);
    // PSIAA OK
    mockSegmentMembership(SegmentForm.PSIAA);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 2, 0, false,
        false);

    //SIAA
    mockPrimaryDown(SegmentForm.PSIAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, false, 1, 0, true,
        false);

    //PIAA
    mockSecondaryDown(SegmentForm.PSIAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 1, 0, false,
        true);

    //PSIA
    mockArbiterDown(SegmentForm.PSIAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 2, 0, false,
        false);

    //PSI AA
    mockAllArbiterDown(SegmentForm.PSIAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 2, 0, false,
        false);

    //AAI PS
    mockPrimaryAndSecondaryDown(SegmentForm.PSIAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 0, 0, true,
        true);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PSIAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 0, 0, true,
        true);

  }

  @Test
  public void testpsiai() {
    mockVolumeTyep(VolumeType.LARGE);
    // PSIAI OK
    mockSegmentMembership(SegmentForm.PSIAI);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 2, 0, false,
        false);

    //SIAI
    mockPrimaryDown(SegmentForm.PSIAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 1, 0, true,
        false);

    //PIAI
    mockSecondaryDown(SegmentForm.PSIAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, true, 1, 0, false,
        true);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PSIAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 0, 0, true,
        true);
  }

  @Test
  public void testpsiii() {

  }

  @Test
  public void testpjjaa() {
    mockVolumeTyep(VolumeType.LARGE);
    // PJJAA OK
    mockSegmentMembership(SegmentForm.PJJAA);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 3, 0, false,
        true, false);

    //JJAA
    mockPrimaryDown(SegmentForm.PJJAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 2, 0, true,
        true, false);

    //PJAA
    mockJoiningSecondaryDown(SegmentForm.PJJAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 2, 0, false,
        true, false);

    //PJJA
    mockArbiterDown(SegmentForm.PJJAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 3, 0, false,
        true, false);

    //JAA PJ
    mockPrimaryAndJoiningSecondaryDown(SegmentForm.PJJAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 1, 0, true,
        true, false);

    //JJA
    mockPrimaryAndArbiterDown(SegmentForm.PJJAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 2, 0, true,
        true, false);

    //PJA
    mockJoingingSeconaryAndArbiterDown(SegmentForm.PJJAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 2, 0, false,
        true, false);

    //PJJ
    mockAllArbiterDown(SegmentForm.PJJAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 3, 0, false,
        true, false);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PJJAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 0, true,
        true, true);

  }

  @Test
  public void testpjjai() {
    mockVolumeTyep(VolumeType.LARGE);
    // PJJAI OK
    mockSegmentMembership(SegmentForm.PJJAI);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 3, 0, false,
        true, false);

    //JJAI
    mockPrimaryDown(SegmentForm.PJJAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 2, 0, true,
        true, false);

    //PJAI
    mockJoiningSecondaryDown(SegmentForm.PJJAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 2, 0, false,
        true, false);

    //PJJI
    mockArbiterDown(SegmentForm.PJJAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 3, 0, false,
        true, false);

    //JAI
    mockPrimaryAndJoiningSecondaryDown(SegmentForm.PJJAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 1, 0, true,
        true, false);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PJJAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 0, true,
        true, true);

  }

  @Test
  public void testpjjii() {
    mockVolumeTyep(VolumeType.LARGE);
    // PJJII OK
    mockSegmentMembership(SegmentForm.PJJII);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 3, 0, false,
        true, false);

    //JJII
    mockPrimaryDown(SegmentForm.PJJII);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 2, 0, true,
        true, false);

    //PJII
    mockJoiningSecondaryDown(SegmentForm.PJJII);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, true, 2, 0, false,
        true, false);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PJJII);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 0, true,
        true, true);

  }

  @Test
  public void testpjiaa() {
    mockVolumeTyep(VolumeType.LARGE);
    // PJIAA OK
    mockSegmentMembership(SegmentForm.PJIAA);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 2, 0, false,
        true, false);

    //JIAA
    mockPrimaryDown(SegmentForm.PJIAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 1, 0, true,
        true, false);

    //PIAA
    mockJoiningSecondaryDown(SegmentForm.PJIAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, true, 1, 0, false,
        true, true);

    //PJAI
    mockArbiterDown(SegmentForm.PJIAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 2, 0, false,
        true, false);

    //AAI PJ
    mockPrimaryAndJoiningSecondaryDown(SegmentForm.PJIAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 0, true,
        true, true);

    //PAI JA
    mockJoingingSeconaryAndArbiterDown(SegmentForm.PJIAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, true, 1, 0, false,
        true, true);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PJIAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 0, true,
        true, true);

  }

  @Test
  public void testpjiai() {
    mockVolumeTyep(VolumeType.LARGE);
    // PJIAI OK
    mockSegmentMembership(SegmentForm.PJIAI);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 2, 0, false,
        true, false);

    //JIAI
    mockPrimaryDown(SegmentForm.PJIAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 1, 0, true,
        true, false);

    //PIAI
    mockJoiningSecondaryDown(SegmentForm.PJIAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, true, 1, 0, false,
        true, true);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PJIAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 0, true,
        true, true);

  }

  @Test
  public void testpjiii() {

  }

  @Test
  public void testpiiaa() {

    mockVolumeTyep(VolumeType.LARGE);
    // PIIAA OK
    mockSegmentMembership(SegmentForm.PIIAA);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 1, 0, false,
        true);

    //IIAA
    mockPrimaryDown(SegmentForm.PIIAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 0, 0, true,
        true);

    //PIIA
    mockArbiterDown(SegmentForm.PIIAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 1, 0, false,
        true);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PIIAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 0, 0, true,
        true);

  }

  @Test
  public void testpiiai() {

  }

  @Test
  public void testpiiii() {

  }

  @Test
  public void testpsaa() {
    mockVolumeTyep(VolumeType.LARGE);
    // PSAA OK
    mockSegmentMembership(SegmentForm.PSAA);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 2, 0, false,
        false);

    //SAA
    mockPrimaryDown(SegmentForm.PSAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, false, 1, 0, true,
        false);

    //PAA
    mockSecondaryDown(SegmentForm.PSAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 1, 0, false,
        true);

    //PSA
    mockArbiterDown(SegmentForm.PSAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 2, 0, false,
        false);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PSAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 0, 0, true,
        true);

  }

  @Test
  public void testpsai() {
    mockVolumeTyep(VolumeType.LARGE);
    // PSAI OK
    mockSegmentMembership(SegmentForm.PSAI);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 2, 0, false,
        false);

    //SAI
    mockPrimaryDown(SegmentForm.PSAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 1, 0, true,
        false);

    //PAI
    mockSecondaryDown(SegmentForm.PSAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, true, 1, 0, false,
        true);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PSAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
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
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 1, 0, false,
        true);

    //AAI
    mockPrimaryDown(SegmentForm.PIAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 0, 0, true,
        true);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PIAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
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
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 2, 0, false,
        true, false);

    //JAA
    mockPrimaryDown(SegmentForm.PJAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 1, 0, true,
        true, false);

    //PAA
    mockJoiningSecondaryDown(SegmentForm.PJAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, false, 1, 0, false,
        true);

    //PJA
    mockArbiterDown(SegmentForm.PJAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 2, 0, false,
        true, false);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PJAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, true, true, 0, 0, true,
        true);

  }

  @Test
  public void testpjai() {
    mockVolumeTyep(VolumeType.LARGE);
    // PJAI OK
    mockSegmentMembership(SegmentForm.PJAI);
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 2, 0, false,
        true, false);

    //JAI
    mockPrimaryDown(SegmentForm.PJAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 1, 0, true,
        true, false);

    //PAI
    mockJoiningSecondaryDown(SegmentForm.PJAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValuePrimaryAndSecondary(ioActionContext, false, true, 1, 0, false,
        true);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PJAI);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
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
    IoActionContext ioActionContext = writeFactory
        .generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, false, false, 1, 0, false,
        true, true);

    //AA
    mockPrimaryDown(SegmentForm.PAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 0, true,
        true, true);

    // ALL not OK
    mockAllMemberDown(SegmentForm.PAA);
    ioActionContext = writeFactory.generateIoMembers(membership, VolumeType.LARGE, segId, 0L);
    checkValueAll(ioActionContext, true, true, 0, 0, true,
        true, true);

  }


}
