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

import java.util.Set;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.membership.IoActionContext;
import py.membership.IoMember;
import py.membership.MemberIoStatus;
import py.membership.SegmentForm;
import py.membership.SegmentMembership;
import py.proto.Broadcastlog;
import py.volume.VolumeType;


public class ReadFactory extends AbstractIoFactory implements IoFactory {

  private static final Logger logger = LoggerFactory.getLogger(ReadFactory.class);

  public ReadFactory(InstanceStore instanceStore) {
    super(instanceStore, null);
  }

  public ReadFactory(InstanceStore instanceStore, Set<InstanceId> subHealthyDatanodes) {
    super(instanceStore, subHealthyDatanodes);
  }

  @Override
  public IoActionContext generateIoMembers(SegmentMembership segmentMembership,
      VolumeType volumeType, SegId segId, 
      Long requestId) {
    logger.info(
        "request:{} use membership:{} and volume type:{} to generate read io action context at:{}",
        requestId, segmentMembership, volumeType, segId);
    IoActionContext ioActionContext = new IoActionContext();
    SegmentForm segmentForm = SegmentForm.getSegmentForm(segmentMembership, volumeType);
    ioActionContext.setSegmentForm(segmentForm);
    ioActionContext.setMembershipWhenIoCome(segmentMembership);

    InstanceId primary = segmentMembership.getPrimary();
    Validate.notNull(primary);
    Validate.isTrue(segmentMembership.getMemberIoStatus(primary).isPrimary());
    MemberIoStatus primaryIoStatus = segmentMembership.getMemberIoStatus(primary);

    int primaryReadCount = 0;
    int secondariesReadCount = 0;
    int joiningSecondaryReadCount = 0;
    int arbiterReadCount = 0;

   
   
    boolean primaryDown = primaryIoStatus.isReadDown();
    if (primaryDown) {
      logger.info("reading primary:{} is down with segment form:{} at:{}", primary, segmentForm,
          segId);
      ioActionContext.setZombieRequest(true);
    } else {
      addUnHealThyInstance(primary, ioActionContext);
      primaryReadCount++;
      ioActionContext.addIoMember(
          new IoMember(primary, getEndPoint(primary), MemberIoStatus.Primary,
              Broadcastlog.ReadCause.FETCH));
    }

   

   
    Set<InstanceId> secondaries = segmentMembership.getSecondaries();
    for (InstanceId secondary : secondaries) {
     
      MemberIoStatus secondaryMemberIoStatus = segmentMembership.getMemberIoStatus(secondary);
      Validate.isTrue(secondaryMemberIoStatus.isSecondary());

     
      if (secondaryMemberIoStatus.isReadDown() && !secondaryMemberIoStatus.isDown()) {
        ioActionContext.setMetReadDownSecondary(true);
      }

      if (!secondaryMemberIoStatus.isReadDown()) {
        secondariesReadCount++;
        if (primaryDown) {
          if (secondaryMemberIoStatus.isTempPrimary()) {
            logger.warn("we will read from tp {}, {}", secondary, segId);
            ioActionContext.addIoMember(
                new IoMember(secondary, getEndPoint(secondary), secondaryMemberIoStatus,
                    Broadcastlog.ReadCause.FETCH));
          } else {
            ioActionContext.addIoMember(
                new IoMember(secondary, getEndPoint(secondary), secondaryMemberIoStatus,
                    Broadcastlog.ReadCause.CHECK));
          }
        } else {
          ioActionContext.addIoMember(
              new IoMember(secondary, getEndPoint(secondary), secondaryMemberIoStatus,
                  Broadcastlog.ReadCause.CHECK));
        }
        addUnHealThyInstance(secondary, ioActionContext);
      } else {
        logger
            .info("reading secondary:{} is down with segment form:{} at:{}", secondary, segmentForm,
                segId);
      }
    }

   
    Set<InstanceId> joiningSecondaries = segmentMembership.getJoiningSecondaries();
    for (InstanceId joiningSecondary : joiningSecondaries) {
     
      Validate.isTrue(segmentMembership.getMemberIoStatus(joiningSecondary).isJoiningSecondary());
      MemberIoStatus joiningSecondaryMemberIoStatus = segmentMembership
          .getMemberIoStatus(joiningSecondary);
      if (!joiningSecondaryMemberIoStatus.isReadDown()) {
        joiningSecondaryReadCount++;
        addUnHealThyInstance(joiningSecondary, ioActionContext);
        ioActionContext.addIoMember(
            new IoMember(joiningSecondary, getEndPoint(joiningSecondary),
                joiningSecondaryMemberIoStatus,
                Broadcastlog.ReadCause.CHECK));
      } else {
        logger.info("reading joining secondary:{} is down with segment form:{} at:{}",
            joiningSecondary,
            segmentForm, segId);
      }
    }

   
    Set<InstanceId> arbiters = segmentMembership.getArbiters();
    for (InstanceId arbiter : arbiters) {
     
      MemberIoStatus arbiterMemberIoStatus = segmentMembership.getMemberIoStatus(arbiter);
      Validate.isTrue(arbiterMemberIoStatus.isArbiter());
      if (!arbiterMemberIoStatus.isReadDown()) {
        arbiterReadCount++;
        ioActionContext
            .addIoMember(new IoMember(arbiter, getEndPoint(arbiter), arbiterMemberIoStatus,
                Broadcastlog.ReadCause.CHECK));
      } else {
        logger.info("reading arbiter:{} is down with segment form:{} at:{}", arbiter, segmentForm,
            segId);
      }
    }

    segmentForm.processReadIoActionContext(primaryReadCount, secondariesReadCount,
        joiningSecondaryReadCount,
        arbiterReadCount, ioActionContext, volumeType);

    logger.info("request:{} generate read io action context:{} at:{} at:{} at:{} at:{}",
        requestId, ioActionContext, segId);
    logger.info("request:{} generate read io action context:{} at:{}", requestId, ioActionContext,
        segId);
    return ioActionContext;
  }

  private void addUnHealThyInstance(InstanceId instanceId, IoActionContext ioActionContext) {
    if (getSubHealthyDatanodes() != null && getSubHealthyDatanodes().contains(instanceId)) {
      ioActionContext.addUnHealthyInstanceId(instanceId);
    }
  }
}
