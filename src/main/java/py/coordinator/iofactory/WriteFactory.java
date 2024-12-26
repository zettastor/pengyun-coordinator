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
import py.volume.VolumeType;


public class WriteFactory extends AbstractIoFactory implements IoFactory {

  private static final Logger logger = LoggerFactory.getLogger(WriteFactory.class);

  public WriteFactory(InstanceStore instanceStore) {
    super(instanceStore, null);
  }

  public WriteFactory(InstanceStore instanceStore, Set<InstanceId> subHealthyDatanodes) {
    super(instanceStore, subHealthyDatanodes);
  }

  @Override
  public IoActionContext generateIoMembers(SegmentMembership segmentMembership,
      VolumeType volumeType,SegId segId, 
      Long requestId) {
    if (logger.isInfoEnabled()) {
      logger.info(
          "request:{} use membership:{} and volume type:{} to generate write io action context "
              + "at:{}",
          requestId, segmentMembership, volumeType, segId);
    }
    IoActionContext ioActionContext = new IoActionContext();
    SegmentForm segmentForm = SegmentForm.getSegmentForm(segmentMembership, volumeType);
    ioActionContext.setSegmentForm(segmentForm);
    ioActionContext.setMembershipWhenIoCome(segmentMembership);

    InstanceId primary = segmentMembership.getPrimary();
    Validate.notNull(primary);
    Validate.isTrue(segmentMembership.getMemberIoStatus(primary).isPrimary());
    MemberIoStatus primaryIoStatus = segmentMembership.getMemberIoStatus(primary);

    int primaryWriteCount = 0;
    int secondariesWriteCount = 0;
    int joiningSecondaryWriteCount = 0;

   
   
    if (primaryIoStatus.isWriteDown()) {
      logger.info("writing primary:{} is down with segment form:{} at:{}", primary, segmentForm,
          segId);
      ioActionContext.setZombieRequest(true);
    } else {
      primaryWriteCount++;
      if (primaryIoStatus.isUnstablePrimary()) {
        ioActionContext.setUnstablePrimaryWrite(true);
      }
      addUnHealThyInstance(primary, ioActionContext);
      ioActionContext.addIoMember(new IoMember(primary, getEndPoint(primary), primaryIoStatus));
    }

   
    Set<InstanceId> secondaries = segmentMembership.getSecondaries();
    for (InstanceId secondary : secondaries) {
     
      MemberIoStatus memberIoStatus = segmentMembership.getMemberIoStatus(secondary);
      if (!memberIoStatus.isWriteDown()) {
        secondariesWriteCount++;
        addUnHealThyInstance(secondary, ioActionContext);
        ioActionContext
            .addIoMember(new IoMember(secondary, getEndPoint(secondary), memberIoStatus));
      } else {
        logger.info("writing secondary:{} is down with segment form:{} at:{}", primary, segmentForm,
            segId);
      }
    }

   
    Set<InstanceId> joiningSecondaries = segmentMembership.getJoiningSecondaries();
    for (InstanceId joiningSecondary : joiningSecondaries) {
     
      if (!segmentMembership.getMemberIoStatus(joiningSecondary).isWriteDown()) {
        joiningSecondaryWriteCount++;
        addUnHealThyInstance(joiningSecondary, ioActionContext);
        ioActionContext.addIoMember(
            new IoMember(joiningSecondary, getEndPoint(joiningSecondary),
                MemberIoStatus.JoiningSecondary));
      } else {
        logger.info("writing joining secondary:{} is down with segment form:{} at:{}", primary,
            segmentForm,
            segId);
      }
    }

    segmentForm.processWriteIoActionContext(primaryWriteCount, secondariesWriteCount,
        joiningSecondaryWriteCount,
        ioActionContext, volumeType);

    ioActionContext
        .setTotalWriteCount(primaryWriteCount + secondariesWriteCount + joiningSecondaryWriteCount);
    if (logger.isInfoEnabled()) {
      logger.info("request:{} generate write io action context:{} at:{} write total:{}", requestId,
          ioActionContext, segId,
          (primaryWriteCount + secondariesWriteCount + joiningSecondaryWriteCount));
    }
    return ioActionContext;
  }

  private void addUnHealThyInstance(InstanceId instanceId, IoActionContext ioActionContext) {
    if (getSubHealthyDatanodes() != null && getSubHealthyDatanodes().contains(instanceId)) {
      ioActionContext.addUnHealthyInstanceId(instanceId);
    }
  }
}
