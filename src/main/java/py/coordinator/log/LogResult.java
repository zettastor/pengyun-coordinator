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

package py.coordinator.log;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.common.struct.Pair;
import py.coordinator.logmanager.WriteIoContextManager;
import py.coordinator.logmanager.WriteMethodCallback;
import py.icshare.BroadcastLogStatus;
import py.membership.IoActionContext;
import py.membership.IoMember;
import py.membership.SegmentForm;
import py.proto.Broadcastlog;
import py.proto.Broadcastlog.PbBroadcastLog;
import py.proto.Broadcastlog.PbBroadcastLogManager;
import py.proto.Broadcastlog.PbIoUnitResult;
import py.proto.Broadcastlog.PbWriteResponse;
import py.volume.VolumeType;


public class LogResult {

  private static final Logger logger = LoggerFactory.getLogger(LogResult.class);



  public static Pair<BroadcastLogStatus, Long> mergeCreateResult(WriteMethodCallback[] callbacks,
      int index,
      IoActionContext ioActionContext, VolumeType volumeType) {

    Pair<BroadcastLogStatus, Long> logStatusAndId = new Pair<>();

    logStatusAndId.setSecond(-1L);
    if (callbacks == null || callbacks.length == 0) {
      logger.error("write response is null or empty");
      logStatusAndId.setFirst(BroadcastLogStatus.Creating);
      return logStatusAndId;
    }

    SegmentForm segmentForm = ioActionContext.getSegmentForm();

    int goodPrimaryCount = 0;

    int goodSecondariesCount = 0;

    int goodJoiningSecondariesCount = 0;

    int primaryCommittedCount = 0;

    for (int i = 0; i < callbacks.length; i++) {
      PbWriteResponse response = callbacks[i].getResponse();
      if (response == null) {
        continue;
      } else {
        Broadcastlog.PbWriteResponseUnit logUnit = response.getResponseUnits(index);
        if (logStatusAndId.getSecond() < 0) {
          if (logUnit.getLogId() > 0) {
            logStatusAndId.setSecond(logUnit.getLogId());
          }
        } else {
          if (logUnit.getLogId() > 0) {
            if (logStatusAndId.getSecond() != logUnit.getLogId()) {
              Validate.isTrue(false,
                  "pair second:" + logStatusAndId.getSecond() + ", unit log id:" + logUnit
                      .getLogId());
            }
          }
        }

        PbIoUnitResult result = logUnit.getLogResult();

        if (callbacks[i].getMemberIoStatus().isPrimary()) {
          if (result == PbIoUnitResult.PRIMARY_COMMITTED || result == PbIoUnitResult.OK) {
            goodPrimaryCount++;
            if (result == PbIoUnitResult.PRIMARY_COMMITTED) {
              primaryCommittedCount++;
            }
          } else if (result == PbIoUnitResult.OUT_OF_RANGE
              || result == PbIoUnitResult.INPUT_HAS_NO_DATA) {

            logStatusAndId.setFirst(BroadcastLogStatus.Abort);
            return logStatusAndId;
          }
        } else if (callbacks[i].getMemberIoStatus().isSecondary()) {
          if (result == PbIoUnitResult.OK || result == PbIoUnitResult.PRIMARY_COMMITTED
              || result == PbIoUnitResult.SECONDARY_NOT_STABLE) {
            goodSecondariesCount++;
            if (result == PbIoUnitResult.PRIMARY_COMMITTED && ioActionContext.isPrimaryDown()) {
              primaryCommittedCount++;
            }
          }
        } else if (callbacks[i].getMemberIoStatus().isJoiningSecondary()) {
          if (result == PbIoUnitResult.OK || result == PbIoUnitResult.SECONDARY_NOT_STABLE) {
            goodJoiningSecondariesCount++;
          }
        } else {
          Validate.isTrue(false, "unknown status: " + callbacks[i].getMemberIoStatus());
        }
      }
    }
    BroadcastLogStatus broadcastLogStatus = null;

    if (primaryCommittedCount > 0) {
      boolean createSuccess = segmentForm
          .mergeCreateLogResult(goodPrimaryCount, goodSecondariesCount, goodJoiningSecondariesCount,
              ioActionContext, volumeType);
      logger.info(" get the mergeCreateLogResult :{}", createSuccess);
      if (createSuccess) {
        broadcastLogStatus = BroadcastLogStatus.Created;
        if (logStatusAndId.getSecond() <= 0) {
          Validate.isTrue(false,
              "log id:" + logStatusAndId.getSecond() + ", current io action context:"
                  + ioActionContext);
        }

      } else {
        broadcastLogStatus = BroadcastLogStatus.Creating;
      }
    } else {
      broadcastLogStatus = BroadcastLogStatus.Creating;
    }
    logger.info(
        " get the primaryCommittedCount :{}  goodPrimaryCount :{}  goodSecondariesCount :{}  "
            + "goodJoiningSecondariesCount :{} + "

            + "broadcastLogStatus :{}", primaryCommittedCount, goodPrimaryCount,
        goodSecondariesCount,
        goodJoiningSecondariesCount, broadcastLogStatus);
    logStatusAndId.setFirst(broadcastLogStatus);
    return logStatusAndId;
  }



  public static Pair<BroadcastLogStatus, Long> mergeCreateResult(
      Map<IoMember, PbWriteResponse> writeResponseMap, int index,
      IoActionContext ioActionContext, VolumeType volumeType,
      AtomicBoolean beNeedReadDataFromSourceVolume) {

    beNeedReadDataFromSourceVolume.set(false);
    Pair<BroadcastLogStatus, Long> logStatusAndId = new Pair<>();

    logStatusAndId.setSecond(-1L);
    if (writeResponseMap == null || writeResponseMap.isEmpty()) {
      logger.error("write response is null or empty");
      logStatusAndId.setFirst(BroadcastLogStatus.Creating);
      return logStatusAndId;
    }

    SegmentForm segmentForm = ioActionContext.getSegmentForm();

    int goodPrimaryCount = 0;

    int goodSecondariesCount = 0;

    int goodJoiningSecondariesCount = 0;

    int primaryCommittedCount = 0;

    int skipForSourceDataCount = 0;

    for (Map.Entry<IoMember, PbWriteResponse> entry : writeResponseMap.entrySet()) {
      if (entry == null) {
        continue;
      }
      IoMember ioMember = entry.getKey();
      PbWriteResponse response = entry.getValue();
      if (response == null) {
        continue;
      } else {
        Broadcastlog.PbWriteResponseUnit logUnit = response.getResponseUnits(index);
        if (logStatusAndId.getSecond() < 0) {
          if (logUnit.getLogId() > 0) {
            logStatusAndId.setSecond(logUnit.getLogId());
          }
        } else {
          if (logUnit.getLogId() > 0) {
            if (logStatusAndId.getSecond() != logUnit.getLogId()) {
              Validate.isTrue(false,
                  "pair second:" + logStatusAndId.getSecond() + ", unit log id:" + logUnit
                      .getLogId());
            }
          }
        }

        PbIoUnitResult result = logUnit.getLogResult();

        if (ioMember.getMemberIoStatus().isPrimary()) {
          if (result == PbIoUnitResult.PRIMARY_COMMITTED || result == PbIoUnitResult.OK) {
            goodPrimaryCount++;
            if (result == PbIoUnitResult.PRIMARY_COMMITTED) {
              primaryCommittedCount++;
            }
          } else if (result == PbIoUnitResult.OUT_OF_RANGE
              || result == PbIoUnitResult.INPUT_HAS_NO_DATA) {

            logStatusAndId.setFirst(BroadcastLogStatus.Abort);
            return logStatusAndId;
          } else if (result == PbIoUnitResult.FREE) {
            logStatusAndId.setFirst(BroadcastLogStatus.Creating);
            beNeedReadDataFromSourceVolume.set(true);
            return logStatusAndId;
          }

        } else if (ioMember.getMemberIoStatus().isSecondary()) {
          if (result == PbIoUnitResult.OK || result == PbIoUnitResult.PRIMARY_COMMITTED
              || result == PbIoUnitResult.SECONDARY_NOT_STABLE) {
            goodSecondariesCount++;
            if (result == PbIoUnitResult.PRIMARY_COMMITTED && ioActionContext.isPrimaryDown()) {
              primaryCommittedCount++;
            }
          } else if (result == PbIoUnitResult.FREE) {
            skipForSourceDataCount++;
          }
        } else if (ioMember.getMemberIoStatus().isJoiningSecondary()) {
          if (result == PbIoUnitResult.OK || result == PbIoUnitResult.SECONDARY_NOT_STABLE) {
            goodJoiningSecondariesCount++;
          } else if (result == PbIoUnitResult.FREE) {
            skipForSourceDataCount++;
          }
        } else {
          Validate.isTrue(false, "unknown status: " + ioMember.getMemberIoStatus());
        }
      }
    }
    BroadcastLogStatus broadcastLogStatus;

    if (primaryCommittedCount > 0) {
      boolean createSuccess = segmentForm
          .mergeCreateLogResult(goodPrimaryCount, goodSecondariesCount, goodJoiningSecondariesCount,
              ioActionContext, volumeType);
      logger.info(" get the mergeCreateLogResult :{}", createSuccess);
      if (createSuccess) {
        broadcastLogStatus = BroadcastLogStatus.Created;
        if (logStatusAndId.getSecond() <= 0) {
          Validate.isTrue(false,
              "log id:" + logStatusAndId.getSecond() + ", current io action context:"
                  + ioActionContext);
        }

      } else {
        if (segmentForm.isSkipFromSourceVolumeData(skipForSourceDataCount)) {
          beNeedReadDataFromSourceVolume.set(true);
        }
        broadcastLogStatus = BroadcastLogStatus.Creating;
      }
    } else {
      if (segmentForm.isSkipFromSourceVolumeData(skipForSourceDataCount)) {
        beNeedReadDataFromSourceVolume.set(true);
      }
      broadcastLogStatus = BroadcastLogStatus.Creating;
    }
    logger.info(
        " get the primaryCommittedCount :{}  goodPrimaryCount :{}  goodSecondariesCount :{}  "
            + "goodJoiningSecondariesCount :{} + "

            + "broadcastLogStatus :{}", primaryCommittedCount, goodPrimaryCount,
        goodSecondariesCount,
        goodJoiningSecondariesCount, broadcastLogStatus);
    logStatusAndId.setFirst(broadcastLogStatus);
    return logStatusAndId;
  }


  public static void mergeCommitResult(WriteMethodCallback[] callbacks, int managerToCommitIndex,
      WriteIoContextManager managerToCommit, IoActionContext ioActionContext,
      VolumeType volumeType) {
    if (callbacks == null || callbacks.length == 0) {
      logger.error("write response is null or empty");
      return;
    }
    SegmentForm segmentForm = ioActionContext.getSegmentForm();
    for (int index = 0; index < managerToCommit.getLogsToCommit().size(); index++) {
      PbBroadcastLog pbLog = null;
      int goodPrimaryCount = 0;
      int goodSecondariesCount = 0;
      int goodJoiningSecondariesCount = 0;

      for (int i = 0; i < callbacks.length; i++) {
        PbWriteResponse response = callbacks[i].getResponse();
        if (response == null) {
          continue;
        }
        PbBroadcastLogManager pbManager = response.getLogManagersToCommit(managerToCommitIndex);
        Validate.isTrue(managerToCommit.getRequestId() == pbManager.getRequestId());
        pbLog = pbManager.getBroadcastLogs(index);
        if (!PbRequestResponseHelper.isFinalStatus(pbLog.getLogStatus())) {
          continue;
        }

        if (callbacks[i].getResponse() == null) {
          continue;
        } else {
          if (callbacks[i].getMemberIoStatus().isPrimary()) {
            if (PbRequestResponseHelper.isFinalStatus(pbLog.getLogStatus())) {
              goodPrimaryCount++;
            }
          } else if (callbacks[i].getMemberIoStatus().isSecondary()) {
            if (PbRequestResponseHelper.isFinalStatus(pbLog.getLogStatus())) {
              goodSecondariesCount++;
            }
          } else if (callbacks[i].getMemberIoStatus().isJoiningSecondary()) {
            if (PbRequestResponseHelper.isFinalStatus(pbLog.getLogStatus())) {
              goodJoiningSecondariesCount++;
            }
          } else {
            logger.error("unknown write member:{}, {}", i, callbacks[i].getMemberIoStatus());
            Validate.isTrue(false);
          }
        }
      }

      boolean canCommit = segmentForm
          .mergeCommitLogResult(goodPrimaryCount, goodSecondariesCount, goodJoiningSecondariesCount,
              ioActionContext, volumeType);

      if (canCommit) {
        managerToCommit.commitPbLog(index, pbLog);
      }
    }
  }


  public static void mergeCommitResult(Map<IoMember, PbWriteResponse> writeResponseMap,
      int managerToCommitIndex,
      WriteIoContextManager managerToCommit, IoActionContext ioActionContext,
      VolumeType volumeType) {
    if (writeResponseMap == null || writeResponseMap.isEmpty()) {
      logger.error("write response is null or empty");
      return;
    }
    SegmentForm segmentForm = ioActionContext.getSegmentForm();
    for (int index = 0; index < managerToCommit.getLogsToCommit().size(); index++) {
      PbBroadcastLog pbLog = null;
      int goodPrimaryCount = 0;
      int goodSecondariesCount = 0;
      int goodJoiningSecondariesCount = 0;

      for (Map.Entry<IoMember, PbWriteResponse> entry : writeResponseMap.entrySet()) {
        if (entry == null) {
          continue;
        }
        IoMember ioMember = entry.getKey();
        PbWriteResponse response = entry.getValue();
        if (response == null) {
          continue;
        }
        PbBroadcastLogManager pbManager = response.getLogManagersToCommit(managerToCommitIndex);
        Validate.isTrue(managerToCommit.getRequestId() == pbManager.getRequestId());
        pbLog = pbManager.getBroadcastLogs(index);
        if (!PbRequestResponseHelper.isFinalStatus(pbLog.getLogStatus())) {
          continue;
        }

        if (response == null) {
          continue;
        } else {
          if (ioMember.getMemberIoStatus().isPrimary()) {
            if (PbRequestResponseHelper.isFinalStatus(pbLog.getLogStatus())) {
              goodPrimaryCount++;
            }
          } else if (ioMember.getMemberIoStatus().isSecondary()) {
            if (PbRequestResponseHelper.isFinalStatus(pbLog.getLogStatus())) {
              goodSecondariesCount++;
            }
          } else if (ioMember.getMemberIoStatus().isJoiningSecondary()) {
            if (PbRequestResponseHelper.isFinalStatus(pbLog.getLogStatus())) {
              goodJoiningSecondariesCount++;
            }
          } else {
            logger.error("unknown write member:{}", ioMember.getMemberIoStatus());
            Validate.isTrue(false);
          }
        }
      }

      boolean canCommit = segmentForm
          .mergeCommitLogResult(goodPrimaryCount, goodSecondariesCount, goodJoiningSecondariesCount,
              ioActionContext, volumeType);

      if (canCommit) {
        managerToCommit.commitPbLog(index, pbLog);
      }
    }
  }
}
