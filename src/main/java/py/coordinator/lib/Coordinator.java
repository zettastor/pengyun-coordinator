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

package py.coordinator.lib;

import com.google.common.collect.Multimap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.RequestResponseHelper;
import py.archive.segment.CloneType;
import py.archive.segment.SegId;
import py.client.thrift.GenericThriftClientFactory;
import py.common.Counter;
import py.common.DelayManager;
import py.common.LoggerTracer;
import py.common.NamedThreadFactory;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.common.lock.SemaphoreWithCounter;
import py.common.struct.EndPoint;
import py.common.struct.LimitQueue;
import py.connection.pool.udp.detection.DefaultDetectionTimeoutPolicyFactory;
import py.connection.pool.udp.detection.NetworkIoHealthChecker;
import py.connection.pool.udp.detection.UdpDetectorImpl;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.coordinator.iofactory.ReadFactory;
import py.coordinator.iofactory.WriteFactory;
import py.coordinator.iorequest.iorequest.IoRequest;
import py.coordinator.iorequest.iorequest.IoRequestImpl;
import py.coordinator.iorequest.iorequest.IoRequestType;
import py.coordinator.iorequest.iounitcontext.IoUnitContext;
import py.coordinator.iorequest.iounitcontextpacket.IoUnitContextPacket;
import py.coordinator.log.BroadcastLog;
import py.coordinator.logmanager.DelayManagerImpl;
import py.coordinator.logmanager.IoContextManager;
import py.coordinator.logmanager.LogRecorder;
import py.coordinator.logmanager.LogRecorderImpl;
import py.coordinator.logmanager.ReadIoContextManager;
import py.coordinator.logmanager.ReadMethodCallback;
import py.coordinator.logmanager.UncommittedLogManager;
import py.coordinator.logmanager.UncommittedLogManagerImpl;
import py.coordinator.logmanager.WriteIoContextManager;
import py.coordinator.logmanager.WriteMethodCallback;
import py.coordinator.pbrequest.ReadRequestBuilder;
import py.coordinator.pbrequest.WriteRequestBuilder;
import py.coordinator.response.CheckPrimaryReachableCallbackCollector;
import py.coordinator.response.CheckRequestCallback;
import py.coordinator.response.CheckSecondaryReachableCallbackCollector;
import py.coordinator.response.CheckTempPrimaryReachableCallbackCollector;
import py.coordinator.response.GetMembershipCallbackForIO;
import py.coordinator.response.NotifyDatanodeStartOnlineMigrationCallback;
import py.coordinator.response.TempPrimaryCheckSecondaryReachableCallbackCollector;
import py.coordinator.response.TriggerByCheckCallback;
import py.coordinator.task.CreateSegmentContext;
import py.coordinator.task.GetMembershipTask;
import py.coordinator.task.ResendRequest;
import py.coordinator.task.UpdateMembershipRequest;
import py.coordinator.utils.DummyNetworkDelayRecorder;
import py.coordinator.utils.NetworkDelayRecorder;
import py.coordinator.volumeinfo.SpaceSavingVolumeMetadata;
import py.coordinator.volumeinfo.VolumeInfoRetriever;
import py.coordinator.volumeinfo.VolumeInfoRetrieverImpl;
import py.coordinator.worker.CommitLogWorker;
import py.coordinator.worker.CommitLogWorkerImpl;
import py.coordinator.worker.GetMembershipFromDatanodesEngine;
import py.coordinator.worker.GetMembershipFromInfoCenterEngine;
import py.coordinator.worker.OutputPerformanceWorkerFactory;
import py.exception.GenericThriftClientFactoryException;
import py.exception.StorageException;
import py.infocenter.client.InformationCenterClientFactory;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.instance.SimpleInstance;
import py.io.sequential.RandomSequentialIdentifierV2Impl;
import py.membership.IoActionContext;
import py.membership.IoMember;
import py.membership.MemberIoStatus;
import py.membership.SegmentMembership;
import py.monitor.common.OperationName;
import py.netty.client.GenericAsyncClientFactory;
import py.netty.client.TransferenceClientOption;
import py.netty.core.TransferenceConfiguration;
import py.netty.datanode.AsyncDataNode;
import py.netty.datanode.PyWriteRequest;
import py.netty.exception.InvalidProtocolException;
import py.netty.exception.NetworkUnhealthyBySdException;
import py.netty.message.ProtocolBufProtocolFactory;
import py.performance.PerformanceManager;
import py.periodic.impl.ExecutionOptionsReader;
import py.periodic.impl.PeriodicWorkExecutorImpl;
import py.proto.Broadcastlog;
import py.proto.Broadcastlog.PbReadRequest;
import py.proto.Broadcastlog.ReadCause;
import py.proto.Broadcastlog.RequestOption;
import py.proto.Commitlog;
import py.thrift.datanode.service.DataNodeService;
import py.utils.BroadcastLogUuidGenerator;
import py.volume.VolumeType;


public class Coordinator extends StorageDriver {

  protected static final Logger logger = LoggerFactory.getLogger(Coordinator.class);
  private static final int MAX_IO_DEPTH = 2048;
  public final AtomicLong requestVolumeId = new AtomicLong(0);
  public final AtomicInteger requestSnapshotId = new AtomicInteger(0);
  public final AtomicBoolean pause = new AtomicBoolean(false);
  public final AtomicBoolean isMoveOnline = new AtomicBoolean(true);
  private final InstanceStore instanceStore;
  private final LogRecorder logRecorder;
  private final UncommittedLogManager uncommittedLogManager;
  private final DelayManager delayManager;
  private final CommitLogWorker commitLogWorker;
  private final Semaphore ioDepth;
  private final GetMembershipFromInfoCenterEngine getMembershipFromInfoCenterEngine;
  private final GetMembershipFromDatanodesEngine getMembershipFromDatanodesEngine;
  private final String className;
  private final IoSeparatorImpl ioSeparator;
  private final IoConvertorImpl ioConvertor;
  private final VolumeInfoHolderManager volumeInfoHolderManager;
  private final Map<InstanceId, Long> instanceProcessingSnapshotVersionMismatch =
      new ConcurrentHashMap<>();
  Set<InstanceId> subhealthyDatanodeInstance;


  private InstanceId driverContainerId;
  private InstanceId myInstanceId;
  private InstanceId trueInstanceId;

  private GenericThriftClientFactory<DataNodeService.AsyncIface> dataNodeAsyncClientFactory = null;

  private GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory = null;
  private InformationCenterClientFactory informationCenterClientFactory;
  private VolumeInfoRetriever volumeInfoRetriever;
  private boolean openFlag = false;
  private ByteBufAllocator allocator;
  private CoordinatorConfigSingleton cfg = CoordinatorConfigSingleton.getInstance();
  private GenericAsyncClientFactory<AsyncDataNode.AsyncIface> clientFactory;
  private WriteFactory writeFactory;
  private ReadFactory readFactory;

  private OutputPerformanceWorkerFactory outputPerformanceWorkerFactory;
  private PeriodicWorkExecutorImpl executor;
  private ScheduledExecutorService checkNetHealThyService = Executors
      .newSingleThreadScheduledExecutor(new NamedThreadFactory("CheckNetHealThyService"));
  private NetworkDelayRecorder networkDelayRecorder;
  private RandomSequentialIdentifierV2Impl randomSequentialIdentifierV2;


  public Coordinator(InstanceStore instanceStore,
      GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory,
      GenericThriftClientFactory<DataNodeService.AsyncIface> dataNodeAsyncClientFactory,
      ByteBufAllocator allocator) {
    this(instanceStore, dataNodeSyncClientFactory, dataNodeAsyncClientFactory, allocator, null);
  }


  public Coordinator(InstanceStore instanceStore,
      GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory,
      GenericThriftClientFactory<DataNodeService.AsyncIface> dataNodeAsyncClientFactory,
      ByteBufAllocator allocator,
      GenericAsyncClientFactory<AsyncDataNode.AsyncIface> clientFactory) {
    super("Coordinator-");

    this.className = getClass().getSimpleName();

    try {
      Validate.notNull(instanceStore);
      Validate.notNull(dataNodeSyncClientFactory);
      Validate.notNull(dataNodeAsyncClientFactory);
      Validate.notNull(allocator);
    } catch (Throwable e) {
      logger.error("none of these params can not be null", e);
      System.exit(-1);
    }

    this.dataNodeSyncClientFactory = dataNodeSyncClientFactory;
    this.dataNodeAsyncClientFactory = dataNodeAsyncClientFactory;
    this.instanceStore = instanceStore;
    this.volumeInfoRetriever = new VolumeInfoRetrieverImpl();
    this.allocator = allocator;

    // randomly get an instance id. Driver container might set its instance id later
    this.myInstanceId = new InstanceId(RequestIdBuilder.get());
    this.logRecorder = new LogRecorderImpl();
    this.delayManager = new DelayManagerImpl(this, this.volumeInfoRetriever);
    this.uncommittedLogManager = new UncommittedLogManagerImpl();
    this.commitLogWorker = new CommitLogWorkerImpl(this);
    this.uncommittedLogManager.setCommitLogWorker(getCommitLogWorker());

    this.volumeInfoHolderManager = new VolumeInfoHolderManagerImpl(volumeInfoRetriever);




    this.networkDelayRecorder = new DummyNetworkDelayRecorder();

    // inc one to io depth for no-blocking try semaphore
    Validate.isTrue(cfg.getIoDepth() > 0);
    Validate.isTrue(cfg.getTrySemaphoreMaxCountPerTime() > 0);
    if (cfg.getIoDepth() > MAX_IO_DEPTH) {
      logger.warn("can not accept io depth:{} bigger than MAX_IO_DEPTH:{}, so we use MAX_IO_DEPTH",
          cfg.getIoDepth(), MAX_IO_DEPTH);
      cfg.setIoDepth(MAX_IO_DEPTH);
    }

    int percent = 4;
    int calcTrySemaphoreMaxCountPerTime = cfg.getIoDepth() / percent;
    if (calcTrySemaphoreMaxCountPerTime == 0) {
      calcTrySemaphoreMaxCountPerTime = 1;
      logger.warn(
          "IO depth:{} is smaller than percent:{}, so we chang TrySemaphoreMaxCountPerTime to:{}",
          cfg.getIoDepth(), percent, calcTrySemaphoreMaxCountPerTime);
      cfg.setTrySemaphoreMaxCountPerTime(calcTrySemaphoreMaxCountPerTime);
    } else if (cfg.getTrySemaphoreMaxCountPerTime() > calcTrySemaphoreMaxCountPerTime) {
      logger
          .warn("can not set trySemaphoreMaxCountPerTime:{} bigger than :{}, so we chang it to:{}",
              cfg.getTrySemaphoreMaxCountPerTime(), calcTrySemaphoreMaxCountPerTime,
              calcTrySemaphoreMaxCountPerTime);
      cfg.setTrySemaphoreMaxCountPerTime(calcTrySemaphoreMaxCountPerTime);
    }

    this.getMembershipFromInfoCenterEngine = new GetMembershipFromInfoCenterEngine(
        GetMembershipFromInfoCenterEngine.class.getSimpleName(), this);
    this.getMembershipFromDatanodesEngine = new GetMembershipFromDatanodesEngine(
        GetMembershipFromDatanodesEngine.class.getSimpleName(), this);
    this.ioSeparator = new IoSeparatorImpl(cfg.getMaxReadDataSizePerRequest(),
        cfg.getMaxWriteDataSizePerRequest(),
        this);

    this.randomSequentialIdentifierV2 = new RandomSequentialIdentifierV2Impl(
        cfg.getIoLogSequentialCondition(), "coordinator");

    this.ioConvertor = new IoConvertorImpl(this, this.volumeInfoHolderManager, ioSeparator,
        cfg.getSegmentSize(),
        cfg.getPageSize());

    this.subhealthyDatanodeInstance = new HashSet<>();
    int networkHealthyTime = CoordinatorConfigSingleton.getInstance().getNetworkHealthyCheckTime();
    checkNetHealThyService.scheduleWithFixedDelay(() -> {
      subhealthyDatanodeInstance.clear();
    }, networkHealthyTime, networkHealthyTime, TimeUnit.SECONDS);
    TransferenceConfiguration transferenceConfiguration = TransferenceConfiguration
        .defaultConfiguration();
    transferenceConfiguration
        .option(TransferenceClientOption.CONNECTION_COUNT_PER_ENDPOINT,
            cfg.getConnectionPoolSize());
    transferenceConfiguration
        .option(TransferenceClientOption.IO_TIMEOUT_MS, cfg.getNettyRequestTimeoutMs());
    transferenceConfiguration
        .option(TransferenceClientOption.IO_CONNECTION_TIMEOUT_MS, cfg.getNettyConnectTimeoutMs());
    transferenceConfiguration.option(TransferenceClientOption.MAX_BYTES_ONCE_ALLOCATE,
        cfg.getMaxBufLengthForNettyAllocateAdapter());
    transferenceConfiguration.option(TransferenceClientOption.CLIENT_IO_EVENT_GROUP_THREADS_MODE,
        cfg.getClientIoEventGroupThreadsMode());
    transferenceConfiguration
        .option(TransferenceClientOption.CLIENT_IO_EVENT_GROUP_THREADS_PARAMETER,
            cfg.getClientIoEventGroupThreadsParameter());
    transferenceConfiguration.option(TransferenceClientOption.CLIENT_IO_EVENT_HANDLE_THREADS_MODE,
        cfg.getClientIoEventHandleThreadsMode());
    transferenceConfiguration
        .option(TransferenceClientOption.CLIENT_IO_EVENT_HANDLE_THREADS_PARAMETER,
            cfg.getClientIoEventHandleThreadsParameter());


    try {
      UdpDetectorImpl.INSTANCE.start(cfg.getNetworkConnectionDetectServerListeningPort(),
          new DefaultDetectionTimeoutPolicyFactory(
              cfg.getNetworkConnectionDetectRetryMaxtimes(),
              cfg.getPingHostTimeoutMs()));
      NetworkIoHealthChecker.INSTANCE.start(UdpDetectorImpl.INSTANCE);
    } catch (SocketException e) {
      logger.error("failed to start udp detector...", e);
    }

    this.ioDepth = new SemaphoreWithCounter(cfg.getIoDepth(), Counter.NullCounter);

    if (clientFactory == null) {
      try {
        logger.warn("going to construct netty client factory");
        this.clientFactory = new GenericAsyncClientFactory<>(AsyncDataNode.AsyncIface.class,
            ProtocolBufProtocolFactory.create(AsyncDataNode.AsyncIface.class),
            transferenceConfiguration);
        this.clientFactory.setAllocator(this.allocator);
        this.clientFactory.init();

      } catch (InvalidProtocolException e) {
        logger.error("can not init client factory", e);
        System.exit(1);
      }
    } else {
      this.clientFactory = clientFactory;
    }
  }

  private void initVolumeHolder(Long volumeId, int snapshotId) {

    logger.warn("the current volume id is:{}", volumeId);
    try {
      this.volumeInfoHolderManager.addVolumeInfoHolder(volumeId, snapshotId);
      requestVolumeId.set(volumeId);
      requestSnapshotId.set(snapshotId);
    } catch (Exception e) {
      logger.error("caught an exception when init volume holder:{}", volumeId, e);
      throw new RuntimeException();
    }
  }

  @Override
  public void open(Long volumeId, int snapshotId) throws StorageException {
    logger.warn("going to open volumeId:{}, snapshotId:{}", volumeId, snapshotId);
    if (!openFlag) {
      logger.warn("begin open volumeId:{}, snapshotId:{}, coordinator's configuration:{}", volumeId,
              snapshotId,
              cfg);
      try {

        Validate.notNull(informationCenterClientFactory);

        this.volumeInfoRetriever.setInfoCenterClientFactory(informationCenterClientFactory);

        initVolumeHolder(volumeId, snapshotId);

        /*
         * the commit work pool will take task that commits logs which can not be carried by
         * write request
         */

        getCommitLogWorker().start();

        /*
         * async get membership from info center
         */
        this.getMembershipFromInfoCenterEngine.start();

        /*
         * async get membership from datanodes
         */
        this.getMembershipFromDatanodesEngine.start();

        /*
         * generate an task to update membership if no recently IO
         */
        UpdateMembershipRequest updateMembershipRequest = new UpdateMembershipRequest(
                DelayManagerImpl.DEFAULT_UPDATE_MEMBERSHIP_IF_NO_IO);
        this.delayManager.put(updateMembershipRequest);

        this.writeFactory = new WriteFactory(instanceStore, subhealthyDatanodeInstance);
        this.readFactory = new ReadFactory(instanceStore, subhealthyDatanodeInstance);


        if (cfg.isEnableLoggerTracer()) {

          LoggerTracer.getInstance().start();
          LoggerTracer.getInstance().enableLoggerTracer();
        } else {
          LoggerTracer.getInstance().disableLoggerTracer();
        }

        this.openFlag = true;
      } catch (Exception e) {
        this.openFlag = false;
        logger.error("can't open coordinator with first volumeId:{}", requestVolumeId.get(), e);
        throw new StorageException(e);
      }
      logger.warn("end open volume:{}, snapshotId:{}", volumeId, snapshotId);
    } else {
      logger.error("volume:{}, snapshotId:{} has been opened before, DO NOT need open again",
              volumeId,
              snapshotId);
    }

  }


  public void resendWrite(IoContextManager ioContextManager, boolean needRetrieveMembership)
      throws StorageException {
    final WriteIoContextManager manager = (WriteIoContextManager) ioContextManager;
    int resendTimes = ioContextManager.incFailTimes();
    logger
        .info("re-write process, ori:{}, need retrieve membership:{}, resendTimes:{}, at segId:{}",
            ioContextManager.getRequestId(), needRetrieveMembership, resendTimes,
            ioContextManager.getSegId());
    LoggerTracer.getInstance().mark(ioContextManager.getRequestId(), className,
        "re-write process, ori:{}, need retrieve membership:{}, resendTimes:{}, at segId:{}",
        ioContextManager.getRequestId(), needRetrieveMembership, resendTimes,
        ioContextManager.getSegId());

    final SpaceSavingVolumeMetadata volumeMetadata = this.volumeInfoHolderManager
        .getVolumeInfoHolder(ioContextManager.getVolumeId()).getVolumeMetadata();
    WriteRequestBuilder oldBuilder = (WriteRequestBuilder) ioContextManager.getRequestBuilder();






    WriteRequestBuilder newBuilder = new WriteRequestBuilder(oldBuilder.getRequestType());
    newBuilder.setRequestId(ioContextManager.getRequestId());
    newBuilder.setSegId(ioContextManager.getSegId());
    newBuilder.setFailTimes(resendTimes);
    newBuilder.setRequestTime(oldBuilder.getRequestTime());
    newBuilder.setLogsToCreate(manager.getLogsToCreate());
    newBuilder.setManagersToCommit(manager.getManagersToCommit());
    ioContextManager.setRequestBuilder(newBuilder);


    if (needRetrieveMembership) {
      boolean update = updateMembershipForWrite(manager, newBuilder);
      if (update) {
        return;
      }
    }
    SegmentMembership membership = volumeMetadata
        .getHighestMembership(ioContextManager.getLogicalSegmentIndex());
    logger.info("rewrite, ori {}, logsToCreate {} membership {} seg {}",
        ioContextManager.getRequestId(), manager.getLogsToCreate(), membership,
        ioContextManager.getSegId());
    asyncWrite(manager, membership, newBuilder, false, volumeMetadata.getVolumeType());
  }



  public void resendWriteAfterRetrieveMembership(WriteIoContextManager ioContextManager,
      WriteRequestBuilder builder) {
    SpaceSavingVolumeMetadata volumeMetadata = this.volumeInfoHolderManager
        .getVolumeInfoHolder(ioContextManager.getVolumeId()).getVolumeMetadata();
    SegmentMembership membership = volumeMetadata
        .getHighestMembership(ioContextManager.getLogicalSegmentIndex());
    logger.info("re-write after retrieve membership process, ori:{} write to:{}",
        ioContextManager.getRequestId(),
        membership);
    LoggerTracer.getInstance().mark(ioContextManager.getRequestId(), className,
        "re-write after retrieve membership process, ori:{} write to:{}",
        ioContextManager.getRequestId(),
        membership);

    WriteRequestBuilder newBuilder = new WriteRequestBuilder(builder.getRequestType());
    newBuilder.setRequestTime(builder.getRequestTime());
    newBuilder.setRequestId(ioContextManager.getRequestId());
    newBuilder.setSegId(ioContextManager.getSegId());
    newBuilder.setLogsToCreate(ioContextManager.getLogsToCreate());
    newBuilder.setManagersToCommit(ioContextManager.getManagersToCommit());
    newBuilder.setFailTimes(ioContextManager.getFailTimes());

    ioContextManager.setRequestBuilder(newBuilder);

    asyncWrite(ioContextManager, membership, newBuilder, false, volumeMetadata.getVolumeType());
  }

  @Deprecated
  private synchronized void completeVolume(Long volumeId, int segmentIndex,
      IoUnitContextPacket context) {

    // convert segment index to the start segment index of the passel of segments this segment
    // belongs to
    SpaceSavingVolumeMetadata volumeMetadata = this.volumeInfoHolderManager
        .getVolumeInfoHolder(volumeId).getVolumeMetadata();
    Multimap<Integer, IoUnitContextPacket> ioContextWaitingForVolumeCompletion =
        this.volumeInfoHolderManager
            .getVolumeInfoHolder(volumeId)
            .getIoContextWaitingForVolumeCompletion();
    int segmentPasselNum = volumeMetadata.getSegmentNumToCreateEachTime();
    int startSegmentIndex = (segmentIndex / segmentPasselNum) * segmentPasselNum;
    logger.warn("complete volume segment index:{}, passel number:{}, start segment index:{}",
        segmentIndex,
        segmentPasselNum, startSegmentIndex);

    Collection<IoUnitContextPacket> contexts = ioContextWaitingForVolumeCompletion
        .get(startSegmentIndex);
    if (contexts == null || contexts.isEmpty()) {
      ioContextWaitingForVolumeCompletion.put(startSegmentIndex, context);
      int segmentNumToCreate = 0;
      for (int i = 0; i < segmentPasselNum; i++) {
        int innerIndex = startSegmentIndex + i;
        if (innerIndex > volumeMetadata.getSegmentCount()) {
          break;
        } else if (volumeMetadata.getSegId(innerIndex) == null) {
          segmentNumToCreate++;
        } else {
          break;
        }
      }
      if (segmentNumToCreate == 0) {
        Validate.isTrue(false, "can not be zero" + volumeMetadata);
      }
      logger.warn(
          "need to create segment for a uncompleted volume, segment number:{} should be created",
          segmentNumToCreate);

      delayManager.put(new CreateSegmentContext(5000, volumeId, startSegmentIndex,
          volumeMetadata.getVolumeType(),
          volumeMetadata.getCacheType(), segmentNumToCreate, volumeMetadata.getDomainId(),
          volumeMetadata.getStoragePoolId()));

    } else {
      logger.warn("already have segment index:{} to complete volume", startSegmentIndex);
      ioContextWaitingForVolumeCompletion.put(startSegmentIndex, context);
    }
  }

  public long generateLogUuid() {
    return BroadcastLogUuidGenerator.getInstance().generateUuid();
  }


  public void firstWrite(IoUnitContextPacket callback) throws StorageException {
    int segmentIndex = callback.getLogicalSegIndex();
    Long volumeId = callback.getVolumeId();
    VolumeInfoHolder volumeInfoHolder = this.volumeInfoHolderManager.getVolumeInfoHolder(volumeId);
    SpaceSavingVolumeMetadata volumeMetadata = volumeInfoHolder.getVolumeMetadata();
    SegId segId = volumeMetadata.getSegId(segmentIndex);
    if (segId == null) {
      logger
          .error("firstWrite, can not get the segId, the segmentIndex{} in volume {}", segmentIndex,
              volumeId);
      throw new StorageException();
    }


    List<BroadcastLog> newLogs = new ArrayList<>(callback.getIoContext().size());
    Map<SegId, AtomicLong> recordLastIoTime = this.volumeInfoHolderManager
        .getVolumeInfoHolder(volumeId).getRecordLastIoTime();

    if (!recordLastIoTime.containsKey(segId)) {
      recordLastIoTime.putIfAbsent(segId, new AtomicLong(0));
    }
    AtomicLong lastIoTime = recordLastIoTime.get(segId);
    lastIoTime.set(System.currentTimeMillis());

    List<IoUnitContext> ioContexts = callback.getIoContext();
    int logCount = ioContexts.size();
    int latestSnapshotVersion = 0;
    for (int i = 0; i < logCount; i++) {
      IoUnitContext context = ioContexts.get(i);
      long logUuid = generateLogUuid();
      BroadcastLog newLog = new BroadcastLog(logUuid, context, latestSnapshotVersion);
      if (callback.getRequestType().isWrite()) {
        newLog.setChecksum(0L);
      }
      newLogs.add(newLog);
    }
    randomSequentialIdentifierV2.judgeIoIsSequential(newLogs);
    WriteIoContextManager ioContextManager = new WriteIoContextManager(volumeId, segId, callback,
        this, newLogs,
        callback.getRequestType());

    ioContextManager.setExpiredTime(cfg.getWriteIoTimeoutMs() + System.currentTimeMillis());
    final SegmentMembership membership = volumeMetadata.getHighestMembership(segmentIndex);

    WriteRequestBuilder builder = new WriteRequestBuilder(callback.getRequestType());
    builder.setRequestTime(System.currentTimeMillis());
    builder.setRequestId(ioContextManager.getRequestId());
    builder.setSegId(segId);
    builder.setLogsToCreate(ioContextManager.getLogsToCreate());

    builder.setManagersToCommit(ioContextManager.getManagersToCommit());
    builder.setFailTimes(0);
    ioContextManager.setRequestBuilder(builder);

    logger.info("first write process, ori:{}, segId:{}, max timeout:{}",
        ioContextManager.getRequestId(), segId,
        cfg.getWriteIoTimeoutMs());
    if (CoordinatorConfigSingleton.getInstance().isTraceAllLogs()) {
      LoggerTracer.getInstance().mark(ioContextManager.getRequestId(), className,
          "first write process, ori:{}, segId:{}, max timeout:{}", ioContextManager.getRequestId(),
          segId,
          cfg.getWriteIoTimeoutMs());
    }
    asyncWrite(ioContextManager, membership, builder, true, volumeMetadata.getVolumeType());
  }

  private void processErrorBeforeSending(IoContextManager ioContextManager) {
    if (ioContextManager.isExpired()) {
      ioContextManager.doResult();
    } else {
      ResendRequest resendRequest = new ResendRequest(ioContextManager, true);
      delayManager.put(resendRequest);
    }
  }

  private void asyncWrite(WriteIoContextManager ioContextManager, SegmentMembership membership,
      WriteRequestBuilder builder, boolean firstWrite, VolumeType volumeType) {
    // general set membership here
    builder.setSegmentMembership(membership);
    IoActionContext ioActionContext;
    try {
      ioActionContext = writeFactory
          .generateIoMembers(membership, volumeType, ioContextManager.getSegId(),
              ioContextManager.getRequestId());
    } catch (Exception e) {
      logger.error("ori:{} can not get endpoints by:{} for write", ioContextManager.getRequestId(),
          membership,
          e);
      processErrorBeforeSending(ioContextManager);
      return;
    }
    // update io action context to io manager
    ioContextManager.setIoActionContext(ioActionContext);
    if (ioContextManager.getFailTimes() > 0) {
      logger.info(
          "ori:{} write request io action:{}, stream io:{} is primary down:{}, got TP:{}, "
              + "resendDirectly:{}",
          ioContextManager.getRequestId(), ioActionContext, ioContextManager.streamIO(),
          ioActionContext.isPrimaryDown(), ioActionContext.gotTempPrimary(),
          ioActionContext.isResendDirectly());
      logger.info("ori:{} at:{}, membership:{} member io status:{}, re-write times:{}",
          ioContextManager.getRequestId(), ioContextManager.getSegId(), membership,
          membership.getMemberIoStatusMap(), ioContextManager.getFailTimes());
    }
    if (ioActionContext.isResendDirectly() || (ioContextManager.streamIO() && (
        ioActionContext.isPrimaryDown()
            && !ioActionContext.gotTempPrimary()))) {
      if (ioContextManager.streamIO()) {
        if (firstWrite) {
          GetMembershipTask getMembershipTask = new GetMembershipTask(
              ioContextManager.getVolumeId(),
              ioContextManager.getSegId(), ioContextManager.getOriRequestId(), membership);
          getMembershipFromDatanodesEngine.putTask(getMembershipTask);
        }
        ioContextManager.doneDirectly();
        return;
      }
      logger.info(
          "ori:{} first:{} directly put write request back, TO members:{}, at:{}, segmentForm:{},"
              + "TotalWriteCount:{}",
          ioContextManager.getRequestId(), firstWrite, ioActionContext.getIoMembers(),
          ioContextManager.getSegId(), ioActionContext.getSegmentForm(),
          ioActionContext.getTotalWriteCount());
      LoggerTracer.getInstance().mark(ioContextManager.getRequestId(), className,
          "ori:{} first:{} directly put write request back, TO members:{}, at:{}, segmentForm:{},"
              + " TotalWriteCount:{}",
          ioContextManager.getRequestId(), firstWrite, ioActionContext.getIoMembers(),
          ioContextManager.getSegId(), ioActionContext.getSegmentForm(),
          ioActionContext.getTotalWriteCount());

      processErrorBeforeSending(ioContextManager);
      return;
    }

    logger.info(
        "ori:{} first:{} get write request, TO members:{}, at:{}, segmentForm:{},"
            + "TotalWriteCount:{}",
        ioContextManager.getRequestId(), firstWrite, ioActionContext.getIoMembers(),
        ioContextManager.getSegId(), ioActionContext.getSegmentForm(),
        ioActionContext.getTotalWriteCount());
    LoggerTracer.getInstance().mark(ioContextManager.getRequestId(), className,
        "ori:{} first:{} get write request, TO members:{}, at:{}, segmentForm:{}, "
            + "TotalWriteCount:{}",
        ioContextManager.getRequestId(), firstWrite, ioActionContext.getIoMembers(),
        ioContextManager.getSegId(), ioActionContext.getSegmentForm(),
        ioActionContext.getTotalWriteCount());

    final AtomicInteger counter = new AtomicInteger(ioActionContext.getIoMembers().size());
    ioContextManager.initRequestCount(counter.get());

    if (ioActionContext.isZombieRequest()) {
      builder.setZombieWrite(true);
    } else {
      builder.setZombieWrite(false);
    }

    if (ioActionContext.isUnstablePrimaryWrite()) {
      builder.setUnstablePrimaryWrite(true);
    } else {
      builder.setUnstablePrimaryWrite(false);
    }

    PyWriteRequest request = null;
    Broadcastlog.PbWriteRequest pbWriteRequest = null;

    if (builder.getRequestType().isWrite()) {
      request = builder.getRequest();
    } else if (builder.getRequestType().isDiscard()) {
      pbWriteRequest = builder.getPbWriteRequest();
    } else {
      Validate.isTrue(false, "can not happen this way");
    }

    boolean hasIoSetFail = false;
    for (IoMember ioMember : ioActionContext.getIoMembers()) {
      MemberIoStatus memberIoStatus = ioMember.getMemberIoStatus();
      Validate.notNull(memberIoStatus);
      logger.info("ori:{} begin write to [{}] endPoint:{} at:{}, write unit count:{}",
          ioContextManager.getRequestId(), ioMember.getMemberIoStatus(), ioMember.getEndPoint(),
          ioContextManager.getSegId(), ioContextManager.getLogsToCreate().size());
      if (CoordinatorConfigSingleton.getInstance().isTraceAllLogs()) {
        LoggerTracer.getInstance().mark(ioContextManager.getRequestId(), className,
            "ori:{} begin write to [{}] endPoint:{} at:{}, write unit count:{}",
            ioContextManager.getRequestId(), ioMember.getMemberIoStatus(), ioMember.getEndPoint(),
            ioContextManager.getSegId(), ioContextManager.getLogsToCreate().size());
      }

      WriteMethodCallback callback = new WriteMethodCallback(ioContextManager, counter, this,
          ioMember);
      if (needSetIoFailByInstance(ioActionContext, ioMember, hasIoSetFail)) {
        hasIoSetFail = true;
        callback.fail(new NetworkUnhealthyBySdException());
      } else {
        if (builder.getRequestType().isWrite()) {
          clientFactory.generate(ioMember.getEndPoint()).write(request.clone(), callback);
        } else if (builder.getRequestType().isDiscard()) {
          clientFactory.generate(ioMember.getEndPoint()).discard(pbWriteRequest, callback);
        } else {
          if (builder.getRequestType().isWrite()) {
            clientFactory.generate(ioMember.getEndPoint()).write(request.clone(), callback);
          } else if (builder.getRequestType().isDiscard()) {
            clientFactory.generate(ioMember.getEndPoint()).discard(pbWriteRequest, callback);
          } else {
            Validate.isTrue(false, "can not happen this way");
          }
        }
      }
    }
  }

  @Override
  public void asyncWrite(long pos, byte[] buf, int off, int len, AsyncIoCallBack asyncIoCallBack)
      throws StorageException {
    Validate.isTrue(buf.length - off >= len);
    IoRequest ioRequest = new IoRequestImpl(pos, len, IoRequestType.Write,
        Unpooled.wrappedBuffer(buf, off, len),
        asyncIoCallBack, 1);
    Long requestUuid = RequestIdBuilder.get();
    asyncProcessIoRequest(requestUuid, ioRequest, true);

  }

  @Override
  public void asyncWrite(long pos, ByteBuffer buffer, AsyncIoCallBack asyncIoCallBack)
      throws StorageException {
    Validate.isTrue(buffer.remaining() > 0);
    IoRequest ioRequest = new IoRequestImpl(pos, buffer.remaining(), IoRequestType.Write,
        Unpooled.wrappedBuffer(buffer), asyncIoCallBack, 1);
    Long requestUuid = RequestIdBuilder.get();
    asyncProcessIoRequest(requestUuid, ioRequest, true);
  }

  private boolean needSetIoFailByInstance(IoActionContext ioActionContext, IoMember ioMember,
      boolean hasIoSetFail) {
    if (ioActionContext.getNetUnHealthyInstanceId().size() == 0) {
      return false;
    } else if (ioActionContext.getNetUnHealthyInstanceId().size() == 1) {
      if (ioActionContext.getNetUnHealthyInstanceId().contains(ioMember.getInstanceId())) {
        return true;
      } else {
        return false;
      }
    } else {
      if (ioMember.getMemberIoStatus() == MemberIoStatus.Primary || hasIoSetFail || !ioActionContext
          .getNetUnHealthyInstanceId().contains(ioMember.getInstanceId())) {
        return false;
      } else {
        return true;
      }
    }
  }



  public void resendRead(IoContextManager ioContextManager, boolean needUpdateMembership)
      throws StorageException {
    final int failTimes = ioContextManager.incFailTimes();
    SegId segId = ioContextManager.getSegId();

    ReadIoContextManager manager = (ReadIoContextManager) ioContextManager;
    Set<Long> pageIndexes = manager.getRelatedPageIndexes();
    final List<Long> logsToCommit = getLogRecorder().getCreatedLogs(segId, pageIndexes);


    ReadRequestBuilder newBuilder = new ReadRequestBuilder();
    newBuilder.setRequestId(ioContextManager.getRequestId());
    newBuilder.setSegId(segId);
    newBuilder.setFailTimes(failTimes);
    newBuilder.setLogsToCommit(logsToCommit);
    newBuilder.setRequestUnits(manager.getRequestUnits());

    ioContextManager.setRequestBuilder(newBuilder);

    logger.info("re-read process, ori:{}, resendTimes:{}, needUpdateMembership:{}, at:{}",
        ioContextManager.getRequestId(), failTimes, needUpdateMembership,
        ioContextManager.getSegId());
    LoggerTracer.getInstance().mark(ioContextManager.getRequestId(), className,
        "re-read process, ori:{}, resendTimes:{}, needUpdateMembership:{}, at:{}",
        ioContextManager.getRequestId(), failTimes, needUpdateMembership,
        ioContextManager.getSegId());
    if (needUpdateMembership) {
      boolean update = updateMembershipForRead(manager, newBuilder);
      if (update) {
        return;
      }
    }
    SpaceSavingVolumeMetadata volumeMetadata = this.volumeInfoHolderManager
        .getVolumeInfoHolder(ioContextManager.getVolumeId()).getVolumeMetadata();
    SegmentMembership membership = volumeMetadata
        .getHighestMembership(ioContextManager.getLogicalSegmentIndex());
    asyncRead(manager, membership, newBuilder, false, volumeMetadata.getVolumeType());
  }



  public void resendReadAfterRetrieveMembership(ReadIoContextManager ioContextManager,
      ReadRequestBuilder builder) {
    SpaceSavingVolumeMetadata volumeMetadata = this.volumeInfoHolderManager
        .getVolumeInfoHolder(ioContextManager.getVolumeId()).getVolumeMetadata();
    SegmentMembership membership = volumeMetadata
        .getHighestMembership(ioContextManager.getLogicalSegmentIndex());
    logger
        .info("re-read after retrieve process,ori:{} read from:{}", ioContextManager.getRequestId(),
            membership);
    LoggerTracer.getInstance()
        .mark(ioContextManager.getRequestId(), className,
            "re-read after retrieve process,ori:{} read from:{}",
            ioContextManager.getRequestId(), membership);
    SegId segId = ioContextManager.getSegId();
    Set<Long> pageIndexes = ioContextManager.getRelatedPageIndexes();
    List<Long> logsToCommit = getLogRecorder().getCreatedLogs(segId, pageIndexes);

    ReadRequestBuilder newBuilder = new ReadRequestBuilder();
    newBuilder.setRequestId(ioContextManager.getRequestId());
    newBuilder.setSegId(segId);
    newBuilder.setFailTimes(ioContextManager.getFailTimes());
    newBuilder.setLogsToCommit(logsToCommit);
    newBuilder.setRequestUnits(ioContextManager.getRequestUnits());

    ioContextManager.setRequestBuilder(newBuilder);

    asyncRead(ioContextManager, membership, newBuilder, false, volumeMetadata.getVolumeType());
  }



  public void firstRead(IoUnitContextPacket contextCallback) throws StorageException {
    int segmentIndex = contextCallback.getLogicalSegIndex();
    Long volumeId = contextCallback.getVolumeId();
    SpaceSavingVolumeMetadata volumeMetadata = this.volumeInfoHolderManager
        .getVolumeInfoHolder(volumeId).getVolumeMetadata();
    SegId segId = volumeMetadata.getSegId(segmentIndex);
    if (segId == null) {
      logger
          .error("firstRead, can not get the segId, the segmentIndex{} in volume {}", segmentIndex,
              volumeId);
      throw new StorageException();
    }

    Set<Long> pageIndexes = new HashSet<>();


    Map<SegId, AtomicLong> recordLastIoTime = this.volumeInfoHolderManager
        .getVolumeInfoHolder(volumeId).getRecordLastIoTime();
    if (!recordLastIoTime.containsKey(segId)) {
      recordLastIoTime.putIfAbsent(segId, new AtomicLong(0));
    }
    AtomicLong lastIoTime = recordLastIoTime.get(segId);
    lastIoTime.set(System.currentTimeMillis());


    for (IoUnitContext ioContext : contextCallback.getIoContext()) {
      pageIndexes.add(ioContext.getPageIndexInSegment());
    }

    final List<Long> logsToCommit = getLogRecorder().getCreatedLogs(segId, pageIndexes);

    ReadIoContextManager ioContextManager = new ReadIoContextManager(volumeId, segId,
        contextCallback, this);

    ioContextManager.setRelatedPageIndexes(pageIndexes);
    ioContextManager.setExpiredTime(cfg.getReadIoTimeoutMs() + System.currentTimeMillis());
    ReadRequestBuilder builder = new ReadRequestBuilder();
    builder.setRequestId(ioContextManager.getRequestId());
    builder.setSegId(segId);
    builder.setFailTimes(ioContextManager.getFailTimes());
    builder.setLogsToCommit(logsToCommit);
    builder.setRequestUnits(ioContextManager.getRequestUnits());

    ioContextManager.setRequestBuilder(builder);

    logger.info("first read process, ori:{}, segId:{}", ioContextManager.getRequestId(), segId);
    if (CoordinatorConfigSingleton.getInstance().isTraceAllLogs()) {
      LoggerTracer.getInstance()
          .mark(ioContextManager.getRequestId(), className, "first read process, ori:{}, segId:{}",
              ioContextManager.getRequestId(), segId);
    }
    asyncRead(ioContextManager, volumeMetadata.getHighestMembership(segmentIndex), builder, true,
        volumeMetadata.getVolumeType());
  }

  private void asyncRead(ReadIoContextManager ioContextManager, SegmentMembership membership,
      ReadRequestBuilder builder, boolean firstRead, VolumeType volumeType) {

    builder.setPbMembership(membership);
    IoActionContext ioActionContext;

    try {
      ioActionContext = readFactory
          .generateIoMembers(membership, volumeType, ioContextManager.getSegId(),
              ioContextManager.getRequestId());
    } catch (Exception e) {
      logger.error("ori:{} can not get endpoints by:{} for read", ioContextManager.getRequestId(),
          membership, e);
      processErrorBeforeSending(ioContextManager);
      return;
    }

    ioContextManager.setIoActionContext(ioActionContext);
    if (ioActionContext.isResendDirectly()) {

      if (ioContextManager.streamIO()) {
        if (firstRead) {
          GetMembershipTask getMembershipTask = new GetMembershipTask(
              ioContextManager.getVolumeId(),
              ioContextManager.getSegId(), ioContextManager.getRequestId(), membership);
          getMembershipFromDatanodesEngine.putTask(getMembershipTask);
        }
        ioContextManager.doneDirectly();
        return;
      }

      logger.info(
          "ori:{} first:{} directly put read request back, from members:{} at:{} segmentForm:{}",
          ioContextManager.getRequestId(), firstRead, ioActionContext.getIoMembers(),
          ioContextManager.getSegId(), ioActionContext.getSegmentForm());
      LoggerTracer.getInstance().mark(ioContextManager.getRequestId(), className,
          "ori:{} first:{} directly put read request back, from members:{} at:{} segmentForm:{}",
          ioContextManager.getRequestId(), firstRead, ioActionContext.getIoMembers(),
          ioContextManager.getSegId(), ioActionContext.getSegmentForm());
      processErrorBeforeSending(ioContextManager);
      return;
    }

    logger
        .info("ori:{} first:{}  begin get read request back, from members:{} at:{} segmentForm:{}",
            ioContextManager.getRequestId(), firstRead, ioActionContext.getIoMembers(),
            ioContextManager.getSegId(),
            ioActionContext.getSegmentForm());

    final AtomicInteger counter = new AtomicInteger(ioActionContext.getIoMembers().size());
    ioContextManager.initRequestCount(counter.get());

    PbReadRequest request;
    boolean hasSetIoFail = false;

    Set<IoMember> fetchReader = ioActionContext.getFetchReader();
    if (fetchReader.size() != 1) {
      logger.error("fetch reader:{}, not exist ", ioActionContext);
      processErrorBeforeSending(ioContextManager);
    } else {
      IoMember fetchIoMember = fetchReader.iterator().next();
      builder.setReadCause(ReadCause.FETCH);
      request = builder.getRequest();

      logger.info("ori:{} try to real read from: {} at:{}, read unit count:{}",
          ioContextManager.getRequestId(), fetchIoMember, ioContextManager.getSegId(),
          ioContextManager.getRequestUnits().size());
      if (CoordinatorConfigSingleton.getInstance().isTraceAllLogs()) {
        LoggerTracer.getInstance().mark(ioContextManager.getRequestId(), className,
            "ori:{} try to real read from: {} at:{}, read unit count:{}",
            ioContextManager.getRequestId(), fetchIoMember, ioContextManager.getSegId(),
            ioContextManager.getRequestUnits().size());
      }
      ReadMethodCallback callback = new ReadMethodCallback(ioContextManager, counter, this,
          fetchIoMember);
      if (needSetIoFailByInstance(ioActionContext, fetchIoMember, hasSetIoFail)) {
        hasSetIoFail = true;
        callback.fail(new NetworkUnhealthyBySdException());
      } else {
        clientFactory.generate(fetchIoMember.getEndPoint()).read(request, callback);
      }
    }

    if (!ioActionContext.getCheckReaders().isEmpty()) {
      builder.setReadCause(Broadcastlog.ReadCause.CHECK);
      request = builder.getRequest();
      Validate.isTrue(Broadcastlog.ReadCause.CHECK == request.getReadCause());
      for (IoMember ioMember : ioActionContext.getCheckReaders()) {
        logger.info("ori:{} try to check read from: {} at:{}", ioContextManager.getRequestId(),
            ioMember,
            ioContextManager.getSegId());
        if (CoordinatorConfigSingleton.getInstance().isTraceAllLogs()) {
          LoggerTracer.getInstance()
              .mark(ioContextManager.getRequestId(), className,
                  "ori:{} try to check read from: {} at:{}",
                  ioContextManager.getRequestId(), ioMember, ioContextManager.getSegId());
        }
        ReadMethodCallback callback = new ReadMethodCallback(ioContextManager, counter, this,
            ioMember);
        if (needSetIoFailByInstance(ioActionContext, ioMember, hasSetIoFail)) {
          hasSetIoFail = true;
          callback.fail(new NetworkUnhealthyBySdException());
        } else {
          clientFactory.generate(ioMember.getEndPoint()).read(request, callback);
        }
      }
    }
  }

  @Override
  public void asyncRead(long pos, ByteBuffer buffer, AsyncIoCallBack asyncIoCallBack)
      throws StorageException {
    Validate.notNull(buffer);
    Validate.notNull(asyncIoCallBack);
    asyncIoCallBack.setReadDstBuffer(Unpooled.wrappedBuffer(buffer));

    IoRequest ioRequest = new IoRequestImpl(pos, buffer.remaining(), IoRequestType.Read, null,
        asyncIoCallBack, 1);
    Long requestUuid = RequestIdBuilder.get();
    asyncProcessIoRequest(requestUuid, ioRequest, true);
  }

  @Override
  public void asyncRead(long pos, byte[] dstBuf, int off, int len, AsyncIoCallBack asyncIoCallBack)
      throws StorageException {
    Validate.isTrue(dstBuf.length - off >= len);
    Validate.notNull(asyncIoCallBack);
    asyncIoCallBack.setReadDstBuffer(Unpooled.wrappedBuffer(dstBuf, off, len));

    IoRequest ioRequest = new IoRequestImpl(pos, len, IoRequestType.Read, null, asyncIoCallBack, 1);
    Long requestUuid = RequestIdBuilder.get();
    asyncProcessIoRequest(requestUuid, ioRequest, true);
  }

  private void asyncReadForCloneSourceVolume(ReadIoContextManager ioContextManager,
      SegmentMembership sourceVolumeMembership,
      ReadRequestBuilder builder, boolean firstRead, VolumeType volumeType) {

    builder.setPbMembership(sourceVolumeMembership);
    IoActionContext ioActionContext;

    try {
      ioActionContext = readFactory
          .generateIoMembers(sourceVolumeMembership, volumeType, ioContextManager.getSegId(),
              ioContextManager.getRequestId());
    } catch (Exception e) {
      logger.error("ori:{} can not get endpoints by:{} for read", ioContextManager.getRequestId(),
          sourceVolumeMembership, e);
      processErrorBeforeSending(ioContextManager);
      return;
    }

    ioContextManager.setIoActionContext(ioActionContext);
    if (ioActionContext.isResendDirectly()) {

      if (ioContextManager.streamIO()) {
        if (firstRead) {
          GetMembershipTask getMembershipTask = new GetMembershipTask(
              ioContextManager.getVolumeId(),
              ioContextManager.getSegId(), ioContextManager.getRequestId(), sourceVolumeMembership);
          getMembershipFromDatanodesEngine.putTask(getMembershipTask);
        }
        ioContextManager.doneDirectly();
        return;
      }

      logger.info(
          "ori:{} first:{} directly put read request back, from members:{} at:{} segmentForm:{}",
          ioContextManager.getRequestId(), firstRead, ioActionContext.getIoMembers(),
          ioContextManager.getSegId(), ioActionContext.getSegmentForm());
      LoggerTracer.getInstance().mark(ioContextManager.getRequestId(), className,
          "ori:{} first:{} directly put read request back, from members:{} at:{} segmentForm:{}",
          ioContextManager.getRequestId(), firstRead, ioActionContext.getIoMembers(),
          ioContextManager.getSegId(), ioActionContext.getSegmentForm());
      processErrorBeforeSending(ioContextManager);
      return;
    }

    logger
        .info("ori:{} first:{}  begin get read request back, from members:{} at:{} segmentForm:{}",
            ioContextManager.getRequestId(), firstRead, ioActionContext.getIoMembers(),
            ioContextManager.getSegId(),
            ioActionContext.getSegmentForm());

    final AtomicInteger counter = new AtomicInteger(ioActionContext.getIoMembers().size());
    ioContextManager.initRequestCount(counter.get());

    PbReadRequest request;

    boolean hasSetIoFail = false;

    Set<IoMember> fetchReader = ioActionContext.getFetchReader();
    if (fetchReader.size() != 1) {
      logger.error("fetch reader:{}, not exist ", ioActionContext);
      processErrorBeforeSending(ioContextManager);
    } else {
      IoMember ioMember = fetchReader.iterator().next();
      builder.setReadCause(ReadCause.FETCH);

      request = builder.getRequest();

      logger.info("ori:{} try to real  read from: {} at:{}, read unit count:{}",
          ioContextManager.getRequestId(), ioMember, ioContextManager.getSegId(),
          ioContextManager.getRequestUnits().size());
      if (CoordinatorConfigSingleton.getInstance().isTraceAllLogs()) {
        LoggerTracer.getInstance().mark(ioContextManager.getRequestId(), className,
            "ori:{} try to real  read from: {} at:{}, read unit count:{}",
            ioContextManager.getRequestId(), ioMember, ioContextManager.getSegId(),
            ioContextManager.getRequestUnits().size());
      }
      ReadMethodCallback callback = new ReadMethodCallback(ioContextManager, counter, this,
          ioMember);
      if (needSetIoFailByInstance(ioActionContext, ioMember, hasSetIoFail)) {
        hasSetIoFail = true;
        callback.fail(new NetworkUnhealthyBySdException());
      } else {
        clientFactory.generate(ioMember.getEndPoint()).read(request, callback);
      }
    }

    // check reader might be empty
    if (!ioActionContext.getCheckReaders().isEmpty()) {
      builder.setReadCause(Broadcastlog.ReadCause.CHECK);
      request = builder.getRequest();
      Validate.isTrue(Broadcastlog.ReadCause.CHECK == request.getReadCause());
      for (IoMember ioMember : ioActionContext.getCheckReaders()) {
        logger.info("ori:{} try to check read from: {} at:{}", ioContextManager.getRequestId(),
            ioMember,
            ioContextManager.getSegId());
        if (CoordinatorConfigSingleton.getInstance().isTraceAllLogs()) {
          LoggerTracer.getInstance()
              .mark(ioContextManager.getRequestId(), className,
                  "ori:{} try to check read from: {} at:{}",
                  ioContextManager.getRequestId(), ioMember, ioContextManager.getSegId());
        }
        ReadMethodCallback callback = new ReadMethodCallback(ioContextManager, counter, this,
            ioMember);
        if (needSetIoFailByInstance(ioActionContext, ioMember, hasSetIoFail)) {
          hasSetIoFail = true;
          callback.fail(new NetworkUnhealthyBySdException());
        } else {
          clientFactory.generate(ioMember.getEndPoint()).read(request, callback);
        }
      }
    }
  }

  @Override
  public void close() throws StorageException {
    try {
      logger.warn("shutting down coordinator, exit commit log thread");

      int tryTime = 3;
      while (tryTime > 0) {
        tryTime--;
        if (getLogRecorder().hasCommitAllLog()) {
          break;
        } else {
          try {
            Thread.sleep(1000);
          } catch (Exception e) {
            logger.error("caught an exception", e);
          }
        }
      }
      if (tryTime <= 0) {
        logger.warn("logPointerRecorder still has uncommitted logs:{} ",
            getLogRecorder().printAllLogs());
      }

      checkNetHealThyService.shutdownNow();

      getCommitLogWorker().stop();


      logger.warn("exit get membership from datanodes engine");
      getMembershipFromDatanodesEngine.stop();


      logger.warn("exit get membership from info center engine");
      getMembershipFromInfoCenterEngine.stop();


      logger.warn("exit resend delay queue");
      delayManager.stop();

      logger.warn("exit netty4 response worker");
      try {
        clientFactory.close();
      } catch (Exception e) {
        logger.error("caught an exception when exit netty4 response worker", e);
      }

      logger.warn("exit logger tracer");
      LoggerTracer.getInstance().stop();

      logger.warn("now going to exit coordinator");
    } catch (Exception e) {
      logger.error("something wrong when closing the coordinator", e);
    }
  }

  private boolean updateMembershipForWrite(WriteIoContextManager writeIoContextManager,
      WriteRequestBuilder writeRequestBuilder) {
    return updateMembershipForResendIo(writeIoContextManager, writeRequestBuilder, null, null);
  }

  private boolean updateMembershipForRead(ReadIoContextManager readIoContextManager,
      ReadRequestBuilder readRequestBuilder) {
    return updateMembershipForResendIo(null, null, readIoContextManager, readRequestBuilder);
  }

  private boolean updateMembershipForResendIo(WriteIoContextManager writeIoContextManager,
      WriteRequestBuilder writeRequestBuilder, ReadIoContextManager readIoContextManager,
      ReadRequestBuilder readRequestBuilder) {
    SegId segId;
    Long rootVolumeId;
    if (writeIoContextManager != null) {
      segId = writeIoContextManager.getSegId();
      rootVolumeId = writeIoContextManager.getVolumeId();
    } else {
      Validate.notNull(readIoContextManager);
      segId = readIoContextManager.getSegId();
      rootVolumeId = readIoContextManager.getVolumeId();
    }
    Map<SegId, Integer> mapSegIdToIndex = this.volumeInfoHolderManager
        .getVolumeInfoHolder(rootVolumeId).getMapSegIdToIndex();
    SpaceSavingVolumeMetadata volumeMetadata = this.volumeInfoHolderManager
        .getVolumeInfoHolder(rootVolumeId).getVolumeMetadata();
    int segmentIndex = mapSegIdToIndex.get(segId);
    SegmentMembership currentMembership = volumeMetadata.getHighestMembership(segmentIndex);
    List<EndPoint> endpoints = null;

    try {
      endpoints = RequestResponseHelper
          .buildEndPoints(instanceStore, currentMembership, PortType.IO, true, null);
    } catch (Exception e) {
      logger.error("failed to get endpoints by:{}", currentMembership, e);
    }

    if (endpoints != null && !endpoints.isEmpty()) {
      Broadcastlog.PbGetMembershipRequest.Builder builder = Broadcastlog.PbGetMembershipRequest
          .newBuilder();
      builder.setRequestId(RequestIdBuilder.get());
      builder.setVolumeId(segId.getVolumeId().getId());
      builder.setSegIndex(segId.getIndex());
      Broadcastlog.PbGetMembershipRequest request = builder.build();
      GetMembershipCallbackForIO getMembershipCallbackForIo;

      if (writeIoContextManager != null) {
        Validate.notNull(writeRequestBuilder);
        getMembershipCallbackForIo = new GetMembershipCallbackForIO(this, endpoints.size(),
            writeIoContextManager, writeRequestBuilder);
      } else {
        Validate.notNull(readIoContextManager);
        Validate.notNull(readRequestBuilder);
        getMembershipCallbackForIo = new GetMembershipCallbackForIO(this, endpoints.size(),
            readIoContextManager, readRequestBuilder);
      }

      try {
        for (EndPoint endPoint : endpoints) {
          clientFactory.generate(endPoint).getMembership(request, getMembershipCallbackForIo);
        }
        return true;
      } catch (Exception e) {
        logger.error("can not send get membership request to:{}", endpoints, e);
      }
    }

    return false;
  }



  public void updateMembershipFromDatanode(Long volumeId, SegId segId, SegmentMembership membership,
      TriggerByCheckCallback triggerByCheckCallback) {
    Long requestId = 0L;
    if (triggerByCheckCallback != null) {
      requestId = triggerByCheckCallback.getOriRequestId();
    }
    updateMembershipFromDatanode(volumeId, segId, membership, triggerByCheckCallback, false,
        requestId);
  }


  private void updateMembershipFromDatanode(Long volumeId, SegId segId,
      SegmentMembership membership,
      TriggerByCheckCallback triggerByCheckCallback, boolean forceUpdate, /*just for logger*/
      Long requestId) {

    if (triggerByCheckCallback != null) {
      triggerByCheckCallback.noNeedUpdateMembership();
    }

    Map<SegId, Integer> mapSegIdToIndex;
    SpaceSavingVolumeMetadata volumeMetadata;

    try {
      mapSegIdToIndex = this.volumeInfoHolderManager.getVolumeInfoHolder(volumeId)
          .getMapSegIdToIndex();
      volumeMetadata = this.volumeInfoHolderManager.getVolumeInfoHolder(volumeId)
          .getVolumeMetadata();
    } catch (Exception e) {
      logger.error("to do: deal with the nullpointer or some exception ", e);
      return;
    }

    int segmentIndex = mapSegIdToIndex.get(segId);
    synchronized (volumeMetadata) {
      SegmentMembership currentMembership = volumeMetadata.getHighestMembership(segmentIndex);
      boolean shouldUpdate = false;
      if (forceUpdate) {
        if (currentMembership.compareTo(membership) != 0) {
          logger.warn(
              "ori:{} force update membership:{} at segmentIndex:{} {}, current membership:{}",
              requestId, membership, segmentIndex, segId, currentMembership);
          shouldUpdate = true;
        }

      } else if (currentMembership.compareTo(membership) < 0) {
        shouldUpdate = true;

      } else if (currentMembership.compareTo(membership) == 0) {

        logger.info(
            "ori:{} at:{} and logic segment index:{} want to update membership is equal to "
                + "current membership, still need to merge status of members",
            segId, segmentIndex, requestId);
        currentMembership.mergeMemberStatus(requestId, membership);

      } else {
        logger.warn(
            "ori:{} It's not a newer membership:{}, at segmentIndex:{}, segId:{}, current "
                + "membership:{}",
            requestId, membership, segmentIndex, segId, currentMembership);
      }

      if (shouldUpdate) {
        logger.warn(
            "ori:{} segmentIndex:{}, segId:{} try update membership:{}, current membership:{}",
            requestId, segmentIndex, segId, membership, currentMembership);

        if (membership.getTempPrimary() != null) {
          logger.warn(
              "ori:{} segmentIndex:{}, segId:{}, membership:{} got temp primary, should mark "
                  + "primary:{} down directly",
              requestId, segmentIndex, segId, membership, membership.getPrimary());
          markPrimaryDown(membership, membership.getPrimary());
        }
        volumeMetadata.updateMembership(segmentIndex, membership);
      }
    }
  }

  public void forceUpdateMembershipFromDatanode(Long volumeId, SegId segId,
      SegmentMembership membership,
      Long requestId) {
    updateMembershipFromDatanode(volumeId, segId, membership, null, true, requestId);
  }



  public boolean checkPrimaryReachable(SegmentMembership membershipWhenIoCome, InstanceId primary,
      boolean deletedCase, TriggerByCheckCallback callback,
      Exception checkPrimaryCause,
      SegId segId) {

    if (membershipWhenIoCome.getMemberIoStatus(primary).isDown()) {
      logger.info("ori:{} primary:{} at:{} is down, no more processing", callback.getOriRequestId(),
          primary,
          segId);
      callback.triggeredByCheckCallback();
      return false;
    }

    List<SimpleInstance> checkInstances = new ArrayList<>();
    for (InstanceId instanceId : membershipWhenIoCome.getAliveSecondaries()) {
      try {
        Instance instance = instanceStore.get(instanceId);
        EndPoint checkEndPoint = instance.getEndPointByServiceName(PortType.IO);
        SimpleInstance simpleInstance = new SimpleInstance(instanceId, checkEndPoint);
        checkInstances.add(simpleInstance);
      } catch (Exception e) {
        logger.error(
            "ori:{} caught an exception when check primary reachable, get {} endpoint failed",
            callback.getOriRequestId(), instanceId, e);
      }
    }

    SimpleInstance primaryInstance = null;
    try {
      Instance instance = instanceStore.get(primary);
      EndPoint primaryEndPoint = instance.getEndPointByServiceName(PortType.IO);
      primaryInstance = new SimpleInstance(primary, primaryEndPoint);
    } catch (Exception e) {
      logger.error("ori:{} caught an exception when check primary reachable:{}",
          callback.getOriRequestId(),
          primary, e);
    }

    if (checkInstances.isEmpty() || primaryInstance == null) {
      logger.warn("ori:{} can not check primary:{} any more", callback.getOriRequestId());
      callback.triggeredByCheckCallback();
      return false;
    }

    logger
        .info("ori:{} try to check primary:{} status at:{} in membership:{} through secondaries:{}",
            callback.getOriRequestId(), primary, segId, membershipWhenIoCome, checkInstances);
    LoggerTracer.getInstance().mark(callback.getOriRequestId(), className,
        "ori:{} try to check primary:{} status at:{} in membership:{} through secondaries:{}",
        callback.getOriRequestId(), primary, segId, membershipWhenIoCome, checkInstances);

    CheckPrimaryReachableCallbackCollector checkPrimaryCollector =
        new CheckPrimaryReachableCallbackCollector(
            this,
            checkInstances, primaryInstance, segId, callback, membershipWhenIoCome,
            checkPrimaryCause);
    Broadcastlog.PbCheckRequest.Builder checkRequestBuilder = Broadcastlog.PbCheckRequest
        .newBuilder();
    checkRequestBuilder.setRequestId(RequestIdBuilder.get());
    checkRequestBuilder.setCheckInstanceId(primary.getId());
    checkRequestBuilder.setRequestOption(RequestOption.CHECK_PRIMARY);
    checkRequestBuilder.setRequestPbMembership(
        PbRequestResponseHelper.buildPbMembershipFrom(membershipWhenIoCome));
    checkRequestBuilder.setVolumeId(segId.getVolumeId().getId());
    checkRequestBuilder.setSegIndex(segId.getIndex());
    checkRequestBuilder.setStreamIo(callback.streamIO());
    if (deletedCase) {
      checkRequestBuilder.setMemberHasGone(true);
    }

    Broadcastlog.PbCheckRequest checkPrimaryRequest = checkRequestBuilder.build();
    for (SimpleInstance simpleInstance : checkInstances) {
      CheckRequestCallback checkPrimaryReachableCallback = new CheckRequestCallback(
          checkPrimaryCollector,
          simpleInstance.getInstanceId());
      clientFactory.generate(simpleInstance.getEndPoint())
          .check(checkPrimaryRequest, checkPrimaryReachableCallback);
    }

    return true;
  }



  public boolean checkTempPrimaryReachable(SegmentMembership membershipWhenIoCome,
      InstanceId tempPrimary,
      boolean deletedCase, TriggerByCheckCallback callback,
      Exception checkPrimaryCause,
      SegId segId) {


    if (membershipWhenIoCome.getMemberIoStatus(tempPrimary).isDown()) {
      logger.info("ori:{} tp primary:{} at:{} is down, no more processing",
          callback.getOriRequestId(),
          tempPrimary, segId);
      callback.triggeredByCheckCallback();
      return false;
    }

    List<SimpleInstance> checkInstances = new ArrayList<>();

    Set<InstanceId> aliveSecondaries = membershipWhenIoCome.getAliveSecondaries();
    aliveSecondaries.remove(tempPrimary);
    for (InstanceId instanceId : aliveSecondaries) {
      try {
        Instance instance = instanceStore.get(instanceId);
        EndPoint checkEndPoint = instance.getEndPointByServiceName(PortType.IO);
        SimpleInstance simpleInstance = new SimpleInstance(instanceId, checkEndPoint);
        checkInstances.add(simpleInstance);
      } catch (Exception e) {
        logger.error(
            "ori:{} tp caught an exception when check primary reachable, get {} endpoint failed",
            callback.getOriRequestId(), instanceId, e);
      }
    }

    SimpleInstance primaryInstance = null;
    try {
      Instance instance = instanceStore.get(tempPrimary);
      EndPoint primaryEndPoint = instance.getEndPointByServiceName(PortType.IO);
      primaryInstance = new SimpleInstance(tempPrimary, primaryEndPoint);
    } catch (Exception e) {
      logger.error("ori:{} tp caught an exception when check primary reachable:{}",
          callback.getOriRequestId(),
          tempPrimary, e);
    }

    if (checkInstances.isEmpty() || primaryInstance == null) {
      logger.warn("ori:{} tp can not check primary:{} any more", callback.getOriRequestId());
      callback.triggeredByCheckCallback();
      return false;
    }

    logger.info(
        "ori:{} tp try to check primary:{} status at:{} in membership:{} through secondaries:{}",
        callback.getOriRequestId(), tempPrimary, segId, membershipWhenIoCome, checkInstances);
    LoggerTracer.getInstance().mark(callback.getOriRequestId(), className,
        "ori:{} tp try to check primary:{} status at:{} in membership:{} through secondaries:{}",
        callback.getOriRequestId(), tempPrimary, segId, membershipWhenIoCome, checkInstances);

    CheckTempPrimaryReachableCallbackCollector checkPrimaryCollector =
        new CheckTempPrimaryReachableCallbackCollector(
            this, checkInstances, primaryInstance, segId, callback, membershipWhenIoCome,
            checkPrimaryCause);
    Broadcastlog.PbCheckRequest.Builder checkRequestBuilder = Broadcastlog.PbCheckRequest
        .newBuilder();
    checkRequestBuilder.setRequestId(RequestIdBuilder.get());
    checkRequestBuilder.setCheckInstanceId(tempPrimary.getId());
    checkRequestBuilder.setRequestOption(RequestOption.CHECK_TEMP_PRIMARY);
    checkRequestBuilder.setRequestPbMembership(
        PbRequestResponseHelper.buildPbMembershipFrom(membershipWhenIoCome));
    checkRequestBuilder.setVolumeId(segId.getVolumeId().getId());
    checkRequestBuilder.setSegIndex(segId.getIndex());
    checkRequestBuilder.setStreamIo(callback.streamIO());
    if (deletedCase) {
      checkRequestBuilder.setMemberHasGone(true);
    }

    Broadcastlog.PbCheckRequest checkPrimaryRequest = checkRequestBuilder.build();
    for (SimpleInstance simpleInstance : checkInstances) {
      CheckRequestCallback checkPrimaryReachableCallback = new CheckRequestCallback(
          checkPrimaryCollector,
          simpleInstance.getInstanceId());
      clientFactory.generate(simpleInstance.getEndPoint())
          .check(checkPrimaryRequest, checkPrimaryReachableCallback);
    }

    return true;
  }



  public boolean checkSecondaryReachable(SegmentMembership membershipWhenIoCome,
      InstanceId secondary,
      boolean deletedCase, TriggerByCheckCallback callback, SegId segId) {


    if (membershipWhenIoCome.getMemberIoStatus(secondary).isDown()) {
      logger.info("ori:{} secondary:{} at:{} is down status:{}, no more processing",
          callback.getOriRequestId(),
          secondary, membershipWhenIoCome.getMemberIoStatus(secondary), segId);
      callback.triggeredByCheckCallback();
      return false;
    }

    EndPoint primaryEndPoint;
    Instance primary = null;
    SimpleInstance primaryInstance;
    List<SimpleInstance> checkInstances = new ArrayList<>();

    try {
      primary = instanceStore.get(membershipWhenIoCome.getPrimary());
      primaryEndPoint = primary.getEndPointByServiceName(PortType.IO);
      primaryInstance = new SimpleInstance(membershipWhenIoCome.getPrimary(), primaryEndPoint);
      checkInstances.add(primaryInstance);
    } catch (Exception e) {
      logger.error("ori:{} can not get primary:{} client to check:{} status at {}",
          callback.getOriRequestId(),
          primary, secondary, segId, e);

      callback.triggeredByCheckCallback();
      return true;
    }

    SimpleInstance secondaryInstance = null;

    try {
      Instance instance = instanceStore.get(secondary);
      EndPoint endPoint = instance.getEndPointByServiceName(PortType.IO);
      secondaryInstance = new SimpleInstance(secondary, endPoint);
    } catch (Exception e) {
      logger.error("ori:{} can not get secondary:{} endpoint to check status at {}",
          callback.getOriRequestId(),
          secondary, segId, e);
    }

    if (checkInstances.isEmpty() || secondaryInstance == null) {
      logger.warn("ori:{} can not check secondary:{} any more", callback.getOriRequestId(),
          secondary);
      callback.triggeredByCheckCallback();
      return false;
    }

    logger.info("ori:{} try to check secondary:{} status at:{} in membership:{} through primary:{}",
        callback.getOriRequestId(), secondary, segId, membershipWhenIoCome, primaryEndPoint);
    LoggerTracer.getInstance().mark(callback.getOriRequestId(), className,
        "ori:{} try to check secondary:{} status at:{} in membership:{} through primary:{}",
        callback.getOriRequestId(), secondary, segId, membershipWhenIoCome, primaryEndPoint);



    final CheckSecondaryReachableCallbackCollector checkSecondaryCollector =
        new CheckSecondaryReachableCallbackCollector(
            this, checkInstances, secondaryInstance, segId, callback, membershipWhenIoCome);
    Broadcastlog.PbCheckRequest.Builder checkRequestBuilder = Broadcastlog.PbCheckRequest
        .newBuilder();
    checkRequestBuilder.setRequestId(RequestIdBuilder.get());
    checkRequestBuilder.setCheckInstanceId(secondary.getId());
    checkRequestBuilder.setRequestOption(RequestOption.CHECK_SECONDARY);
    checkRequestBuilder.setRequestPbMembership(
        PbRequestResponseHelper.buildPbMembershipFrom(membershipWhenIoCome));
    checkRequestBuilder.setVolumeId(segId.getVolumeId().getId());
    checkRequestBuilder.setSegIndex(segId.getIndex());
    if (deletedCase) {
      checkRequestBuilder.setMemberHasGone(true);
    }

    Broadcastlog.PbCheckRequest checkSecondaryRequest = checkRequestBuilder.build();

    CheckRequestCallback checkSecondaryReachableRequestCallback = new CheckRequestCallback(
        checkSecondaryCollector,
        membershipWhenIoCome.getPrimary());
    clientFactory.generate(primaryEndPoint)
        .check(checkSecondaryRequest, checkSecondaryReachableRequestCallback);

    return true;
  }



  public boolean tpCheckSecondaryReachable(SegmentMembership membershipWhenIoCome,
      InstanceId secondary,
      boolean deletedCase, TriggerByCheckCallback callback, SegId segId) {


    if (membershipWhenIoCome.getMemberIoStatus(secondary).isDown()) {
      logger.warn("ori:{} secondary:{} at:{} is down status:{}, no more processing",
          callback.getOriRequestId(),
          secondary, membershipWhenIoCome.getMemberIoStatus(secondary), segId);
      callback.triggeredByCheckCallback();
      return false;
    }


    EndPoint tempPrimaryEndPoint;
    Instance tempPrimary = null;
    SimpleInstance tempPrimaryInstance;
    List<SimpleInstance> checkInstances = new ArrayList<>();



    try {
      tempPrimary = instanceStore.get(membershipWhenIoCome.getTempPrimary());
      tempPrimaryEndPoint = tempPrimary.getEndPointByServiceName(PortType.IO);
      tempPrimaryInstance = new SimpleInstance(membershipWhenIoCome.getTempPrimary(),
          tempPrimaryEndPoint);
      checkInstances.add(tempPrimaryInstance);
    } catch (Exception e) {
      logger.error(
          "ori:{} tpCheckSecondaryReachable can not get tempPrimary:{} client to check:{} status "
              + "at {}",
          callback.getOriRequestId(), tempPrimary, secondary, segId, e);

      callback.triggeredByCheckCallback();
      return true;
    }

    SimpleInstance secondaryInstance = null;
    try {
      Instance instance = instanceStore.get(secondary);
      EndPoint endPoint = instance.getEndPointByServiceName(PortType.IO);
      secondaryInstance = new SimpleInstance(secondary, endPoint);
    } catch (Exception e) {
      logger.error(
          "ori:{} tpCheckSecondaryReachable can not get secondary:{} endpoint to check status at "
              + "{}",
          callback.getOriRequestId(), secondary, segId, e);
    }

    if (checkInstances.isEmpty() || secondaryInstance == null) {
      logger.warn("ori:{} tpCheckSecondaryReachable can not check secondary:{} any more",
          callback.getOriRequestId(), secondary);
      callback.triggeredByCheckCallback();
      return false;
    }

    logger.warn(
        "ori:{} tpCheckSecondaryReachable try to check secondary:{} status at:{} in membership:{}"
            + " through temp primary:{}",
        callback.getOriRequestId(), secondary, segId, membershipWhenIoCome, tempPrimaryEndPoint);
    LoggerTracer.getInstance().mark(callback.getOriRequestId(), className,
        "ori:{} tpCheckSecondaryReachable try to check secondary:{} status at:{} in membership:{}"
            + " through temp primary:{}",
        callback.getOriRequestId(), secondary, segId, membershipWhenIoCome, tempPrimaryEndPoint);


    final TempPrimaryCheckSecondaryReachableCallbackCollector checkSecondaryCollector =
        new TempPrimaryCheckSecondaryReachableCallbackCollector(
            this, checkInstances, secondaryInstance, segId, callback, membershipWhenIoCome);
    Broadcastlog.PbCheckRequest.Builder checkRequestBuilder = Broadcastlog.PbCheckRequest
        .newBuilder();
    checkRequestBuilder.setRequestId(RequestIdBuilder.get());
    checkRequestBuilder.setCheckInstanceId(secondary.getId());
    checkRequestBuilder.setRequestOption(RequestOption.TP_CHECK_SECONDARY);
    checkRequestBuilder.setRequestPbMembership(
        PbRequestResponseHelper.buildPbMembershipFrom(membershipWhenIoCome));
    checkRequestBuilder.setVolumeId(segId.getVolumeId().getId());
    checkRequestBuilder.setSegIndex(segId.getIndex());
    if (deletedCase) {
      checkRequestBuilder.setMemberHasGone(true);
    }

    Broadcastlog.PbCheckRequest checkSecondaryRequest = checkRequestBuilder.build();

    CheckRequestCallback tpCheckSecondaryReachableRequestCallback = new CheckRequestCallback(
        checkSecondaryCollector, membershipWhenIoCome.getTempPrimary());
    clientFactory.generate(tempPrimaryEndPoint)
        .check(checkSecondaryRequest, tpCheckSecondaryReachableRequestCallback);

    return true;
  }



  public void makePrimaryDeleteSeconary(SegId segId, SegmentMembership membershipWhenIoCome,
      InstanceId secondaryToBeDeleted) {
    EndPoint primaryEndPoint = instanceStore.get(membershipWhenIoCome.getPrimary()).getEndPoint();
    try {
      DataNodeService.Iface dataNodeClient = dataNodeSyncClientFactory
          .generateSyncClient(primaryEndPoint);
      if (dataNodeClient != null) {
        dataNodeClient.departFromMembership(RequestResponseHelper
            .buildDepartFromMembershipRequest(segId, membershipWhenIoCome,
                secondaryToBeDeleted.getId()));
      }
    } catch (Exception e) {
      logger.error("can not delete secondary segment");
      return;
    }
  }

  public InstanceId getDriverContainerId() {
    return driverContainerId;
  }

  public void setDriverContainerId(InstanceId driverContainerId) {
    logger.warn("set driver container id:{} to coordinator", driverContainerId);
    this.driverContainerId = driverContainerId;
  }



  public SpaceSavingVolumeMetadata getVolumeMetaData(Long volumeId) {
    SpaceSavingVolumeMetadata volumeMetadata = this.volumeInfoHolderManager
        .getVolumeInfoHolder(volumeId).getVolumeMetadata();
    Validate.notNull(volumeMetadata);
    return volumeMetadata;
  }

  public VolumeInfoRetriever getVolumeInfoRetriever() {
    return volumeInfoRetriever;
  }



  public void updateVolumeMetadataIfVolumeExtended(Long volumeId) {
    try {
      this.volumeInfoHolderManager.getVolumeInfoHolder(volumeId)
          .updateVolumeMetadataIfVolumeExtended();
    } catch (Exception e) {
      logger.error("caught an exception", e);
    }
  }

  @Override
  public long size() {
    SpaceSavingVolumeMetadata volumeMetadata = this.volumeInfoHolderManager
        .getVolumeInfoHolder(requestVolumeId.get()).getVolumeMetadata();
    return volumeMetadata.getVolumeSize();
  }

  protected void submit(IoUnitContextPacket context) throws StorageException {
    logger.debug("submit context notifyAllListeners: {}", context);

    IoRequestType requestType = context.getRequestType();
    try {
      if (requestType.isRead()) {
        firstRead(context);
      } else if (requestType.isWrite() || requestType.isDiscard()) {
        firstWrite(context);
      } else {
        logger.error("io request type:{}", requestType);
        throw new RuntimeException("not support the type: " + requestType);
      }
    } catch (Exception e) {
      logger.error("error occurs while {}", requestType, e);
      throw new StorageException("can not deal with context:" + context, e);
    }
  }

  public InstanceStore getInstanceStore() {
    return instanceStore;
  }



  public SegmentMembership getSegmentMembership(Long volumeId, SegId segId) {
    SpaceSavingVolumeMetadata volumeMetadata = this.volumeInfoHolderManager
        .getVolumeInfoHolder(volumeId).getVolumeMetadata();
    Map<SegId, Integer> mapSegIdToIndex = this.volumeInfoHolderManager.getVolumeInfoHolder(volumeId)
        .getMapSegIdToIndex();
    int segmentIndex = mapSegIdToIndex.get(segId);
    SegmentMembership membership = volumeMetadata.getHighestMembership(segmentIndex);
    Validate.notNull(membership);
    return membership;
  }



  public int getSegmentIndex(Long volumeId, SegId segId) {
    Map<SegId, Integer> mapSegIdToIndex = this.volumeInfoHolderManager.getVolumeInfoHolder(volumeId)
        .getMapSegIdToIndex();
    return mapSegIdToIndex.get(segId);
  }



  public VolumeType getVolumeType(Long volumeId) {
    SpaceSavingVolumeMetadata volumeMetadata = this.volumeInfoHolderManager
        .getVolumeInfoHolder(volumeId).getVolumeMetadata();
    return volumeMetadata.getVolumeType();
  }



  public boolean isLinkedCloneVolume(Long volumeId) {
    SpaceSavingVolumeMetadata volumeMetadata = this.volumeInfoHolderManager
        .getVolumeInfoHolder(volumeId).getVolumeMetadata();
    return volumeMetadata.getCloneType() == CloneType.LINKED_CLONE;
  }



  @Deprecated
  public DataNodeService.AsyncIface getDatanodeAsyncClient(InstanceId instanceId) {
    EndPoint endPoint = null;
    DataNodeService.AsyncIface client = null;
    try {
      endPoint = instanceStore.get(instanceId).getEndPoint();
      client = dataNodeAsyncClientFactory
          .generateAsyncClient(endPoint, 300, cfg.getThriftRequestTimeoutMs());
    } catch (GenericThriftClientFactoryException e) {
      logger.error("failed to get datanode async client:{}", endPoint);
    }
    return client;
  }



  public AsyncDataNode.AsyncIface getDatanodeNettyClient(InstanceId instanceId) {
    EndPoint endPoint = null;
    AsyncDataNode.AsyncIface client = null;
    try {
      Instance datanode = instanceStore.get(instanceId);
      Validate.notNull(datanode);
      endPoint = datanode.getEndPointByServiceName(PortType.IO);
      Validate.notNull(endPoint);
      client = clientFactory.generate(endPoint);
    } catch (Exception e) {
      logger.error("failed to get datanode netty client:{}", endPoint, e);
    }
    return client;
  }

  public UncommittedLogManager getUncommittedLogManager() {
    return uncommittedLogManager;
  }

  public CommitLogWorker getCommitLogWorker() {
    return commitLogWorker;
  }

  public DelayManager getDelayManager() {
    return delayManager;
  }

  public void segmentCreated(Long volumeId, int segmentIndex) {
    VolumeInfoHolder volumeInfoHolder = this.volumeInfoHolderManager.getVolumeInfoHolder(volumeId);
    volumeInfoHolder.updateVolumeMetadataWhenSegmentCreated(segmentIndex);

    Multimap<Integer, IoUnitContextPacket> ioContextWaitingForVolumeCompletion =
        this.volumeInfoHolderManager
            .getVolumeInfoHolder(volumeId)
            .getIoContextWaitingForVolumeCompletion();
    Collection<IoUnitContextPacket> contexts = ioContextWaitingForVolumeCompletion
        .removeAll(segmentIndex);
    for (IoUnitContextPacket context : contexts) {
      try {
        IoRequestType requestType = context.getRequestType();
        if (requestType == IoRequestType.Read) {
          firstRead(context);
        } else if (requestType == IoRequestType.Write) {
          firstWrite(context);
        } else {
          logger.error("io request type:{}", requestType);
        }
      } catch (Exception e) {
        logger.error("error occurs while writing", e);
      }
    }
  }

  public ByteBufAllocator getAllocator() {
    return allocator;
  }

  public LogRecorder getLogRecorder() {
    return logRecorder;
  }



  public void markPrimaryDown(SegmentMembership membershipWhenIoCome, InstanceId instanceId) {
    MemberIoStatus memberIoStatus = membershipWhenIoCome.getMemberIoStatus(instanceId);
    if (!memberIoStatus.isPrimary()) {
      logger.error("{} {} is not primary any more in:{}", instanceId, memberIoStatus,
          membershipWhenIoCome);
      return;
    }
    membershipWhenIoCome.markMemberIoStatus(instanceId, MemberIoStatus.PrimaryDown);
  }

  public void markTempPrimaryDown(SegmentMembership membershipWhenIoCome, InstanceId instanceId) {
    MemberIoStatus memberIoStatus = membershipWhenIoCome.getMemberIoStatus(instanceId);
    if (!memberIoStatus.isTempPrimary()) {
      logger.error("{} {}  is not temp primary any more in:{}", instanceId, memberIoStatus,
          membershipWhenIoCome);
      return;
    }

    membershipWhenIoCome.markMemberIoStatus(instanceId, MemberIoStatus.SecondaryDown);
  }

  public void markPrimaryAsUnstablePrimary(SegmentMembership membershipWhenIoCome,
      InstanceId instanceId) {
    MemberIoStatus memberIoStatus = membershipWhenIoCome.getMemberIoStatus(instanceId);
    if (!memberIoStatus.isPrimary()) {
      logger.error("{} is not primary any more in:{}", instanceId, membershipWhenIoCome);
      return;
    }
    membershipWhenIoCome.markMemberIoStatus(instanceId, MemberIoStatus.UnstablePrimary);
  }

  public void markSecondaryDown(SegmentMembership membershipWhenIoCome, InstanceId instanceId) {
    MemberIoStatus memberIoStatus = null;
    if (membershipWhenIoCome.isSecondary(instanceId)) {
      memberIoStatus = MemberIoStatus.SecondaryDown;
    } else if (membershipWhenIoCome.isJoiningSecondary(instanceId)) {
      memberIoStatus = MemberIoStatus.JoiningSecondaryDown;
    } else if (membershipWhenIoCome.isArbiter(instanceId)) {
      memberIoStatus = MemberIoStatus.ArbiterDown;
    } else {
      logger.error("{} is not secondary, current membership:{}", instanceId, membershipWhenIoCome);
      Validate.isTrue(false);
    }
    membershipWhenIoCome.markMemberIoStatus(instanceId, memberIoStatus);
  }

  public boolean markSecondaryAsTempPrimary(SegmentMembership membershipWhenIoCome,
      InstanceId instanceId) {
    MemberIoStatus memberIoStatus;
    if (membershipWhenIoCome.isSecondary(instanceId)) {
      memberIoStatus = MemberIoStatus.TempPrimary;
    } else {
      logger.warn("{} is not secondary, can not be TempPrimary, current membership:{}", instanceId,
          membershipWhenIoCome);
      return false;
    }
    membershipWhenIoCome.markMemberIoStatus(instanceId, memberIoStatus);
    return true;
  }

  public boolean markSecondaryReadDown(SegmentMembership membershipWhenIoCome,
      InstanceId instanceId) {
    MemberIoStatus memberIoStatus;
    if (membershipWhenIoCome.isSecondary(instanceId)) {
      memberIoStatus = MemberIoStatus.SecondaryReadDown;
    } else {
      logger.warn("{} is not secondary, can not be SecondaryReadDown, current membership:{}",
          instanceId,
          membershipWhenIoCome);
      return false;
    }
    membershipWhenIoCome.markMemberIoStatus(instanceId, memberIoStatus);
    return true;
  }

  public void processUpdateMembership() {
    logger.info("going to update membership in cycles");
    for (VolumeInfoHolder volumeInfoHolder : this.volumeInfoHolderManager
        .getAllVolumeInfoHolder()) {
      SpaceSavingVolumeMetadata volumeMetadata = volumeInfoHolder.getVolumeMetadata();
      Map<SegId, AtomicLong> recordLastIoTime = volumeInfoHolder.getRecordLastIoTime();
      Map<Integer, LimitQueue<SegmentMembership>> mapIndexToMembership = volumeMetadata
          .getMemberships();
      for (Map.Entry<Integer, LimitQueue<SegmentMembership>> entry : mapIndexToMembership
          .entrySet()) {
        int segIndex = entry.getKey();
        SegId segId = volumeMetadata.getSegId(segIndex);
        boolean needUpdate = false;
        if (!recordLastIoTime.containsKey(segId)) {
          needUpdate = true;
        } else {
          AtomicLong lastIoTime = recordLastIoTime.get(segId);
          long timePassedByLastIo = System.currentTimeMillis() - lastIoTime.get();
          if (timePassedByLastIo > DelayManagerImpl.DEFAULT_UPDATE_MEMBERSHIP_IF_NO_IO) {
            needUpdate = true;
          }
        }
        if (needUpdate) {
          GetMembershipTask getMembershipTask = new GetMembershipTask(volumeMetadata.getVolumeId(),
              segId, 0L,
              entry.getValue().getLast());
          getMembershipFromDatanodesEngine.putTask(getMembershipTask);
        }
      }
    }
  }


  public WriteFactory getWriteFactory() {
    return writeFactory;
  }

  public GenericAsyncClientFactory<AsyncDataNode.AsyncIface> getClientFactory() {
    return clientFactory;
  }

  public GetMembershipFromInfoCenterEngine getGetMembershipFromInfoCenterEngine() {
    return getMembershipFromInfoCenterEngine;
  }

  @Override
  public void read(long pos, byte[] dstBuf, int off, int len) throws StorageException {
    Validate.isTrue(dstBuf.length - off >= len);
    Semaphore semaphore = new Semaphore(1);

    try {
      AsyncIoCallBack asyncIoCallBack = new AsyncIoCallBackImpl(semaphore);
      asyncRead(pos, dstBuf, off, len, asyncIoCallBack);
      semaphore.acquire();
    } catch (InterruptedException e) {
      logger.error("caught an exception when sync write", e);
      throw new StorageException();
    } finally {
      semaphore.release();
    }
  }

  @Override
  public void read(long pos, ByteBuffer buffer) throws StorageException {
    Validate.notNull(buffer);
    Semaphore semaphore = new Semaphore(1);

    try {
      AsyncIoCallBack asyncIoCallBack = new AsyncIoCallBackImpl(semaphore);
      asyncRead(pos, buffer, asyncIoCallBack);
      semaphore.acquire();
    } catch (InterruptedException e) {
      logger.error("caught an exception when sync write", e);
      throw new StorageException();
    } finally {
      semaphore.release();
    }
  }

  @Override
  public void write(long pos, byte[] buf, int off, int len) throws StorageException {
    Validate.isTrue(buf.length - off >= len);
    Semaphore semaphore = new Semaphore(1);

    try {
      AsyncIoCallBack asyncIoCallBack = new AsyncIoCallBackImpl(semaphore);
      asyncWrite(pos, buf, off, len, asyncIoCallBack);
      semaphore.acquire();
    } catch (InterruptedException e) {
      logger.error("caught an exception when sync write", e);
      throw new StorageException();
    } finally {
      semaphore.release();
    }
  }

  @Override
  public void write(long pos, ByteBuffer buffer) throws StorageException {
    Validate.notNull(buffer);
    Semaphore semaphore = new Semaphore(1);
    try {
      AsyncIoCallBack asyncIoCallBack = new AsyncIoCallBackImpl(semaphore);
      asyncWrite(pos, buffer, asyncIoCallBack);
      semaphore.acquire();
    } catch (InterruptedException e) {
      logger.error("caught an exception when sync write", e);
      throw new StorageException();
    } finally {
      semaphore.release();
    }
  }

  @Override
  public void getTickets(int ticketCount) throws StorageException {
    if (ticketCount == 0) {
      return;
    }
    try {
      this.ioDepth.acquire(ticketCount);
    } catch (InterruptedException e) {
      logger.error("caught an exception", e);
      throw new StorageException();
    }
  }


  @Override
  public void accumulateIoRequest(Long requestUuid, IoRequest ioRequest) throws StorageException {
    asyncProcessIoRequest(requestUuid, ioRequest, false);
  }


  @Override
  public void submitIoRequests(Long requestUuid) throws StorageException {
    ioConvertor.fireRead(requestUuid);
    ioConvertor.fireWrite(requestUuid);
    ioConvertor.fireDiscard(requestUuid);
  }

  private void asyncProcessIoRequest(Long requestUuid, IoRequest ioRequest, boolean fireNow)
      throws StorageException {
    try {
      try {
        if (fireNow) {
          ioRequest.getTicket(this.ioDepth);
        } else {
          ioRequest.setTicketForRelease(this.ioDepth);
        }
      } catch (Exception e) {
        logger.error("caught an exception", e);
        throw e;
      }

      ioRequest.setVolumeId(requestVolumeId.get());
      if (ioRequest.getIoRequestType().isRead()) {
        PerformanceManager.getInstance().getPerformanceManagerMap().get(requestVolumeId.get())
            .addReadCounter(ioRequest.getIoSum());
        PerformanceManager.getInstance().getPerformanceManagerMap().get(requestVolumeId.get())
            .addReadDataSizeBytes(ioRequest.getLength());
        ioConvertor.processReadRequest(requestUuid, ioRequest, fireNow);
      } else if (ioRequest.getIoRequestType().isWrite()) {
        PerformanceManager.getInstance().getPerformanceManagerMap().get(requestVolumeId.get())
            .addWriteCounter(ioRequest.getIoSum());
        PerformanceManager.getInstance().getPerformanceManagerMap().get(requestVolumeId.get())
            .addWriteDataSizeBytes(ioRequest.getLength());
        ioConvertor.processWriteRequest(requestUuid, ioRequest, fireNow);
      } else if (ioRequest.getIoRequestType().isDiscard()) {
        ioConvertor.processDiscardRequest(requestUuid, ioRequest, fireNow);
      } else {
        Validate.isTrue(false, "unknown io request type" + ioRequest.getIoRequestType());
      }
    } catch (Throwable t) {
      logger.error("async process io caught an exception", t);
      ioRequest.reply();
      throw new StorageException();
    }
  }



  public void startOutput(String performanceDir, String performanceName, String myHostname)
      throws Exception {
    File file = new File(performanceDir);
    if (!file.exists() || (file.exists() && !file.isDirectory())) {
      file.mkdir();
    }


    outputPerformanceWorkerFactory = new OutputPerformanceWorkerFactory();

    outputPerformanceWorkerFactory.setVolumeInfoHolderManager(volumeInfoHolderManager);
    outputPerformanceWorkerFactory.setFilePath(performanceDir + "/" + performanceName);
    outputPerformanceWorkerFactory.setMyHostname(myHostname);
    outputPerformanceWorkerFactory.setNetworkDelayRecorder(networkDelayRecorder);
    outputPerformanceWorkerFactory
        .setNetworkDelayMeanOutputIntervalSec(cfg.getNetworkDelayMeanOutputIntervalSec());
    executor = new PeriodicWorkExecutorImpl(
        new ExecutionOptionsReader(1, 1, null, OperationName.Volume.getEvnetDataGeneratingPeriod()),
        outputPerformanceWorkerFactory, "volume-performance-reporter");
    executor.start();
  }

  public InstanceId getMyInstanceId() {
    return myInstanceId;
  }


  public void setMyInstanceId(long myInstanceId) {

    logger.warn("set my instance id {}, but we won't use this instance id to update log's "
            + "uuid generator, new an exception to show stack trace: ",
        myInstanceId, new Exception());
    this.myInstanceId = new InstanceId(myInstanceId);
  }

  public void setTrueInstanceId(InstanceId instanceId) {
    this.trueInstanceId = instanceId;
  }

  public void setInformationCenterClientFactory(
      InformationCenterClientFactory informationCenterClientFactory) {
    this.informationCenterClientFactory = informationCenterClientFactory;
  }


  @Override
  public AtomicLong getRequestVolumeId() {
    return requestVolumeId;
  }

  @Override
  public void pause() {
    logger.warn("pause coordinator");
    this.pause.set(true);
  }

  @Override
  public void restart() {
    logger.warn("restart coordinator");
    this.pause.set(false);
  }

  @Override
  public boolean isPause() {
    return this.pause.get();
  }

  @Override
  public boolean hasIoRequest() {
    logger.warn("pause coordinator, current permits:{}", this.ioDepth.availablePermits());
    return this.ioDepth.availablePermits() < cfg.getIoDepth();
  }

  private Commitlog.PbStartOnlineMigrationRequest buildPbStartOnlineMigrationRequest() {
    Commitlog.PbStartOnlineMigrationRequest.Builder builder =
        Commitlog.PbStartOnlineMigrationRequest
            .newBuilder();
    builder.setRequestId(RequestIdBuilder.get());
    builder.setVolumeId(this.requestVolumeId.get());
    return builder.build();
  }

  @Override
  public void sendStartOnlineMigrationRequestToAllDatanode() {
    logger.warn("begin to send start online migration request to all datanodes");
    Set<InstanceId> allDatanodes = new HashSet<>();
    VolumeInfoHolder volumeInfoHolder = this.volumeInfoHolderManager
        .getVolumeInfoHolder(this.requestVolumeId.get());
    Validate.notNull(volumeInfoHolder);
    SpaceSavingVolumeMetadata volumeMetadata = volumeInfoHolder.getVolumeMetadata();

    Map<Integer, LimitQueue<SegmentMembership>> mapIndexToMembership = volumeMetadata
        .getMemberships();
    for (LimitQueue<SegmentMembership> limitQueue : mapIndexToMembership.values()) {
      SegmentMembership membership = limitQueue.getLast();
      for (InstanceId instanceId : membership.getMembers()) {
        allDatanodes.add(instanceId);
      }
    }
    logger.warn("send request to datanodes:{}", allDatanodes);
    volumeInfoHolder.setNotifyDatanodeCount(allDatanodes.size());

    for (InstanceId requestDatanode : allDatanodes) {
      try {
        AsyncDataNode.AsyncIface datanodeClient = getDatanodeNettyClient(requestDatanode);
        NotifyDatanodeStartOnlineMigrationCallback callback =
            new NotifyDatanodeStartOnlineMigrationCallback(
                volumeInfoHolder, requestDatanode);
        Commitlog.PbStartOnlineMigrationRequest request = buildPbStartOnlineMigrationRequest();
        datanodeClient.startOnlineMigration(request, callback);
      } catch (Exception e) {
        logger.error("caught an exception when first send start online migration to:{}",
            requestDatanode, e);
        volumeInfoHolder.addFailedDatanode(requestDatanode);
        volumeInfoHolder.decNotifyDatanodeCount();
      }
    }
  }

  @Override
  public boolean checkAllDatanodeNotifySuccessfullyOrResend() {
    VolumeInfoHolder volumeInfoHolder = this.volumeInfoHolderManager
        .getVolumeInfoHolder(this.requestVolumeId.get());
    Validate.notNull(volumeInfoHolder);

    boolean result;

    if (volumeInfoHolder.allDatanodeResponse()) {
      if (volumeInfoHolder.hasFailedDatanode()) {
        List<InstanceId> failedDatanode = volumeInfoHolder.pullAllFailedDatanodes();
        Validate.notEmpty(failedDatanode);
        logger.warn("still has:{} not notify successfully", failedDatanode);
        volumeInfoHolder.setNotifyDatanodeCount(failedDatanode.size());

        for (InstanceId requestDatanode : failedDatanode) {
          try {
            AsyncDataNode.AsyncIface datanodeClient = getDatanodeNettyClient(requestDatanode);
            NotifyDatanodeStartOnlineMigrationCallback callback =
                new NotifyDatanodeStartOnlineMigrationCallback(
                    volumeInfoHolder, requestDatanode);
            Commitlog.PbStartOnlineMigrationRequest request = buildPbStartOnlineMigrationRequest();
            datanodeClient.startOnlineMigration(request, callback);
          } catch (Exception e) {
            logger.error("caught an exception when check and resend start online migration to:{}",
                requestDatanode, e);
            volumeInfoHolder.addFailedDatanode(requestDatanode);
            volumeInfoHolder.decNotifyDatanodeCount();
          }
        }
        result = false;
      } else {
        logger.warn("volume:{} has notify all datanode start online migration",
            this.requestVolumeId.get());
        result = true;
      }
    } else {

      logger.warn("still has:{} datanode not response",
          volumeInfoHolder.getLeftNotifyDatanodeCount());
      result = false;
    }
    return result;
  }

  public VolumeInfoHolderManager getVolumeInfoHolderManager() {
    return volumeInfoHolderManager;
  }

  public NetworkDelayRecorder getNetworkDelayRecorder() {
    return networkDelayRecorder;
  }

  public void setNetworkDelayRecorder(NetworkDelayRecorder networkDelayRecorder) {
    this.networkDelayRecorder = networkDelayRecorder;
  }


  public boolean getStartupStatusByVolumeId(long volumeId) {
    VolumeInfoHolder volumeInfoHolder = volumeInfoHolderManager.getVolumeInfoHolder(volumeId);
    if (volumeInfoHolder == null) {
      logger.warn("can not get any VolumeInfoHolder by volumeId:{}", volumeId);
      return false;
    }
    if (!volumeInfoHolder.volumeIsAvailable()) {
      return false;
    }
    return true;
  }

}

