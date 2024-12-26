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

package py.coordinator.main;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.List;
import org.junit.Before;
import org.mockito.Mock;
import py.archive.segment.SegId;
import py.coordinator.lib.Coordinator;
import py.coordinator.response.TriggerByCheckCallback;
import py.instance.SimpleInstance;
import py.membership.SegmentMembership;
import py.proto.Broadcastlog;
import py.test.TestBase;
import py.volume.VolumeType;

public class CheckResponseCollectorTest extends TestBase {

  @Mock
  Broadcastlog.PbCheckResponse response;

  @Mock
  SimpleInstance checkInstance;

  @Mock
  SegId segId;

  @Mock
  Coordinator coordinator;

  @Mock
  TriggerByCheckCallback triggerByCheckCallback;

  @Mock
  List<SimpleInstance> checkThroughList;

  @Mock
  SegmentMembership segmentMembership;

  @Before
  public void init() {
    when(coordinator.getVolumeType(any(Long.class))).thenReturn(VolumeType.REGULAR);
  }

}
