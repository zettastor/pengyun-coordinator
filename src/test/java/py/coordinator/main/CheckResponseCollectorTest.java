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
