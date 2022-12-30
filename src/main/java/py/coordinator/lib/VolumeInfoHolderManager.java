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

import java.util.Collection;
import java.util.Map;
import py.coordinator.volumeinfo.VolumeInfoRetriever;


public interface VolumeInfoHolderManager {

  public VolumeInfoHolder getVolumeInfoHolder(Long rootVolumeId);

  public void removeVolumeInfoHolder(Long rootVolumeId);

  public void addVolumeInfoHolder(Long rootVolumeId, int snapshotId) throws Exception;


  public void addVolumeInfoHolder(Long rootVolumeId, VolumeInfoHolder volumeInfoHolder);

  public boolean containVolumeInfoHolder(Long rootVolumeId);

  public Collection<VolumeInfoHolder> getAllVolumeInfoHolder();


  public Map<Long, Long> getRootVolumeIdMap();


  public void setVolumeInfoRetrieve(VolumeInfoRetriever volumeInfoRetriever);

}
