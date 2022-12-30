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

import py.coordinator.iorequest.iorequest.IoRequest;
import py.drivercontainer.exception.ProcessPositionException;
import py.exception.StorageException;


public interface IoConvertor {

  public void processWriteRequest(Long requestUuid, IoRequest ioRequest, boolean fireNow)
      throws ProcessPositionException, StorageException;

  public void processReadRequest(Long requestUuid, IoRequest ioRequest, boolean fireNow)
      throws ProcessPositionException, StorageException;

  public void processDiscardRequest(Long requestUuid, IoRequest ioRequest, boolean fireNow)
      throws ProcessPositionException, StorageException;

  public void fireWrite(Long requestUuid) throws StorageException;

  public void fireRead(Long requestUuid) throws StorageException;

  public void fireDiscard(Long requestUuid) throws StorageException;

}
