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

import java.util.List;
import py.coordinator.iorequest.iounitcontext.IoUnitContext;


public interface IoSeparator {

  public void splitRead(List<IoUnitContext> contexts);

  public void splitWrite(List<IoUnitContext> contexts);

  public void processDiscard(List<IoUnitContext> contexts);
}
