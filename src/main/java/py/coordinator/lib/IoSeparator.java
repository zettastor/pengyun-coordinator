
package py.coordinator.lib;

import java.util.List;
import py.coordinator.iorequest.iounitcontext.IoUnitContext;


public interface IoSeparator {

  public void splitRead(List<IoUnitContext> contexts);

  public void splitWrite(List<IoUnitContext> contexts);

  public void processDiscard(List<IoUnitContext> contexts);
}
