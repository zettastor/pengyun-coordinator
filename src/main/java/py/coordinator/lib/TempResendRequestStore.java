
package py.coordinator.lib;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import py.coordinator.nbd.request.Request;


public class TempResendRequestStore {

  private static LinkedBlockingDeque<Request> queue = new LinkedBlockingDeque<>();

  public static void addResendRequest(Request request) {
    queue.offer(request);
  }


  
  public static List<Request> pullResendRequests() {
    List<Request> requestList = new ArrayList<>();
    queue.drainTo(requestList);
    return requestList;
  }

}
