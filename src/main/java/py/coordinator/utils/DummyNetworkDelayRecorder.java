
package py.coordinator.utils;


public class DummyNetworkDelayRecorder implements NetworkDelayRecorder {

  @Override
  public void recordDelay(String destIpAddress, Long delayMs) {

  }

  @Override
  public Double getMeanDelay(String destIpAddress) {
    return null;
  }

  @Override
  public void outputAllMeanDelay(String myIpAddress) {

  }
}
