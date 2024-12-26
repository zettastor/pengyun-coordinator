
package py.coordinator.utils;


public interface NetworkDelayRecorder {

  public void recordDelay(String destIpAddress, Long delayMs);

  public Double getMeanDelay(String destIpAddress);

  public void outputAllMeanDelay(String myIpAddress);
}
