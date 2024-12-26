
package py.coordinator.worker;


public enum CommitWorkProgress {
  Commitable(1), CommitWaiting(2), Committing(3);
  private int value;

  CommitWorkProgress(int value) {
    this.value = value;
  }
}
