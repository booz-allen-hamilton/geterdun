package com.bah.geterdun;

public class FinalFailureException extends Exception {

  private static final long serialVersionUID = -6058321375714843781L;
  private final Object event;
  private final int eventId;
  private final String logName;

  FinalFailureException(Object event, Exception source, int eventId,
      String logName) {
    super(source);
    this.event = event;
    this.eventId = eventId;
    this.logName = logName;
  }

  public Object getEvent() {
    return event;
  }

  public int getEventId() {
    return eventId;
  }

  public String getLogName() {
    return logName;
  }
}