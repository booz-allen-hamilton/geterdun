package com.bah.geterdun;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tries to process an event using an {@link EventProcessor}. If the event
 * processes successfully, the event is committed to the log. If not, then no
 * action is taken on the log. If the event fails permenantly then a
 * {@link FinalFailureException} is thrown.
 * 
 * @param <EVENT>
 */
class TryHandler<EVENT> {

  private static final Logger LOG = LoggerFactory.getLogger(TryHandler.class);

  public static class FinalFailureException extends Exception {

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

  private final EventProcessor<EVENT> processor;
  private FailureHandler<EVENT> failureHandler;

  TryHandler(EventProcessor<EVENT> processor,
      FailureHandler<EVENT> failureHandler) {
    this.failureHandler = failureHandler;
    this.processor = processor;
  }

  /**
   * Tries to process an event. If the event is successfully processed, it is
   * committed to the underlying write ahead log for this handler. If an
   * unhandled exception is thrown from the event, then it is propagated as a
   * <tt>FinalFailureException</tt>. If the event fails to process but no
   * unhandled exception is thrown, then the framework will continue to attempt
   * to process the event at a future time.
   * 
   * @param event
   * @param eventId
   * @throws FinalFailureException
   *           If the event cannot be processed.
   */
  public void tryProcess(EVENT event, int eventId, WriteAheadLog<EVENT> log)
      throws FinalFailureException {
    try {
      if(processor.processEvent(event)){
        log.commit(eventId);
      }
    } catch (Exception e) {
      try {
        log.commit(eventId);
      } catch (IOException e1) {
        LOG.error("Unable to commit failure due to IOException.", e1);
      }
      failureHandler.handleFailure(event);
      throw new FinalFailureException(event, e, eventId, log.getPath());
    }
  }
}
