package com.bah.geterdun;

/**
 * Processes events.
 * 
 * @param <EVENT>
 *          The type of the event.
 */
public interface EventProcessor<EVENT> {

  /**
   * Durably process an event.
   * 
   * @param event
   *          An event to process.
   * @return <tt>true</tt> if the event is successfully processed,
   *         <tt>false</tt> if the event failed to process due to a recoverable
   *         error.
   * @throws Exception
   *           if the event failed to process due to an unrecoverable error and
   *           it will not be retried.
   */
  public boolean processEvent(EVENT event) throws Exception;

}
