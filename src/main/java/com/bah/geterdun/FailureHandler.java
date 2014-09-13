package com.bah.geterdun;

/**
 * Handles events that have failed permanently.
 * 
 * @param <EVENT>
 *          The type of the event.
 */
public interface FailureHandler<EVENT> {

  /**
   * Handle an event that cannot be processed due to an unrecoverable error in
   * the {@link EventProcessor}
   * 
   * @param action
   *          The type of the event.
   */
  public void handleFailure(EVENT action);

}
