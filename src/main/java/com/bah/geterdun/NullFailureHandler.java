package com.bah.geterdun;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs an unrecoverable failure to process an event in the SLF4J logs.
 * 
 */
public class NullFailureHandler<EVENT> implements FailureHandler<EVENT> {

  private static final Logger LOG = LoggerFactory
      .getLogger(NullFailureHandler.class);

  public void handleFailure(EVENT action) {
    LOG.error("Event permentantly failed: " + action);
  }

}
