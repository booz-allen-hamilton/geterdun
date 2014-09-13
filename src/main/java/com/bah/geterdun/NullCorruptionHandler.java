package com.bah.geterdun;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bah.geterdun.LogResolver.CorruptLogException;

/**
 * Logs log corruption as an error in the SLF4J logs.
 */
public class NullCorruptionHandler implements CorruptionHandler {

  private final Logger LOG = LoggerFactory
      .getLogger(NullCorruptionHandler.class);

  public void handleCorruption(CorruptLogException e) {
    LOG.error(
        "Corrupt write ahead log " + e.getLocation() + " at " + e.getPosition()
            + " of " + e.getLength(), e);

  }

}
