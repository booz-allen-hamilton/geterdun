package com.bah.geterdun;

import static java.lang.Thread.yield;

import java.io.IOException;
import java.util.Collection;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bah.geterdun.TryHandler.FinalFailureException;

/**
 * Retries events and removes the file if
 */
class Retrier<EVENT> {

  private static final Logger LOG = LoggerFactory.getLogger(Retrier.class);
  private final TryHandler<EVENT> tryHandler;
  private FailureHandler<EVENT> failureHandler;

  Retrier(TryHandler<EVENT> tryHandler, FailureHandler<EVENT> failureHandler) {
    this.tryHandler = tryHandler;
    this.failureHandler = failureHandler;
  }

  public void cleanup(WriteAheadLogManager<EVENT> manager) {
    manager.pauseRotation();
    WriteAheadLog<EVENT> logToCleanup;
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug(getClass().getSimpleName() + " paused rotation.");
      }
      try {
        logToCleanup = manager.getRandomOldLog();
      } catch (IOException e1) {
        LOG.error("Unable to get log to clean up");
        return;
      }
      if (LOG.isDebugEnabled() && logToCleanup != null) {
        LOG.debug("Got log to cleanup " + logToCleanup.getPath());
      }
      if (logToCleanup != null) {
        Iterable<Entry<Integer, EVENT>> entries = logToCleanup
            .getUncommittedEvents();
        if (LOG.isDebugEnabled()) {
          if (entries instanceof Collection) {
            LOG.debug(((Collection<?>) entries).size() + " entries to cleanup.");
          } else {
            LOG.debug("Cannot find the size of non-collection iterable representing cleanup entries");
          }
        }
        for (Entry<Integer, EVENT> entry : entries) {
          try {
            tryHandler.tryProcess(entry.getValue(), entry.getKey(),
                logToCleanup);
          } catch (FinalFailureException e) {
            try {
              logToCleanup.commit(entry.getKey());
            } catch (IOException e1) {
              LOG.error(
                  "Unable to commit final failed processing of log event id:"
                      + entry.getKey() + " event:" + entry.getValue(), e1);
            }
            failureHandler.handleFailure(entry.getValue());
          }
          yield();
        }
        if (logToCleanup.isFullyCommitted()) {
          Path path = new Path(logToCleanup.getPath());
          try {
            FileSystem fs = path.getFileSystem(new Configuration());
            fs.delete(path, false);
          } catch (IOException e) {
            LOG.error("Unable to clean up fully-committed log:" + path);
          }
        }
      }
    } finally {
      manager.resumeRotation();
      if (LOG.isDebugEnabled()) {
        LOG.debug(getClass().getSimpleName() + " resumed rotation.");
      }
    }

    if (logToCleanup == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Waiting until next rotation.");
      }
      manager.waitUntilNextRotation();
      return;
    }

  }

}
