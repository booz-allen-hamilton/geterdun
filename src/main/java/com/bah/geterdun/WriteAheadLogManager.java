package com.bah.geterdun;

import static com.google.common.collect.Lists.reverse;
import static com.google.common.util.concurrent.Uninterruptibles.joinUninterruptibly;
import static java.util.Arrays.asList;

import java.io.Closeable;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class WriteAheadLogManager<EVENT> implements Closeable {

  private class LogRotatationTimer extends TimerTask {

    @Override
    public void run() {
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Attempting background rotation.");
        }
        if (!stopCleanup) {
          rotateLogs();
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Background rotation complete.");
        }
      } catch (IOException e) {
        LOG.error("Unable to rotate logs ", e);
      }
    }

  }

  private class CleanupThread extends Thread {
    {
      this.setName("GeterDun log retrier");
      this.setDaemon(true);
    }

    @Override
    public void run() {
      while (!stopCleanup) {
        retrier.cleanup(WriteAheadLogManager.this);
        Thread.yield();
      }
    }
  }

  static final Timer logRotator = new Timer(
      "Write ahead logging rotation thread", true);
  static final Logger LOG = LoggerFactory.getLogger(WriteAheadLogManager.class);
  private final Path path;
  private final FileSystem fs;
  private WriteAheadLog<EVENT> currentLog;
  private final ReadWriteLock currentLogLock = new ReentrantReadWriteLock();
  private final Condition logsRotated = currentLogLock.writeLock()
      .newCondition();
  private final WriteAheadLog.Factory<EVENT> logFactory;
  private final Retrier<EVENT> retrier;
  private final SecureRandom random = new SecureRandom();
  private final CleanupThread cleanupThread;
  private final LogRotatationTimer rotationTimerTask;
  private boolean stopCleanup = false;

  WriteAheadLogManager(String location, long frequency,
      WriteAheadLog.Factory<EVENT> logFactory, Retrier<EVENT> retrier,
      CorruptionHandler corruptionHandler) throws IOException {
    path = new Path(location);
    this.retrier = retrier;
    this.logFactory = logFactory;
    fs = path.getFileSystem(new Configuration());
    rotateLogs();
    rotationTimerTask = new LogRotatationTimer();
    logRotator.scheduleAtFixedRate(rotationTimerTask, frequency, frequency);
    (cleanupThread = new CleanupThread()).start();
  }

  public void rotateLogs() throws IOException {
    currentLogLock.writeLock().lock();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Rotating logs");
    }
    try {
      // there will be no current log on the first rotation
      if (currentLog != null) {
        currentLog.close();
        // if the current log is fully committed there's no reason to hang on to
        // it
        if (currentLog.isFullyCommitted()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Current log was fully committed.");
          }
          fs.delete(new Path(currentLog.getPath()), false);
        }
      }

      FileStatus[] fileStatuses = fs.listStatus(path, new PathFilter() {
        public boolean accept(Path path) {
          return path.getName().startsWith("part.");
        }
      });
      int part = fileStatuses.length - 1;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Found " + fileStatuses.length + " uncommitted logs.");
      }
      for (FileStatus status : reverse(asList(fileStatuses))) {
        Path newPath = new Path(path, "part." + part--);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Renaming " + status.getPath() + " to " + newPath);
        }
        fs.rename(status.getPath(), newPath);
      }
      currentLog = logFactory.getLog(new Path(path, "part.current").toUri()
          .toString());
      logsRotated.signalAll();
    } finally {
      currentLogLock.writeLock().unlock();
    }
  }

  /**
   * Get the current log. Invocations of this should be guarded with
   * {@link #pauseRotation()} and {@link #resumeRotation()} in order to prevent
   * the current log from being rotated out and closed while it is being logged
   * to.
   * 
   * @return The current <tt>WriteAheadLog</tt>.
   */
  public WriteAheadLog<EVENT> getCurrentLog() {
    return currentLog;
  }

  /**
   * Pause rotation so that the current log can be written to.
   */
  public void pauseRotation() {
    // note even though this is used for write operations, the log is actually
    // thread safe, so we only need to prevent rotation. Therefore we use the
    // multi-holder semantics of read locking.
    currentLogLock.readLock().lock();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Entering rotation read lock.");
    }
  }

  /**
   * Resume rotation after writing to the current log.
   */
  public void resumeRotation() {
    // note even though this is used for write operations, the log is actually
    // thread safe, so we only need to prevent rotation. Therefore we use the
    // multi-holder semantics of read locking.
    currentLogLock.readLock().unlock();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Leaving rotation read lock.");
    }
  }

  public WriteAheadLog<EVENT> getRandomOldLog() throws IOException {
    FileStatus[] fileStatuses = fs.listStatus(path, new PathFilter() {
      public boolean accept(Path path) {
        return path.getName().startsWith("part.")
            && !path.getName().equals("part.current");
      }
    });
    if (fileStatuses.length > 0) {
      FileStatus fs = fileStatuses[random.nextInt(fileStatuses.length)];
      return logFactory.getLog(fs.getPath().toUri().toString());
    } else {
      return null;
    }
  }

  public void waitUntilNextRotation() {
    currentLogLock.writeLock().lock();
    try {
      if (!stopCleanup) {
        logsRotated.awaitUninterruptibly();
      }
    } finally {
      currentLogLock.writeLock().unlock();
    }
  }

  public void close() throws IOException {
    currentLogLock.writeLock().lock();
    try {
      stopCleanup = true;
      rotationTimerTask.cancel();
      logsRotated.signalAll();
    } finally {
      currentLogLock.writeLock().unlock();
    }
    joinUninterruptibly(cleanupThread);
  }

}
