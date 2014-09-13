package com.bah.geterdun;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bah.geterdun.TryHandler.FinalFailureException;
import com.bah.geterdun.WriteAheadLog.NeedsRotationException;

/**
 * Processes events and retries them periodically if they fail to process.
 * 
 * @param <EVENT>
 *          The type of the event to process.
 */
public class GeterDun<EVENT> implements Closeable{
  
  private static Logger LOG = LoggerFactory.getLogger(GeterDun.class);

  public static class CantGeterDunException extends Exception {

    private static final long serialVersionUID = 8282641582206939079L;

    public CantGeterDunException(Throwable source) {
      super(source);
    }

    public CantGeterDunException(String message) {
      super(message);
    }
  }

  private final TryHandler<EVENT> tryHandler;
  private final WriteAheadLogManager<EVENT> manager;

  /**
   * 
   * 
   * @param eventClass
   *          The class of the event to process. Presently, must inherit from
   *          {@link Writable}.
   * @param location
   *          A URI referencing a location for storing logs.
   * @param processor
   *          An implementation of a processing algorithm that will be applied
   *          to each event.
   * @param corruptionHandler
   *          Gets notified of log corruption exceptions. These events should be
   *          very rare, but are conditioned on the reliability of the
   *          underlying file system.
   * @param frequency
   *          How frequently to rotate logs. Lower this parameter if you
   *          encounter issues with {@link OutOfMemoryError OutOfMemoryErrors},
   *          or if your log frequency exceeds more than about 4 billion entries
   *          per hour.
   * @param failureHandler
   *          Gets notified of unrecoverable processing failures.
   * @return A <tt>GeterDun</tt> that you can post events to.
   * @throws CantGeterDunException
   *           If a prerequisite is not met, such as access to the underlying
   *           file system or one of the conditions above not being met.
   */
  public static <EVENT> GeterDun<EVENT> geterDun(Class<EVENT> eventClass,
      String location, EventProcessor<EVENT> processor,
      CorruptionHandler corruptionHandler, long frequency,
      FailureHandler<EVENT> failureHandler) throws CantGeterDunException {
    if (!Writable.class.isAssignableFrom(eventClass)) {
      throw new CantGeterDunException(
          "eventClass must be assignable to Writable.");
    }
    try {
      new Path(new URI(location)).getFileSystem(new Configuration());
    } catch (URISyntaxException e) {
      throw new CantGeterDunException("Parameter location (" + location
          + ") is not a valid URI.");
    } catch (IOException e) {
      throw new CantGeterDunException("Parameter location (" + location
          + ") references an unreachable filesystem.");
    }
    TryHandler<EVENT> tryHandler = new TryHandler<EVENT>(processor,
        failureHandler);

    WriteAheadLog.Factory<EVENT> factory = new WriteAheadLog.Factory<EVENT>(
        eventClass, corruptionHandler);
    WriteAheadLogManager<EVENT> manager;
    try {
      manager = new WriteAheadLogManager<EVENT>(location, frequency, factory,
          new Retrier<EVENT>(tryHandler, failureHandler), corruptionHandler);
    } catch (IOException e) {
      throw new CantGeterDunException(e);
    }
    return new GeterDun<EVENT>(tryHandler, manager);
  }

  /**
   * Creates a new <tt>GeterDun</tt> that swallows log corruption exceptions and
   * processing exceptions, and rotates logs every hour.
   * 
   * @param eventClass
   *          The class of the event to process. Presently, must inherit from
   *          {@link Writable}.
   * @param location
   *          A URI referencing a location for storing logs.
   * @param processor
   *          An implementation of a processing algorithm that will be applied
   *          to each event.
   * @return A <tt>GeterDun</tt> that you can post events to.
   * @throws CantGeterDunException
   *           If a prerequisite is not met, such as access to the underlying
   *           file system or one of the conditions above not being met.
   */
  public static <EVENT> GeterDun<EVENT> geterDun(Class<EVENT> eventClass,
      String location, EventProcessor<EVENT> processor)
      throws CantGeterDunException {
    return geterDun(eventClass, location, processor,
        new NullCorruptionHandler(), 360000, new NullFailureHandler<EVENT>());
  }

  GeterDun(TryHandler<EVENT> tryHandler, WriteAheadLogManager<EVENT> manager) {
    this.tryHandler = tryHandler;
    this.manager = manager;
  }

  /**
   * Durably records an event and will periodically retry it if the first
   * attempt to write the event doesn't succeed.
   * 
   * @param event
   *          The event to write.
   * @throws FinalFailureException
   *           If the event cannot be processed and will never be processed.
   * @throws CantGeterDunException
   *           If the event was recorded to the log and either was processed, or
   *           may be processed in the future, but an error has prevented
   *           communication with the log.
   */
  public void geterDun(EVENT event) throws FinalFailureException,
      CantGeterDunException {
    manager.pauseRotation();
    WriteAheadLog<EVENT> currentLog = null;
    try {
      if(LOG.isDebugEnabled()){
        LOG.debug(getClass().getSimpleName() + " paused rotation.");
      }
      currentLog = manager.getCurrentLog();
      boolean needsRotation = false;
      int eventId = 0;
      do {
        try {
          eventId = currentLog.begin(event);
        } catch (NeedsRotationException e) {
          needsRotation = true;
        }
      } while (needsRotation);
      tryHandler.tryProcess(event, eventId, currentLog);
    } catch (IOException e) {
      int counter = currentLog == null? -1 : currentLog.getCounter() + 1;
      String path = currentLog.getPath();
      throw new FinalFailureException(event, e, counter,
          path);
    } finally {
      manager.resumeRotation();
      if(LOG.isDebugEnabled()){
        LOG.debug(getClass().getSimpleName() + " resumed rotation.");
      }
    }
    if (currentLog !=null && currentLog.needsRotation()) {
      try {
        manager.rotateLogs();
      } catch (IOException e) {
        throw new CantGeterDunException(e);
      }
    }
  }
  
  public void close() throws IOException {
    manager.close();
  }
}
