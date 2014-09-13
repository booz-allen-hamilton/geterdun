package com.bah.geterdun;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.openhft.koloboke.collect.map.hash.HashIntObjMaps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Writable;

import com.bah.geterdun.HeaderManagement.HeaderBasedInitialization;
import com.bah.geterdun.LogResolver.CorruptLogException;

class WriteAheadLog<EVENT> implements Closeable {

  public static class NeedsRotationException extends Exception {

    private static final long serialVersionUID = 8899834790240724127L;

  }

  public static class Factory<EVENT> {

    private final Class<EVENT> eventClass;
    private final HeaderManagement headerManagement;
    private final LogResolver<EVENT> logResolver;
    private final CorruptionHandler corruptionHandler;

    Factory(Class<EVENT> eventClass, HeaderManagement headerManagement,
        LogResolver<EVENT> logResolver, CorruptionHandler corruptionHandler) {
      this.eventClass = eventClass;
      this.headerManagement = headerManagement;
      this.logResolver = logResolver;
      this.corruptionHandler = corruptionHandler;
    }

    Factory(Class<EVENT> eventClass, CorruptionHandler corruptionHandler) {
      this(eventClass, new HeaderManagement(), new LogResolver<EVENT>(),
          corruptionHandler);
    }

    public WriteAheadLog<EVENT> getLog(String location) throws IOException {
      return new WriteAheadLog<EVENT>(location, eventClass, headerManagement,
          logResolver, corruptionHandler);
    }
  }

  static final int TYPE_BEGIN = 0;
  static final int TYPE_COMMIT = 1;

  private String location;
  private FSDataOutputStream output;
  private int counter;
  private Map<Integer, EVENT> uncommittedEvents = HashIntObjMaps
      .newMutableMap();
  private Lock writeLock = new ReentrantLock();

  WriteAheadLog(String location, Class<EVENT> eventClass,
      HeaderManagement headerManagement, LogResolver<EVENT> logResolver,
      CorruptionHandler corruptionHandler) throws IOException {
    this.location = location;
    Path path = new Path(location);
    FileSystem fs;
    if (location.startsWith("file:/")) {
      fs = new RawLocalFileSystem() {
        {
          setConf(new Configuration());
          initialize(FsConstants.LOCAL_FS_URI, getConf());
        }
      };

      fs.setConf(new Configuration());
    } else {
      fs = path.getFileSystem(new Configuration());
    }
    HeaderBasedInitialization inOut;
    if (!fs.exists(path)) {
      inOut = headerManagement.createAndWriteHeader(fs, path, eventClass);
    } else {
      inOut = headerManagement.verifyHeaderAndOpen(fs, path, eventClass);
    }
    try {
      counter = logResolver.resolveUncommittedEvents(inOut.getIn(), uncommittedEvents,
          eventClass, fs.getFileStatus(path).getLen(), location);
      inOut.getIn().close();
      output = inOut.getOut();
    } catch (CorruptLogException e) {
      corruptionHandler.handleCorruption(e);
    }

  }

  public void commit(int eventId) throws IOException {
    writeLock.lock();
    try {
      if (eventId > counter) {
        throw new IOException("Cannot commit an ID that has not been created.");
      }
      output.writeInt(TYPE_COMMIT);
      output.writeInt(eventId);
      output.hsync();
      uncommittedEvents.remove(eventId);
    } finally {
      writeLock.unlock();
    }
  }

  public String getPath() {
    return location;
  }

  public int begin(EVENT event) throws IOException, NeedsRotationException {
    writeLock.lock();
    try {
      if (needsRotation()) {
        throw new NeedsRotationException();
      }
      int eventId = ++counter;
      output.writeInt(TYPE_BEGIN);
      output.writeInt(eventId);
      // TODO: support other types besides writable
      ((Writable) event).write(output);
      output.hsync();
      uncommittedEvents.put(eventId, event);
      return eventId;
    } finally {
      writeLock.unlock();
    }
  }

  public boolean needsRotation() {
    return counter == Integer.MAX_VALUE || counter < 0;
  }

  public void close() throws IOException {
    writeLock.lock();
    try {
      if (output != null) {
        output.close();
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Returns a deep copy of the uncommitted events for this write ahead log at
   * the time of method invocation.
   * 
   * @return An iterable copy of the uncommitted events for this write ahead
   *         log.
   */
  public Iterable<Entry<Integer, EVENT>> getUncommittedEvents() {
    writeLock.lock();
    try {
      return HashIntObjMaps.newImmutableMap(uncommittedEvents).entrySet();
    } finally {
      writeLock.unlock();
    }
  }

  public boolean isFullyCommitted() {
    return uncommittedEvents.size() == 0;
  }

  public int getCounter() {
    return counter;
  }

}
