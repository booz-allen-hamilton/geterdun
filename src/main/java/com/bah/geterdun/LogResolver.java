package com.bah.geterdun;

import java.io.EOFException;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Writable;

/**
 * Populates a map of created events that are uncommited.
 * 
 * @param <EVENT>
 */
class LogResolver<EVENT> {

  public static class CorruptLogException extends Exception {

    private static final long serialVersionUID = 2595135433270442103L;
    private long position;
    private long length;
    private String location;

    public CorruptLogException(long position, long length, String location) {
      super("Corrupt log " + location + " discovered at " + position + " of "
          + length);
      this.position = position;
      this.length = length;
      this.location = location;
    }

    public long getPosition() {
      return position;
    }

    public String getLocation() {
      return location;
    }

    public long getLength() {
      return length;
    }
  }

  public LogResolver() {
  }

  public int resolveUncommittedEvents(FSDataInputStream stream,
      Map<Integer, EVENT> eventMap, Class<EVENT> eventClass, long streamLength,
      String location) throws IOException, CorruptLogException {
    int code, id = 0;
    while (true) {
      try {
        code = stream.readInt();
      } catch (EOFException e) {
        // if we've reached the end of the file we're done
        return id;
      }
      try {
        if (code == WriteAheadLog.TYPE_BEGIN) {
          id = stream.readInt();
          EVENT event;
          try {
            event = eventClass.newInstance();
          } catch (InstantiationException e) {
            throw new IOException(e);
          } catch (IllegalAccessException e) {
            throw new IOException(e);
          }
          ((Writable) event).readFields(stream);
          eventMap.put(id, event);
        } else if (code == WriteAheadLog.TYPE_COMMIT) {
          int commitId = stream.readInt();
          eventMap.remove(commitId);
        }
      } catch (IOException e) {
        throw new CorruptLogException(stream.getPos(), streamLength, location);
      }
    }
  }
}
