package com.bah.geterdun;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Writes and reads the headers of the write ahead log format.
 */
class HeaderManagement {

  public static class HeaderBasedInitialization {
    private final FSDataOutputStream out;
    private final FSDataInputStream in;

    public HeaderBasedInitialization(FSDataOutputStream out,
        FSDataInputStream in) {
      super();
      this.out = out;
      this.in = in;
    }

    public FSDataInputStream getIn() {
      return in;
    }

    public FSDataOutputStream getOut() {
      return out;
    }

  }

  private HeaderWritable writable;

  public HeaderManagement(HeaderWritable writable) {
    this.writable = writable;
  }

  public HeaderManagement() {
    this(new HeaderWritable());
  }

  public HeaderBasedInitialization createAndWriteHeader(FileSystem fileSystem,
      Path path, Class<?> eventClass) throws IOException {
    fileSystem.createNewFile(path);
    FSDataOutputStream out = fileSystem.append(path);
    writable.setClassName(eventClass.getName());
    writable.write(out);
    out.hsync();
    FSDataInputStream in = fileSystem.open(path);
    writable.readFields(in);
    return new HeaderBasedInitialization(out, in);
  }

  public HeaderBasedInitialization verifyHeaderAndOpen(FileSystem fileSystem,
      Path path, Class<?> eventClass) throws IOException {
    FSDataInputStream in = fileSystem.open(path);
    writable.readFields(in);
    if (!eventClass.getName().equals(writable.getClassName())) {
      in.close();
      throw new IOException(
          "Invalid class in existing write ahead log, expected "
              + eventClass.getName() + " got " + writable.getClassName());
    }
    FSDataOutputStream out = fileSystem.append(path);
    return new HeaderBasedInitialization(out, in);
  }

}
