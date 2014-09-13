package com.bah.geterdun;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

class HeaderWritable implements Writable {

  public static final int SYNC_SIZE = 128;
  private final int version = 0;
  private String className;

  public void readFields(DataInput in) throws IOException {
    int version = in.readInt();
    if (version != this.version) {
      throw new IOException("Version mismatch, expected " + this.version
          + " got " + version);
    }
    className = in.readUTF();
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(version);
    out.writeUTF(className);
  }

  public void setClassName(String name) {
    this.className = name;
  }

  public String getClassName() {
    return className;
  }

}
