package com.bah.geterdun;

import static com.bah.geterdun.GeterDun.geterDun;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GeterDunTest {

  public static class NothingWritable implements Writable {

    public void write(DataOutput out) throws IOException {
      // TODO Auto-generated method stub

    }

    public void readFields(DataInput in) throws IOException {
      // TODO Auto-generated method stub

    }

  }

  public static class SimpleWritable implements Writable {
    private int number;
    private String string;

    public int getNumber() {
      return number;
    }

    public String getString() {
      return string;
    }

    public void setNumber(int number) {
      this.number = number;
    }

    public void setString(String string) {
      this.string = string;
    }

    public void readFields(DataInput in) throws IOException {
      number = in.readInt();
      string = in.readUTF();
    }

    public void write(DataOutput out) throws IOException {
      out.writeInt(number);
      out.writeUTF(string);
    }

  }

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void createsWithValidInput() throws Throwable {
    geterDun(NothingWritable.class,
        "file://" + tempFolder.newFolder().getAbsolutePath(),
        new EventProcessor<NothingWritable>() {
          public boolean processEvent(NothingWritable event) throws Exception {
            return true;
          }
        }).close();
  }

  @Test
  public void processesThreeNullEvents() throws Throwable {
    final int[] counterArray = new int[1];
    GeterDun<NothingWritable> geterDun = geterDun(NothingWritable.class,
        "file://" + tempFolder.newFolder().getAbsolutePath(),
        new EventProcessor<NothingWritable>() {
          public boolean processEvent(NothingWritable event) throws Exception {
            counterArray[0]++;
            return true;
          }
        });
    NothingWritable nw = new NothingWritable();
    for (int i = 0; i < 3; i++) {
      geterDun.geterDun(nw);
    }
    Assert.assertEquals(3, counterArray[0]);
    geterDun.close();
  }

  @Test
  public void serializesAndReadsEvents() throws Throwable {
    final Map<Integer, SimpleWritable> events = new HashMap<Integer, GeterDunTest.SimpleWritable>();
    final boolean[] fail = { true };
    GeterDun<SimpleWritable> geterDun = geterDun(SimpleWritable.class,
        "file://" + tempFolder.newFolder().getAbsolutePath(),
        new EventProcessor<SimpleWritable>() {
          public boolean processEvent(SimpleWritable event) throws Exception {
            if (fail[0])
              return false;
            events.put(event.getNumber(), event);
            return true;
          }
        }, new NullCorruptionHandler(), 500,
        new NullFailureHandler<SimpleWritable>());
    SimpleWritable sw = new SimpleWritable();
    for (int i = 0; i < 3; i++) {
      sw.setNumber(i);
      sw.setString(Integer.toString(i));
      geterDun.geterDun(sw);
    }
    fail[0] = false;
    Thread.sleep(1000);
    Assert.assertEquals(3, events.size());

    for (int i = 0; i < 3; i++) {
      Assert.assertEquals(i, events.get(i).getNumber());
      Assert.assertEquals(Integer.toString(i), events.get(i).getString());
    }
    geterDun.close();
  }

  @Test
  public void reprocessesNullFailures() throws Throwable {
    final int[] counterArray = new int[1];
    final boolean[] fail = { true };
    GeterDun<NothingWritable> geterDun = geterDun(NothingWritable.class,
        "file://" + tempFolder.newFolder().getAbsolutePath(),
        new EventProcessor<NothingWritable>() {
          public boolean processEvent(NothingWritable event) throws Exception {
            if (fail[0])
              return false;
            counterArray[0]++;
            return true;
          }
        }, new NullCorruptionHandler(), 500,
        new NullFailureHandler<NothingWritable>());
    NothingWritable nw = new NothingWritable();
    for (int i = 0; i < 3; i++) {
      geterDun.geterDun(nw);
    }
    fail[0] = false;
    Thread.sleep(1000);
    Assert.assertEquals(3, counterArray[0]);
    geterDun.close();
  }
  
  
  @Test
  public void successfullyReopens() throws Throwable {
    final Map<Integer, SimpleWritable> events = new HashMap<Integer, GeterDunTest.SimpleWritable>();
    String location ="file://" + tempFolder.newFolder().getAbsolutePath();
    GeterDun<SimpleWritable> geterDun = geterDun(SimpleWritable.class,
        location,
        new EventProcessor<SimpleWritable>() {
          public boolean processEvent(SimpleWritable event) throws Exception {
            return false;
          }
        }, new NullCorruptionHandler(), 500,
        new NullFailureHandler<SimpleWritable>());
    SimpleWritable sw = new SimpleWritable();
    for (int i = 0; i < 3; i++) {
      sw.setNumber(i);
      sw.setString(Integer.toString(i));
      geterDun.geterDun(sw);
    }
    geterDun.close();
    geterDun = geterDun(SimpleWritable.class,
        location,
        new EventProcessor<SimpleWritable>() {
          public boolean processEvent(SimpleWritable event) throws Exception {
            events.put(event.getNumber(), event);
            return true;
          }
        }, new NullCorruptionHandler(), 500,
        new NullFailureHandler<SimpleWritable>());
    Thread.sleep(1000);
    Assert.assertEquals(3, events.size());

    for (int i = 0; i < 3; i++) {
      Assert.assertEquals(i, events.get(i).getNumber());
      Assert.assertEquals(Integer.toString(i), events.get(i).getString());
    }
    geterDun.close();
  }

}
