package dynathrift.stats;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.junit.Test;

import com.twitter.elephantbird.thrift.test.TestName;
import com.twitter.elephantbird.thrift.test.TestPerson;
import com.twitter.elephantbird.thrift.test.TestPhoneType;

public class TestStats {

  @Test
  public void test() throws Exception {
    Stats stats = new Stats();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    TestPerson p = new TestPerson(new TestName("foo", "bar"), new HashMap<TestPhoneType, String>());
    p.write(new TBinaryProtocol(new TIOStreamTransport(out)));
    stats.countOne(new TBinaryProtocol(new TIOStreamTransport(new ByteArrayInputStream(out.toByteArray()))));
    System.out.println(stats);
    System.out.println(stats.toJson());
  }
}
