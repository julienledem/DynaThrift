package dynathrift;

import org.apache.thrift.TFieldIdEnum;

public class DynaField implements TFieldIdEnum {

  private final short thriftFieldId;
  private final byte type;

  public DynaField(short thriftFieldId, byte type) {
    this.thriftFieldId = thriftFieldId;
    this.type = type;
  }

  @Override
  public short getThriftFieldId() {
    return thriftFieldId;
  }

  @Override
  public String getFieldName() {
    return null;
  }

  public byte getType() {
    return type;
  }

  @Override
  public int hashCode() {
    return thriftFieldId;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof DynaField) {
      return ((DynaField)obj).thriftFieldId == thriftFieldId;
    }
    return false;
  }
}
