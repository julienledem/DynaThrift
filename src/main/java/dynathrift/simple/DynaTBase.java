package dynathrift.simple;

import static org.apache.thrift.protocol.TType.BOOL;
import static org.apache.thrift.protocol.TType.BYTE;
import static org.apache.thrift.protocol.TType.DOUBLE;
import static org.apache.thrift.protocol.TType.ENUM;
import static org.apache.thrift.protocol.TType.I16;
import static org.apache.thrift.protocol.TType.I32;
import static org.apache.thrift.protocol.TType.I64;
import static org.apache.thrift.protocol.TType.LIST;
import static org.apache.thrift.protocol.TType.MAP;
import static org.apache.thrift.protocol.TType.SET;
import static org.apache.thrift.protocol.TType.STRING;
import static org.apache.thrift.protocol.TType.STRUCT;
import static org.apache.thrift.protocol.TType.VOID;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.thrift.TBase;
import org.apache.thrift.TEnum;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;

import dynathrift.DynaField;

public class DynaTBase implements TBase<DynaTBase, DynaField> {

  private static final long serialVersionUID = 1L;

  static final String getThriftTypeName(byte type) {
    switch (type) {
    case BOOL:
      return "BOOL";
    case BYTE:
      return "BYTE";
    case DOUBLE:
      return "DOUBLE";
    case I16:
      return "I16";
    case I32:
      return "I32";
    case I64:
      return "I64";
    case STRING:
      return "STRING";
    case STRUCT:
      return "STRUCT";
    case MAP:
      return "MAP";
    case SET:
      return "SET";
    case LIST:
      return "LIST";
    case ENUM:
      return "EMUM";
    case VOID:;
      return "VOID";
    default:
      return "unkown" + type;
    }
  }


  private Map<Short, DynaField> fields = new HashMap<Short, DynaField>();
  private Map<DynaField, Object> content = new HashMap<DynaField, Object>();

  public DynaTBase() {
  }

  @Override
  public int compareTo(DynaTBase o) {
    int sizeDiff = o.content.size() - content.size();
    if (sizeDiff != 0) {
      return sizeDiff;
    }
    for (Entry<DynaField, Object> entry : content.entrySet()) {
      Object oValue = o.content.get(entry.getKey());
      if (oValue == null) {
        return 1;
      }
      int fieldDiff = compare(entry.getKey().getType(), oValue, entry.getValue());
      if (fieldDiff != 0) {
        return fieldDiff;
      }
    }
    return 0;
  }

  private int compare(byte type, Object v1, Object v2) {
    throw new UnsupportedOperationException("NYI");
  }

  @Override
  public void read(TProtocol iprot) throws TException {
    iprot.readStructBegin();
    TField field;
    while ((field = iprot.readFieldBegin()).type != TType.STOP) {
      this.setFieldValue(new DynaField(field.id, field.type), readValue(field.type, iprot));
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();
  }

  private Object readValue(byte type, TProtocol iprot) throws TException {
    switch (type) {
    case BOOL:
      return iprot.readBool();
    case BYTE:
      return iprot.readByte();
    case DOUBLE:
      return iprot.readDouble();
    case I16:
      return iprot.readI16();
    case I32:
      return iprot.readI32();
    case I64:
      return iprot.readI64();
    case STRING:
      return iprot.readBinary();
    case STRUCT:
      return readStruct(iprot);
    case MAP:
      return readMap(iprot);
    case SET:
      return readSet(iprot);
    case LIST:
      return readList(iprot);
    case ENUM:
      final int value = iprot.readI32();
      return new TEnum() {
        @Override
        public int getValue() {
          return value;
        }
      };
    case VOID:
      return null;
    default:
      throw new RuntimeException("can't read type " + type);
    }
  }

  private Object readList(TProtocol iprot) throws TException {
    TList tList = iprot.readListBegin();
    List<Object> list = new DynaList(tList);
    readToCollection(iprot, tList.elemType, tList.size, list);
    iprot.readListEnd();
    return list;
  }

  private Object readSet(TProtocol iprot) throws TException {
    TSet tSet = iprot.readSetBegin();
    Set<Object> set = new DynaSet(tSet);
    readToCollection(iprot, tSet.elemType, tSet.size, set);
    iprot.readSetEnd();
    return set;
  }

  private void readToCollection(TProtocol iprot, byte type, int size, Collection<Object> c) throws TException {
    for (int i = 0; i < size; i++) {
      c.add(readValue(type, iprot));
    }
  }

  private Object readMap(TProtocol iprot) throws TException {
    TMap tMap = iprot.readMapBegin();
    Map<Object, Object> map = new DynaMap(tMap);
    for (int i = 0; i < tMap.size; i++) {
      Object key = readValue(tMap.keyType, iprot);
      Object value = readValue(tMap.valueType, iprot);
      map.put(key, value);
    }
    iprot.readMapEnd();
    return map;
  }

  private DynaTBase readStruct(TProtocol iprot) throws TException {
    DynaTBase struct = new DynaTBase();
    struct.read(iprot);
    return struct;
  }

  @Override
  public void write(TProtocol oprot) throws TException {
    oprot.writeStructBegin(new TStruct());
    for (Entry<DynaField, Object> entry : content.entrySet()) {
      writeValue(entry.getKey().getType(), entry.getValue(), oprot);
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  private void writeValue(byte type, Object object, TProtocol oprot) throws TException {
    switch (type) {
    case BOOL:
      oprot.writeBool((Boolean)object);
      break;
    case BYTE:
      oprot.writeByte((Byte)object);
      break;
    case DOUBLE:
      oprot.writeDouble((Double)object);
      break;
    case I16:
      oprot.writeI16((Short)object);
      break;
    case I32:
      oprot.writeI32((Integer)object);
      break;
    case I64:
      oprot.writeI64((Long)object);
      break;
    case STRING:
      oprot.writeString((String)object);
      break;
    case STRUCT:
      ((DynaTBase)object).write(oprot);
      break;
    case MAP:
      writeMap((DynaMap)object, oprot);
      break;
    case SET:
      writeSet((DynaSet)object, oprot);
      break;
    case LIST:
      writeList((DynaList)object, oprot);
      break;
    case ENUM:
      oprot.writeI32(((TEnum)object).getValue());
      break;
    case VOID:
      break;
    default:
      throw new RuntimeException("can't write value for type " + type + " " + object);
    }
  }

  private void writeList(DynaList list, TProtocol oprot) throws TException {
    oprot.writeListBegin(list.getTList());
    writeCollection(list.getTList().elemType, list, oprot);
    oprot.writeListEnd();
  }

  private void writeCollection(byte type, Collection<Object> list, TProtocol oprot) throws TException {
    for (Object object : list) {
      writeValue(type, object, oprot);
    }
  }

  private void writeSet(DynaSet set, TProtocol oprot) throws TException {
    oprot.writeSetBegin(set.getTSet());
    writeCollection(set.getTSet().elemType, set, oprot);
    oprot.writeSetEnd();
  }

  private void writeMap(DynaMap map, TProtocol oprot) throws TException {
    oprot.writeMapBegin(map.getTMap());
    for (Entry<Object, Object> entry : map.entrySet()) {
      writeValue(map.getTMap().keyType, entry.getKey(), oprot);
      writeValue(map.getTMap().valueType, entry.getValue(), oprot);
    }
    oprot.writeMapEnd();
  }

  @Override
  public DynaField fieldForId(int fieldId) {
    return fields.get(fieldId);
  }

  @Override
  public boolean isSet(DynaField field) {
    return content.containsKey(field);
  }

  @Override
  public Object getFieldValue(DynaField field) {
    return content.get(field);
  }

  @Override
  public void setFieldValue(DynaField field, Object value) {
    fields.put(field.getThriftFieldId(), field);
    content.put(field, value);
  }

  @Override
  public DynaTBase deepCopy() {
    DynaTBase copy = new DynaTBase();
    for (Entry<DynaField, Object> entry : content.entrySet()) {
      copy.setFieldValue(entry.getKey(), copy(entry.getKey().getType(), entry.getValue()));
    }
    return copy;
  }

  static Object copy(byte type, Object value) {
    switch (type) {
    case BOOL:
    case BYTE:
    case DOUBLE:
    case I16:
    case I32:
    case I64:
    case STRING:
    case ENUM:
    case VOID:
      return value;
    case STRUCT:
      return ((DynaTBase)value).deepCopy();
    case MAP:
      return ((DynaMap)value).deepCopy();
    case SET:
      return ((DynaSet)value).deepCopy();
    case LIST:
      return ((DynaList)value).deepCopy();
    default:
      throw new RuntimeException("can't clone value for type " + type + " " + value);
    }
  }

  @Override
  public void clear() {
    content.clear();
  }

  static void copyCollection(byte elemType, Collection<Object> from, Collection<Object> to) {
    for (Object object : from) {
      to.add(copy(elemType, object));
    }
  }

  @Override
  public String toString() {
    StringBuilderProtocol p = new StringBuilderProtocol();
    try {
      this.write(p);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
    return p.toString();
  }

}
