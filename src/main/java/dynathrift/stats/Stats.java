package dynathrift.stats;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TType;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class Stats {

  public class FieldStat {

    private final List<FieldStat> fields = new ArrayList<FieldStat>();
    private FieldStat listItems = null;
    private FieldStat setItems = null;
    private FieldStat mapKeys = null;
    private FieldStat mapValues = null;
    private final long[] valueCountPerType = new long[Byte.MAX_VALUE + 1];

    public Map<Integer, FieldStat> getFields() {
      Map<Integer, FieldStat> result = new HashMap<Integer, Stats.FieldStat>();
      for (int i = 0; i < fields.size(); i++) {
        FieldStat field = fields.get(i);
        if (field != null) {
          result.put(i, field);
        }
      }
      if (result.size() > 0) {
        return result;
      }
      return null;
    }

    public FieldStat getListItems() {
      return listItems;
    }

    public FieldStat getSetItems() {
      return setItems;
    }

    public FieldStat getMapKeys() {
      return mapKeys;
    }

    public FieldStat getMapValues() {
      return mapValues;
    }

    public Map<String, Long> getValueCountPerType() {
      Map<String, Long> result = new HashMap<String, Long>();
      for (int i = 0; i < valueCountPerType.length; i++) {
        long l = valueCountPerType[i];
        if (l > 0) {
          result.put(typeName((byte)i), l);
        }
      }
      if (result.size() > 0) {
        return result;
      }
      return null;
    }

    private FieldStat getFieldStat(short id) {
      int index = (char)id;
      while (fields.size() <= index) {
        fields.add(null);
      }
      FieldStat fieldStat = fields.get(index);
      if (fieldStat == null) {
        fieldStat = new FieldStat();
        fields.set(index, fieldStat);
      }
      return fieldStat;
    }

    private void countStruct(TProtocol iprot) throws TException {
      iprot.readStructBegin();
      TField field;
      while ((field = iprot.readFieldBegin()).type != TType.STOP) {
        countField(field.id, field.type, iprot);
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
    }

    private void countField(short id, byte type, TProtocol iprot) throws TException {
      FieldStat s = getFieldStat(id);
      s.countValue(type, iprot);
    }

    public void countValue(byte type, TProtocol iprot) throws TException {
      valueCountPerType[type] += 1;
      switch (type) {
      case BOOL:
        iprot.readBool();
        break;
      case BYTE:
        iprot.readByte();
        break;
      case DOUBLE:
        iprot.readDouble();
        break;
      case I16:
        iprot.readI16();
        break;
      case I32:
        iprot.readI32();
        break;
      case I64:
        iprot.readI64();
        break;
      case STRING:
        iprot.readBinary();
        break;
      case STRUCT:
        countStruct(iprot);
        break;
      case MAP:
        countMap(iprot);
        break;
      case SET:
        countSet(iprot);
        break;
      case LIST:
        countList(iprot);
        break;
      case ENUM:
        iprot.readI32();
        break;
      case VOID:
        break;
      default:
        throw new RuntimeException("unknown type " + type);
      }
    }

    private void countList(TProtocol iprot) throws TException {
      TList tList = iprot.readListBegin();
      if (listItems == null) {
        listItems = new FieldStat();
      }
      countCollection(iprot, tList.elemType, tList.size, listItems);
      iprot.readListEnd();
    }

    private void countSet(TProtocol iprot) throws TException {
      TSet tSet = iprot.readSetBegin();
      if (setItems == null) {
        setItems = new FieldStat();
      }
      countCollection(iprot, tSet.elemType, tSet.size, setItems);
      iprot.readSetEnd();
    }

    private void countCollection(TProtocol iprot, byte type, int size, FieldStat items) throws TException {
      for (int i = 0; i < size; i++) {
        items.countValue(type, iprot);
      }
    }

    private void countMap(TProtocol iprot) throws TException {
      TMap tMap = iprot.readMapBegin();
      if (mapKeys == null) {
        mapKeys = new FieldStat();
      }
      if (mapValues == null) {
        mapValues = new FieldStat();
      }
      for (int i = 0; i < tMap.size; i++) {
        mapKeys.countValue(tMap.keyType, iprot);
        mapValues.countValue(tMap.valueType, iprot);
      }
      iprot.readMapEnd();
    }

    @Override
    public String toString() {
      List<String> result = new ArrayList<String>();
      result.add("valueCountPerType=" + valueCountPerTypeToString());
      if (fields.size() > 0) {
        result.add("fields=" + fieldsToString());
      }
      addToString(result, "listItems", listItems);
      addToString(result, "setItems", setItems);
      addToString(result, "mapKeys", mapKeys);
      addToString(result, "mapValues", mapValues);
      return "FieldStat " + result;
    }

    private String typeName(byte type) {
      switch (type) {
      case BOOL:
        return "bool";
      case BYTE:
        return "byte";
      case DOUBLE:
        return "double";
      case I16:
        return "i16";
      case I32:
        return "i32";
      case I64:
        return "i64";
      case STRING:
        return "string";
      case STRUCT:
        return "struct";
      case MAP:
        return "map";
      case SET:
        return "set";
      case LIST:
        return "list";
      case ENUM:
        return "enum";
      case VOID:
        return "void";
      default:
        return "unknown:"+type;
      }
    }

    private String valueCountPerTypeToString() {
      List<String> result = new ArrayList<String>();
      for (int i = 0; i < valueCountPerType.length; i++) {
        long l = valueCountPerType[i];
        if (l > 0) {
          result.add(typeName((byte)i) + "=" + l);
        }
      }
      return result.toString();
    }

    private void addToString(List<String> result, String name, FieldStat field) {
      if (field != null) {
        result.add(name + "=" + field.toString());
      }
    }

    private String fieldsToString() {
      List<String> result = new ArrayList<String>();
      for (int i = 0; i < fields.size(); i++) {
        addToString(result, String.valueOf(i), fields.get(i));
      }
      return result.toString();
    }

  }

  private final FieldStat root = new FieldStat();

  public void countOne(TProtocol iprot) throws TException {
    root.countValue(STRUCT, iprot);
  }

  @Override
  public String toString() {
    return "Stats [" + root + "]";
  }

  public String toJson() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.setSerializationInclusion(Include.NON_NULL);
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    try {
      return mapper.writer().writeValueAsString(root);
    } catch (JsonGenerationException e) {
      throw new RuntimeException(e);
    } catch (JsonMappingException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
