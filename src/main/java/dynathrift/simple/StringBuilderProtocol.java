package dynathrift.simple;

import java.nio.ByteBuffer;
import java.util.Stack;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;

import dynathrift.BaseProtocol;

public class StringBuilderProtocol extends BaseProtocol {

  private static enum Container { MESSAGE, STRUCT, LIST, SET, MAP, MAPKEY, MAPVALUE, FIELD };

  private StringBuilder sb = new StringBuilder();
  private int indent = 0;
  private Stack<Container> containers;

  private void writeField(int id, byte type) {
    indent();
    sb.append(id).append("(").append(DynaTBase.getThriftTypeName(type)).append("): ");
  }

  private void indent() {
    for (int i = 0; i < indent; i++) {
      sb.append(" ");
    }
  }

  @Override
  public void writeMessageBegin(TMessage message) throws TException {
    writeField(message.seqid, message.type);
    sb.append("{\n");
    incIndent();
    setContainer(Container.MESSAGE);
  }

  private void setContainer(Container container) {
    containers.push(container);
  }

  private void incIndent() {
    ++ indent;
  }

  @Override
  public void writeMessageEnd() throws TException {
    decIndent();
    sb.append("}\n");
    unsetContainer(Container.MESSAGE);
  }

  private void unsetContainer(Container container) {
    Container popped = containers.pop();
    if (popped != container) {
      throw new RuntimeException("container was " + popped + " instead of expected " + container);
    }
  }

  private void decIndent() {
    -- indent;
  }

  @Override
  public void writeStructBegin(TStruct struct) throws TException {
    indent();
    sb.append("{\n");
    incIndent();
    setContainer(Container.STRUCT);
  }

  @Override
  public void writeStructEnd() throws TException {
    unsetContainer(Container.STRUCT);
    decIndent();
    indent();sb.append("}");
  }

  @Override
  public void writeFieldBegin(TField field) throws TException {
    writeField(field.id, field.type);
    setContainer(Container.FIELD);
  }

  @Override
  public void writeFieldEnd() throws TException {
    unsetContainer(Container.FIELD);
    sb.append(";\n");
  }

  @Override
  public void writeFieldStop() throws TException {
    sb.append("STOP");
  }

  @Override
  public void writeMapBegin(TMap map) throws TException {
    sb.append("[\n");
    setContainer(Container.MAP);
    setContainer(Container.MAPKEY);
  }

  @Override
  public void writeMapEnd() throws TException {
    unsetContainer(Container.MAPVALUE);
    sb.append("]");
  }

  @Override
  public void writeListBegin(TList list) throws TException {
    sb.append("[\n");
    setContainer(Container.LIST);
  }

  @Override
  public void writeListEnd() throws TException {
    unsetContainer(Container.LIST);
    sb.append("]");
  }

  @Override
  public void writeSetBegin(TSet set) throws TException {
    sb.append("[\n");
    setContainer(Container.SET);
  }

  @Override
  public void writeSetEnd() throws TException {
    unsetContainer(Container.SET);
    sb.append("]");
  }

  private void beforeValue() {

  }

  private void afterValue() {
    Container peeked = containers.peek();
    unsetContainer(peeked);
    switch (peeked) {
    case MAPKEY:
      setContainer(Container.MAPVALUE);
      break;
    case MAPVALUE:
      setContainer(Container.MAPKEY);
      break;
    default:
      throw new RuntimeException("we should be in  map: " + peeked);
    }
  }

  @Override
  public void writeBool(boolean b) throws TException {
    beforeValue();
    sb.append(b);
    afterValue();
  }

  @Override
  public void writeByte(byte b) throws TException {
    beforeValue();
    sb.append(b);
    afterValue();
  }

  @Override
  public void writeI16(short i16) throws TException {
    beforeValue();
    sb.append(i16);
    afterValue();
  }

  @Override
  public void writeI32(int i32) throws TException {
    beforeValue();
    sb.append(i32);
    afterValue();
  }

  @Override
  public void writeI64(long i64) throws TException {
    beforeValue();
    sb.append(i64);
    afterValue();
  }

  @Override
  public void writeDouble(double dub) throws TException {
    beforeValue();
    sb.append(dub);
    afterValue();
  }

  @Override
  public void writeString(String str) throws TException {
    beforeValue();
    sb.append(str);
    afterValue();
  }

  @Override
  public void writeBinary(ByteBuffer buf) throws TException {
    beforeValue();
    sb.append("[").append(buf.limit() - buf.position()).append(" bytes]");
    afterValue();
  }

}