package dynathrift.simple;

import static dynathrift.simple.DynaTBase.copyCollection;

import java.util.ArrayList;

import org.apache.thrift.protocol.TList;

public class DynaList extends ArrayList<Object> {
  private static final long serialVersionUID = 1L;

  private final TList tList;

  public DynaList(TList tList) {
    super(tList.size);
    this.tList = tList;
  }

  public TList getTList() {
    return tList;
  }

  public DynaList deepCopy() {
    DynaList copy = new DynaList(tList);
    copyCollection(tList.elemType, this, copy);
    return copy;
  }

}
