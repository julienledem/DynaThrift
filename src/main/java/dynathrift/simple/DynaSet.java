package dynathrift.simple;

import static dynathrift.simple.DynaTBase.copyCollection;

import java.util.HashSet;

import org.apache.thrift.protocol.TSet;

public class DynaSet extends HashSet<Object> {
  private static final long serialVersionUID = 1L;

  private final TSet tSet;

  public DynaSet(TSet tSet) {
    super(tSet.size);
    this.tSet = tSet;
  }

  public TSet getTSet() {
    return tSet;
  }

  public DynaSet deepCopy() {
    DynaSet copy = new DynaSet(tSet);
    copyCollection(tSet.elemType, this, copy);
    return copy;
  }

}
