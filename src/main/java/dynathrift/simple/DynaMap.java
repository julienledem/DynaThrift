package dynathrift.simple;

import static dynathrift.simple.DynaTBase.copy;

import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.protocol.TMap;

public class DynaMap extends HashMap<Object, Object> {
  private static final long serialVersionUID = 1L;

  private final TMap tMap;

  public DynaMap(TMap tMap) {
    super(tMap.size);
    this.tMap = tMap;
  }

  public TMap getTMap() {
    return tMap;
  }

  public DynaMap deepCopy() {
    DynaMap copy = new DynaMap(tMap);
    for (Map.Entry<Object, Object> entry : this.entrySet()) {
      copy.put(
          copy(tMap.keyType, entry.getKey()),
          copy(tMap.valueType, entry.getValue()));
    }
    return copy;
  }

}
