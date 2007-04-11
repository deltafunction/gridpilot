package gridpilot;

public class DBRecord{
  
  public String [] fields = null;
  public String [] values = null;
  public static String identifier = null;
  
  public DBRecord(){
    fields = new String [] {""};
  }
  
  public DBRecord(String [] _fields, String [] _values){
    fields = _fields;
    values = _values;
  }

  public DBRecord(String [] _fields, Object [] _values){
    fields = _fields;
    values = new String [_values.length];
    for(int i=0; i<_values.length; i++){
      values[i] = _values.toString();
    }
  }

  public Object getAt(int i){
    return values[i];  
  }
  
  public Object getValue(String col){
    for(int i=0; i<fields.length; i++){
      if(col.equalsIgnoreCase(fields[i])){
        return values[i];
      }
    }
    //return "no such field "+col;
    return "";
  }
  
  public void setValue(String col, /*Object*/ /*force strings*/String val) throws Exception{
     for(int i=0; i<fields.length; i++){
      if(col.equalsIgnoreCase(fields[i])){
        values[i] = val;
        //Debug.debug("Set field "+fields[i]+" to value "+values[i],3);
        // TODO: Should set field to value. Seems not to work
        //DBRecord.class.getField(col).set(this,val);
        return;
      }
    }
    throw new Exception("no such field "+col);
  }
  
  public String toString(){
    return Util.arrayToString(fields)+"-->"+Util.arrayToString(values);
  }
  
}
