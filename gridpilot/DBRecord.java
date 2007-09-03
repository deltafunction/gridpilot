package gridpilot;

/**
 * Abstract representation of database records,
 * regardless of actual backend.
 */
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
    return "";
  }
  
  public void setValue(String col, /*Object*/ /*force strings*/String val) throws Exception{
     for(int i=0; i<fields.length; i++){
      if(col.equalsIgnoreCase(fields[i])){
        values[i] = val;
        return;
      }
    }
    throw new Exception("no such field "+col);
  }
  
  /**
   * Removes all values in a given column.
   * @param col column/field name
   */
  public void remove(String col){
    String [] newFields = new String[fields.length-1];
    String [] newValues = new String[values.length-1];
    int j = 0;
    for(int i=0; i<fields.length; i++){
      if(!col.equalsIgnoreCase(fields[i])){
        newFields[j] = fields[i];
        newValues[j] = values[i];
        ++j;
      }
      else{
        continue;
      }
    }
    fields = newFields;
    values = newValues;
  }
  
  public String toString(){
    return Util.arrayToString(fields)+"-->"+Util.arrayToString(values);
  }
  
}
