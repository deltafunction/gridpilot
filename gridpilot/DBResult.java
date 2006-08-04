package gridpilot;

public class DBResult{
  
  public String[]    fields;
  public Object[][]  values;

  public DBResult(int nrFields, int nrValues){
    fields = new String [nrFields];
    values = new String [nrValues][nrFields];
  }

  public DBResult(String[] _fields, String[][] _values) {
    fields = _fields;
    values = _values;
  }

  public DBResult(){
    String [] f = {};
    String [] [] v = {};
    fields = f;
    values = v;
  }

  public Object getAt(int row, int column){
    if (row>values.length-1){
      return "no such row";
    }
    if(column>values[0].length-1){
      return "no such column";
    }
    return values[row][column];
  }

  public Object getValue(int row, String col){
    if(row>values.length-1){
      return "no such row";
    }
    Debug.debug("fields: "+Util.arrayToString(fields), 3);
    for(int i=0; i<fields.length; i++){
      Debug.debug("checking value "+values[row][i], 3);
      if(col.equalsIgnoreCase(fields[i])){
        return values[row][i];
      }
    }
    return "no such field";
  }

  public DBRecord getRow(int row){
    DBRecord ret = new DBRecord();
    if(row>values.length-1){
      return ret;
    }
    ret.fields = this.fields;
    ret.values = this.values[row];
    return ret;
  }

  public boolean setValue(int row, String col, String value){
    if(row>values.length-1){
      return false;
    }
    for(int i=0; i<fields.length; i++){
      if(col.equalsIgnoreCase(fields[i])){
        values[row][i] = value;
        return true;
      }
    }
    return false;
  }
}