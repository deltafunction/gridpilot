package gridpilot;

import java.util.Vector;

//import gridpilot.DBVector;
import gridpilot.Database.DBRecord;

import javax.swing.JLabel;
import javax.swing.table.AbstractTableModel;

/**
 * Extention of AbstractTableModel. <p>
 * Contains (but does not own) a Vector. <p>
 * uses direct sorting instead of redirection table
 */


public class DBVectorTableModel extends AbstractTableModel {

  private static final long serialVersionUID = 1L;
  Vector theRecords;
  //String [] columnNames = {"name","partID","jobID","newStatus","host"};
  String [] columnNames = {""};
  //Class [] columnClasses ={String.class, String.class, String.class, String.class, String.class} ;
  Object [][] values;
  //String [] columnNames;
  Class [] columnClass;
  int [] indexes; // indirection array for sorting

  int sortColumn;
  boolean ascending;

/**
 * Constructors
 */

  /*public DBVectorTableModel() {
    setTable(null, null);
  }*/

  public DBVectorTableModel(Object [][] _values, String [] _columnNames){
    setTable(_values, columnNames);
  }

  public DBVectorTableModel(int rowCount, int colCount){
    Object [][] _values = new Object[rowCount][];
    for(int i=0; i<_values.length; ++i){
      _values[i] = new Object[colCount];
    }
    setTable(_values, new String[colCount]);
  }

  public DBVectorTableModel(String [] _columnNames){
    setTable(new Object[0][], _columnNames);
  }

  public DBVectorTableModel(Vector _theRecords, String [] _columnNames) {
    theRecords = _theRecords;
    columnNames = _columnNames;
    setTable(_theRecords, _columnNames);
  }
  
  public void setColumnNames(String [] _columnNames) {
    columnNames = _columnNames;
  }
  
  public void setRecords(Vector _theRecords) {
    theRecords = _theRecords;
  }

  /**
   * Properties
   */

  public synchronized int getColumnCount() {
    return columnNames.length;
  }

  public synchronized int getRowCount() {
    //return theRecords.size() ;
    return values.length;
  }

  /*synchronized int getRow(int row){
    return row;
  }*/

  synchronized int getRow(int row){
    int i;
    for(i=0; i<indexes.length && indexes[i] != row; ++i)
      ;
    return i;
  }
  public synchronized String getColumnName(int col) {
    return columnNames[col];
  }

  public synchronized String [] getColumnNames(){
    return columnNames;
  }

  /*public synchronized Object getValueAt(int row, int col) {
      return theRecords.get(row).getAt(col);
  }*/

  public synchronized Object getValueAt(int row, int col) {
    return values[indexes[row]][col];
  }
  
  /*public Object getUnsortedValueAt(int row, int col){
    return getValueAt(row,col);
  }*/
  
  public Object getUnsortedValueAt(int row, int col){
    return values[row][col];
  }


  public int getColumnSort(){
    return sortColumn;
  }

  //public synchronized Class getColumnClass(int col){
  //  if(columnClasses[col] != null)
  //    return columnClasses[col];

  //  else return String.class;
  //}

  public boolean isSortAscending(){
    return ascending;
  }

  /**
   * Operations
   */

  synchronized public void setValueAt(Object val, int row, int col){
    values[row][col] = val;
    if(val != null)
      columnClass[col] = val.getClass();
    fireTableCellUpdated(row, col);
  }

  synchronized public void setTable(Object [][] _values, String [] _columnNames){
    if(_values!=null && _values.length!=0 && _values[0]!=null && _columnNames!=null &&
       _values[0].length!= _columnNames.length){
        Debug.debug("ERROR: MyTableModel : column count for " +
            "values and columnNames are different", 1);
        return;
    }

    if(_values == null || _columnNames == null){
      values = new Object[0][];
      columnNames = new String[0];
      theRecords = new Vector();
    }else{
      values = _values;
      columnNames = _columnNames;
      theRecords = new Vector();
      for(int i=0; i<values.length; ++i){
        theRecords.add(new DBRecord(columnNames, values[i]));
      }
      columnClass = new Class[columnNames.length];
      indexes = new int [getRowCount()];
      if(values.length !=0  && values[0] !=null){
        resetIndexes();

        for(int i=0; i< columnClass.length; ++i){
          if(values[0][i] == null)
            columnClass[i] = String.class;
          else
            columnClass[i] = values[0][i].getClass();
        }
      }

    }
    fireTableStructureChanged();
  }

  synchronized public void setTable(String [] _columnNames){
    setTable(new Object[0][], _columnNames);
  }

  /*synchronized public void setTable(DBVector _theRecords){
    Object [][] values = new Object[_theRecords.size()][_theRecords.get(0).fields.length];
    for(int i=0; i<_theRecords.size(); ++i){
      values[i] = _theRecords.get(i).values;
    }
    setTable(values, _theRecords.get(0).fields);
  }*/

  synchronized public void setTable(Vector _theRecords, String [] _columnNames){
    Object [][] values = new Object[_theRecords.size()][_columnNames.length];
    for(int i=0; i<_theRecords.size(); ++i){
      values[i] = ((DBRecord) _theRecords.get(i)).values;
    }
    setTable(values, _columnNames);
  }

  synchronized public void createRows(int r){
    if(getRowCount() == r){
      return;
    }
    else{
      int oldr = getRowCount();
      Object[][] tmpValues = new Object[r][];
      int i;
      for(i=0; i<r && i< oldr; ++i)  // recovering of old rows
        tmpValues[i]=values[i];
      for(;i<r; ++i) // creation of new rows
        tmpValues[i]=new Object[columnNames.length];

    //  for(;i<oldr;++i)
    //  " free tableData[i]"

      indexes = new int [r];
      resetIndexes();
      values = tmpValues;
    }
//    fireTableStructureChanged();*/
  }

  /*FO: this is buggy, doesn't work when sorted on columns. Use removeRow instead*/
  synchronized public void deleteRows(int[] _indexes){
    if (_indexes.length == 0) {
      return;
    }
    else{
      int oldr = getRowCount();
      int newr = oldr-_indexes.length;
      int cur = 0; // current index of row to be removed
      Object[][] tmpValues = new Object[newr][];
      for (int i=0; i<oldr; i++) {
        if (cur<_indexes.length && i==_indexes[cur]) { // skip element
          cur++;
        } else {
          tmpValues[i-cur]=values[i];
        }
      }
      indexes = new int [newr];
      resetIndexes();
      values = tmpValues;
    }
  }

  /**
   * @see Table#removeRow
   */
  synchronized public void removeRow(int r){
    if(r >= getRowCount()){
      return;
    }
    else{
     Object[][] tmpValues = new Object[getRowCount()-1][];
      int i;
      for(i=0; i<r; ++i)  // recovering of old rows
        tmpValues[i]=values[i];
      for(i=r+1;i<getRowCount(); ++i)
        tmpValues[i-1]=values[i];

      //indexes = new int [getRowCount()-1];
      values = tmpValues;
      //setTable(values, this.columnNames);
      resetIndexes();
    }
  }

  /*
  synchronized public void deleteRow(int index){
    deleteRows(new int[
    int newr = getRowCount()-1;
    Object[][] tmpValues = new Object[newr][];
    for (int i=0; i<newr; i++) {
      if (i<index) {
        tmpValues[i]=values[i];
      } else if (i>index) {
        tmpValues[i]=values[i+1];
      }
    }
    indexes = new int [newr];
    resetIndexes();
    values = tmpValues;
  }


  /**
   * Sorts the table, based on specified column
   */
  synchronized public void sort(int col, boolean _ascending){
    Debug.debug("sort " + col + ", "+ _ascending, 2);
    ascending = _ascending;
    sortColumn = col;
    int begin;
    int end;
    int step;
    DBRecord a,b ;
    if(_ascending){
      begin = 0;
      end = getRowCount();
      step = 1;
    }
    else{
      begin = getRowCount()-1;
      end = -1;
      step = -1;
    }

    for(int i=begin; i!=end; i+=step){
      int iMin = i;
      for(int j=i+step; j!=end; j+=step)
        if(isGreaterThan(getValueAt(iMin, col), getValueAt(j, col)))
           iMin = j;
      if(iMin != i) {
        swapRows(i, iMin);
        Debug.debug("Record "+theRecords.size()+" : "+i, 3);
        a = (DBRecord) theRecords.get(i);
        b = (DBRecord) theRecords.get(iMin);
        theRecords.setElementAt(b,i);
        theRecords.setElementAt(a,iMin);
      }
    }
    fireTableDataChanged();
  }

  private boolean isGreaterThan(Object o1, Object o2){
    if(o1 == null)
      return false;
    if(o2 == null)
      return true;
    if(o1 instanceof Integer)
      return (new Integer(o1.toString()).intValue() >
             new Integer(o2.toString()).intValue());
    if(o1 instanceof JLabel)
      return ((JLabel)o1).getText().compareTo(((JLabel)o2).getText()) >0;
    return o1.toString().compareTo(o2.toString()) > 0;
  }
  /**
   * Swaps row i and j (via <code>indexes</code>)
   */
  private void swapRows(int i, int j){
    int tmp = indexes[i];
    indexes[i] = indexes[j];
    indexes[j] = tmp;
  }

  public int [] reorder(int [] rows){
    int [] res = new int [rows.length];
    for(int i=0;i<res.length; ++i){
      if(rows[i]<indexes.length)
        res[i] = indexes[rows[i]];
      else{
        Debug.debug("reorder : index out of bound (" + rows[i] +">=" + indexes.length + ")", 3);
        new Exception().printStackTrace();
        return new int[0];
      }
    }
    return res;
  }

  /**
   * Private methods
   */
  /**
   * Reinit indexes
   */
  private void resetIndexes(){
    if(indexes!=null){
      for(int i=0; i<indexes.length; ++i)
        indexes[i] = i;
  //    fireTableDataChanged();
    }
  }

}