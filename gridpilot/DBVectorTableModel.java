package gridpilot;

import gridpilot.DBVector;
import gridpilot.Database.DBRecord;

import javax.swing.table.*;
import javax.swing.JLabel;

/**
 * Extention of AbstractTableModel. <p>
 * Contains (but does not own) a DBVector. <p>
 * uses direct sorting instead of redirection table
 */


public class DBVectorTableModel extends AbstractTableModel {

  DBVector theRecords;
  //String [] columnNames = {"name","partID","jobID","newStatus","host"};
  String [] columnNames = {""};
  //Class [] columnClasses ={String.class, String.class, String.class, String.class, String.class} ;

  int sortColumn;
  boolean ascending;

/**
 * Constructors
 */

  public DBVectorTableModel(DBVector _theRecords) {
    theRecords = _theRecords;
  }

  public DBVectorTableModel(DBVector _theRecords, String [] _columnNames) {
    theRecords = _theRecords;
    columnNames = _columnNames;
  }
  
  public void setColumnNames(String [] _columnNames) {
    columnNames = _columnNames;
  }
  
  public void setRecords(DBVector _theRecords) {
    theRecords = _theRecords;
  }

  /**
   * Properties
   */

  public synchronized int getColumnCount() {
    return columnNames.length;
  }

  public synchronized int getRowCount() {
    return theRecords.size() ;
  }

  synchronized int getRow(int row){
    return row;
  }

  public synchronized String getColumnName(int col) {
    return columnNames[col];
  }

  public synchronized String [] getColumnNames(){
    return columnNames;
  }

  public synchronized Object getValueAt(int row, int col) {
      return theRecords.get(row).getAt(col);
  }

  public Object getUnsortedValueAt(int row, int col){
    return getValueAt(row,col);
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
   * Sorts the table, based on specified column
   */
  synchronized public void sort(int col, boolean _ascending){
    Debug.debug2("sort " + col + ", "+ _ascending);
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
        //swapRows(i, iMin);
        a = theRecords.get(i);
        b = theRecords.get(iMin);
        theRecords.setRecordAt(b,i);
        theRecords.setRecordAt(a,iMin);
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

}