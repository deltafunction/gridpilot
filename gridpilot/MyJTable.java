package gridpilot;

import gridfactory.common.DBVectorTableModel;
import gridfactory.common.Debug;
import gridfactory.common.Table;

import javax.swing.table.*;
import javax.swing.event.*;
import java.awt.event.*;
import java.awt.Component;
import javax.swing.*;

import java.awt.*;
import java.util.Vector;

/**
 * Extension of class JTable, which contains its TableModel.
 * Allow sorting, hiding of columns or rows, support String, Label, ImageIcon, JTextPane.
 * When the table is sorted, it is only a "view" ; values are not really sorted.
 *
 */

public class MyJTable extends JTable implements Table {
  
  private static final long serialVersionUID = 1L;
  private ListSelectionListener lsl;
  private String [] hide;
  private String [] colorMapping;
  //private int timeOut = 10*1000;

  public DBVectorTableModel tableModel;

  /** Popup menu shown when the user right-clicks on this table */
  private JPopupMenu popupMenu = new JPopupMenu();

  /** Menu in popupMenu showing all column names allowing to sort this table*/
  private JMenu menuSort = new JMenu("Sort by");
  private int maxItem = 20;
  /** Menu in popupMenu showing all column names allowing to hide or sort some column*/
  private JMenu menuShow = new JMenu("Show columns");

  /** Minimun column witdh for a shown column */
  private int minColumnWitdh = 10;

  /** Button group which contains all item from <code>menuSort</code> */
  private ButtonGroup bgSort = new ButtonGroup();

  /** TableCellRenderer of this table*/
  private TableCellRenderer tableCellRenderer = new DefaultTableCellRenderer(){

    private static final long serialVersionUID = 1L;

    public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected,
        boolean hasFocus, int row, int column){
      synchronized(table){

        if(colorMapping!=null  && value!=null && (
            table.getColumnName(column).toLowerCase().endsWith("status") ||
            table.getColumnName(column).toLowerCase().endsWith("state"))){
          for(int i=0; i<colorMapping.length/2; ++i){
            if(value.toString().equalsIgnoreCase(colorMapping[2*i])){
              JLabel l = new JLabel(value.toString());
              l.setForeground(new Color(Integer.parseInt(colorMapping[2*i+1], 16 )));
              if(isSelected){
                l.setBackground(table.getSelectionBackground());
                l.setOpaque(true);
              }
              return l;
            }
          }
        }

        if(value instanceof JLabel){          
          JLabel l = (JLabel) value;
          
          if(isSelected){
            l.setBackground(table.getSelectionBackground());
            l.setOpaque(true);
          }
          else
            l.setOpaque(false);
          return l;
        }

        if(value instanceof ImageIcon){
          JLabel l = new JLabel((ImageIcon) value);
          if(isSelected){
            l.setBackground(table.getSelectionBackground());
            l.setOpaque(true);
          }
          else
            l.setOpaque(false);

          return l;
        }

        if(value instanceof JTextPane){
          JTextPane tp = (JTextPane) value;
          int rowHeigth = (int)tp.getPreferredSize().getHeight();
          if(table.getRowHeight(row) != rowHeigth)
            table.setRowHeight(row, rowHeigth);
          return tp;
        }

        return super.getTableCellRendererComponent(table, value, isSelected,
            hasFocus, row, column);
      }
  }};


  /**
  * Constructs an empty table.
  */
/*  public Table(){
    tableModel = new DBVectorTableModel();
    setModel(tableModel);
    
    initTable();
  }*/

  /**
   * Constructs an empty table with the columns hide hidden.
   * @throws Exception 
   */

  public MyJTable(String [] _hide, String [] fieldNames) throws Exception{
    tableModel = new DBVectorTableModel(fieldNames);
    setModel(tableModel);
    hide = _hide;
    initTable();
  }

  /**
   * Constructs an empty table with the columns hide hidden and
   * colored according to colorMapping.
   * @throws Exception 
   */

  public MyJTable(String [] _hide,
               String [] fieldNames,
               String [] _colorMapping) throws Exception{
    tableModel = new DBVectorTableModel(fieldNames);
    setModel(tableModel);
    hide = _hide;
    colorMapping = _colorMapping;
    initTable();
  }

  /**
   * Constructs an empty table.
   */
  /* public Table(MyTableModel _model){
     tableModel = _model;
     setModel(_model);
     initTable();
   }*/

   /**
    * Constructs a table.
    */
    public MyJTable(DBVectorTableModel _model){
      tableModel = _model;
      setModel(_model);
      initTable();
    }

    /**
     * Constructs a with the columns hide hidden.
     */
     public MyJTable(DBVectorTableModel _model, String [] _hide){
       tableModel = _model;
       setModel(_model);
       hide = _hide;
     }

  /**
   * Constructs a table, with rowCount rows and colCount.
   * @throws Exception 
   */
  public MyJTable(int rowCount, int colCount) throws Exception{
    tableModel = new DBVectorTableModel(rowCount, colCount);
    setModel(tableModel);
    
    initTable();
  }

  /**
   * Constructs a table with 0 rows, where column titles are in <code>columnNames</code>.
   * @throws Exception 
   */
  public MyJTable(String [] columnNames) throws Exception{
    tableModel = new DBVectorTableModel(columnNames);

    setModel(tableModel);
    initTable();
  }


  /**
   * Constructs a table using values in values, and where column titles are in columnNames.
   * If length of values and columnNames are incompatibles,
   * an empty table is created.
   * @throws Exception 
   */
  public MyJTable(Object [][] values, String [] columnNames) throws Exception{
    tableModel = new DBVectorTableModel(values, columnNames);
    setModel(tableModel);

    initTable();
  }

  /**
   * Sets values and column names to this table, using values and columnNames.
   * If length of values and columnNames are incompatible,
   * nothing is changed
   * @throws Exception 
   */
  public synchronized void setTable(final Object [][] values, final String [] columnNames) throws Exception{
    if(SwingUtilities.isEventDispatchThread()){
      doSetTable(values, columnNames);
    }
    else{
      SwingUtilities.invokeLater(
        new Runnable(){
          public void run(){
            try{
              doSetTable(values, columnNames);
            }
            catch(Exception ex){
              Debug.debug("Could not create panel ", 1);
              ex.printStackTrace();
            }
          }
        }
      );
    }
  }
  
  private void doSetTable(Object [][] values, String [] columnNames) throws Exception{
    Debug.debug("DBVectorTableModel.setTable", 2);
    tableModel.setTable(values, columnNames);
    //createMenu();
    
    // createMenu() sometimes freezes the whole UI. Don't know why.
    // And all seems to work fine without...
    /*MyThread t = new MyThread(){
      public void run(){
        try{
          createMenu();
        }
        catch(Throwable t){
          Debug.debug(
              (t instanceof Exception ? "Exception" : "Error") +
                             " from Table.setTable " +
                             " during createMenu", 1);
        }
      }
    };

    t.start();

    Util.waitForThread(t, "", timeOut, "createMenu");*/
    
    //Debug.debug("updateUI", 2);
    //updateUI();
    Debug.debug("setTable done", 2);
  }

  /**
   * Sets column names of this table, using columnNames. 
   * The "new" table has 0 rows
   * @throws Exception 
   */

  synchronized public void setTable(String [] columnNames) throws Exception{
    tableModel.setTable(columnNames);
    Debug.debug("creating menu", 2);
    createMenu();
  }

  /**
   * Hides the last columns of this table, and removes them from sort- and show-menu.
   */
  public void hideLastColumns(int nrCols){
    if(tableModel.getColumnCount()>nrCols){
      for (int i=0; i<nrCols; i++){
        hideColumn(tableModel.getColumnCount()-(i+1));
        removeLastSubItem(menuShow);
        removeLastSubItem(menuSort);
      }
    }
  }

  public void removeLastSubItem(JMenu m){
    JMenuItem i;
    do{
      i = m.getItem(m.getItemCount()-1);
      if(i instanceof JMenu)
        m = (JMenu) i;
      else
        break;
    }while(true);
    m.remove(i);
  }

  /**
   * Returns an int array which contains all selected rows.
   * The rows are real, that is, corresponding to the real indexes in values set by user,
   * even if this table has been sorted.
   *
   */
  public synchronized int [] getSelectedRows(){
    return tableModel.reorder(super.getSelectedRows());
  }

  /**
   * Returns the first selected row, or -1 if there is not any.
   */
  public synchronized int getSelectedRow(){
    int selRow = super.getSelectedRow();
//    return (selRow==-1) ? -1 : tableModel.indexes[selRow];
      return (selRow==-1 || selRow>=getRowCount()) ? -1 : tableModel.indexes[selRow];
  }

  /**
   * Returns the column names.
   */
  public synchronized String[] getColumnNames(){
    return tableModel.getColumnNames();
  }

  /**
   * Sets value at row, col.
   *
   */
  public synchronized void setValueAt(final Object value, final int row, final int col){
    if(SwingUtilities.isEventDispatchThread()){
      doSetValueAt(value, row, col);
    }
    else{
      SwingUtilities.invokeLater(
        new Runnable(){
          public void run(){
            try{
              doSetValueAt(value, row, col);
            }
            catch(Exception ex){
              Debug.debug("Could not set value.", 1);
              ex.printStackTrace();
            }
          }
        }
      );
    }
  }

  private void doSetValueAt(Object value, int row, int col) {
    tableModel.setValueAt(value, row, col);
    repaint();//getCellRect(row, col, true));
    //getUI().installUI(this);
    //updateUI();
    revalidate();
  }

  /**
   * Returns the object which is shown at row,col ,
   * that is, depending of the sort (but not hide/shown column).
   */
  public synchronized Object getUnsortedValueAt(int row, int col){
    return tableModel.getUnsortedValueAt(row, col);
  }

  /**
   * Returns the object wich is really at row, col.
   */
  public synchronized Object getValueAt(int row, int col){
    try{
      return tableModel.getValueAt(row, col);
    }
    catch(Exception e){
      return null;
    }
  }

  public synchronized Object[][] getValues(){
    return tableModel.getValues();
  }
  
  /**
   * Returns the table model.
   * For some reason it crashes startup when there...
   */
  /*public synchronized TableModel getModel(){
    return tableModel;
  }*/

  /**
   * Creates r rows in this table.
   * All possible current values are kept ; if r is greater than the current number of rows,
   * some rows are appened, otherwise, last rows are deleted.
   */
  public synchronized void createRows(final int r){
    if(SwingUtilities.isEventDispatchThread()){
      doCreateRows(r);
    }
    else{
      SwingUtilities.invokeLater(
        new Runnable(){
          public void run(){
            try{
              doCreateRows(r);
            }
            catch(Exception ex){
              Debug.debug("Could not create panel ", 1);
              ex.printStackTrace();
            }
          }
        }
      );
    }
  }
  
  public synchronized void doCreateRows(final int r){
    clearSelection();
    tableModel.createRows(r);
    revalidate();
    //updateUI();
  }

  /**
   * Delete rows from this table.
   */
  public synchronized void deleteRows(int[] indexes){
    clearSelection();
    for(int i=0; i<indexes.length; ++i){
      tableModel.removeRow(indexes[i]);
    }
    revalidate();
  }
  
  synchronized public void deleteRows(Vector vecIndexes){
    int[] indexes = new int[vecIndexes.size()];
    for (int i=0; i<vecIndexes.size(); i++){
      indexes[i] = ((Integer)vecIndexes.elementAt(i)).intValue();
    }
    clearSelection();
    for(int i=0; i<indexes.length; ++i){
      tableModel.removeRow(indexes[i]);
    }
    revalidate();
  }

  /**
   * Removes a row in this table. <br>
   * All possible current values are kept.
   */
  public synchronized void removeRow(int r){
    tableModel.removeRow(r);
    //revalidate();
  }
  
  public synchronized TableCellRenderer getCellRenderer(int row, int column){
    return tableCellRenderer;
  }

  /**
   * Adds a menu separator in the menu shown when user right-clicks on this table
   */
  public void addMenuSeparator(){
    popupMenu.addSeparator();
  }

  /**
   * Adds a item in the menu shown when user right-clicks on this table
   */
  public void addMenuItem(JMenuItem menuItem){
    popupMenu.add(menuItem);
  }

  /**
   * Shows the (hidden) column at index col
   * Called when user chooses to show this column in popup menu
   */
  public synchronized void showColumn(int col){
    getColumnModel().getColumn(col).sizeWidthToFit();
    int width = getColumnModel().getTotalColumnWidth()/getColumnCount();
    getColumnModel().getColumn(col).setMaxWidth(getColumnModel().getTotalColumnWidth());
    getColumnModel().getColumn(col).setMinWidth(minColumnWitdh);
    getColumnModel().getColumn(col).setPreferredWidth(width);
    updateUI();
  }

  /**
   * Hides the (shown) column at index col.
   * Called when user chooses to hide this column in popup menu
   */
  public synchronized void hideColumn(int col){
    getColumnModel().getColumn(col).setMinWidth(0);
    getColumnModel().getColumn(col).setMaxWidth(0);
    updateUI();
  }

  /**
   * Hides the specified row.
   * Also saves the id in a map for further "undeletion".
   */
  public void hideRow(int row){
    Debug.debug("row nr:"+row, 2);
    Debug.debug("tableModel.getValueAt(row,1):"+tableModel.getValueAt(row,1), 2);
    Debug.debug("tableModel.getRow(row):"+tableModel.getRow(row), 2);
    Debug.debug("--> tableModel.getUnsortedValueAt(row,1):"+tableModel.getUnsortedValueAt(row,1), 2);
    setRowHeight(tableModel.getRow(row), 1);
  }

  /**
   * Shows the specified job given the partition id.
   */
  public void showRow(int row){
    setRowHeight(tableModel.getRow(row), getRowHeight());
  }

  /**
   * Shows all rows.
   */
  public void showAllRows(){
    setRowHeight(getRowHeight());
  }

  private void initTable(){
    getTableHeader().setReorderingAllowed(false);
    Debug.debug("Hiding fields "+hide.length, 3);
    createMenu();
    initListeners();
  }

  private void initListeners(){
//    removeAll();

    this.addMouseListener(new java.awt.event.MouseAdapter(){
      public void mousePressed(MouseEvent e){
        if (e.getButton() != MouseEvent.BUTTON1){ // right button

          int row = rowAtPoint(e.getPoint());
          if(!isRowSelected(row))
            changeSelection(row, columnAtPoint(e.getPoint()), false, false);

          popupMenu.show(e.getComponent(), e.getX(), e.getY());

        }
      }
    });

    this.getTableHeader().addMouseListener(new java.awt.event.MouseAdapter (){
      public void mouseClicked(MouseEvent e){
        if(e.getButton()==MouseEvent.BUTTON1){ // left button
          int column = columnAtPoint(e.getPoint());//getTableHeader().getColumnModel().getColumnIndexAtX(e.getX());
          if(tableModel.getColumnSort()==column)
            tableModel.sort(column, !tableModel.isSortAscending());
          else
            tableModel.sort(column, true);
          // select item in menuSort
          int i = column;
          JMenu m = menuSort;
          Debug.debug("sort : " + i, 2);
          while(i>=maxItem){
            m = (JMenu) m.getItem(maxItem);
            i = i-maxItem;
            Debug.debug(i + ",  " + m.getText(), 2);
          }
          Debug.debug("select " + i, 2);
          m.getItem(i).setSelected(true);
        }
        else{ // right button
          popupMenu.show(e.getComponent(), e.getX(), e.getY());
        }
    }});
  }

  /**
   * Creates the menu shown when the user right-clicks on this table.
   * The first menu allows to select the sort column.
   * This first menu contains a radio button for each column name.
   * The second menu allows to hide or to show some columns.
   * This second menu contains a check box for each column name.
   * In both of these menus, if the number of item is to large, the menu is divided
   * in several block, each of them being accessible via the last item of the previous block.
   *
   * This menu contains also an item allowing to hide all selected rows, and an item
   * allowing to shows all rows.
   *
   */
  private void createMenu(){

      popupMenu.removeAll();
      menuSort.removeAll();
      menuShow.removeAll();

    // menuSort initialisation

    JMenu currentMenu = menuSort;

    for(int i=0; i<tableModel.getColumnCount() ;){
      for(int j=0; j<maxItem && i<tableModel.getColumnCount() ; ++i, ++j){
        JRadioButtonMenuItem item = new JRadioButtonMenuItem(tableModel.getColumnName(i));
        item.setMnemonic(i);
        item.addActionListener(new ActionListener(){
          public void actionPerformed(ActionEvent e){
            changeSort(e);
        }});
        currentMenu.add(item);
        bgSort.add(item);

      }
      if(i!=tableModel.getColumnCount()){
        JMenu tmp = new JMenu(">>>");
        currentMenu.add(tmp);
        currentMenu = tmp;
      }

    }
    
    if(GridPilot.ADVANCED_MODE){
      popupMenu.add(menuSort);
    }
    

    // menuShow initialisation

    currentMenu = menuShow;

    boolean show = true;
    
    for(int i=0; i<tableModel.getColumnCount() ; ++i){
      for(int j=0; j<maxItem && i<tableModel.getColumnCount() ; ++i, ++j){
        JCheckBoxMenuItem item = new JCheckBoxMenuItem(tableModel.getColumnName(i));
        item.setMnemonic(i);
        item.addActionListener(new ActionListener(){
          public void actionPerformed(ActionEvent e){
            changeShow(e);
        }});
        
        show = true;
        //Debug.debug("Hide fields "+hide.length, 3);
        for(int k=0; k<hide.length; ++k){
          //Debug.debug("Checking fields "+hide[k]+"<->"+tableModel.getColumnName(i), 3);
          if(hide[k].equalsIgnoreCase(tableModel.getColumnName(i))){
            show = false;
            break;
          }
        }
        
        if(show){
          item.setSelected(true);
        }
        else{
          hideColumn(i);
        }
        currentMenu.add(item);

      }
      if(i!=tableModel.getColumnCount()){
        JMenu tmp = new JMenu(">>>");
        currentMenu.add(tmp);
        currentMenu = tmp;
      }
    }

    if(GridPilot.ADVANCED_MODE){
      popupMenu.add(menuShow);
    }
/*
    popupMenu.addSeparator();

    JMenuItem miHideRows = new JMenuItem("Hide rows");
    miHideRows.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        int [] selectedRows = getSelectedRows();
        for(int i=0; i<selectedRows.length ; ++i){
          //hideRow(selectedRows[i]);
          ;
        }
      }
    });

    popupMenu.add(miHideRows);

    JMenuItem miShowAllRows = new JMenuItem("Show all rows");
    miShowAllRows.addActionListener(new ActionListener (){
      public void actionPerformed(ActionEvent e){
        showAllRows();
      }
    });

    popupMenu.add(miShowAllRows);
*/
  }

  /**
   * Called when an item is selected in menu sort.
   */
  private void changeSort(ActionEvent e){
    int column = ((JRadioButtonMenuItem)e.getSource()).getMnemonic();
    if(tableModel.getColumnSort()==column){
      tableModel.sort(column, !tableModel.isSortAscending());
    }
    else{
      tableModel.sort(column, true);
    }
  }

  public void addListSelectionListener(ListSelectionListener _lsl){
    getSelectionModel().addListSelectionListener(_lsl);
    lsl = _lsl;
  }

  /**
   * Forces processing of ListSelelectionEvent
   */
  public synchronized void updateSelection(){
    lsl.valueChanged(new ListSelectionEvent(getSelectionModel(), 0, getRowCount(), false));
  }

  /**
   * Called when an item in menu Show is selected
   */
  private void changeShow(ActionEvent e){
    JCheckBoxMenuItem cbmu = (JCheckBoxMenuItem)e.getSource();
    Debug.debug("Show " + cbmu.getMnemonic(), 1);
    if(cbmu.isSelected()){
      showColumn(cbmu.getMnemonic());
    }
    else{
      hideColumn(cbmu.getMnemonic());
    }
  }

  public synchronized void paintImmediately(int x,int y,int w, int h){
    try{
      super.paintImmediately(x, y, w, h);
    }
    catch(Exception e){
      GridPilot.getClassMgr().getLogFile().addMessage(" Exception in paintImmediately(" + x + ", " + y + ", " +
                         w + ", " + h +")", e);
      e.printStackTrace();
    }
  }

  public synchronized Graphics getGraphics(){
    Graphics g = super.getGraphics();
    if(g==null){
      Debug.debug("getGraphics : g==null", 3);
      g = new JTable().getGraphics();
    }
    return g;
  }
}
