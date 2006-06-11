package gridpilot;

import gridpilot.Table;
import gridpilot.Database.DBResult;
import gridpilot.Database.DBRecord;
import gridpilot.SelectPanel.SPanel;
import gridpilot.Debug;
import gridpilot.GridPilot;

import javax.swing.*;
import javax.swing.event.*;

import java.awt.*;
import java.awt.event.*;
import java.util.HashSet;
import java.util.Vector;

import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.ClipboardOwner;
import java.awt.datatransfer.Transferable;
import java.awt.datatransfer.StringSelection;
import java.awt.datatransfer.DataFlavor;
import java.awt.datatransfer.UnsupportedFlavorException;
import java.awt.Toolkit;
import java.io.*;

/**
 * This panel contains one SelectPanel.
 *
 */
public class DBPanel extends JPanel implements ListPanel, ClipboardOwner{

  private static final long serialVersionUID = 1L;
  private JScrollPane spSelectPanel = new JScrollPane();
  private SelectPanel selectPanel;
  private JPanel pButtonSelectPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
  public JPanel panelSelectPanel = new JPanel(new GridBagLayout());

  private JScrollPane spTableResults = new JScrollPane();
  private Table tableResults = null;
  private JPanel pButtonTableResults = new JPanel(new FlowLayout(FlowLayout.RIGHT));
  private JPanel panelTableResults = new JPanel(new GridBagLayout());
  
  private JButton bCreateRecords = new JButton("Define new record(s)");
  private JButton bEditRecord = new JButton("Edit record");
  private JPopupMenu pmSubmitMenu = new JPopupMenu();
  private JButton bSubmit = new JButton("Submit job(s)");
  private JButton bMonitor = new JButton("Monitor job(s)");
  private JButton bDeleteRecord = new JButton("Delete record(s)");
  private JButton bSearch = new JButton("Search");
  private JButton bClear = new JButton("Clear");
  private JButton bViewJobDefinitions = new JButton("Show jobDefinitions");
  private JButton bDefineJobDefinitions = new JButton("Create jobDefinitions");
  private JMenuItem miEdit = null;
  
  private int [] identifiers;
  // lists of field names with table name as key
  private String [] fieldNames = null;
  private String dbName = null;
  
  private String tableName;
  private String identifier = null;
  private String jobDefIdentifier = null;
  private String [] defaultFields = null;
  private String [] hiddenFields = null;
  private String [] shownFields = null;
  private String [] selectFields = null;

  private JMenu jmSetFieldValue = null;
  
  private StatusBar statusBar = null;
  
  private GridBagConstraints ct = new GridBagConstraints();
  
  private DBPluginMgr dbPluginMgr = null;
  private int parentId = -1;

  private SubmissionControl submissionControl;
  
  private boolean clipboardOwned = false;
  
  private JMenuItem menuEditCopy = null;
  private JMenuItem menuEditCut = null;
  private JMenuItem menuEditPaste = null;

  private Thread workThread;
  // WORKING THREAD SEMAPHORE
  private boolean working = false;
  // the following semaphore assumes correct usage
  // try grabbing the semaphore
  private synchronized boolean getWorking(){
    if (!working){
      working = true;
      return true;
    }
    return false;
  }
  // release the semaphore
  private synchronized void stopWorking(){
    working = false;
  }
  

  /**
   * Constructor
   */

  /**
   * Create a new DBPanel from scratch.
   */

   public DBPanel(/*name of database*/
                  String _dbName,
                  /*name of tables for the select*/
                  String _tableName) throws Exception{
  
     dbName = _dbName;     
     tableName = _tableName;
     dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
     statusBar = GridPilot.getClassMgr().getStatusBar();
     
     identifier = dbPluginMgr.getIdentifierField(tableName);
     jobDefIdentifier = dbPluginMgr.getIdentifierField("jobDefinition");
          
     ct.fill = GridBagConstraints.HORIZONTAL;
     ct.anchor = GridBagConstraints.NORTH;
     ct.insets = new Insets(0,0,0,0);
     ct.gridwidth=1;
     ct.gridheight=1;  
     ct.weightx = 0.0;
     ct.gridx = 0;
     ct.gridy = 1;   
     ct.ipady = 250;

     defaultFields = dbPluginMgr.getDBDefFields(tableName);
     Debug.debug("Default fields "+defaultFields.length, 3);

    fieldNames = dbPluginMgr.getFieldNames(tableName);
    
    hiddenFields = dbPluginMgr.getDBHiddenFields(tableName);
    Debug.debug("Hidden fields "+Util.arrayToString(hiddenFields), 3);
    
    
    // Pass on only non-hidden fields to
    // Table. Perhaps rethink: - Table hides fields...
    Vector fieldSet = new Vector();
    boolean ok;
    for(int i=0; i<fieldNames.length; ++i){
      ok = true;
      for(int j=0; j<hiddenFields.length; ++j){
        //Debug.debug("Checking fields for hiding: "+fieldNames[i]+"<->"+hiddenFields[j], 3);
        if(fieldNames[i].equalsIgnoreCase(hiddenFields[j])){
          ok = false;
          break;
        }
      }
      if(ok){
        fieldSet.add(fieldNames[i]);
      }
    }
    
    fieldNames = new String[fieldSet.size()];
    for(int i=0; i<fieldSet.size(); ++i){
      fieldNames[i] = fieldSet.get(i).toString();
    }
    
    tableResults = new Table(hiddenFields, fieldNames,
        GridPilot.colorMapping);
    
    submissionControl = GridPilot.getClassMgr().getSubmissionControl();
    
    setFieldArrays();
    
    menuEditCopy = GridPilot.getClassMgr().getGlobalFrame().menuEditCopy;
    menuEditCut = GridPilot.getClassMgr().getGlobalFrame().menuEditCut;
    menuEditPaste = GridPilot.getClassMgr().getGlobalFrame().menuEditPaste;
    
    initGUI();
  }
   
   public Table getTable(){
     return tableResults;
   }

   private void setFieldArrays(){
     Vector shownSet = new Vector();  
     boolean ok = true;
     boolean hiddenOk = true;
     Debug.debug("Finding fields to show from: "+Util.arrayToString(defaultFields), 3);
     for(int i=0; i<defaultFields.length; ++i){
       ok = false;
       for(int j=0; j<fieldNames.length; ++j){
         Debug.debug("Checking fields for showing: "+defaultFields[i]+"<->"+fieldNames[j], 3);
         if(defaultFields[i].equalsIgnoreCase(fieldNames[j]) ||
             defaultFields[i].equalsIgnoreCase("*")){
           ok = true;
           break;
         }
       }
       hiddenOk = true;
       for(int j=0; j<hiddenFields.length; ++j){
         //Debug.debug("Checking fields for hiding "+defaultFields[i]+"<->"+hiddenFields[j], 3);
         if(defaultFields[i].equalsIgnoreCase(hiddenFields[j]) &&
             !hiddenFields[j].equalsIgnoreCase("*")){
           hiddenOk = false;
           break;
         }
       }
       if(ok && hiddenOk){
         Debug.debug("Showing "+defaultFields[i], 3);
         shownSet.add(defaultFields[i]);
       }
     }
          
     if(selectPanel!=null){
       Debug.debug("Checking "+selectPanel.sPanel.spDisplayList.getComponentCount()+":"+shownFields.length, 3);
       boolean fieldOk = true;
       for(int i=0; i<selectPanel.sPanel.spDisplayList.getComponentCount(); ++i){
         fieldOk = true;
         SPanel.DisplayPanel cb =
           ((SPanel.DisplayPanel) selectPanel.sPanel.spDisplayList.getComponent(i));
         for(int j=0; j<shownFields.length; ++j){
           Debug.debug("Checking fields "+
               cb.cbDisplayAttribute.getSelectedItem().toString()+"<->"+shownFields[j], 3);
           if((cb.cbDisplayAttribute.getSelectedItem().toString()
                   ).equalsIgnoreCase(shownFields[j])){
             fieldOk = false;
             break;
           }
         }
         if(fieldOk){
           shownSet.add(cb.cbDisplayAttribute.getSelectedItem().toString());
         }
       }    
     }

     shownFields = new String[shownSet.size()];
     for(int i=0; i<shownSet.size(); ++i){
       shownFields[i] = shownSet.get(i).toString();
     }
     
     for(int k=0; k<shownFields.length; ++k){
       shownFields[k] = tableName+"."+shownFields[k];
     }
     
     // Set the default values of the selection drop downs.
     // selectFields only used by clear().
     Vector selectSet = new Vector();  
     for(int i=0; i<defaultFields.length; ++i){
       boolean fieldOk = false;
       for(int j=0; j<fieldNames.length; ++j){
         if(fieldNames[j].equalsIgnoreCase(defaultFields[i])){
           fieldOk = true;
           break;
         }
       }
       if(fieldOk){
         Debug.debug("Selecting "+defaultFields[i], 3);
         selectSet.add(defaultFields[i]);
       }
     }
     
     selectFields = new String[selectSet.size()];
     for(int i=0; i<selectSet.size(); ++i){
       selectFields[i] = selectSet.get(i).toString();
     }
     
     for(int k=0; k<selectFields.length; ++k){
       selectFields[k] = tableName+"."+selectFields[k];
     }
   }
   
   /**
    * Create a new DBPanel from a parent panel.
    */

  public DBPanel(/*name of tables for the select*/
                 String _tableName,
                 /*pointer to the db in use for this panel*/
                 DBPluginMgr _dbPluginMgr,
                 /*identifier of the parent record (dataset <- jobDefinition)*/
                 int _parentId) throws Exception{
      this(_dbPluginMgr.getDBName(), _tableName);
      dbPluginMgr = _dbPluginMgr;
      parentId = _parentId;
  }
    
  /**
  * GUI initialisation
  */

  private void initGUI() throws Exception{

//// SelectPanel

    selectPanel = new SelectPanel(tableName, fieldNames);
    selectPanel.initGUI();
    clear();

    this.setLayout(new GridBagLayout());

    spSelectPanel.getViewport().add(selectPanel);

    panelSelectPanel.add(spSelectPanel,
        new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0,
            GridBagConstraints.CENTER,
            GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    panelSelectPanel.add(new JLabel(dbName),
        new GridBagConstraints(0, 1, 1, 1, 1.0, 0.0,
            GridBagConstraints.WEST,
            GridBagConstraints.NONE, new Insets(10, 10, 10, 10), 0, 0));
    panelSelectPanel.add(pButtonSelectPanel,
        new GridBagConstraints(0, 1, 1, 1, 1.0, 0.0,
            GridBagConstraints.EAST,
            GridBagConstraints.NONE,new Insets(10, 10, 10, 10), 0, 0));

    selectPanel.setConstraint(dbPluginMgr.getNameField(tableName), "", 1);
    
    // Listen for enter key in text field
    this.selectPanel.spcp.tfConstraintValue.addKeyListener(new KeyAdapter(){
      public void keyPressed(KeyEvent e){
        switch(e.getKeyCode()){
          case KeyEvent.VK_ENTER:
            search();
        }
        if(KeyEvent.getKeyText(e.getKeyCode()).equalsIgnoreCase("c") ||
            KeyEvent.getKeyText(e.getKeyCode()).equalsIgnoreCase("x")){
          if(e.isControlDown()){
            clipboardOwned = false;
            menuEditPaste.setEnabled(clipboardOwned);
          }
        }

      }
    });

    // panel table results
    panelTableResults.add(spTableResults,
        new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0,
            GridBagConstraints.CENTER,
            GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    panelTableResults.add(pButtonTableResults,
        new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0,
            GridBagConstraints.EAST,
            GridBagConstraints.NONE, new Insets(10, 10, 10, 10), 0, 0));

    ct.fill = GridBagConstraints.HORIZONTAL;
    ct.anchor = GridBagConstraints.NORTH;
    ct.insets = new Insets(0,0,0,0);
    ct.weightx = 0.5;
    ct.gridx = 0;
    ct.gridy = 0;         
    ct.gridwidth=1;
    ct.gridheight=1;  
    ct.ipady = 100;
    this.add(panelSelectPanel,ct);
    ct.weightx = 0.0;
    ct.gridx = 0;
    ct.gridy = 1;   
    ct.ipady = 250;
    this.add(panelTableResults,ct);
    this.updateUI();
    
    // Disable clipboard handling inherited from JPanel
    TransferHandler th = new TransferHandler(null);
    tableResults.setTransferHandler(th);
    
    tableResults.addKeyListener(new KeyAdapter(){
      public void keyPressed(KeyEvent e){
        //Debug.debug("key code: "+KeyEvent.getKeyText(e.getKeyCode()), 3);
        if(e.getKeyCode()==KeyEvent.VK_F1){
          //menuHelpAbout_actionPerformed();
        }
        else if(KeyEvent.getKeyText(e.getKeyCode()).equalsIgnoreCase("x")){
          if(e.isControlDown()){
            cut();
          }
        }
        else if(KeyEvent.getKeyText(e.getKeyCode()).equalsIgnoreCase("c")){
          if(e.isControlDown()){
            copy();
          }
        }
        else if(KeyEvent.getKeyText(e.getKeyCode()).equalsIgnoreCase("v")){
          if(e.isControlDown()){
            paste();
          }
        }
      }
    });
    
    //// buttons
    // Costumized for each type of table
    bSearch.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        search();
      }
    });
    bSearch.setToolTipText("Search results for this request");
    bClear.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        clear();
      }
    });
    
    if(tableName.equalsIgnoreCase("dataset")){
      
      tableResults.addMouseListener(new MouseAdapter(){
        public void mouseClicked(MouseEvent e){
          if(e.getClickCount()==2){
            editDataset();
          }
        }
      });

      bViewJobDefinitions.addActionListener(new java.awt.event.ActionListener(){
        public void actionPerformed(ActionEvent e){
          viewJobDefinitions();
        }
      }
      );
      
      bDefineJobDefinitions.addActionListener(new java.awt.event.ActionListener(){
        public void actionPerformed(ActionEvent e){
          defineJobDefinitions();
        }
      }
      );
      
      bCreateRecords.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          createDatasets();
        }
      });

      bEditRecord.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          editDataset();
        }
      });

      bDeleteRecord.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          deleteDatasets();
        }
      });
      
      addButtonResultsPanel(bViewJobDefinitions);
      addButtonResultsPanel(bDefineJobDefinitions);
      addButtonResultsPanel(bCreateRecords);
      addButtonResultsPanel(bEditRecord);
      addButtonResultsPanel(bDeleteRecord);
      addButtonSelectPanel(bClear);
      addButtonSelectPanel(bSearch);
      bViewJobDefinitions.setEnabled(false);
      bDefineJobDefinitions.setEnabled(false);
      bEditRecord.setEnabled(false);
      bDeleteRecord.setEnabled(false);
      updateUI();
    }
    else if(tableName.equalsIgnoreCase("jobDefinition")){
      
      tableResults.addMouseListener(new MouseAdapter(){
        public void mouseClicked(MouseEvent e){
          if(e.getClickCount()==2){
            editJobDef();
          }
        }
      });

      bSubmit.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          bSubmit_mousePressed();
        }
      });
      
      bMonitor.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          monitor();
        }
      });

      bEditRecord.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          editJobDef();
        }
      });

      bDeleteRecord.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          deleteJobDefs();
        }
      });

      bCreateRecords.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          createJobDefinitions();
        }
      });
      
      String [] csNames = GridPilot.getClassMgr().getCSPluginMgr().getCSNames();
      for(int i=0; i< csNames.length ; ++i){
        JMenuItem mi = new JMenuItem(csNames[i]);
        //mi.setMnemonic(i);
        mi.addActionListener(new ActionListener(){
          public void actionPerformed(final ActionEvent e){
                submit(e);
          }});
        pmSubmitMenu.add(mi);
      }

      
      addButtonResultsPanel(bSubmit);
      addButtonResultsPanel(bMonitor);
      addButtonResultsPanel(bCreateRecords);
      addButtonResultsPanel(bEditRecord);
      addButtonResultsPanel(bDeleteRecord);
      addButtonSelectPanel(bClear);
      addButtonSelectPanel(bSearch);
      bSubmit.setEnabled(false);
      bMonitor.setEnabled(false);
      bEditRecord.setEnabled(false);
      bDeleteRecord.setEnabled(false);
      updateUI();
    }
    else if(tableName.equalsIgnoreCase("transformation")){
      
      tableResults.addMouseListener(new MouseAdapter(){
        public void mouseClicked(MouseEvent e){
          if(e.getClickCount()==2){
            editTransformation();
          }
        }
      });

      bCreateRecords.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          createTransformation();
        }
      });

      bEditRecord.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          editTransformation();
        }
      });

      bDeleteRecord.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          deleteTransformations();
        }
      });
      
      addButtonResultsPanel(bCreateRecords);
      addButtonResultsPanel(bEditRecord);
      addButtonResultsPanel(bDeleteRecord);
      addButtonSelectPanel(bClear);
      addButtonSelectPanel(bSearch);
      bViewJobDefinitions.setEnabled(false);
      bDefineJobDefinitions.setEnabled(false);
      bEditRecord.setEnabled(false);
      bDeleteRecord.setEnabled(false);
      updateUI();
    }    
    else if(tableName.equalsIgnoreCase("runtimeEnvironment")){
      
      tableResults.addMouseListener(new MouseAdapter(){
        public void mouseClicked(MouseEvent e){
          if(e.getClickCount()==2){
            editRuntimeEnvironment();
          }
        }
      });
      
      bCreateRecords.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          createRuntimeEnvironment();
        }
      });

      bEditRecord.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          editRuntimeEnvironment();
        }
      });

      bDeleteRecord.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          deleteRuntimeEnvironments();
        }
      });
      
      addButtonResultsPanel(bCreateRecords);
      addButtonResultsPanel(bEditRecord);
      addButtonResultsPanel(bDeleteRecord);
      addButtonSelectPanel(bClear);
      addButtonSelectPanel(bSearch);
      bViewJobDefinitions.setEnabled(false);
      bDefineJobDefinitions.setEnabled(false);
      bEditRecord.setEnabled(false);
      bDeleteRecord.setEnabled(false);
      menuEditCopy.setEnabled(false);
      menuEditCut.setEnabled(false);
      menuEditPaste.setEnabled(false);
      updateUI();
    }    
  }

  public String getTitle(){
    return tableName/*+"s"*//*jobTranss looks bad...*/;
  }

  public void panelShown(){
    Debug.debug("panelShown", 1);
    boolean rowsAreSelected = tableResults.getSelectedRows().length>0;
    menuEditCopy.setEnabled(rowsAreSelected);
    menuEditCut.setEnabled(rowsAreSelected);
    // Check if clipboard is of the form "db table id1 id2 id3 ..."
    String clip = getClipboardContents();
    String clips [] = null;
    if(clip!=null){
      clips = Util.split(clip);
    }
    if(clips!=null && clips.length>2 && clips[1].equalsIgnoreCase(tableName)){
      clipboardOwned = true;
    }
    else{
      clipboardOwned = false;
    }
    menuEditPaste.setEnabled(clipboardOwned);
  }

  public void panelHidden(){
    Debug.debug("", 1);
  }

  /**
   * Returns identifiers of the selected jobDefinitions, corresponding to
   * jobDefinition.identifier in DB.
   */
  public int [] getSelectedIdentifiers(){

    int [] selectedRows = tableResults.getSelectedRows();
    int [] selectedIdentifiers = new int[selectedRows.length];
    for(int i=0; i<selectedIdentifiers.length; ++i){
      selectedIdentifiers[i] = identifiers[selectedRows[i]];
    }
    return selectedIdentifiers;
  }

  /**
   * Returns the Id of the first selected row, -1 if no row is selected.
   *
   * @return Id of the first selected row, -1 if no row is selected
   *
   */
  public int getSelectedIdentifier(){
    int selRow = tableResults.getSelectedRow();
    return (selRow==-1) ? -1 : identifiers[selRow];
  }

  /**
   * Adds a button on the left of the buttons shown when the panel with results is shown.
   */
  public void addButtonResultsPanel(JButton b){
    pButtonTableResults.add(b);
  }

  /**
   * Adds a button on the select panel.
   */
  public void addButtonSelectPanel(JButton b){
    pButtonSelectPanel.add(b);
  }

  /**
   * Carries out search according to selection
   */
  public void search(){
    if(tableResults==null){
      searchRequest();
    }
    else{
      DBVectorTableModel tableModel = (DBVectorTableModel) tableResults.getModel();
      int sortColumn = tableModel.getColumnSort();
      boolean isAscending = tableModel.isSortAscending();
      searchRequest(sortColumn, isAscending);
    }
    remove(panelTableResults);
    add(panelTableResults, ct);
    updateUI();
  }

  /**
   * Resets fields to default fields and clear values
   */
  public void clear(){
    String [][] values = new String[selectFields.length][2];
    Debug.debug("Clearing "+selectFields.length, 3);
    for(int i=0; i<selectFields.length; ++i){
      String [] split = selectFields[i].split("\\.");
      if(split.length != 2){
          Debug.debug(selectFields[i] + " " + " : wrong format in config file ; " +
                    "should be : \ndefault dataset fields = table.field1 table.field2", 3);
      }
      else{
        Debug.debug("Setting default value "+split[0]+" "+split[1],3);
        values[i][0] = split[0];
        values[i][1] = split[1];
      }
    }
    selectPanel.setDisplayFieldValue(values);
    selectPanel.resetConstraintList(tableName);
    selectPanel.setConstraint(dbPluginMgr.getNameField(tableName), "", 1);
    selectPanel.updateUI();
  }

  /**
   * Request DBPluginMgr for select request from SelectPanel, and fill tableResults
   * with results. The request is performed in a separeted thread, avoiding to block
   * all the GUI during this action.
   * Called when button "Search" is clicked
   */
  public void searchRequest(final int sortColumn, final boolean isAscending){
        
   // TODO: why does it not work as thread when
   // not in it's own pane?
    //workThread = new Thread(){
      //public void run(){
        //if(!getWorking()){
          //Debug.debug("please wait ...", 2);
          //return;
        //}
        setFieldArrays();
        String selectRequest;
        selectRequest = selectPanel.getRequest(shownFields);
        if(selectRequest == null){
            return;
        }
        DBResult res = null;
        res = dbPluginMgr.select(selectRequest, identifier);

        bViewJobDefinitions.setEnabled(false);
        bDefineJobDefinitions.setEnabled(false);
        bEditRecord.setEnabled(false);
        bDeleteRecord.setEnabled(false);
        bSubmit.setEnabled(false);
        bMonitor.setEnabled(false);
        menuEditCopy.setEnabled(false);
        menuEditCut.setEnabled(false);
        menuEditPaste.setEnabled(clipboardOwned);
        
        tableResults.setTable(res.values, res.fields);
        spTableResults.getViewport().add(tableResults);
        
        identifiers = new int[tableResults.getRowCount()];
        // 'col' is the column with the jobDefinition identifier
        int col = tableResults.getColumnCount()-1;
        for(int i=0; i<tableResults.getColumnCount(); ++i){
          Debug.debug("Column: "+tableResults.getColumnName(i)+"<->"+identifier, 3);
          if(tableResults.getColumnName(i).equalsIgnoreCase(identifier)){
            col = i;
            Debug.debug("OK: "+i, 3);
            break;
          }
        }
        for(int i=0; i<identifiers.length; ++i){
          identifiers[i] =
            new Integer(tableResults.getUnsortedValueAt(i, col).toString()).intValue();
        }

        if(tableName.equalsIgnoreCase("dataset")){
          tableResults.getSelectionModel().setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
          tableResults.addListSelectionListener(new ListSelectionListener(){
            public void valueChanged(ListSelectionEvent e){
              if (e.getValueIsAdjusting()) return;
              ListSelectionModel lsm = (ListSelectionModel)e.getSource();
              //Debug.debug("lsm indices: "+
              //    lsm.getMaxSelectionIndex()+" : "+lsm.getMinSelectionIndex(), 3);
              bViewJobDefinitions.setEnabled(!lsm.isSelectionEmpty() &&
                  lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
              bDefineJobDefinitions.setEnabled(!lsm.isSelectionEmpty());
              bDeleteRecord.setEnabled(!lsm.isSelectionEmpty());
              bEditRecord.setEnabled(!lsm.isSelectionEmpty() &&
                  lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
              miEdit.setEnabled(!lsm.isSelectionEmpty() &&
                  lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
              menuEditCopy.setEnabled(!lsm.isSelectionEmpty());
              menuEditCut.setEnabled(!lsm.isSelectionEmpty());
              menuEditPaste.setEnabled(clipboardOwned);
            }
          });

          makeDatasetMenu();
        }
        else if(tableName.equalsIgnoreCase("jobDefinition")){
          tableResults.getSelectionModel().setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
          tableResults.addListSelectionListener(new ListSelectionListener(){
            public void valueChanged(ListSelectionEvent e){
              if (e.getValueIsAdjusting()) return;
              ListSelectionModel lsm = (ListSelectionModel)e.getSource();
              //Debug.debug("lsm indices: "+
              //    lsm.getMaxSelectionIndex()+" : "+lsm.getMinSelectionIndex(), 3);
              bSubmit.setEnabled(!lsm.isSelectionEmpty());
              bMonitor.setEnabled(!lsm.isSelectionEmpty());
              bDeleteRecord.setEnabled(!lsm.isSelectionEmpty());
              bEditRecord.setEnabled(!lsm.isSelectionEmpty() &&
                  lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
              miEdit.setEnabled(!lsm.isSelectionEmpty() &&
                  lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
              menuEditCopy.setEnabled(!lsm.isSelectionEmpty());
              menuEditCut.setEnabled(!lsm.isSelectionEmpty());
              menuEditPaste.setEnabled(clipboardOwned);
            }
          });

          makeJobDefMenu();
        }
        else if(tableName.equalsIgnoreCase("transformation")){
          tableResults.getSelectionModel().setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
          tableResults.addListSelectionListener(new ListSelectionListener(){
            public void valueChanged(ListSelectionEvent e){
              if (e.getValueIsAdjusting()) return;
              ListSelectionModel lsm = (ListSelectionModel)e.getSource();
              //Debug.debug("lsm indices: "+
              //    lsm.getMaxSelectionIndex()+" : "+lsm.getMinSelectionIndex(), 3);
              bDeleteRecord.setEnabled(!lsm.isSelectionEmpty());
              bEditRecord.setEnabled(!lsm.isSelectionEmpty() &&
                  lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
              miEdit.setEnabled(!lsm.isSelectionEmpty() &&
                  lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
              menuEditCopy.setEnabled(!lsm.isSelectionEmpty());
              menuEditCut.setEnabled(!lsm.isSelectionEmpty());
              menuEditPaste.setEnabled(clipboardOwned);
            }
          });

          makeTransformationMenu();
        }
        else if(tableName.equalsIgnoreCase("runtimeEnvironment")){
          tableResults.getSelectionModel().setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
          tableResults.addListSelectionListener(new ListSelectionListener(){
            public void valueChanged(ListSelectionEvent e){
              if (e.getValueIsAdjusting()) return;
              ListSelectionModel lsm = (ListSelectionModel)e.getSource();
              //Debug.debug("lsm indices: "+
              //    lsm.getMaxSelectionIndex()+" : "+lsm.getMinSelectionIndex(), 3);
              bDeleteRecord.setEnabled(!lsm.isSelectionEmpty());
              bEditRecord.setEnabled(!lsm.isSelectionEmpty() &&
                  lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
              miEdit.setEnabled(!lsm.isSelectionEmpty() &&
                  lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
              menuEditCopy.setEnabled(!lsm.isSelectionEmpty());
              menuEditCut.setEnabled(!lsm.isSelectionEmpty());
              menuEditPaste.setEnabled(clipboardOwned);
            }
          });
          makeRuntimeEnvironmentMenu();
        }
        
        GridPilot.getClassMgr().getGlobalFrame().menuEdit.updateUI();
        
        statusBar.setLabel("Records found: "+tableResults.getRowCount(), 20);
        
        if(sortColumn>-1){
          Debug.debug("Sorting: "+sortColumn+":"+isAscending, 3);
          ((DBVectorTableModel) tableResults.getModel()).sort(sortColumn, isAscending);
        }
        //stopWorking();
      //}
    //};
    //workThread.start();

  }
  
  public void searchRequest(){
    searchRequest(-1, true);
  }
 
  /**
   * Add menu items to the table with search results. This function is called from within DBPanel
   * after the results table is filled
   */
  public void makeDatasetMenu(){
    Debug.debug("Making dataset menu", 3);
    JMenuItem miViewJobDefinitions = new JMenuItem("Show job definitions");
    miViewJobDefinitions.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        viewJobDefinitions();
      }
    });
    JMenuItem miDelete = new JMenuItem("Delete");
    miEdit = new JMenuItem("Edit");
    miDelete.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        deleteDatasets();
      }
    });
    miEdit.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        editDataset();
      }
    });
    miViewJobDefinitions.setEnabled(true);
    miDelete.setEnabled(true);
    miEdit.setEnabled(true);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miViewJobDefinitions);
    tableResults.addMenuSeparator();
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miDelete);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miEdit);
  }

  public void makeTransformationMenu(){
    Debug.debug("Making transformation menu", 3);
    JMenuItem miDelete = new JMenuItem("Delete");
    miEdit = new JMenuItem("Edit");
    miDelete.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        deleteTransformations();
      }
    });
    miEdit.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        editTransformation();
      }
    });
    miDelete.setEnabled(true);
    miEdit.setEnabled(true);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miDelete);
    tableResults.addMenuItem(miEdit);
  }
  public void makeRuntimeEnvironmentMenu(){
    Debug.debug("Making runtime environment menu", 3);
    JMenuItem miDelete = new JMenuItem("Delete");
    miEdit = new JMenuItem("Edit");
    miDelete.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        deleteRuntimeEnvironments();
      }
    });
    miEdit.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        editRuntimeEnvironment();
      }
    });
    miDelete.setEnabled(true);
    miEdit.setEnabled(true);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miDelete);
    tableResults.addMenuItem(miEdit);
  }

  public void makeJobDefMenu(){
    JMenuItem miDelete = new JMenuItem("Delete");
    miEdit = new JMenuItem("Edit");
    jmSetFieldValue = new JMenu("Set field value");
    JMenu jmSubmit = new JMenu("Submit job(s)");
    JMenuItem miMonitor = new JMenuItem("Monitor job(s)");
    String [] fieldNames = tableResults.getColumnNames();
    JMenuItem [] miSetFields = new JMenuItem[fieldNames.length];
    for(int i=0; i<fieldNames.length; ++i){
      if(fieldNames[i]!=null && !fieldNames[i].equalsIgnoreCase("") &&
          !fieldNames[i].equalsIgnoreCase(jobDefIdentifier)){
           miSetFields[i] = new JMenuItem(fieldNames[i]);
           miSetFields[i].setName(fieldNames[i]);
           miSetFields[i].addActionListener(new ActionListener(){
             public void actionPerformed(ActionEvent e){
               String choiceStr = JOptionPane.showInputDialog(JOptionPane.getRootFrame(),
                   "New value of "+((JMenuItem) e.getSource()).getName()+":", "",
                   JOptionPane.QUESTION_MESSAGE);
               Debug.debug("choiceStr: "+choiceStr, 3);
               if(choiceStr==null || choiceStr.equals("")){
                 return;
               }
               else{
                 setFieldValues(((JMenuItem) e.getSource()).getName(), choiceStr);
               }
             }
           });
           jmSetFieldValue.add(miSetFields[i]);
      }
    }
    String [] csNames = GridPilot.getClassMgr().getCSPluginMgr().getCSNames();
    for(int i=0; i<csNames.length; ++i){
      JMenuItem mi = new JMenuItem(csNames[i]);
      mi.addActionListener(new ActionListener(){
        public void actionPerformed(final ActionEvent e){
              submit(e);
        }});
      jmSubmit.add(mi);
    }
    miMonitor.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        monitor();
      }
    });
    miDelete.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        deleteJobDefs();
      }
    });
    miEdit.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        editJobDef();
      }
    });
    miDelete.setEnabled(true);
    miEdit.setEnabled(true);
    miMonitor.setEnabled(true);
    tableResults.addMenuItem(jmSetFieldValue);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miEdit);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miDelete);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miMonitor);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(jmSubmit);
  }
  
  private void setFieldValues(String field, String value){
    dbPluginMgr.setJobDefsField(getSelectedIdentifiers(), field, value);
    refresh();
  }
    
   /**
   * Open dialog with jobDefintion creation panel (from datasets)
   */ 
  private void createJobDefs(){
    Debug.debug("Creating job definitions, "+getSelectedIdentifiers().length, 3);
    //JobDefCreationPanel panel = new JobDefCreationPanel(dbName, datasetMgr, this, false);
    JobCreationPanel panel = new JobCreationPanel(dbPluginMgr, this);
    CreateEditDialog pDialog = new CreateEditDialog(panel, false, true);
    pDialog.setTitle("jobDefinition");
  }

  private void createJobDefinitions(){
    Debug.debug("Creating job definitions, "+getSelectedIdentifiers().length, 3);
    JobDefCreationPanel panel = new JobDefCreationPanel(dbName, null, this, new Boolean(false));
    CreateEditDialog pDialog = new CreateEditDialog(panel, false, false);
    pDialog.setTitle("jobDefinition");
  }

  private void editJobDef(){
    DatasetMgr datasetMgr = null;
    int selectedDatasetID = dbPluginMgr.getJobDefDatasetID(getSelectedIdentifier());
    if(parentId<0){
      parentId = selectedDatasetID;
    }
    Debug.debug("Got dbPluginMgr:"+dbPluginMgr+":"+parentId, 1);
    try{
      datasetMgr = GridPilot.getClassMgr().getDatasetMgr(dbName, selectedDatasetID);
    }
    catch(Throwable e){
      Debug.debug("ERROR: could not get DatasetMgr. "+e.getMessage(), 1);
      e.printStackTrace();
      return;
    }
    // Try to load the job definition panel defined by the db plugin
    String panelClass = dbPluginMgr.getJobDefCreationPanelClass();
    Class [] argTypes = new Class []
       {String.class, DatasetMgr.class, DBPanel.class, Boolean.class};
    Object [] args = new Object [] {dbName, datasetMgr, this, new Boolean(true)};
    JobDefCreationPanel panel = null;
    try{
      panel = (JobDefCreationPanel) Util.loadClass(
          panelClass, argTypes, args);
    }
    catch(Throwable e){
      Debug.debug("Could not load job definition creation panel class for " +
          "this db plugin, loading default panel", 1);
      panel = new JobDefCreationPanel(dbName, datasetMgr, this,
          new Boolean(true));
    }
    CreateEditDialog pDialog = new CreateEditDialog(panel, true, false);
    pDialog.setTitle(tableName);
    //pDialog.setVisible(true);
  }

  // From AtCom1
  // TODO: try if not better than deleteJobDefs
  /**
   * Deletes selected job definitions from the database. 
   * Returns list of successfully deleted job definitions.
   */
  public synchronized HashSet deleteJobdefinitions(boolean showResults) {
  // TODO: Delete job definitions only if not running.
  
  boolean skipAll = false;
  boolean skip = false;

  boolean showThis;
  int choice = 3;
  HashSet deleted = new HashSet();
  int [] selectedJobDefs = getSelectedIdentifiers();
  JProgressBar pb = new JProgressBar();
  pb.setMaximum(pb.getMaximum()+selectedJobDefs.length);
  statusBar.setProgressBar(pb);
  showThis = showResults;
  Debug.debug("Deleting "+selectedJobDefs.length+" logical files",2);
  JCheckBox cbCleanup = null;
  
  for(int i=selectedJobDefs.length-1; i>=0 && !skipAll; --i){
    
    if(skipAll){
      break;
    }
    
    if(showThis && !skipAll){
        
      ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame()); 
      cbCleanup = new JCheckBox("Cleanup runtime info", true);
      try{
        if(i>0){
          choice = confirmBox.getConfirm("Confirm delete",
             "Really delete logical file # "+selectedJobDefs[i]+"?",
             new Object[] {"OK", "Skip", "OK for all", "Skip all", cbCleanup});
        }
        else{
          choice = confirmBox.getConfirm("Confirm delete",
           "Really delete logical file # "+selectedJobDefs[i]+"?",
           new Object[] {"OK",  "Skip", cbCleanup});        
        }
      }
      catch(java.lang.Exception e){Debug.debug("Could not get confirmation, "+e.getMessage(),1);}
        switch(choice){
          case 0  : skip = false;  break;  // OK
          case 1  : skip = true;   break; // Skip
          case 2  : skip = false;  showThis = false ; break;   //OK for all
          case 3  : skip = true;   showThis = false ; skipAll = true; break;// Skip all
          default : skip = true;   skipAll = true; break;// other (closing the dialog). Same action as "Skip all"
        }
      }
      if(!skipAll && !skip){
        Debug.debug("deleting logical file # " + selectedJobDefs[i], 2);
        pb.setValue(pb.getValue()+1);
        if(cbCleanup.isSelected()){
          if(!dbPluginMgr.cleanRunInfo(selectedJobDefs[i])){
            GridPilot.getClassMgr().getLogFile().addMessage(
                "WARNING: Deleting runtime record for logicalFile # "+selectedJobDefs[i]+
                " failed."+"Please clean up by hand."+dbPluginMgr.getError());
          }
        }
        if(dbPluginMgr.deleteJobDefinition(selectedJobDefs[i])){
          deleted.add(Integer.toString(selectedJobDefs[i]));
          statusBar.setLabel("Job definition # " + selectedJobDefs[i] + " deleted.");
        }
        else{
          statusBar.setLabel("Job definition # " + selectedJobDefs[i] + " NOT deleted.");
          Debug.debug("WARNING: Job definition "+selectedJobDefs[i]+" could not be deleted",1);
        }   
      }
    }
    return deleted;
  }

  private void deleteJobDefs(){
    String msg = "Are you sure you want to delete jobDefinition";
    if(getSelectedIdentifiers().length>1){
      msg += "s";
    }
    int [] ids = getSelectedIdentifiers();
    for(int i=0; i<getSelectedIdentifiers().length; ++i){
      if(i>0){
        msg += ",";
      }
      msg += " " + ids[i];
    }
    msg += "?";
    int choice = JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(),
        msg, "Delete?",
        JOptionPane.YES_NO_OPTION);
    if(choice == JOptionPane.NO_OPTION){
      return;
    }
    workThread = new Thread(){
      public void run(){
        if(!getWorking()){
          return;
        }
        int [] ids = getSelectedIdentifiers();

        // Update job monitoring display
        for(int i=ids.length-1; i>=0; --i){
          int currentDatasetID = dbPluginMgr.getJobDefDatasetID(ids[i]);
          Debug.debug("Got dbPluginMgr:"+dbPluginMgr+":"+parentId, 1);
          DatasetMgr datasetMgr = null;
          try{
            datasetMgr = GridPilot.getClassMgr().getDatasetMgr(dbName, currentDatasetID);
          }
          catch(Throwable e){
            Debug.debug("ERROR: could not get DatasetMgr. "+e.getMessage(), 1);
            e.printStackTrace();
            return;
          }
          datasetMgr.removeRow(ids[i]);
        }
        
        int [] rows = tableResults.getSelectedRows();
        Debug.debug("Deleting "+ids.length+" rows", 2);
        if(ids.length != 0){
          GridPilot.getClassMgr().getStatusBar().setLabel(
             "Deleting job definition(s). Please wait ...");
          JProgressBar pb = new JProgressBar();
          pb.setMaximum(ids.length);
          for(int i=ids.length-1; i>=0; i--){
            boolean success = dbPluginMgr.deleteJobDefinition(ids[i]);
            if(!success){
              String msg = "Deleting job definition "+ids[i]+" failed";
              Debug.debug(msg, 1);
              GridPilot.getClassMgr().getStatusBar().setLabel(msg);
              GridPilot.getClassMgr().getLogFile().addMessage(msg);
              continue;
            }
            pb.setValue(pb.getValue()+1);
            tableResults.removeRow(rows[i]);
            tableResults.tableModel.fireTableDataChanged();
          }
          GridPilot.getClassMgr().getStatusBar().setLabel(
             "Deleting job definition(s) done.");
        }
        refresh();
        stopWorking();
      }
    };
    workThread.start();
  }

  /**
   * Open dialog with dataset creation panel in creation mode
   */ 
  private void createDatasets(){
    CreateEditDialog pDialog = new CreateEditDialog(
        new DatasetCreationPanel(dbPluginMgr, this, false), false, false);
    pDialog.setTitle(tableName);
 }
  
  /**
   * Open dialog with dataset creation panel in editing mode
   */ 
 private void editDataset(){
   CreateEditDialog pDialog = new CreateEditDialog(
     new DatasetCreationPanel(dbPluginMgr, this, true), true, false);
   pDialog.setTitle(tableName);
 }

  /**
   *  Delete datasets. Returns HashSet of identifier strings.
   *  From AtCom1.
   */
  public HashSet deleteDatasets(){
    boolean skip = false;
    boolean okAll = false;
    int choice = 3;
    HashSet deleted = new HashSet();
    JCheckBox cbCleanup = null;
    int [] datasetIdentifiers = getSelectedIdentifiers();
    statusBar.setLabel(
    "Deleting dataset(s).");
    for(int i=datasetIdentifiers.length-1; i>=0; --i){
      if(datasetIdentifiers[i]!=-1){
        if(!okAll){
          ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame()/*,"",""*/); 
          cbCleanup = new JCheckBox("Delete child records", true);    
          if(i<1){
            try{
              choice = confirmBox.getConfirm("Confirm delete",
                                   "Really delete dataset # "+datasetIdentifiers[i]+"?",
                                new Object[] {"OK", "Skip", cbCleanup});
            }
            catch(java.lang.Exception e){
              Debug.debug("Could not get confirmation, "+e.getMessage(),1);
            }
          }
          else{
            try{
              choice = confirmBox.getConfirm("Confirm delete",
                                   "Really delete dataset # "+datasetIdentifiers[i]+"?",
                                new Object[] {"OK", "Skip", "OK for all", "Skip all", cbCleanup});
              }
            catch(java.lang.Exception e){
              Debug.debug("Could not get confirmation, "+e.getMessage(),1);
            }
          }
    
          switch(choice){
          case 0  : skip = false; break;  // OK
          case 1  : skip = true ; break;  // Skip
          case 2  : skip = false; okAll = true ;break;  // OK for all
          case 3  : skip = true ; return deleted; // Skip all
          default : skip = true;    // other (closing the dialog). Same action as "Skip"
          }
        }
        if(!skip || okAll){
          Debug.debug("deleting dataset # " + datasetIdentifiers[i], 2);
          if(dbPluginMgr.deleteDataset(datasetIdentifiers[i], cbCleanup.isSelected())){
            deleted.add(Integer.toString(datasetIdentifiers[i]));
            statusBar.setLabel("Dataset # " + datasetIdentifiers[i] + " deleted.");
          }
          else{
            Debug.debug("WARNING: dataset "+datasetIdentifiers[i]+" could not be deleted",1);
            statusBar.setLabel("Dataset # " + datasetIdentifiers[i] + " NOT deleted.");
          }
        }
      }
      else{
        Debug.debug("WARNING: dataset undefined and could not be deleted",1);
      }
    }
    statusBar.setLabel(
    "Deleting runtime environment(s) done.");
    refresh();
    if(datasetIdentifiers.length>1){
      statusBar.setLabel(deleted.size()+" of "+
          datasetIdentifiers.length+" datasets deleted.");
    }
    return deleted;
  }
  
  /**
   * Refresh search results.
   */ 
  public void refresh(){
    Debug.debug("Refreshing search results", 3);
    if(tableResults==null /*|| tableResults.getRowCount()==0*/){
      searchRequest();
      //return;
    }
    else{
      DBVectorTableModel tableModel = (DBVectorTableModel) tableResults.getModel();
      int sortColumn = tableModel.getColumnSort();
      boolean isAscending = tableModel.isSortAscending();
      searchRequest(sortColumn, isAscending);
    }
  }
  
  /**
   * Open dialog with transformation creation panel.
   */ 
  private void createTransformation(){
    CreateEditDialog pDialog = new CreateEditDialog(
       new TransformationCreationPanel(dbPluginMgr, this, false),
       false, false);
    pDialog.setTitle(tableName);
  }
  /**
   * Open dialog with runtime environment creation panel
   */ 
  private void createRuntimeEnvironment(){
    CreateEditDialog pDialog = new CreateEditDialog(
       new RuntimeCreationPanel(dbPluginMgr, this, false),
       false, false);
    pDialog.setTitle(tableName);
  }

  private void editTransformation(){
    CreateEditDialog pDialog = new CreateEditDialog(
       new TransformationCreationPanel(dbPluginMgr, this, true),
       true, false);
    pDialog.setTitle(tableName);
  }
  
  private void editRuntimeEnvironment(){
    CreateEditDialog pDialog = new CreateEditDialog(
       new RuntimeCreationPanel(dbPluginMgr, this, true),
       true, false);
    pDialog.setTitle(tableName);
  }

  private void deleteTransformations(){
    String msg = "Are you sure you want to delete transformation record";
    if(getSelectedIdentifiers().length>1){
      msg += "s";
    }
    int [] ids = getSelectedIdentifiers();
    for(int i=0; i<getSelectedIdentifiers().length; ++i){
      if(i>0){
        msg += ",";
      }
      msg += " " + ids[i];
    }
    msg += "?";
    int choice = JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(),
        msg, "Delete?",
        JOptionPane.YES_NO_OPTION);
    if(choice == JOptionPane.NO_OPTION){
      return;
    }
    workThread = new Thread(){
      public void run(){
        if(!getWorking()){
          return;
        }
        int [] ids = getSelectedIdentifiers();
        int [] rows = tableResults.getSelectedRows();
        Debug.debug("Deleting "+ids.length+" rows", 2);
        if(ids.length != 0){
          GridPilot.getClassMgr().getStatusBar().setLabel(
             "Deleting transformation(s). Please wait ...");
          JProgressBar pb = new JProgressBar();
          pb.setMaximum(ids.length);
          for(int i = ids.length-1; i>=0; i--){
            boolean success = dbPluginMgr.deleteTransformation(ids[i]);
            if(!success){
              String msg = "Deleting transformation "+ids[i]+" failed";
              Debug.debug(msg, 1);
              GridPilot.getClassMgr().getStatusBar().setLabel(msg);
              GridPilot.getClassMgr().getLogFile().addMessage(msg);
              continue;
            }
            pb.setValue(pb.getValue()+1);
            tableResults.removeRow(rows[i]);
            tableResults.tableModel.fireTableDataChanged();
          }
          GridPilot.getClassMgr().getStatusBar().setLabel(
             "Deleting transformation(s) done.");
        }
        stopWorking();
        refresh();
      }
    };
    workThread.start();
  }
  private void deleteRuntimeEnvironments(){
    String msg = "Are you sure you want to delete runtime environment record";
    if(getSelectedIdentifiers().length>1){
      msg += "s";
    }
    int [] ids = getSelectedIdentifiers();
    for(int i=0; i<getSelectedIdentifiers().length; ++i){
      if(i>0){
        msg += ",";
      }
      msg += " " + ids[i];
    }
    msg += "?";
    int choice = JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(),
        msg, "Delete?",
        JOptionPane.YES_NO_OPTION);
    if(choice == JOptionPane.NO_OPTION){
      return;
    }
    workThread = new Thread(){
      public void run(){
        if(!getWorking()){
          return;
        }
        int [] ids = getSelectedIdentifiers();
        int [] rows = tableResults.getSelectedRows();
        Debug.debug("Deleting "+ids.length+" rows", 2);
        if(ids.length != 0){
          GridPilot.getClassMgr().getStatusBar().setLabel(
             "Deleting runtime environment(s). Please wait ...");
          JProgressBar pb = new JProgressBar();
          pb.setMaximum(ids.length);
          for(int i = ids.length-1; i>=0; i--){
            boolean success = dbPluginMgr.deleteRuntimeEnvironment(ids[i]);
            if(!success){
              String msg = "Deleting runtime environment "+ids[i]+" failed";
              Debug.debug(msg, 1);
              GridPilot.getClassMgr().getStatusBar().setLabel(msg);
              GridPilot.getClassMgr().getLogFile().addMessage(msg);
              continue;
            }
            pb.setValue(pb.getValue()+1);
            tableResults.removeRow(rows[i]);
            tableResults.tableModel.fireTableDataChanged();
          }
          GridPilot.getClassMgr().getStatusBar().setLabel(
             "Deleting runtime environment(s) done.");
        }
        stopWorking();
        refresh();
      }
    };
    workThread.start();
  }

  /**
   * Open new pane with list of jobDefinitions.
   */
  private void viewJobDefinitions(){
    if(getSelectedIdentifier()!=-1){
      new Thread(){
        public void run(){
          try{
            // Create new panel with jobDefinitions.         
            int id = getSelectedIdentifier();
            DBPanel dbPanel = new DBPanel("jobDefinition",
                dbPluginMgr, id);
            String [] jobDefDatasetReference =
              dbPluginMgr.getJobDefDatasetReference();
            dbPanel.selectPanel.setConstraint(jobDefDatasetReference[1],
                dbPluginMgr.getDataset(id).getValue(
                    jobDefDatasetReference[0]).toString(),
                0);
            dbPanel.searchRequest();           
            GridPilot.getClassMgr().getGlobalFrame().addPanel(dbPanel);                   
          }
          catch(Exception e){
            Debug.debug("Couldn't create panel for dataset " + "\n" +
                               "\tException\t : " + e.getMessage(), 2);
            e.printStackTrace();
          }
        }
      }.start();
    }
  }
 
  /**
   * Open job definition window.
   */
  private void defineJobDefinitions(){
    Debug.debug("defining job definitions, "+getSelectedIdentifiers().length, 3);
    if(getSelectedIdentifiers()!=null && getSelectedIdentifiers().length!=0){
      new Thread(){
        public void run(){
          try{
            createJobDefs();
          }
          catch(Exception e){
            Debug.debug("Couldn't create job definition window " + "\n" +
                               "\tException\t : " + e.getMessage(), 2);
            e.printStackTrace();
          }
        }
      }.start();
    }
  }
 
  /**
   * Called when mouse is pressed on Monitor button
   */
  private void monitor(){
    GridPilot.getClassMgr().getGlobalFrame().showMonitoringPanel();
    new Thread(){
      public void run(){        
        DBRecord jobDef;
        int[] selectedJobIdentifiers = getSelectedIdentifiers();
        String idField = dbPluginMgr.getIdentifierField("jobDefintition");
        for(int i=0; i<selectedJobIdentifiers.length; ++i){
          jobDef = dbPluginMgr.getJobDefinition(
              selectedJobIdentifiers[i]);
          DatasetMgr datasetMgr = GridPilot.getClassMgr().getDatasetMgr(dbName,
              dbPluginMgr.getJobDefDatasetID(Integer.parseInt(
                  jobDef.getValue(idField).toString())));
          datasetMgr.addJobs(new int [] {selectedJobIdentifiers[i]});
        }
      }
    }.start();
  }

  /**
   * Called when mouse is pressed on Submit button
   */
  private void bSubmit_mousePressed(){
    // if a partition is selected, shows the menu with computing systems
    if(getSelectedIdentifiers().length != 0){
      pmSubmitMenu.show(this, 0, 0); // without this, pmSubmitMenu.getWidth == 0

      pmSubmitMenu.show(bSubmit, -pmSubmitMenu.getWidth(),
                        -pmSubmitMenu.getHeight() + bSubmit.getHeight());
    }
  }

  /**
   * Called when a computing system in pmSubmitMenu is selected
   * Submits all selected logicalFiles (partitions) in computing system chosen in the popupMenu
   */
  private void submit(ActionEvent e){
    int[] selectedJobDefIdentifiers = getSelectedIdentifiers();
    Vector selectedJobDefinitions = new Vector();
    for(int i=0; i<selectedJobDefIdentifiers.length; ++i){
      selectedJobDefinitions.add(dbPluginMgr.getJobDefinition(selectedJobDefIdentifiers[i]));
    }
    String csName = ((JMenuItem)e.getSource()).getText();
    // submit the jobs
    submissionControl.submitJobDefinitions(selectedJobDefinitions, csName, dbPluginMgr);
  }
  
  public void copy(){
    Debug.debug("Copying!", 3);
    int [] ids = getSelectedIdentifiers();
    StringSelection stringSelection = new StringSelection(
        dbName+" "+tableName+" "+Util.arrayToString(ids));
    Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
    clipboard.setContents(stringSelection, this);
    clipboardOwned = true;
    menuEditPaste.setEnabled(true);
    GridPilot.getClassMgr().getGlobalFrame().cutting = false;
  }
  
  public void cut(){
    Debug.debug("Cutting!", 3);
    copy();
    GridPilot.getClassMgr().getGlobalFrame().cutting = true;
    GridPilot.getClassMgr().getGlobalFrame().cutPanel = this;
  }
  
  public void paste(){
    String clip = getClipboardContents();
    Debug.debug("Pasting "+clip, 3);
    String [] records = Util.split(clip);
    if(records.length<3){
      return;
    }
    if(!clipboardOwned){
      return;
    }
    try{
      String prefix = "";
      for(int i=2; i<records.length; ++i){
        // Check if record is a dataset and already there and
        // ask for prefix if this is the case.
        // Only datasets have unique names.
        try{
          if(tableName.equalsIgnoreCase("dataset") &&
              GridPilot.getClassMgr().getDBPluginMgr(dbName).getDatasetID(
                  GridPilot.getClassMgr().getDBPluginMgr(records[0]).getDatasetName(
                      Integer.parseInt(records[i])))>-1){
            prefix = Util.getFileName("Cannot overwrite, please give prefix", "new-");
          }
        }
        catch(Exception e){
        }
        insertRecord(records[0], records[1], dbName, tableName,
            Integer.parseInt(records[i]), prefix);
      }
    }
    catch(Exception e){
      return;
    }
    // If records were inserted in target table and we're cutting,
    // delete source records
    if(GridPilot.getClassMgr().getGlobalFrame().cutting){
      Debug.debug("Deleting "+(records.length-2)+" rows", 2);
      statusBar.setLabel(
      "Deleting job definition(s). Please wait ...");
      JProgressBar pb = new JProgressBar();
      pb.setMaximum((records.length-2));
      for(int i=2; i<records.length; ++i){
        try{
          deleteRecord(records[0], records[1],
              Integer.parseInt(records[i]));
        }
        catch(Exception e){
          String msg = "Deleting record "+(i-2)+" failed. "+
             GridPilot.getClassMgr().getDBPluginMgr(records[0]).getError();
          Debug.debug(msg, 1);
          statusBar.setLabel(msg);
          GridPilot.getClassMgr().getLogFile().addMessage(msg);
          continue;
        }
        pb.setValue(pb.getValue()+1);
      }
      statusBar.setLabel(
         "Deleting job definition(s) done.");
    }
    GridPilot.getClassMgr().getGlobalFrame().cutting = false;
    refresh();
    try{
      ((DBPanel) GridPilot.getClassMgr().getGlobalFrame().cutPanel).refresh();
    }
    catch(Exception e){
      e.printStackTrace();
    }
  }
  
  public void insertRecord(String sourceDB, String sourceTable,
      String targetDB, String targetTable, int id, String prefix) throws Exception{
    
    DBPluginMgr sourceMgr = GridPilot.getClassMgr().getDBPluginMgr(sourceDB);
    DBPluginMgr targetMgr = GridPilot.getClassMgr().getDBPluginMgr(targetDB);
    
    DBRecord record = null;

    if(tableName.equalsIgnoreCase("jobDefinition")){
      try{
        record = sourceMgr.getJobDefinition(id);
        insertJobDefinition(record, targetMgr);
      }
      catch(Exception e){
        String msg = "ERROR: job definition "+id+" could not be created, "+sourceDB+
        "."+sourceTable+"->"+targetDB+"."+targetTable+". "+e.getMessage();
        Debug.debug(msg, 1);
        statusBar.setLabel(msg);
        GridPilot.getClassMgr().getLogFile().addMessage(msg, e);
        throw e;
      }
    }
    else if(tableName.equalsIgnoreCase("dataset")){
      try{
        record = sourceMgr.getDataset(id);
        insertDataset(record, sourceMgr, targetMgr, prefix);
      }
      catch(Exception e){
        String msg = "ERROR: dataset "+id+" could not be created, "+sourceDB+
        "."+sourceTable+"->"+targetDB+"."+targetTable+". "+e.getMessage();
        Debug.debug(msg, 1);
        statusBar.setLabel(msg);
        GridPilot.getClassMgr().getLogFile().addMessage(msg, e);
        throw e;
      }
    }
    else if(tableName.equalsIgnoreCase("transformation")){
      try{
        record = sourceMgr.getTransformation(id);
        insertTransformation(record, sourceMgr, targetMgr);
      }
      catch(Exception e){
        String msg = "ERROR: transformation "+id+" could not be created, "+sourceDB+
        "."+sourceTable+"->"+targetDB+"."+targetTable+". "+e.getMessage();
        Debug.debug(msg, 1);
        statusBar.setLabel(msg);
        GridPilot.getClassMgr().getLogFile().addMessage(msg, e);
        throw e;
      }
    }
    else if(tableName.equalsIgnoreCase("runtimeEnvironment")){
      try{
        record = sourceMgr.getRuntimeEnvironment(id);
        insertRuntimeEnvironment(record, targetMgr);
      }
      catch(Exception e){
        String msg = "ERROR: runtime environment "+id+" could not be created, "+sourceDB+
        "."+sourceTable+"->"+targetDB+"."+targetTable+". "+e.getMessage();
        Debug.debug(msg, 1);
        statusBar.setLabel(msg);
        GridPilot.getClassMgr().getLogFile().addMessage(msg, e);
        throw e;
      }
    }
  }
  
  public boolean insertJobDefinition(DBRecord jobDef, DBPluginMgr dbMgr) throws Exception{  
    try{
      // Check if parent dataset exists
      String targetJobDefIdentifier = dbMgr.getIdentifierField("jobDefinition");
      int targetDsId = dbMgr.getJobDefDatasetID(Integer.parseInt(jobDef.getValue(targetJobDefIdentifier).toString()));
      if(targetDsId>-1){
        dbMgr.createJobDef(jobDef.fields, jobDef.values);
      }
      else{
        throw(new Exception("ERROR: Parent dataset for job defintion "+
            jobDef.getValue(targetJobDefIdentifier)+" does not exist."));
      }
    }
    catch(Exception e){
      throw e;
    }
    return true;
  }
  
  public boolean insertDataset(DBRecord dataset, DBPluginMgr sourceMgr,
      DBPluginMgr targetMgr, String prefix) throws Exception{
    try{
      boolean ok = false;
      boolean success = true;
      try{
        // Check if referenced transformation exists
        
        String sourceTransName = sourceMgr.getDatasetTransformationName(
            Integer.parseInt(dataset.getValue(sourceMgr.getIdentifierField(
                "dataset")).toString()));
        String sourceTransVersion = sourceMgr.getDatasetTransformationVersion(
            Integer.parseInt(dataset.getValue(sourceMgr.getIdentifierField(
                "dataset")).toString()));  
        
        DBResult targetTransformations = targetMgr.getTransformations();
        Vector transVec = new Vector();
        for(int i=0; i<targetTransformations.values.length; ++i){
          if(targetTransformations.getValue(i, targetMgr.getNameField(
              "transformation")).toString(
              ).equalsIgnoreCase(sourceTransName)){
            transVec.add(targetTransformations.getRow(i));
          }
        }
        for(int i=0; i<transVec.size(); ++i){
          // TODO: consider adding method like getVersionField
          if(((DBRecord) transVec.get(i)).getValue(targetMgr.getVersionField(
              "transformation")
              ).toString().equalsIgnoreCase(sourceTransVersion)){
            ok = true;
            break;
          }
        }
      }
      catch(Exception ee){
        ee.printStackTrace();
      }
      if(ok){
        Debug.debug("Creating dataset: " + Util.arrayToString(dataset.fields, ":") + " ---> " +
            Util.arrayToString(dataset.values, ":"), 3);
        try{
          // try adding prefix
          dataset.setValue(sourceMgr.getNameField("dataset"),
              prefix+dataset.getValue(sourceMgr.getNameField("dataset")));
        }
        catch(Exception e){
          Debug.debug("WARNING: Could not add prefix", 3);
          e.printStackTrace();
        }
        success = targetMgr.createDataset("dataset", dataset.fields, dataset.values);
        if(!success){
          throw(new Exception("ERROR: "+targetMgr.getError()));
        }
      }
      else{
        throw(new Exception("ERROR: Transformation for dataset does not exist."));
      }
    }
    catch(Exception e){
      throw e;
    }
    return true;
  }
  
  public boolean insertTransformation(DBRecord transformation, DBPluginMgr sourceMgr,
      DBPluginMgr targetMgr) throws Exception{
    try{
      // Check if referenced runtime environment exists
      String sourceTransformationIdentifier = sourceMgr.getIdentifierField("transformation");
      String targetTransformationIdentifier = targetMgr.getIdentifierField("transformation");
      String targetRuntimeEnvironmentName = targetMgr.getNameField("runtimeEnvironment");
      String runtimeEnvironment = sourceMgr.getTransformationRuntimeEnvironment(
          Integer.parseInt(transformation.getValue(
              sourceTransformationIdentifier).toString()));
      DBResult targetRuntimes = targetMgr.getRuntimeEnvironments();
      Vector runtimeNames = new Vector();
      for(int i=0; i<targetRuntimes.values.length; ++i){
        runtimeNames.add(targetRuntimes.getValue(i, targetRuntimeEnvironmentName).toString());
      }
      if(runtimeEnvironment!=null && runtimeNames!=null &&
          runtimeNames.contains(runtimeEnvironment)){
        targetMgr.createTransformation(transformation.values);
      }
      else{
        throw(new Exception("ERROR: runtime environment for transformation "+
            transformation.getValue(targetTransformationIdentifier)+" does not exist."));
      }
    }
    catch(Exception e){
      throw e;
    }
    return true;
  }
  
  public boolean insertRuntimeEnvironment(DBRecord pack, DBPluginMgr dbMgr) throws Exception{
    try{
      dbMgr.createRuntimeEnvironment(pack.values);
    }
    catch(Exception e){
      throw e;
    }
    return true;
  }
  
  public void deleteRecord(String sourceDB, String sourceTable, int id) throws Exception{
    DBPluginMgr sourceMgr = GridPilot.getClassMgr().getDBPluginMgr(sourceDB);
    if(tableName.equalsIgnoreCase("jobDefinition")){
      try{
        sourceMgr.deleteJobDefinition(id);
      }
      catch(Exception e){
        String msg = "ERROR: job definition "+id+" could not be deleted from, "+sourceDB+
        "."+sourceTable;
        Debug.debug(msg, 1);
        statusBar.setLabel(msg);
        GridPilot.getClassMgr().getLogFile().addMessage(msg, e);
        throw e;
      }
    }
    else if(tableName.equalsIgnoreCase("dataset")){
      try{
        sourceMgr.deleteDataset(id, true);
      }
      catch(Exception e){
        String msg = "ERROR: dataset "+id+" could not be deleted from, "+sourceDB+
        "."+sourceTable;
        Debug.debug(msg, 1);
        statusBar.setLabel(msg);
        GridPilot.getClassMgr().getLogFile().addMessage(msg, e);
        throw e;
      }
    }
    else if(tableName.equalsIgnoreCase("transformation")){
      try{
        sourceMgr.deleteTransformation(id);
      }
      catch(Exception e){
        String msg = "ERROR: transformation "+id+" could not be deleted from, "+sourceDB+
        "."+sourceTable;
        Debug.debug(msg, 1);
        statusBar.setLabel(msg);
        GridPilot.getClassMgr().getLogFile().addMessage(msg, e);
        throw e;
      }
    }
    else if(tableName.equalsIgnoreCase("runtimeEnvironment")){
      try{
        sourceMgr.deleteRuntimeEnvironment(id);
      }
      catch(Exception e){
        String msg = "ERROR: runtime environment "+id+" could not be deleted from, "+sourceDB+
        "."+sourceTable;
        Debug.debug(msg, 1);
        statusBar.setLabel(msg);
        GridPilot.getClassMgr().getLogFile().addMessage(msg, e);
        throw e;
      }
    }
  }
  
  /**
   * Implementation of the ClipboardOwner interface.
   */
   public void lostOwnership(Clipboard aClipboard, Transferable aContents){
   }

  /**
  * Place a String on the clipboard, and make this class the
  * owner of the Clipboard's contents.
  */
  public void setClipboardContents(String aString){
    StringSelection stringSelection = new StringSelection(aString);
    Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
    clipboard.setContents(stringSelection, this);
  }

  /**
  * Get the String residing on the clipboard.
  *
  * @return any text found on the Clipboard; if none found, return an
  * empty String.
  */
  public String getClipboardContents(){
    String result = "";
    Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
    //odd: the Object param of getContents is not currently used
    Transferable contents = clipboard.getContents(null);
    boolean hasTransferableText =
      (contents != null) &&
      contents.isDataFlavorSupported(DataFlavor.stringFlavor);
    if(hasTransferableText){
      try{
        result = (String)contents.getTransferData(DataFlavor.stringFlavor);
      }
      catch(UnsupportedFlavorException ex){
        //highly unlikely since we are using a standard DataFlavor
        System.out.println(ex);
        ex.printStackTrace();
      }
      catch (IOException ex) {
        System.out.println(ex);
        ex.printStackTrace();
      }
    }
    return result;
  }

}
