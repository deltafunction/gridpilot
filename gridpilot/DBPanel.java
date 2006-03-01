package gridpilot;

import gridpilot.Table;
import gridpilot.Database.DBResult;
import gridpilot.Database.DBRecord;
import gridpilot.Debug;
import gridpilot.GridPilot;

import javax.swing.*;
import javax.swing.event.*;

import java.awt.*;
import java.awt.event.*;
import java.util.Vector;

/**
 * This panel contains one SelectPanel.
 *
 */

public class DBPanel extends JPanel implements JobPanel{

  private JScrollPane spSelectPanel = new JScrollPane();
  private SelectPanel selectPanel;
  private JPanel pButtonSelectPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
  public JPanel panelSelectPanel = new JPanel(new GridBagLayout());

  private JScrollPane spTableResults = new JScrollPane();
  private Table tableResults = null;
  private JPanel pButtonTableResults = new JPanel(new FlowLayout(FlowLayout.RIGHT));
  private JPanel panelTableResults = new JPanel(new GridBagLayout());
  
  private JButton bCreateRecords = new JButton("Define new records");
  private JButton bEditRecord = new JButton("Edit record");
  private JPopupMenu pmSubmitMenu = new JPopupMenu();
  private JButton bSubmit = new JButton("Submit job(s)");
  private JButton bMonitor = new JButton("Monitor job(s)");
  private JButton bDeleteRecord = new JButton("Delete record(s)");
  private JButton bSearch = new JButton("Search");
  private JButton bClear = new JButton("Clear");
  private JButton bViewJobDefinitions = new JButton("Show jobDefinitions");
  private JMenuItem miEdit = null;
  
  private int [] identifiers;
  //private String [] dbIdentifiers;
  //private String [] stepIdentifiers;
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
  
  private GridBagConstraints ct = new GridBagConstraints();
  
  // TODO: clean up getting and setting and usage of dbPluginMgr
  private DBPluginMgr dbPluginMgr = null;
  private int parentId = -1;

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
  
  private SubmissionControl submissionControl;


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
     
     identifier = dbPluginMgr.getIdentifier(dbPluginMgr.getDBName(), tableName);
     jobDefIdentifier = dbPluginMgr.getIdentifier(dbPluginMgr.getDBName(), "jobDefinition");
          
     ct.fill = GridBagConstraints.HORIZONTAL;
     ct.anchor = GridBagConstraints.NORTH;
     ct.insets = new Insets(0,0,0,0);
     ct.gridwidth=1;
     ct.gridheight=1;  
     ct.weightx = 0.0;
     ct.gridx = 0;
     ct.gridy = 1;   
     ct.ipady = 250;

     defaultFields = GridPilot.getClassMgr().getDBPluginMgr(
         dbName).getDBDefFields(dbName, tableName);
     Debug.debug("Default fields "+defaultFields.length, 3);

    fieldNames =
       GridPilot.getClassMgr().getDBPluginMgr(dbName).getFieldNames(tableName);
    
    hiddenFields = GridPilot.getClassMgr().getDBPluginMgr(dbName
       ).getDBHiddenFields(dbName, tableName);
    Debug.debug("Hidden fields "+hiddenFields.length, 3);
    tableResults = new Table(hiddenFields, fieldNames,
        GridPilot.colorMapping);
    
    submissionControl = GridPilot.getClassMgr().getSubmissionControl();
    
    setFieldArrays();
    
    initGUI();
  }
   
   public Table getTable(){
     return tableResults;
   }


   private void setFieldArrays(){
     Vector shownSet = new Vector();  
     boolean ok = true;
     for(int i=0; i<defaultFields.length; ++i){
       ok = false;
       for(int j=0; j<fieldNames.length; ++j){
         Debug.debug("Checking fields for showing"+defaultFields[i]+"<->"+fieldNames[j], 3);
         if(defaultFields[i].equalsIgnoreCase(fieldNames[j]) ||
             defaultFields[i].equalsIgnoreCase("*")){
           ok = true;
           break;
         }
       }
       if(ok){
         Debug.debug("Showing "+defaultFields[i], 3);
         shownSet.add(defaultFields[i]);
       }
     }
     
     shownFields = new String[shownSet.size()];
     for(int i=0; i<shownSet.size(); ++i){
       shownFields[i] = shownSet.get(i).toString();
     }
     
     for(int k=0; k<shownFields.length; ++k){
       shownFields[k] = tableName+"."+shownFields[k];
     }
     
     
     Vector selectSet = new Vector();  
     ok = true;
     for(int i=0; i<defaultFields.length; ++i){
       ok = true;
       for(int j=0; j<hiddenFields.length; ++j){
         Debug.debug("Checking fields for selecting "+defaultFields[i]+"<->"+hiddenFields[j], 3);
         if(defaultFields[i].equalsIgnoreCase(hiddenFields[j]) &&
             !defaultFields[i].equalsIgnoreCase("*")){
           ok = false;
           break;
         }
       }
       boolean fieldOk = false;
       for(int j=0; j<fieldNames.length; ++j){
         if(fieldNames[j].equalsIgnoreCase(defaultFields[i])){
           fieldOk = true;
           break;
         }
       }
       if(ok && fieldOk){
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

  private void initGUI() throws Exception {

//// SelectPanel

    selectPanel = new SelectPanel(tableName, fieldNames);
    selectPanel.initGUI();
    clear();

    this.setLayout(new GridBagLayout());

    spSelectPanel.getViewport().add(selectPanel);

    panelSelectPanel.add(spSelectPanel, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0
        ,GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    panelSelectPanel.add(pButtonSelectPanel, new GridBagConstraints(0, 1, 1, 1, 1.0, 0.0
        ,GridBagConstraints.EAST, GridBagConstraints.NONE, new Insets(10, 10, 10, 10), 0, 0));

    selectPanel.setConstraint(dbName, "name", "", 1);
    
    // Listen for enter key in text field
    this.selectPanel.spcp.tfConstraintValue.addKeyListener(new KeyAdapter(){
      public void keyPressed(KeyEvent e){
        switch(e.getKeyCode()){
          case KeyEvent.VK_ENTER:
            searchRequest();
        }
      }
    });

//// panel table results

    panelTableResults.add(spTableResults,  new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0
        ,GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    panelTableResults.add(pButtonTableResults,    new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0
        ,GridBagConstraints.EAST, GridBagConstraints.NONE, new Insets(10, 10, 10, 10), 0, 0));

    ct.fill = GridBagConstraints.HORIZONTAL;
    ct.anchor = GridBagConstraints.NORTH;
    ct.insets = new Insets(0,0,0,0);
    ct.weightx = 0.5;
    ct.gridx = 0;
    ct.gridy = 0;         
    ct.gridwidth=1;
    ct.gridheight=1;  
    ct.gridx = 0;
    ct.gridy = 0;   
    ct.ipady = 100;
    this.add(panelSelectPanel,ct);
    ct.weightx = 0.0;
    ct.gridx = 0;
    ct.gridy = 1;   
    ct.ipady = 250;
    this.add(panelTableResults,ct);
    this.updateUI();
    
//// buttons
    
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
    
    if(tableName.equalsIgnoreCase("dataset") ||
        // support external schema on proddb
        tableName.equalsIgnoreCase("task")){
      bViewJobDefinitions.addActionListener(new java.awt.event.ActionListener(){
        public void actionPerformed(ActionEvent e){
          viewJobDefinitions();
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
      addButtonResultsPanel(bCreateRecords);
      addButtonResultsPanel(bEditRecord);
      addButtonResultsPanel(bDeleteRecord);
      addButtonSelectPanel(bClear);
      addButtonSelectPanel(bSearch);
      bViewJobDefinitions.setEnabled(false);
      bEditRecord.setEnabled(false);
      bDeleteRecord.setEnabled(false);
      updateUI();
    }
    else if(tableName.equalsIgnoreCase("jobDefinition")){
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
          createJobDefs();
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
      updateUI();
    }
    else if(tableName.equalsIgnoreCase("transformation")){
      bCreateRecords.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          createJobTransRecords();
        }
      });

      bEditRecord.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          editJobTransRecord();
        }
      });

      bDeleteRecord.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          deleteJobTransRecords();
        }
      });
      
      addButtonResultsPanel(bCreateRecords);
      addButtonResultsPanel(bEditRecord);
      addButtonResultsPanel(bDeleteRecord);
      addButtonSelectPanel(bClear);
      addButtonSelectPanel(bSearch);
      bViewJobDefinitions.setEnabled(false);
      //bViewJobTransRecords.setEnabled(false);
      bEditRecord.setEnabled(false);
      bDeleteRecord.setEnabled(false);
      updateUI();
    }    
  }

  public String getTitle(){
    return tableName/*+"s"*//*jobTranss looks bad...*/;
  }

  public void panelShown(){
    Debug.debug("panelShown", 1);
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
   * Returns the name of the first selected row, "-1" if no row is selected.
   */
  /*public String getSelectedName(){
    int selRow = tableResults.getSelectedRow();
    return (selRow==-1) ? "-1" : tableResults.getUnsortedValueAt(selRow, 0).toString();
  }*/

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
    searchRequest();
    remove(panelTableResults);
    add(panelTableResults, ct);
    updateUI();
  }

  /**
   * Resets fields to default fields and clear values
   */
  public void clear(){
    String [][] values = new String[selectFields.length][2];

    for(int i = 0; i<selectFields.length; ++i){
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
    selectPanel.updateUI();
  }

  /**
   * Request DBPluginMgr for select request from SelectPanel, and fill tableResults
   * with results. The request is performed in a separeted thread, avoiding to block
   * all the GUI during this action.
   * Called when button "Search" is clicked
   */
  public void searchRequest(){
        
    // TODO: why does it not work as thread when
   // not in it's own pane?
    //workThread = new Thread(){
      //public void run(){
        //if(!getWorking()){
          //Debug.debug("please wait ...", 2);
          //return;
        //}
        //setFieldArrays();
        String selectRequest;
        selectRequest = selectPanel.getRequest(shownFields);
        if(selectRequest == null)
            return;
        
        /*
         Support several dbs (represented by dbRes[]) and merge them
         all in one big table (res) with an extra column specifying
         the name of the db from which each row came
        */      
        //DBResult [] stepRes = new DBResult[dbs.length];
        DBResult res = null;
        res = GridPilot.getClassMgr().getDBPluginMgr(dbName).select(
            selectRequest,identifier);

        bViewJobDefinitions.setEnabled(false);
        //bViewJobTransRecords.setEnabled(false);
        bEditRecord.setEnabled(false);
        bDeleteRecord.setEnabled(false);
        bSubmit.setEnabled(false);
        bMonitor.setEnabled(false);
        
        tableResults.setTable(res.values, res.fields);
        spTableResults.getViewport().add(tableResults);
        
        identifiers = new int[tableResults.getRowCount()];
        // 'col' is the column with the jobDefinition identifier
        int col = tableResults.getColumnCount()-1;
        for(int i=0; i<tableResults.getColumnCount(); ++i){
          Debug.debug("Column: "+tableResults.getColumnName(i)+"<->"+identifier, 3);
          if(tableResults.getColumnName(i).equalsIgnoreCase(identifier)){
            col = i;
            break;
          }
        }
        for(int i=0; i<identifiers.length; ++i){
          identifiers[i] = new Integer(tableResults.getUnsortedValueAt(i, col).toString()).intValue();
        }

        if(tableName.equalsIgnoreCase("dataset")){
          tableResults.getSelectionModel().setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
          tableResults.addListSelectionListener(new ListSelectionListener(){
            public void valueChanged(ListSelectionEvent e){
              if (e.getValueIsAdjusting()) return;
              ListSelectionModel lsm = (ListSelectionModel)e.getSource();
              Debug.debug("lsm indices: "+
                  lsm.getMaxSelectionIndex()+" : "+lsm.getMinSelectionIndex(), 3);
              bViewJobDefinitions.setEnabled(!lsm.isSelectionEmpty());
              bDeleteRecord.setEnabled(!lsm.isSelectionEmpty());
              bEditRecord.setEnabled(!lsm.isSelectionEmpty());
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
              Debug.debug("lsm indices: "+
                  lsm.getMaxSelectionIndex()+" : "+lsm.getMinSelectionIndex(), 3);
              bSubmit.setEnabled(!lsm.isSelectionEmpty());
              bMonitor.setEnabled(!lsm.isSelectionEmpty());
              bDeleteRecord.setEnabled(!lsm.isSelectionEmpty());
              bEditRecord.setEnabled(!lsm.isSelectionEmpty() &&
                  lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
              miEdit.setEnabled(!lsm.isSelectionEmpty() &&
                  lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
            }
          });

          makeJobDefMenu();
        }
        else if(tableName.equalsIgnoreCase("transformation")){
          tableResults.getSelectionModel().setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
          tableResults.addListSelectionListener(new ListSelectionListener(){
            public void valueChanged(ListSelectionEvent e){
              if (e.getValueIsAdjusting()) return;
              ListSelectionModel lsm = (ListSelectionModel)e.getSource();
              Debug.debug("lsm indices: "+
                  lsm.getMaxSelectionIndex()+" : "+lsm.getMinSelectionIndex(), 3);
              bDeleteRecord.setEnabled(!lsm.isSelectionEmpty());
              bEditRecord.setEnabled(!lsm.isSelectionEmpty());
            }
          });

          makeJobTransMenu();
        }
        
        GridPilot.getClassMgr().getStatusBar().setLabel("Records found: "+tableResults.getRowCount(), 20);
        //stopWorking();
      //}
    //};
    //workThread.start();

  }
  
  /**
   * Add menu items to the table with search results. This function is called from within DBPanel
   * after the results table is filled
   */
  public void makeDatasetMenu(){
    Debug.debug("Making dataset menu", 3);
    JMenuItem miViewJobDefinitions = new JMenuItem("Show job definitions");
    JMenuItem miViewJobTransRecords = new JMenuItem("Show transformations");
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
    miViewJobTransRecords.setEnabled(true);
    miDelete.setEnabled(true);
    miEdit.setEnabled(true);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miViewJobDefinitions);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miViewJobTransRecords);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miDelete);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miEdit);
  }

  public void makeJobTransMenu(){
    Debug.debug("Making jobTrans menu", 3);
    JMenuItem miDelete = new JMenuItem("Delete");
    miEdit = new JMenuItem("Edit");
    miDelete.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        deleteJobTransRecords();
      }
    });
    miEdit.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        editJobTransRecord();
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
    JMenu jmSubmit = new JMenu("Submit job()");
    JMenu miMonitor = new JMenu("Monitor job()");
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
    searchRequest();
  }
    
   /**
   * Open dialog with jobDefintion creation panel
   */ 
  private void createJobDefs(){
    DatasetMgr datasetMgr = null;
    try{
      datasetMgr = GridPilot.getClassMgr().getDatasetMgr(dbPluginMgr.getDBName(), parentId);
    }
    catch(Throwable e){
      Debug.debug("ERROR: could not create DatasetMgr. "+e.getMessage(), 1);
      e.printStackTrace();
    }
    //hiddenFields = dbPluginMgr.getDBHiddenFields(dbs[0], tableName);
    JobDefCreationPanel panel = new JobDefCreationPanel(dbName, datasetMgr, tableResults, false);
    try{
      dbPluginMgr.initJobDefCreationPanel(panel);
    }
    catch(Throwable e){
      Debug.debug("Could not initialize panel "+dbName+". "+e.getMessage(), 1);
    }
    CreateEditDialog pDialog = new CreateEditDialog(
       GridPilot.getClassMgr().getGlobalFrame(),
        panel, false, false);
    pDialog.setTitle(tableName);
    pDialog.setVisible(true);
    /*if(tableResults!=null && tableResults.getRowCount()>0){
      searchRequest();
    }*/
  }

  private void editJobDef(){
    DatasetMgr datasetMgr = null;
    int selectedDatasetID = dbPluginMgr.getJobDefDatasetID(getSelectedIdentifier());
    if(parentId<0){
      parentId = selectedDatasetID;
    }
    Debug.debug("Got dbPluginMgr:"+dbPluginMgr+":"+parentId, 1);
    try{
      datasetMgr = GridPilot.getClassMgr().getDatasetMgr(dbPluginMgr.getDBName(), selectedDatasetID);
    }
    catch(Throwable e){
      Debug.debug("ERROR: could not get DatasetMgr. "+e.getMessage(), 1);
      e.printStackTrace();
      return;
    }
    JobDefCreationPanel panel = new JobDefCreationPanel(dbName, datasetMgr, tableResults, true);
    try{
      dbPluginMgr.initJobDefCreationPanel(panel);
    }
    catch(Throwable e){
      Debug.debug("Could not initialize panel "+dbName+". "+e.getMessage(), 1);
    }
    CreateEditDialog pDialog = new CreateEditDialog(
        GridPilot.getClassMgr().getGlobalFrame(),
        panel, true, false);
    pDialog.setTitle(tableName);
    pDialog.setVisible(true);
    /*searchRequest();*/
  }

  private void deleteJobDefs(){
    String msg = "Are you sure you want to delete jobDefinition(s) ";
    int [] ids = getSelectedIdentifiers();
    for(int i=0; i<getSelectedIdentifiers().length; ++i){
      msg += ", " + ids[i];
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
        //DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
        if(!getWorking()){
          Debug.debug("please wait ...", 2);
          return;
        }
        int [] ids = getSelectedIdentifiers();
        int [] rows = tableResults.getSelectedRows();
        Debug.debug("Deleting "+ids.length+" rows", 2);
        if(ids.length != 0){
          JProgressBar pb = new JProgressBar();
          pb.setMaximum(ids.length);
          for(int i = ids.length-1; i>=0; i--){
            boolean success = dbPluginMgr.deleteJobDefinition(ids[i]);
            pb.setValue(pb.getValue()+1);
            tableResults.removeRow(rows[i]);
            tableResults.tableModel.fireTableDataChanged();
          }
        }
        stopWorking();
      }
    };
    workThread.start();
  }

  /**
   * Open dialog with dataset creation panel
   */ 
  private void createDatasets(){
    CreateEditDialog pDialog = new CreateEditDialog(
       GridPilot.getClassMgr().getGlobalFrame(),
        new DatasetCreationPanel(dbPluginMgr, this, false), false, false);
    pDialog.setTitle(tableName);
    pDialog.setVisible(true);
    /*if(tableResults!=null && tableResults.getRowCount()>0){
      searchRequest();
   }*/
 }

 private void editDataset(){
   CreateEditDialog pDialog = new CreateEditDialog(
       GridPilot.getClassMgr().getGlobalFrame(),
       new DatasetCreationPanel(dbPluginMgr, this, true), true, false);
   pDialog.setTitle(tableName);
   pDialog.setVisible(true);
   /*searchRequest();*/
 }

  private void deleteDatasets(){
    String msg = "Are you sure you want to delete dataset ";
    int [] ids = getSelectedIdentifiers();
    for(int i=0; i<getSelectedIdentifiers().length; ++i){
      msg += ", " + ids[i];
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
          Debug.debug("please wait ...", 2);
          return;
        }
        int [] ids = getSelectedIdentifiers();
        int [] rows = tableResults.getSelectedRows();
        Debug.debug("Deleting "+ids.length+" rows", 2);
        if(ids.length != 0){
          JProgressBar pb = new JProgressBar();
          pb.setMaximum(ids.length);
          for(int i = ids.length-1; i>=0; i--){
            boolean success = dbPluginMgr.deleteDataset(ids[i], true);
            pb.setValue(pb.getValue()+1);
            tableResults.removeRow(rows[i]);
            tableResults.tableModel.fireTableDataChanged();
          }
        }
        stopWorking();
        searchRequest();
      }
    };
    workThread.start();
  }

  /**
   * Open dialog with jobTrans creation panel
   */ 
  private void createJobTransRecords(){
    CreateEditDialog pDialog = new CreateEditDialog(
       GridPilot.getClassMgr().getGlobalFrame(),
          new TransformationCreationPanel(dbPluginMgr, tableResults, false), false, false);
    pDialog.setTitle(tableName);
    pDialog.setVisible(true);
    /*if(tableResults!=null && tableResults.getRowCount()>0){
      searchRequest();
    }*/
  }

  private void editJobTransRecord(){
    //DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(GridPilot.getDBs()[0]);
    CreateEditDialog pDialog = new CreateEditDialog(
       GridPilot.getClassMgr().getGlobalFrame(),
          new TransformationCreationPanel(dbPluginMgr, tableResults, true), true, false);
    pDialog.setTitle(tableName);
    pDialog.setVisible(true);
    /*searchRequest();*/
  }

  private void deleteJobTransRecords(){
    String msg = "Are you sure you want to delete transformation records ";
    int [] ids = getSelectedIdentifiers();
    for(int i=0; i<getSelectedIdentifiers().length; ++i){
      msg += ", " + ids[i];
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
        //DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
        if(!getWorking()){
          Debug.debug("please wait ...", 2);
          return;
        }
        int [] ids = getSelectedIdentifiers();
        int [] rows = tableResults.getSelectedRows();
        Debug.debug("Deleting "+ids.length+" rows", 2);
        if(ids.length != 0){
          JProgressBar pb = new JProgressBar();
          pb.setMaximum(ids.length);
          for(int i = ids.length-1; i>=0; i--){
            boolean success = dbPluginMgr.deleteTransformation(ids[i]);
            pb.setValue(pb.getValue()+1);
            tableResults.removeRow(rows[i]);
            tableResults.tableModel.fireTableDataChanged();
          }
        }
        stopWorking();
        searchRequest();
      }
    };
    workThread.start();
  }

  /**
   * Open new pane with list of jobDefinitions.
   */
  private void viewJobDefinitions(){
    if(getSelectedIdentifier() != -1){
      new Thread(){
        public void run(){
          //DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
          try{
            // Create new panel with jobDefinitions.         
            int id = getSelectedIdentifier();
            DBPanel dbPanel = new DBPanel("jobDefinition",
                dbPluginMgr, id);
            dbPanel.selectPanel.setConstraint("jobDefinition", "datasetFK",
                Integer.toString(id), 0);
            dbPanel.selectPanel.setConstraint("jobDefinition", "dataset",
                dbPluginMgr.getDatasetName(id), 0);
            dbPanel.searchRequest();           
            // Create new dataset panel showing transformation records
            GridPilot.getClassMgr().getGlobalFrame().addPanel(dbPanel);                   
          }catch (Exception e){
            Debug.debug("Couldn't create panel for dataset " + "\n" +
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
    new Thread(){
      public void run(){        
        DBRecord jobDef;
        int[] selectedJobIdentifiers = getSelectedIdentifiers();
        for(int i=0; i<selectedJobIdentifiers.length; ++i){
          jobDef = dbPluginMgr.getJobDefinition(
              selectedJobIdentifiers[i]);
          DatasetMgr datasetMgr = GridPilot.getClassMgr().getDatasetMgr(dbPluginMgr.getDBName(),
              Integer.parseInt(jobDef.getValue("datasetFK").toString()));
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
    int[] selectedJobIdentifiers = getSelectedIdentifiers();
    Vector selectedJobDefinitions = new Vector();
    for(int i=0; i<selectedJobIdentifiers.length; ++i){
      selectedJobDefinitions.add(dbPluginMgr.getJobDefinition(i));
    }
    //String csName = ((JMenuItem)e.getSource()).getMnemonic();
    String csName = ((JMenuItem)e.getSource()).getText();

    // submit the jobs
    submissionControl.submitJobDefinitions(selectedJobDefinitions, csName);
  }
}
