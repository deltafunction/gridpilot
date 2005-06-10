package gridpilot;

import gridpilot.Table;
import gridpilot.Database.DBResult;
import gridpilot.Database.JobDefinition;
import gridpilot.Debug;
import gridpilot.GridPilot;
import gridpilot.ConfigFile;

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

  private ConfigFile configFile;
  private boolean withSplit = true;
  private String [] dbs;

  //private String [] tables = {"task", "jobDefinition", "jobTrans"};
  
  private JScrollPane spSelectPanel = new JScrollPane();
  private SelectPanel selectPanel;
  private JPanel pButtonSelectPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
  public JPanel panelSelectPanel = new JPanel(new GridBagLayout());

  private JScrollPane spTableResults = new JScrollPane();
  private Table tableResults = null;
  private JPanel pButtonTableResults = new JPanel(new FlowLayout(FlowLayout.RIGHT));
  private JPanel panelTableResults = new JPanel(new GridBagLayout());
  
  private JButton bViewJobTransRecords = new JButton("Show jobTrans'");
  private JButton bCreateRecords = new JButton("Define new records");
  private JButton bEditRecord = new JButton("Edit record");
  private JButton bDeleteRecord = new JButton("Delete record(s)");
  private JButton bSearch = new JButton("Search");
  private JButton bClear = new JButton("Clear");
  private JButton bViewJobDefinitions = new JButton("Show jobDefinitions");
  private JMenuItem miEdit = null;
  
  private int [] identifiers;
  private String [] dbIdentifiers;
  private String [] stepIdentifiers;
  // lists of field names with table name as key
  private String [] fieldNames = null;

  private String tableName;
  private String identifier;
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
  private synchronized boolean getWorking() {
    if (!working) {
      working = true;
      return true;
    }
    return false;
  }
  // release the semaphore
  private synchronized void stopWorking() {
    working = false;
  }


  /**
   * Constructor
   */

  /**
   * Create a new DBPanel from scratch.
   */

   public DBPanel( /*name of tables for the select*/
                  String _tableName,
                  /*name of identifier of the table actually to be searched*/
                  String _identifier) throws Exception{
  
     tableName = _tableName;
     identifier = _identifier;
     
     configFile = GridPilot.getClassMgr().getConfigFile();
     
     String [] steps;
     dbs = GridPilot.getDBs();

     String dbName = null;
     String [] previousDefaultFields = null;

     ct.fill = GridBagConstraints.HORIZONTAL;
     ct.anchor = GridBagConstraints.NORTH;
     ct.insets = new Insets(0,0,0,0);
     ct.gridwidth=1;
     ct.gridheight=1;  
     ct.weightx = 0.0;
     ct.gridx = 0;
     ct.gridy = 1;   
     ct.ipady = 250;

     // Check that default fields set in config file agree for the databases used
     for(int i = 0; i < dbs.length; ++i){
       dbName = dbs[i];
       steps = GridPilot.getSteps(dbName);
       if (steps == null) return;     
       for(int j=0; j<steps.length; ++j){
         defaultFields = GridPilot.getClassMgr().getDBPluginMgr(
             dbName, steps[j]).getDBDefFields(dbs[i], tableName);
         if(j>0){
           previousDefaultFields = GridPilot.getClassMgr().getDBPluginMgr(
               dbName, steps[j-1]).getDBDefFields(dbs[i], tableName);
           if(defaultFields.length!=previousDefaultFields.length){
             Debug.debug("WARNING: number of default fields disagree", 1);
           }
           else{
             for(int k=0; k<defaultFields.length; ++k){
               if(!defaultFields[k].equalsIgnoreCase(previousDefaultFields[k])){
                 Debug.debug("WARNING: default fields disagree, " +
                     defaultFields[k]+" != " + previousDefaultFields[k], 1);
               }
             }
           }
         }
       }
     }
     
     Debug.debug("Default fields "+defaultFields.length, 3);

     // the same table from the various databases used should have the same
    // columns, so we use the first db and the first stepName to get the fields.
    // TODO: stepName should go.
    fieldNames =
       GridPilot.getClassMgr().getDBPluginMgr(dbs[0],
           GridPilot.getSteps(dbs[0])[0]).getFieldNames(tableName);
    
    hiddenFields =  GridPilot.getClassMgr().getDBPluginMgr(dbs[0],
        GridPilot.getSteps(dbs[0])[0]).getDBHiddenFields(dbs[0], tableName);
    Debug.debug("Hidden fields "+hiddenFields.length, 3);
    tableResults = new Table(hiddenFields, fieldNames,
        GridPilot.getColorMapping());
    
    setFieldArrays();
    
    initGUI();
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
       if(ok){
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
                 /*name of identifier of the table actually to be searched*/
                 String _identifier,
                 /*pointer to the db in use for this panel*/
                 DBPluginMgr _dbPluginMgr,
                 /*identifier of the parent record (task <- jobDefinition)*/
                 int _parentId) throws Exception{
      this(_tableName, _identifier);
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

    selectPanel.setConstraint("task", "TASKNAME", "", 1);
    
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
    
    bSearch.addActionListener(new java.awt.event.ActionListener() {
      public void actionPerformed(ActionEvent e) {
        search();
      }
    });
    bSearch.setToolTipText("Search results for this request");
    bClear.addActionListener(new java.awt.event.ActionListener() {
      public void actionPerformed(ActionEvent e) {
        clear();
      }
    });
    
    if(tableName.equalsIgnoreCase("task")){
      bViewJobDefinitions.addActionListener(new java.awt.event.ActionListener(){
        public void actionPerformed(ActionEvent e){
          viewJobDefinitions();
        }
      }
      );
      bViewJobTransRecords.addActionListener(new java.awt.event.ActionListener(){
        public void actionPerformed(ActionEvent e){
          viewJobTransRecords();
        }
      }
      );

      bCreateRecords.addActionListener(new ActionListener() {
        public void actionPerformed(ActionEvent e) {
          createTasks();
        }
      });

      bEditRecord.addActionListener(new ActionListener() {
        public void actionPerformed(ActionEvent e) {
          editTask();
        }
      });

      bDeleteRecord.addActionListener(new ActionListener() {
        public void actionPerformed(ActionEvent e) {
          deleteTasks();
        }
      });
      
      addButtonResultsPanel(bViewJobDefinitions);
      addButtonResultsPanel(bViewJobTransRecords);    
      addButtonResultsPanel(bCreateRecords);
      addButtonResultsPanel(bEditRecord);
      addButtonResultsPanel(bDeleteRecord);
      addButtonSelectPanel(bClear);
      addButtonSelectPanel(bSearch);
      bViewJobDefinitions.setEnabled(false);
      bViewJobTransRecords.setEnabled(false);
      bEditRecord.setEnabled(false);
      bDeleteRecord.setEnabled(false);
      updateUI();
    }
    else if(tableName.equalsIgnoreCase("jobDefinition")){
      bEditRecord.addActionListener(new ActionListener() {
        public void actionPerformed(ActionEvent e) {
          editJobDef();
        }
      });

      bDeleteRecord.addActionListener(new ActionListener() {
        public void actionPerformed(ActionEvent e) {
          deleteJobDefs();
        }
      });

      bCreateRecords.addActionListener(new ActionListener() {
        public void actionPerformed(ActionEvent e) {
          createJobDefs();
        }
      });
      
      addButtonResultsPanel(bCreateRecords);
      addButtonResultsPanel(bEditRecord);
      addButtonResultsPanel(bDeleteRecord);
      addButtonSelectPanel(bClear);
      addButtonSelectPanel(bSearch);
      updateUI();
    }
    else if(tableName.equalsIgnoreCase("jobTrans")){
      bCreateRecords.addActionListener(new ActionListener() {
        public void actionPerformed(ActionEvent e) {
          createJobTransRecords();
        }
      });

      bEditRecord.addActionListener(new ActionListener() {
        public void actionPerformed(ActionEvent e) {
          editJobTransRecord();
        }
      });

      bDeleteRecord.addActionListener(new ActionListener() {
        public void actionPerformed(ActionEvent e) {
          deleteJobTransRecords();
        }
      });
      
      addButtonResultsPanel(bCreateRecords);
      addButtonResultsPanel(bEditRecord);
      addButtonResultsPanel(bDeleteRecord);
      addButtonSelectPanel(bClear);
      addButtonSelectPanel(bSearch);
      bViewJobDefinitions.setEnabled(false);
      bViewJobTransRecords.setEnabled(false);
      bEditRecord.setEnabled(false);
      bDeleteRecord.setEnabled(false);
      updateUI();
    }    
  }

  public DBPluginMgr getDbPluginMgr(){
    return dbPluginMgr;
  }
  
  public int getParentId(){
    return parentId;
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
   *
   * Called by : JobCreationPanel.Add()
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
   * Returns the DB name of the first selected row, "-1" if no row is selected.
   */

  public String getSelectedDBName(){
    int selRow = tableResults.getSelectedRow();
    return (selRow==-1) ? "-1" : dbIdentifiers[selRow];
  }

  /**
   * Returns the step name of the first selected row, "-1" if no row is selected.
   */
  
  public String getSelectedStepName(){
    int selRow = tableResults.getSelectedRow();
    return (selRow==-1) ? "-1" : stepIdentifiers[selRow];
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
                    "should be : \ndefault task fields = table.field1 table.field2", 3);
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
    //workThread = new Thread() {
      //public void run(){
        //if(!getWorking()){
          //Debug.debug("please wait ...", 2);
          //return;
        //}
        //setFieldArrays();
        String [] steps = null;
        String selectRequest;
        selectRequest = selectPanel.getRequest(shownFields);
        if(selectRequest == null)
            return;
        
        /*
         Support several dbs and steps (represented by dbRes[] and stepRes[]) and merge them
         all in one big table (res) with two extra column (last columns) specifying
         the name of the db and the step from which each row came
        */      
        DBResult [][] stepRes = new DBResult[dbs.length][dbs[0].length()];
        DBResult res = null;
        int lastCol = 0;
        int notEmptyDbId = -1;
        int notEmptyStepId = -1;
        int nrValues=0;
        for (int h=0; h<dbs.length; h++) {
          steps = GridPilot.getSteps(dbs[h]);
          Debug.debug("Number of steps: "+steps.length, 3);         
          for (int i=0; i<steps.length; i++) {
            // the actual selection
            stepRes[h][i] = GridPilot.getClassMgr().getDBPluginMgr(GridPilot.dbs[h],
                ((String []) GridPilot.steps.get(GridPilot.dbs[h]))[i]).select(
                    selectRequest,identifier);
            nrValues += stepRes[h][i].values.length;
            
            /*
             Creates fields structure for 'res' based on the first element of stepRes
             which has some results in it
             */
            if (notEmptyStepId == -1 && stepRes[h][i].fields.length > 0){
              notEmptyDbId = h;
              notEmptyStepId = i;
              lastCol = stepRes[notEmptyDbId][notEmptyStepId].fields.length;
            }
          }
           
          /*
           If notEmptyStepId is still -1 it means that no DBResult in stepRes was filled with
           anything
          */
          if (notEmptyStepId == -1) {
            continue;
          }
        }
        if (notEmptyStepId == -1) {
          return;
        }
        res = new DBResult(lastCol+2, nrValues);
        System.arraycopy(stepRes[notEmptyDbId][notEmptyStepId].fields,0,res.fields,0,stepRes[notEmptyDbId][notEmptyStepId].fields.length);

        /*
         Heavy performance penalty here!!! Go through everything in table and fill extra column specifying
         to which db and step each row belongs
         */
        int k=0;
        int c=0;
        for (int h=0; h<dbs.length; h++) {
          for (int i=0; i<nrValues; i++) {
            if ((i-c)==stepRes[h][k].values.length) {
              c=i;
              k++;
            }
            for (int j=0; j<lastCol; j++)
              res.values[i][j] = stepRes[h][k].values[i-c][j];
  
            res.values[i][lastCol]=dbs[h];
            res.values[i][lastCol+1]=steps[k];
          }
        }
        /*
         Now we have on 'res' everything. Now for esthetic reasons, hide
         db name, step name identifier and task identifier
         */
        
        bViewJobDefinitions.setEnabled(false);
        bViewJobTransRecords.setEnabled(false);
        bEditRecord.setEnabled(false);
        bDeleteRecord.setEnabled(false);
        
        tableResults.setTable(res.values, res.fields);
        spTableResults.getViewport().add(tableResults);
        
        if(selectRequest.indexOf("*")>-1){
          tableResults.hideLastColumns(2); // hide db name, step name column
        }
        else{
          tableResults.hideLastColumns(3); // hide db name, step name and task identifier column
        }

        identifiers = new int[tableResults.getRowCount()];
        // 'col' is the column with the jobDefinition identifier
        int col = tableResults.getColumnCount()-3;
        for(int i=0; i<identifiers.length; ++i){
          identifiers[i] = new Integer(tableResults.getUnsortedValueAt(i, col).toString()).intValue();
        }

        dbIdentifiers = new String[tableResults.getRowCount()];
        // 'col' is now the column with the db identifier
        col = tableResults.getColumnCount()-2;
        for(int i=0; i<dbIdentifiers.length; ++i)
          dbIdentifiers[i] = tableResults.getUnsortedValueAt(i, col).toString();

        stepIdentifiers = new String[tableResults.getRowCount()];
        // 'col' is now the column with the step identifier
        col = tableResults.getColumnCount()-1;
        for(int i=0; i<stepIdentifiers.length; ++i)
          stepIdentifiers[i] = tableResults.getUnsortedValueAt(i, col).toString();
        
        if(tableName.equalsIgnoreCase("task")){
          tableResults.getSelectionModel().setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
          tableResults.addListSelectionListener(new ListSelectionListener(){
            public void valueChanged(ListSelectionEvent e) {
              if (e.getValueIsAdjusting()) return;
              ListSelectionModel lsm = (ListSelectionModel)e.getSource();
              Debug.debug("lsm indices: "+
                  lsm.getMaxSelectionIndex()+" : "+lsm.getMinSelectionIndex(), 3);
              bViewJobDefinitions.setEnabled(!lsm.isSelectionEmpty());
              bViewJobTransRecords.setEnabled(!lsm.isSelectionEmpty());
              bDeleteRecord.setEnabled(!lsm.isSelectionEmpty());
              bEditRecord.setEnabled(!lsm.isSelectionEmpty());
            }
          });

          makeTaskMenu();
        }
        else if(tableName.equalsIgnoreCase("jobDefinition")){
          tableResults.getSelectionModel().setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
          tableResults.addListSelectionListener(new ListSelectionListener(){
            public void valueChanged(ListSelectionEvent e) {
              if (e.getValueIsAdjusting()) return;
              ListSelectionModel lsm = (ListSelectionModel)e.getSource();
              Debug.debug("lsm indices: "+
                  lsm.getMaxSelectionIndex()+" : "+lsm.getMinSelectionIndex(), 3);
              bDeleteRecord.setEnabled(!lsm.isSelectionEmpty());
              bEditRecord.setEnabled(!lsm.isSelectionEmpty() &&
                  lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
              miEdit.setEnabled(!lsm.isSelectionEmpty() &&
                  lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
            }
          });

          makeJobDefMenu();
        }
        else if(tableName.equalsIgnoreCase("jobTrans")){
          tableResults.getSelectionModel().setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
          tableResults.addListSelectionListener(new ListSelectionListener(){
            public void valueChanged(ListSelectionEvent e) {
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
  public void makeTaskMenu(){
    Debug.debug("Making task menu", 3);
    JMenuItem miViewJobDefinitions = new JMenuItem("Show JobDefinitions");
    JMenuItem miViewJobTransRecords = new JMenuItem("Show JobTrans Records");
    miViewJobDefinitions.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        viewJobDefinitions();
      }
    });
    miViewJobTransRecords.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        viewJobTransRecords();
      }
    });
    JMenuItem miDelete = new JMenuItem("Delete");
    miEdit = new JMenuItem("Edit");
    miDelete.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        deleteTasks();
      }
    });
    miEdit.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        editTask();
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
    String [] fieldNames = tableResults.getColumnNames();
    JMenuItem [] miSetFields = new JMenuItem[fieldNames.length];
    for(int i=0; i<fieldNames.length; ++i){
      if(fieldNames[i]!=null && !fieldNames[i].equalsIgnoreCase("") &&
          !fieldNames[i].equalsIgnoreCase("jobDefinitionID")){
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
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miDelete);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miEdit);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(jmSetFieldValue);
  }
  
  private void setFieldValues(String field, String value) {
    getDbPluginMgr().setJobDefinitionField(getSelectedIdentifiers(), field, value);
    searchRequest();
  }
    
    /**
    * Open dialog with jobDefintion creation panel
    */ 
   private void createJobDefs() {
    TaskMgr taskMgr = new TaskMgr(getDbPluginMgr(), getParentId());
    //hiddenFields = getDbPluginMgr().getDBHiddenFields(dbs[0], tableName);
    CreateEditDialog pDialog = new CreateEditDialog(
       GridPilot.getClassMgr().getGlobalFrame(),
        new JobDefCreationPanel(taskMgr, tableResults, false), false);
    pDialog.setTitle(tableName);
    pDialog.show();
    if(tableResults!=null && tableResults.getRowCount()>0){
      searchRequest();
    }
  }

  private void editJobDef() {
    int parentId = getParentId();
    dbPluginMgr = getDbPluginMgr();
    if(parentId<0){
      dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(getSelectedDBName(),
          getSelectedStepName());
      parentId = dbPluginMgr.getTaskId(getSelectedIdentifier());
    }
    TaskMgr taskMgr = new TaskMgr(dbPluginMgr, parentId);
    CreateEditDialog pDialog = new CreateEditDialog(
        GridPilot.getClassMgr().getGlobalFrame(),
        new JobDefCreationPanel(taskMgr, tableResults, true), true);
    pDialog.setTitle(tableName);
    pDialog.show();
    searchRequest();
  }

  private void deleteJobDefs() {
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
    workThread = new Thread() {
      public void run(){
        DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(getSelectedDBName(),
            getSelectedStepName());
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
          for (int i = ids.length-1; i>=0; i--) {
            boolean success = dbPluginMgr.deleteJobDefinition(new JobDefinition(
                (String []) dbPluginMgr.getJobDefinition(ids[i]).values));
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
   * Open dialog with task creation panel
   */ 
  private void createTasks() {
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(GridPilot.dbs[0],
        ((String []) GridPilot.steps.get(GridPilot.dbs[0]))[0]);
   CreateEditDialog pDialog = new CreateEditDialog(
      GridPilot.getClassMgr().getGlobalFrame(),
       new TaskCreationPanel(dbPluginMgr, tableResults, false), false);
   pDialog.setTitle(tableName);
   pDialog.show();
   if(tableResults!=null && tableResults.getRowCount()>0){
     searchRequest();
   }
 }

 private void editTask() {
   DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(GridPilot.dbs[0],
       ((String []) GridPilot.steps.get(GridPilot.dbs[0]))[0]);
   CreateEditDialog pDialog = new CreateEditDialog(
       GridPilot.getClassMgr().getGlobalFrame(),
       new TaskCreationPanel(dbPluginMgr, tableResults, true), true);
   pDialog.setTitle(tableName);
   pDialog.show();
   searchRequest();
 }

  private void deleteTasks() {
    String msg = "Are you sure you want to delete task ";
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
    workThread = new Thread() {
      public void run(){
        DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(getSelectedDBName(),
            getSelectedStepName());
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
          for (int i = ids.length-1; i>=0; i--) {
            boolean success = dbPluginMgr.deleteTask(ids[i]);
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
  private void createJobTransRecords() {
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(GridPilot.dbs[0],
        ((String []) GridPilot.steps.get(GridPilot.dbs[0]))[0]);
   CreateEditDialog pDialog = new CreateEditDialog(
      GridPilot.getClassMgr().getGlobalFrame(),
       new JobTransCreationPanel(dbPluginMgr, tableResults, false), false);
   pDialog.setTitle(tableName);
   pDialog.show();
   if(tableResults!=null && tableResults.getRowCount()>0){
     searchRequest();
   }
 }

 private void editJobTransRecord() {
   DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(GridPilot.dbs[0],
       ((String []) GridPilot.steps.get(GridPilot.dbs[0]))[0]);
   CreateEditDialog pDialog = new CreateEditDialog(
       GridPilot.getClassMgr().getGlobalFrame(),
       new JobTransCreationPanel(dbPluginMgr, tableResults, true), true);
   pDialog.setTitle(tableName);
   pDialog.show();
   searchRequest();
 }

  private void deleteJobTransRecords() {
    String msg = "Are you sure you want to delete jobTrans records ";
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
    workThread = new Thread() {
      public void run(){
        DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(getSelectedDBName(),
            getSelectedStepName());
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
          for (int i = ids.length-1; i>=0; i--) {
            boolean success = dbPluginMgr.deleteJobTransRecord(ids[i]);
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
  private void viewJobDefinitions() {
    if(getSelectedIdentifier() != -1){
      new Thread(){
        public void run(){
          DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(getSelectedDBName(),
              getSelectedStepName());
          try{
            // Create new panel with jobDefinitions.         
            int id = getSelectedIdentifier();
            DBPanel dbPanel = new DBPanel("jobDefinition", "JOBDEFINITIONID",
                dbPluginMgr, id);
            dbPanel.selectPanel.setConstraint("jobDefinition", "TASKFK",
                Integer.toString(id), 0);
            dbPanel.searchRequest();           
            // Create new task panel showing JobTrans records
            GridPilot.getClassMgr().getGlobalFrame().addPanel(dbPanel);                   
          }catch (Exception e) {
            Debug.debug("Couldn't create panel for task " + "\n" +
                               "\tException\t : " + e.getMessage(), 2);
            e.printStackTrace();
          }
        }
      }.start();
    }
  }
 
  /**
   * Open new pane with list of jobTrans records.
   */
  private void viewJobTransRecords() {
    if(getSelectedIdentifier() != -1){
      new Thread(){
        public void run(){
         DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(getSelectedDBName(),
              getSelectedStepName());
          try{
            // Create new panel with jobTrans records.         
            int id = dbPluginMgr.getTaskTransId(/*taskID*/getSelectedIdentifier());
            DBPanel dbPanel = new DBPanel("jobTrans", "JOBTRANSID",
                dbPluginMgr, id);
            dbPanel.selectPanel.setConstraint("jobTrans", "TASKTRANSFK",
                Integer.toString(id), 0);
            dbPanel.searchRequest();
            // Create new task panel showing JobTrans records
            GridPilot.getClassMgr().getGlobalFrame().addPanel(dbPanel);           
          }catch (Exception e) {
            Debug.debug("Couldn't create panel for task " + "\n" +
                               "\tException\t : " + e.getMessage(), 2);
            e.printStackTrace();
          }
        }
      }.start();
    }
  }
}
