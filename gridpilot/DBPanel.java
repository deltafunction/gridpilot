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

  private String [] tables = {"task", "jobDefinition", "jobTrans"};
  
  private JScrollPane spSelectPanel = new JScrollPane();
  private SelectPanel selectPanel;
  private JPanel pButtonSelectPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
  public JPanel panelSelectPanel = new JPanel(new GridBagLayout());

  private JScrollPane spTableResults = new JScrollPane();
  private Table tableResults = null;
  private JPanel pButtonTableResults = new JPanel(new FlowLayout(FlowLayout.RIGHT));
  private JPanel panelTableResults = new JPanel(new GridBagLayout());
  
  private JButton bViewJobTransRecords = new JButton("Show JobTrans'");
  private JButton bCreateRecords = new JButton("Define new Records");
  private JButton bEditRecord = new JButton("Edit Record");
  private JButton bSearch = new JButton("Search");
  private JButton bClear = new JButton("Clear");
  private JButton bViewJobDefinitions = new JButton("Show jobDefinitions");
  
  private int [] jobDefIdentifiers;
  private String [] dbIdentifiers;
  private String [] stepIdentifiers;
  // lists of field names with table name as key
  private String [] fieldNames = null;

  private String tableName;
  private String identifier;
  private String [] defaultFields = null;
  private String [] hiddenFields = null;
  
  private GridBagConstraints ct = new GridBagConstraints();
  
  // TODO: clean up getting and setting and usage of dbPluginMgr
  private DBPluginMgr dbPluginMgr = null;
  private int parentId = -1;

  private TaskMgr currentTaskMgr;
  private Table currentDbVectorTable;
  
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
    
    // the same table from the various databases used should have the same
    // columns, so we use the first db and the first stepName to get the fields.
    // TODO: stepName should go.
    fieldNames =
       GridPilot.getClassMgr().getDBPluginMgr(dbs[0],
           GridPilot.getSteps(dbs[0])[0]).getFieldNames(tableName);

    initGUI();
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
      addButtonResultsPanel(bViewJobDefinitions);
      addButtonResultsPanel(bViewJobTransRecords);    
      addButtonSelectPanel(bClear);
      addButtonSelectPanel(bSearch);
      bViewJobDefinitions.setEnabled(false);
      bViewJobTransRecords.setEnabled(false);
      updateUI();
    }
    else if(tableName.equalsIgnoreCase("jobDefinition")){
      bEditRecord.addActionListener(new ActionListener() {
        public void actionPerformed(ActionEvent e) {
          editJobDef();
        }
      });

      bCreateRecords.addActionListener(new ActionListener() {
        public void actionPerformed(ActionEvent e) {
          createJobDefs();
        }
      });
      addButtonResultsPanel(bCreateRecords);
      addButtonResultsPanel(bEditRecord);
      addButtonSelectPanel(bClear);
      addButtonSelectPanel(bSearch);
      updateUI();
    }
    else if(tableName.equalsIgnoreCase("jobTrans")){
      addButtonSelectPanel(bClear);
      addButtonSelectPanel(bSearch);
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

  public int [] getSelectedJobDefIdentifiers(){

    int [] selectedRows = tableResults.getSelectedRows();
    int [] selectedIdentifiers = new int[selectedRows.length];
    for(int i=0; i<selectedIdentifiers.length; ++i){
      selectedIdentifiers[i] = jobDefIdentifiers[selectedRows[i]];
    }
    return selectedIdentifiers;
  }

  /**
   * Returns the Id of the first selected row, -1 if no row is selected.
   *
   * @return Id of the first selected row, -1 if no row is selected
   *
   */

  public int getSelectedJobDefIdentifier(){
    int selRow = tableResults.getSelectedRow();
    return (selRow==-1) ? -1 : jobDefIdentifiers[selRow];
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
    String [][] values = new String[defaultFields.length][2];

    for(int i = 0; i<defaultFields.length; ++i){
      String [] split = defaultFields[i].split("\\.");
      if(split.length != 2){
          Debug.debug(defaultFields[i] + " " + " : wrong format in config file ; " +
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
          //System.out.println("please wait ...");
          //return;
        //}
        String [] steps = null;
        String selectRequest;
        selectRequest = selectPanel.getRequest();
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
            //bSearch.setEnabled(true);
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
        
        DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(GridPilot.dbs[0],
            ((String []) GridPilot.steps.get(GridPilot.dbs[0]))[0]);
        Debug.debug("dbPluginMgr: "+dbPluginMgr, 3);
        hiddenFields = dbPluginMgr.getDBHiddenFields(dbs[0], tableName);
        Debug.debug("Hidden fields "+hiddenFields.length, 3);
        tableResults = new Table(hiddenFields);
        tableResults.setTable(res.values, res.fields);
        spTableResults.getViewport().add(tableResults);
        
        String debug = "";
        for(int i = 0; i < res.values.length; ++i){
          for(int j = 0; j < res.values[i].length; ++j){
            debug += res.values[i][j] + " ";
          }
          Debug.debug("-->"+debug, 3);
          debug = "";
        }

        if(selectRequest.indexOf("*")>-1){
          tableResults.hideLastColumns(2); // hide db name, step name column
        }
        else{
          tableResults.hideLastColumns(3); // hide db name, step name and task identifier column
        }

        jobDefIdentifiers = new int[tableResults.getRowCount()];
        // 'col' is the column with the jobDefinition identifier
        int col = tableResults.getColumnCount()-3;
        for(int i=0; i<jobDefIdentifiers.length; ++i){
          jobDefIdentifiers[i] = new Integer(tableResults.getUnsortedValueAt(i, col).toString()).intValue();
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
              bViewJobDefinitions.setEnabled(!lsm.isSelectionEmpty());
              bViewJobTransRecords.setEnabled(!lsm.isSelectionEmpty());
            }
          });

          makeTaskMenu();
        }
        else if(tableName.equalsIgnoreCase("jobDefinition")){
          makeJobDefMenu();
        }
        else if(tableName.equalsIgnoreCase("jobTrans")){
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
    miViewJobDefinitions.setEnabled(true);
    miViewJobTransRecords.setEnabled(true);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miViewJobDefinitions);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miViewJobTransRecords);
  }

  /**
   * Add menu items to the table with search results. This function is called from within DBPanel
   *  after the results table is filled
   */
  public void makeJobDefMenu(){
    JMenuItem miDelete = new JMenuItem("Delete");
    JMenuItem miEdit = new JMenuItem("Edit");
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
  }
  
   /**
    * Open dialog with jobDefintion creation panel
    */ 
   private void createJobDefs() {
    TaskMgr taskMgr = new TaskMgr(getDbPluginMgr(), getParentId());
    hiddenFields = getDbPluginMgr().getDBHiddenFields(dbs[0], tableName);
    Table dbVectorTable = new Table(taskMgr.getVectorTableModel(),
        hiddenFields);
    CreateEditDialog pDialog = new CreateEditDialog(
       GridPilot.getClassMgr().getGlobalFrame(),
        new JobDefCreationPanel(taskMgr, dbVectorTable, false), false);
    pDialog.show();
  }

  private void editJobDef() {
    int parentId = getParentId();
    dbPluginMgr = getDbPluginMgr();
    if(parentId<0){
      dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(getSelectedDBName(),
          getSelectedStepName());
      parentId = dbPluginMgr.getTaskId(getSelectedJobDefIdentifier());
    }
    TaskMgr taskMgr = new TaskMgr(dbPluginMgr, parentId);
    CreateEditDialog pDialog = new CreateEditDialog(
        GridPilot.getClassMgr().getGlobalFrame(),
        new JobDefCreationPanel(taskMgr, tableResults, true), true);
    pDialog.show();
  }

  private void deleteJobDefs() {
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(getSelectedDBName(),
        getSelectedStepName());
    hiddenFields = dbPluginMgr.getDBHiddenFields(dbs[0], tableName);
    TaskMgr taskMgr = new TaskMgr(dbPluginMgr, getSelectedJobDefIdentifier());
    Table dbVectorTable = new Table(taskMgr.getVectorTableModel(), hiddenFields);
    currentTaskMgr = taskMgr;
    currentDbVectorTable = dbVectorTable;
    workThread = new Thread() {
      public void run(){
        if(!getWorking()){
          System.out.println("please wait ...");
          return;
        }
        int [] rows = currentDbVectorTable.getSelectedRows();
        if(rows.length != 0){
          JProgressBar pb = new JProgressBar();
          pb.setMaximum(rows.length);
          // need intermediate vector because removing elts will change indices !!
          Vector v = new Vector();
          for (int i = 0 ; i < rows.length ; i++ ) {
            v.add(currentTaskMgr.shownJobs.get(rows[i])) ;
          }
          for (int i = 0 ; i < v.size() ; i++ ) {
            boolean success = currentTaskMgr.deleteJobDef((JobDefinition) v.get(i));
            pb.setValue(pb.getValue()+1);
          }
         currentDbVectorTable.tableModel.fireTableDataChanged();
          //tableResults.deleteRows(rows);
        }
        stopWorking();
      }
    };
    workThread.start();
  }
  /**
   * Open new pane with list of jobDefinitions.
   */
  private void viewJobDefinitions() {
    if(getSelectedJobDefIdentifier() != -1){
      new Thread(){
        public void run(){
          DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(getSelectedDBName(),
              getSelectedStepName());
          try{
            // Create new panel with jobDefinitions.         
            int id = getSelectedJobDefIdentifier();
            DBPanel dbPanel = new DBPanel("jobDefinition", "JOBDEFINITIONID",
                dbPluginMgr, id);
            dbPanel.selectPanel.setConstraint("jobDefinition", "TASKFK",
                Integer.toString(id));
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
    if(getSelectedJobDefIdentifier() != -1){
      new Thread(){
        public void run(){
         DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(getSelectedDBName(),
              getSelectedStepName());
          try{
            // Create new panel with jobTrans records.         
            int id = dbPluginMgr.getTaskTransId(/*taskID*/getSelectedJobDefIdentifier());
            DBPanel dbPanel = new DBPanel("jobTrans", "JOBTRANSID",
                dbPluginMgr, id);
            dbPanel.selectPanel.setConstraint("jobTrans", "TASKTRANSFK",
                Integer.toString(id));
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
