package gridpilot;

import gridpilot.Table;
import gridpilot.Database.DBResult;
import gridpilot.Debug;
import gridpilot.GridPilot;
import gridpilot.ConfigFile;

import javax.swing.*;
import javax.swing.event.*;


import java.awt.*;
import java.awt.event.*;


/**
 * This panel contains one SelectPanel.
 *
 */

public class DBPanel extends JPanel {

  private ConfigFile configFile;
  private String [] dbs;
  private String [] steps;

  private boolean withSplit = true;

  private JScrollPane spSelectPanel = new JScrollPane();
  private SelectPanel selectPanel;
  private JButton bSearch = new JButton("Search");
  private JButton bClear = new JButton("Clear");
  private JPanel pButtonSelectPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
  private JPanel panelSelectPanel = new JPanel(new GridBagLayout());


  private JScrollPane spTableResults = new JScrollPane();
  private Table tableResults = new Table();
  //private JButton bPrev = new JButton("New Search");
  private JPanel pButtonTableResults = new JPanel(new FlowLayout(FlowLayout.RIGHT));
  private JPanel panelTableResults = new JPanel(new GridBagLayout());
  private int [] jobDefIdentifiers;
  private String [] dbIdentifiers;
  private String [] stepIdentifiers;

  private AllTasksPanel jobDefinitionPanel;
  private String taskTableName = "task";
  private String taskIdentifier = "TASKID";
  private String [] defaultFields = null;
  
  public GridBagConstraints ct = new GridBagConstraints();

  /**
   * Constructor
   */

  /**
   * Create a new DBPanel.
   *
   * Called by :
   * - JobCreationPanel.JobCreationPanel()
   */

   public DBPanel(/*panel in which this panel is to be placed*/
       AllTasksPanel _jobDefinitionPanel) throws Exception{
  
     configFile = GridPilot.getClassMgr().getConfigFile();
     jobDefinitionPanel = _jobDefinitionPanel;
     
     String dbName = null;
     String [] stepList = null;
     String taskTableName1 = null;
     String taskIdentifier1 = null;
     String [] defaultFields1 = null;
     boolean firstIterationDone = false;
     
//// SelectPanel

    // Reads DB structure
     for(int i = 0; i < GridPilot.getDBs().length; ++i){
       dbName = GridPilot.getDBs()[i];
       stepList = GridPilot.getSteps(dbName);
       if (stepList == null) return;
       for(int j = 0; j < stepList.length; ++j){
         // TODO: get from db
         taskTableName = "task";
         taskIdentifier = "TASKID";
         defaultFields = GridPilot.getClassMgr().getDBPluginMgr(dbName, stepList[j]).getDBDefFields(GridPilot.getDBs()[i], taskTableName) ;
         if((firstIterationDone || j > 0) &&
             // TODO: check all fields...
             (!taskTableName1.equals(taskTableName) ||
                 !taskIdentifier1.equals(taskIdentifier) ||
                 !defaultFields1[0].equals(defaultFields[0]))){
            Debug.debug("ERROR: incompatible databases",1); 
            GridPilot.exit(-1);
         }
       }
       firstIterationDone = true;
       taskTableName1 = taskTableName;
       taskIdentifier1 = taskIdentifier;
       defaultFields1 = defaultFields;
     }
    
    // we only need task selection
    String[] taskTableNames = {taskTableName};
    // TODO: SelectPanel... rethink, clean up...
    //selectPanel = new SelectPanel(getSelectedDBName(), getSelectedStepName(),
        //taskTableNames);
    
    // Use the last of the dbs for initializing values.
    selectPanel = new SelectPanel(dbName, stepList[0],
        taskTableNames);
    selectPanel.initGUI();
    selectDefaultValues();

    initGUI();
  }

  /**
   * GUI initialisation
   */

  private void initGUI() throws Exception {
    /**
     * Called by this.DBPanel(...)
     */

    this.setLayout(new GridBagLayout());

    //// panel Select

    bSearch.addActionListener(new java.awt.event.ActionListener() {
      public void actionPerformed(ActionEvent e) {
        searchRequest();
      }
    });

    bSearch.setToolTipText("Search results for this request");

    bClear.addActionListener(new java.awt.event.ActionListener() {
      public void actionPerformed(ActionEvent e) {
        clear();
      }
    });


    pButtonSelectPanel.add(bClear);
    pButtonSelectPanel.add(bSearch);

    spSelectPanel.getViewport().add(selectPanel);

    panelSelectPanel.add(spSelectPanel,  new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0
        ,GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    panelSelectPanel.add(pButtonSelectPanel,    new GridBagConstraints(0, 1, 1, 1, 1.0, 0.0
        ,GridBagConstraints.EAST, GridBagConstraints.NONE, new Insets(10, 10, 10, 10), 0, 0));


    this.selectPanel.spcp.tfConstraintValue.addKeyListener(new KeyAdapter(){
      public void keyPressed(KeyEvent e){
        switch(e.getKeyCode()){
          case KeyEvent.VK_ENTER:
            //Debug.debug("ENTER!", 1);
            searchRequest();
        }
      }
    });


    //// panel table results

    /*bPrev.addActionListener(new java.awt.event.ActionListener() {
      public void actionPerformed(ActionEvent e) {
        previous();
      }
    });

    bPrev.setToolTipText("Create another request");

    pButtonTableResults.add(bPrev);*/

    tableResults.getSelectionModel().setSelectionMode(ListSelectionModel.SINGLE_SELECTION);

    spTableResults.getViewport().add(tableResults);

    panelTableResults.add(spTableResults,  new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0
        ,GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    panelTableResults.add(pButtonTableResults,    new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0
        ,GridBagConstraints.EAST, GridBagConstraints.NONE, new Insets(10, 10, 10, 10), 0, 0));


    //this.add(panelSelectPanel, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0
    //    ,GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    
    //panelSelectPanel.setPreferredSize(new Dimension(590, 180));
    //panelTableResults.setPreferredSize(new Dimension(620, 500));
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
  }


  /**
   * public properties
   */

  public void selectDefaultValues() {
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
      selectPanel.resetConstraintList(taskTableName);
      selectPanel.updateUI();
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

  public int getSelectedIdentifier(){
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
   * public operation
   */

   /**
    * Deletes the selected rows (the selected jobDefinitions)
    */
/*
    public void deleteSelectedJobDefs() {
     int [] selectedRows = tableResults.getSelectedRows();
     tableResults.deleteRows(selectedRows);
   }
  */

  /**
   * Clears all fields
   */
  public void clear(){
    /*spSelectPanel.remove(0);
    try{
      selectPanel.initGUI();
    }catch(Exception e){
      Debug.debug2("Couldn't create init GUI for SelectPanel \n" +
          "\tException\t : " + e.getMessage());
          e.printStackTrace();  
    }*/
    selectDefaultValues();
    //selectPanel.clear();
    //tableResults = new Table();//.setTable(null, null);
    //tableResults.setTable(null, null);
    //previous();
  }

  /**
   * Adds a button on the left of the buttons shown when the panel with results is shown.
   */
  public void addButtonSecondPanel(JButton b){
    pButtonTableResults.add(b);
  }

  /**
   * private methods
   */

  /**
   * Request DBPluginMgr for select request from SelectPanel, and fill tableResults with results.
   * This request is performed in a separeted thread, avoiding to block all the GUI during
   * this action.
   * This search can be stopped by clicking on the indeterminate progress bar.
   * Called when button "Search" is clicked
   */

  private void searchRequest(){

   // TODO: why does it not work as thread when
    // not in it's own pane?
    // new Thread(){
    //  public void run(){

        String selectRequest;
        selectRequest = selectPanel.getRequest(taskTableName);
        if(selectRequest == null)
            return;

        bSearch.setEnabled(false);

        /*
         Support several dbs and steps (represented by dbRes[] and stepRes[]) and merge them
         all in one big table (res) with two extra column (last columns) specifying
         the name of the db and the step from which each row came
         */      
        dbs = GridPilot.getDBs();
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
            stepRes[h][i] = GridPilot.getClassMgr().getDBPluginMgr(GridPilot.dbs[h], ((String []) GridPilot.steps.get(GridPilot.dbs[h]))[i]).select(selectRequest,taskIdentifier);
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
            bSearch.setEnabled(true);
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
        tableResults.setTable(res.values, res.fields);
        
        String debug = "";
        for(int i = 0; i < res.values.length; ++i){
          for(int j = 0; j < res.values[i].length; ++j){
            debug += res.values[i][j] + " ";
          }
          Debug.debug("-->"+debug, 3);
          debug = "";
        }
        jobDefinitionPanel.makeMenu(); // add some items to the popup menu in the table. These items are from the (parent) job definition panel
        tableResults.hideLastColumns(3); // hide db name, step name and task identifier column

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

        bSearch.setEnabled(true);
        next();
     // }
    //}.start();

  }

  /**
   * Called when button "New Search" is clicked.
   * Hides the current panel (Table), shows to the first panel (SelectPanel)
   */
  /*public void previous(){
    this.removeAll();
    this.add(panelSelectPanel, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0
        ,GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    this.updateUI();
  }*/

  /**
   * Called when button "Next" is clicked, or when "searchRequest" is finished.
   * Hides the current panel (SelectPanel), shows to the second panel (Table)
   */
  public void next(){

    //this.removeAll();
    this.remove(1);
    //this.add(panelTableResults, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0
        //,GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));

    //ct.gridx = 0;
    //ct.gridy = 1;   
    this.add(panelTableResults,ct);
    this.updateUI();

  }

  /**
   * Add a ListSelectionListener to the Table showing results.
   */
  public void addListSelectionListener(ListSelectionListener lsl){
    tableResults.addLisSelectionListener(lsl);
  }

  /**
   * Return pointer to tableResults object
   */
  public Table getTableResults(){
    return tableResults;
  }
}
