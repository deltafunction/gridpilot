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

public class DBPanel extends JPanel implements JobPanel{

  private ConfigFile configFile;
  private boolean withSplit = true;
  private String [] dbs;

  private JScrollPane spSelectPanel = new JScrollPane();
  private SelectPanel selectPanel;
  private JPanel pButtonSelectPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
  public JPanel panelSelectPanel = new JPanel(new GridBagLayout());

  private JScrollPane spTableResults = new JScrollPane();
  private Table tableResults = new Table();
  private JPanel pButtonTableResults = new JPanel(new FlowLayout(FlowLayout.RIGHT));
  private JPanel panelTableResults = new JPanel(new GridBagLayout());
  private int [] jobDefIdentifiers;
  private String [] dbIdentifiers;
  private String [] stepIdentifiers;
  // lists of field names with table name as key
  private String [] fieldNames = null;

  private String tableName;
  private String identifier;
  private String [] defaultFields = null;
  
  private GridBagConstraints ct = new GridBagConstraints();
  
  private DBPluginMgr dbPluginMgr = null;
  private int parentId = -1;

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

    public DBPanel( /*name of tables for the select*/
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
    
    public DBPluginMgr getDbPluginMgr(){
      return dbPluginMgr;
    }
   
    public int getParentId(){
      return parentId;
    }
   
   /**
   * GUI initialisation
   */

  private void initGUI() throws Exception {

////SelectPanel

    selectPanel = new SelectPanel(tableName, fieldNames);
    selectPanel.initGUI();
    clear();

    this.setLayout(new GridBagLayout());

    spSelectPanel.getViewport().add(selectPanel);

    panelSelectPanel.add(spSelectPanel,  new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0
        ,GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    panelSelectPanel.add(pButtonSelectPanel,    new GridBagConstraints(0, 1, 1, 1, 1.0, 0.0
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

    tableResults.getSelectionModel().setSelectionMode(ListSelectionModel.SINGLE_SELECTION);

    spTableResults.getViewport().add(tableResults);

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
  }


  /**
   * public properties
   */

  public String getTitle(){
    //return tableName;
    return "Select";
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
   * Add a ListSelectionListener to the Table showing results.
   */
  public void addListSelectionListener(ListSelectionListener lsl){
    tableResults.addListSelectionListener(lsl);
  }

  /**
   * Return pointer to tableResults object
   */
  public Table getTableResults(){
    return tableResults;
  }
  
  /**
   * Return pointer to panelTableResults object
   */
  public JPanel getPanelTableResults(){
    return panelTableResults;
  }

  /**
   * Return pointer to selectPanel object
   */
  public SelectPanel getSelectPanel(){
    return selectPanel;
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
    
    String [] steps = null;
   // TODO: why does it not work as thread when
    // not in it's own pane?
    // new Thread(){
    //  public void run(){

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
        tableResults.setTable(res.values, res.fields);
        
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

     // }
    //}.start();

  }
}
