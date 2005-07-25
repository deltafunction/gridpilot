package gridpilot;

import gridpilot.Debug;
import gridpilot.TaskMgr;
import gridpilot.Database.DBResult;
import gridpilot.XmlNode;
import gridpilot.Database.DBRecord;

import javax.swing.*;
import javax.swing.border.*;
import javax.swing.event.*;
import java.awt.*;
import java.awt.event.*;
import javax.swing.text.*;

import java.util.*;

/**
 * This panel creates records in the DB table. It's shown inside the CreateEditDialog.
 *
 */

public class JobDefCreationPanel extends CreateEditPanel{

  /*
   DBPluginMgr to be used is stored here by initGUI(..). Still, not all functions use this global
   variable (just to make sure no conflicts happen).
   */

  private TaskMgr taskMgr;

  private String jobTransName;
  private String version;
  private JPanel pCounter = new JPanel();
  private JPanel pConstants = new JPanel();
  private JPanel pAttributes = new JPanel();
  private JScrollPane spAttributes = new JScrollPane();
  private JPanel pButtons = new JPanel();
  private JComboBox cbJobTransNameSelection = null;
  private JComboBox cbVersionSelection = null;
  private String [] jobTransNames;
  private String [] versions;
  private String jobTransFK = "-1";
  private String jobDefinitionID = "-1";
  private Table table;
  private JSpinner sFrom = new JSpinner(new SpinnerNumberModel(1, 1, 999999, 1));
  private JSpinner sTo = new JSpinner(new SpinnerNumberModel(1, 1, 999999, 1));
  private String [] cstAttributesNames;
  private JComponent [] tcCstAttributes;
  private JComponent [] tcCstJobDefAttributes;
  private boolean reuseTextFields = true;
  private Vector tcConstant = new Vector(); // contains all text components
  private String [] cstAttr = null;
  private boolean editing = false;
  private JPanel jobXmlContainer = new JPanel(new GridBagLayout());
  private DBResult transformations;
  private boolean loaded = false;
  private boolean versionInit = false;
  private String dbName;
  private DBPluginMgr dbPluginMgr = null;
  private int taskID = -1;
  private String taskName;
  
  // these two variables must either both be static or not.
  private static JPanel jobXmlPanel;
  private static XmlNode xmlParsNode;
  
  private static JComponent [] oldTcCstAttributes;
  private static String oldJobTransFK = "-1";
  private static int TEXTFIELDWIDTH = 32;
  private static int CFIELDWIDTH = 8;
  
  private String jobDefIdentifier = null;
  
  /**
   * Constructor
   */

  public JobDefCreationPanel(/*this is in case DBPanel was opened from the menu and_taskMgr is null*/String _dbName,
      TaskMgr _taskMgr, Table _table,
      boolean _editing){
    
    editing = _editing;
    taskMgr=_taskMgr;
    dbName = _dbName;
    table = _table;
    
    if(!editing){
      jobTransFK = oldJobTransFK;
    }

    if(taskMgr!=null){
      dbPluginMgr = taskMgr.getDBPluginMgr();
      taskID = taskMgr.getTaskIdentifier();
      taskName = taskMgr.getTaskName();
    }
    else{
      taskID = -1;
      dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
      taskName = "";
    }

    //cstAttributesNames = JobDefinition.Fields;
    cstAttributesNames = dbPluginMgr.getFieldNames(
        GridPilot.getClassMgr().getConfigFile().getValue(
            dbPluginMgr.getDBName(),
            "job definition table name"));
    cstAttr = new String[cstAttributesNames.length];
    
    transformations = dbPluginMgr.getJobTransRecords(taskID);
    
    jobDefIdentifier = GridPilot.getClassMgr().getConfigFile().getValue(dbPluginMgr.getDBName(),
       "job definition table identifier");

    /*Debug.debug("Editing job record for task "+dbPluginMgr.getTask(taskID).getValue(
        GridPilot.getClassMgr().getConfigFile().getValue(dbPluginMgr.getDBName(),
    "task table identifier")).toString()+". Rows: "+
        //table.getRowCount()+
        ". Number of transformations: "+
       (transformations!=null ? transformations.values.length : 0), 3);*/

    // Find jobdDefinitionID from table
    if(table.getSelectedRow()>-1 && editing){
      for(int i=0; i<table.getColumnNames().length; ++i){
        //Object fieldVal = table.getValueAt(table.getSelectedRow(),i);
        Object fieldVal = table.getUnsortedValueAt(table.getSelectedRow(),i);
        Debug.debug("Column name: "+table.getColumnNames().length+":"+i+" "+table.getColumnName(i), 3);
        if(fieldVal!=null && table.getColumnName(i).equalsIgnoreCase(jobDefIdentifier)){
          jobDefinitionID = fieldVal.toString();
          break;
        }
      }
      if(jobDefinitionID==null || jobDefinitionID.equals("-1")||
          jobDefinitionID.equals("")){
        Debug.debug("ERROR: could not find jobDefinitionID in table!", 1);
      }
      // Fill cstAttr from db
      DBRecord jobDef = dbPluginMgr.getJobDefinition(Integer.parseInt(jobDefinitionID));
      for(int i=0; i < cstAttributesNames.length; ++i){
        if(editing){
          if(cstAttributesNames[i]!=null && cstAttributesNames[i].equalsIgnoreCase("jobTransFK")){
            jobTransFK = jobDef.getValue(cstAttributesNames[i]).toString();
            Debug.debug("Set jobTransFK from db: "+jobTransFK, 1);
          }
          Debug.debug("filling " + cstAttributesNames[i],  3);
          if(jobDef.getValue(cstAttributesNames[i])!=null){
            cstAttr[i] = jobDef.getValue(cstAttributesNames[i]).toString();
          }
          else{
            cstAttr[i] = "";
          }
          
          Debug.debug("to " + cstAttr[i],  3);
        }
      }
    }

    sFrom.addChangeListener(new ChangeListener(){
      public void stateChanged(ChangeEvent e){
        if(((Integer)sTo.getValue()).intValue() < ((Integer)sFrom.getValue()).intValue())
          sTo.setValue(sFrom.getValue());
      }
    });

    sTo.addChangeListener(new ChangeListener(){

      public void stateChanged(ChangeEvent e){
        if(((Integer)sTo.getValue()).intValue() < ((Integer)sFrom.getValue()).intValue())
          sFrom.setValue(sTo.getValue());
      }
    });
  }

  /**
   * GUI initialisation
   */

  public void initGUI(){

    setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white,new Color(165, 163, 151)),taskName));
    
    spAttributes.setPreferredSize(new Dimension(550, 500));
    spAttributes.setMinimumSize(new Dimension(550, 500));
    
    setLayout(new GridBagLayout());
    removeAll();

    initAttributePanel();
    initJobTransNamePanel();
    initVersionPanel();
    
    GridBagConstraints ct = new GridBagConstraints();
    ct.fill = GridBagConstraints.VERTICAL;
    ct.insets = new Insets(2,2,2,2);
    
    ct.gridx = 0;
    ct.gridy = 0;   
    ct.gridwidth=1;
    ct.gridheight=1;
    
    if(!editing){
      initArithmeticPanel();
      ct.gridx = 0;
      ct.gridy = 0;         
      add(cbJobTransNameSelection,ct);
      
      ct.gridx = 1;
      ct.gridy = 0;         
      add(cbVersionSelection,ct);
      
      ct.gridx = 2;
      ct.gridy = 0;
      ct.gridwidth=1;
      ct.gridheight=2;
      add(pButtons,ct);
      
      ct.gridx = 0;
      ct.gridy = 1;
      ct.gridwidth=2;
      ct.gridheight=1;
      add(pCounter,ct);

      ct.gridx = 0;
      ct.gridy = 2;
      ct.gridwidth=3;
      ct.gridheight=1;
      pConstants.setLayout(new GridBagLayout());
      pConstants.setMinimumSize(new Dimension(550, 50));
      add(pConstants,ct);    
            
      ct.gridx = 0;
      ct.gridy = 4;
      ct.gridwidth=3;
      add(spAttributes, ct);
    }
    else{
      ct.gridx = 0;
      ct.gridy = 0;
      add(cbJobTransNameSelection,ct);
      ct.gridx = 1;
      ct.gridy = 0;         
      add(cbVersionSelection,ct);
      ct.gridx = 0;
      ct.gridy = 1;
      ct.gridwidth=2;
      add(spAttributes,ct);
    }

    setValuesInAttributePanel();
    
    if(!editing){
      setEnabledAttributes(false);
    }
    
    updateUI();
    
    loaded = true;
    
    }

  /**
   * Creates a combobox jobTransNamePanel which with user can select transformation.
   **/
  private void initJobTransNamePanel(){
    Debug.debug("initJobTransNamePanel with jobTransFK "+jobTransFK, 3);    
    String jtName = "-1";
    boolean ok = true;
    Vector vec = new Vector();
    
    if(transformations.values.length > 0){
      if(transformations.getValue(0,"jobTransID").equals(jobTransFK) &&
          transformations.getValue(0,"jobTransName")!=null){
        jtName = transformations.getValue(0,"jobTransName");
      }
      if(transformations.getValue(0,"jobTransName")!=null){
        Debug.debug("Adding jobTransName "+
            transformations.getValue(0,"jobTransName"), 3);
        vec.add(transformations.getValue(0,"jobTransName"));
      }
      else{
        //Debug.debug("WARNING: jobTransName null for transformation 0", 2);
      }
    }
    
    if(vec.size()==0 ||
        GridPilot.getClassMgr().getConfigFile().getValue("Databases", "Show all transformations").equalsIgnoreCase("true")){
      transformations = dbPluginMgr.getJobTransRecords(-1);
      jobTransNames = new String[transformations.values.length];
      for(int i=0; i<transformations.values.length; ++i){
        jobTransNames[i] = transformations.getValue(i, "jobTransName");
      }
    }
    else{
      jobTransNames = new String [vec.size()];
      for(int i = 0; i < vec.size(); ++i){
        jobTransNames[i] = vec.get(i).toString();
      }    
    }

    // Find jobTransName of jobTransFK
    if(transformations.values.length > 1){
      for(int i=1; i<transformations.values.length; ++i){
        ok = true;
        Debug.debug("Checking jobTransName with jobTransID "+
            transformations.getValue(i,"jobTransID"), 3);
        if(transformations.getValue(i,"jobTransID").equals(jobTransFK) &&
            transformations.getValue(i,"jobTransName")!=null){
          jtName = transformations.getValue(i,"jobTransName");
        }
        // Avoid duplicates
        for(int j=0; j<vec.size(); ++j){
          if(transformations.getValue(i,"jobTransName") != null &&
              transformations.getValue(i,"jobTransName").equals(
              vec.get(j))){
            ok = false;
            break;
          }
        }
        if(ok){
          if(transformations.getValue(i,"jobTransName") != null){
            Debug.debug("Adding jobTransName "+
                transformations.getValue(i,"jobTransName"), 3);
            vec.add(transformations.getValue(i,"jobTransName"));
          }    
          else{
            //Debug.debug("WARNING: jobTransName null for transformation "+i, 2);
          }
        }
      }
    }
          
    if(vec.size()==0){
      Debug.debug("WARNING: No jobTransNames found for transformations belonging to task "+
          taskName+". Displaying all jobTransNames...", 2);
    }

    if(cbJobTransNameSelection == null){
      cbJobTransNameSelection = new JComboBox(); 
      cbJobTransNameSelection.addActionListener(new java.awt.event.ActionListener(){
        public void actionPerformed(java.awt.event.ActionEvent e){
          cbJobTransNameSelection_actionPerformed();
      }});
    }
    else{
      cbJobTransNameSelection.removeAllItems();
    }
    
    if(jobTransNames.length == 0){  
      jobTransName = null;
      cbJobTransNameSelection.setEnabled(false);
    }
    if(jobTransNames.length == 1){  
      jobTransName = jobTransNames[0];
      cbJobTransNameSelection.setEnabled(false);
    }
    if(jobTransNames.length > 0){
      for(int i=0; i<jobTransNames.length; ++i){
        cbJobTransNameSelection.addItem(jobTransNames[i]);
      }
      cbJobTransNameSelection.setEnabled(true);
    }
    
    // Set the selection
    if(jobTransNames.length > 1 && cbJobTransNameSelection.getClass().isInstance(new JComboBox())){
      for(int i=0; i<jobTransNames.length; ++i){
        Debug.debug("Trying to set jobTransName, "+jobTransNames[i]+" : "+
            jtName, 3);
        if(jobTransNames[i].equals(jtName)){
          jobTransName = jtName;
          ((JComboBox) cbJobTransNameSelection).setSelectedIndex(i);
        }
      }
    }
  }

  /**
   * Initialises a panel with combobox which with user can select transformation
   */
  private void initVersionPanel(){
    versionInit = true;
    Debug.debug("initVersionPanel with jobTransFK "+jobTransFK, 3);
    String imp = "";
    String jtVersion = "-1";
    Vector vec = new Vector();
    boolean ok = true;
    if(transformations.values.length > 0){
      imp = transformations.getValue(0,"version");
      if(transformations.getValue(0,"jobTransName")!=null &&
          transformations.getValue(0,"jobTransName").equals(jobTransName)){
        vec.add(imp);
      }
    }
    if(transformations.values.length > 1){
      // When editing, find version of original jobTransFK
      for(int i=1; i<transformations.values.length; ++i){
        ok= true;
        imp = transformations.getValue(i,"version");
        if(transformations.getValue(i,"jobTransID").equals(jobTransFK)){
          jtVersion = transformations.getValue(i,"version");
        }
        // Avoid duplicates
        for(int j=0; j<vec.size(); ++j){
          if(transformations.getValue(i,"version")!=null &&
              transformations.getValue(i,"version").equals(
              vec.get(j))){
            ok = false;
            break;
          }
        }
        if(ok && transformations.getValue(i,"jobTransName")!=null &&
            transformations.getValue(i,"jobTransName").equals(jobTransName)){
          vec.add(imp);
        }
      }
    }
    
    versions = new String [vec.size()];
    for(int i = 0; i < vec.size(); ++i){
      
      if((vec.toArray())[i]!=null){
        versions[i] = (vec.toArray())[i].toString();
      }
    }    

    if(vec.size()>0){
      versions = new String [vec.size()];
      for(int i = 0; i < vec.size(); ++i){
        if((vec.toArray())[i]!=null){
          versions[i] = vec.get(i).toString();
        }
      }    
    }
    else{
      Debug.debug("WARNING: No versions found for transformations belonging to task"+
          taskName+" with jobTransName "+jobTransName+
          ". Displaying all versions of jobTransName...", 2);
      versions = dbPluginMgr.getVersions(jobTransName);
    }
    
    Debug.debug("Number of versions: "+versions.length, 3);

    if(cbVersionSelection == null){
      cbVersionSelection = new JComboBox();
      cbVersionSelection.addActionListener(new java.awt.event.ActionListener(){
        public void actionPerformed(java.awt.event.ActionEvent e){
          cbVersionSelection_actionPerformed();
      }});
    }
    else{
      cbVersionSelection.removeAllItems();
    }

    if(versions.length == 0){
      version = null;
      cbVersionSelection.setEnabled(false);
    }
    if(versions.length == 1){
      version = versions[0];
      cbVersionSelection.setEnabled(false);
    }
    
    if(versions.length > 0){
      for(int i=0; i<versions.length; ++i){
        cbVersionSelection.addItem(versions[i]);
      }
      cbVersionSelection.setEnabled(true);
    }
    
    // Set the selection
    if(versions.length > 1 && cbVersionSelection.getClass().isInstance(new JComboBox())){
      for(int i=0; i<versions.length; ++i){
        Debug.debug("Trying to set version, "+versions[i]+" : "+
            jtVersion, 3);
        if(versions[i].equals(jtVersion)){
          version = jtVersion;
          ((JComboBox) cbVersionSelection).setSelectedIndex(i);
          break;
        }
      }
    }
    versionInit = false;
  }

    private void initArithmeticPanel(){

    /**
     * Called when version is selected in combo box cbTransformationSelection
     *
     * Initialises text fields with attributes
     */

    // Panel counter

    pCounter.setLayout(new GridBagLayout());

    pCounter.removeAll();

    if(!reuseTextFields)
      sFrom.setValue(new Integer(1));
    if(!reuseTextFields)
      sTo.setValue(new Integer(1));

    pCounter.add(new JLabel("for i = "));
    pCounter.add(sFrom);

    pCounter.add(new JLabel("  to "));

    pCounter.add(sTo);


// Panel Constants

    if(!reuseTextFields || tcConstant.size() == 0){
      pConstants.removeAll();
      tcConstant.removeAllElements();

      for(int i=0; i<4; ++i)
        addConstant();
      
    }

// panel Button

    JButton bLoad = new JButton("Load");
    JButton bSave = new JButton("Save");
    JButton bAddConstant = new JButton("New Constant");

    bLoad.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        load();
      }
    });

    bSave.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        save();
      }
    });

    bAddConstant.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        addConstant();
      }
    });

    pButtons.setLayout(new GridBagLayout());

    GridBagConstraints cb = new GridBagConstraints();
    cb.fill = GridBagConstraints.VERTICAL;
    cb.anchor = GridBagConstraints.NORTHWEST;
    cb.gridx = 0;
    cb.gridy = 0;         
    pButtons.add(bLoad, cb);
    bLoad.setEnabled(false);
    cb.gridx = 0;
    cb.gridy = 1;         
    pButtons.add(bSave, cb);
    bSave.setEnabled(false);
    cb.gridx = 0;
    cb.gridy = 2;         
    pButtons.add(bAddConstant, cb);
  }


  private void initAttributePanel(){
    
    GridBagConstraints cl = new GridBagConstraints();
    cl.fill = GridBagConstraints.VERTICAL;
    cl.gridx = 1;
    cl.gridy = 0;         
    cl.anchor = GridBagConstraints.NORTHWEST;

    pAttributes.setLayout(new GridBagLayout());
    pAttributes.removeAll();

    spAttributes.getViewport().add(pAttributes, null);
    
    if(oldTcCstAttributes != null){
      tcCstAttributes = oldTcCstAttributes;
      // when creating, zap loaded jobDefinitionID
      /*if(!editing){
        for(int i =0; i<tcCstAttributes.length; ++i){
          if(cstAttributesNames[i].equalsIgnoreCase(jobDefIdentifier)){
            setJText((JComponent) tcCstAttributes[i],"");
            ((JComponent) tcCstAttributes[i]).setEnabled(false);
          }
        }
      }*/
    }

    if(!editing && oldJobTransFK!=null && Integer.parseInt(oldJobTransFK)>-1){
      jobTransFK = oldJobTransFK;
    }

    if(!reuseTextFields || tcCstAttributes == null ||
        tcCstAttributes.length != cstAttributesNames.length){
      Debug.debug("Creating new tcCstAttributes, "+
          tcCstAttributes+", "+(tcCstAttributes==null ? "":Integer.toString(tcCstAttributes.length)),
              3);
      tcCstAttributes = new JComponent[cstAttributesNames.length];
    }
    
    boolean jobParsOk = false;
    boolean jobOutputsOk = false;
    boolean jobLogsOk = false;
    
    for(int i =0; i<cstAttributesNames.length; ++i){
      
      if(cstAttributesNames[i].equalsIgnoreCase("jobPars") /*&& editing*/){
        jobParsOk = true;
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("jobOutputs") /*&& editing*/){
        jobOutputsOk = true;
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("jobLogs") /*&& editing*/){
        jobLogsOk = true;
      }
      
      if(cstAttributesNames[i].equalsIgnoreCase("jobTransFK")){
        cl.gridx=0;
        cl.gridy=i;
        pAttributes.add(new JLabel("jobTransFK" + " : "), cl);
        if(!reuseTextFields || tcCstAttributes[i] == null)
          tcCstAttributes[i] = createTextComponent(TEXTFIELDWIDTH);
        
        ((JTextComponent) tcCstAttributes[i]).setEnabled(false);
      }
      else if((cstAttributesNames[i].equalsIgnoreCase("jobXML") ||
          jobParsOk && jobOutputsOk && jobLogsOk)){
        Debug.debug("Setting jobXML panel", 3);
        cl.gridx=0;
        cl.gridy=i;
        GridBagConstraints cv = new GridBagConstraints();
        cv.fill = GridBagConstraints.VERTICAL;
        cv.weightx = 0.5;
        cv.gridx = 0;
        cv.gridy = 0;         
        cv.ipady = 10;
        cv.weighty = 0.5;
        cv.anchor = GridBagConstraints.NORTHWEST;
        createJobXmlPanel();
        jobXmlContainer.removeAll();
        if(jobXmlPanel!=null){
          jobXmlContainer.add(jobXmlPanel,cv);
        }
        cv.gridx = 0;
        cv.gridy = 1;
        // We add jobXmlContainer to tcCstAttributes
        // although it is displayed in the first column and not the second.
        // This is to allow parsing to XML later.
        tcCstAttributes[i] = jobXmlContainer;
        
        cl.gridwidth = 2;
        pAttributes.add(jobXmlContainer,cl);
        cl.gridwidth=1;
        
        // just to not have the panel added again
        jobParsOk = false;
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("ipConnectivity")){
        cl.gridx=0;
        cl.gridy=i;
        pAttributes.add(new JLabel("ipConnectivity" + " : "), cl);
        tcCstAttributes[i] = new JComboBox();
        ((JComboBox) tcCstAttributes[i]).addItem("yes");
        ((JComboBox) tcCstAttributes[i]).addItem("no");
        if(!editing){
          ((JComboBox) tcCstAttributes[i]).setSelectedItem("no");
        }
        if(editing || cstAttr[i]!=null){
          setJText(tcCstAttributes[i], cstAttr[i]);
        }
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("ramUnit")){
        cl.gridx=0;
        cl.gridy=i;
        pAttributes.add(new JLabel("ramUnit" + " : "), cl);
        tcCstAttributes[i] = new JComboBox();
        ((JComboBox) tcCstAttributes[i]).addItem("MB");
        ((JComboBox) tcCstAttributes[i]).addItem("GB");
        if(!editing){
          ((JComboBox) tcCstAttributes[i]).setSelectedItem("MB");
        }
        if(editing || cstAttr[i]!=null){
          setJText(tcCstAttributes[i], cstAttr[i]);
        }
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("diskUnit")){
        cl.gridx=0;
        cl.gridy=i;
        pAttributes.add(new JLabel("diskUnit" + " : "), cl);
        tcCstAttributes[i] = new JComboBox();
        ((JComboBox) tcCstAttributes[i]).addItem("MB");
        ((JComboBox) tcCstAttributes[i]).addItem("GB");
        if(!editing){
          ((JComboBox) tcCstAttributes[i]).setSelectedItem("GB");
        }
        if(editing || cstAttr[i]!=null){
          setJText(tcCstAttributes[i], cstAttr[i]);
        }
      }
      /*else if(cstAttributesNames[i].equalsIgnoreCase("currentState")){
        cl.gridx=0;
        cl.gridy=i;
        pAttributes.add(new JLabel("currentState" + " : "), cl);
        tcCstAttributes[i] = new JComboBox();
        ((JComboBox) tcCstAttributes[i]).addItem("DEFINED");
        ((JComboBox) tcCstAttributes[i]).addItem("RUNNING");
        ((JComboBox) tcCstAttributes[i]).addItem("TOBEDONE");
        ((JComboBox) tcCstAttributes[i]).addItem("ABORTED");
        ((JComboBox) tcCstAttributes[i]).addItem("FAILED");
        if(!editing){
          ((JComboBox) tcCstAttributes[i]).setSelectedItem("DEFINED");
        }
        if(editing || cstAttr[i]!=null){
          setJText(tcCstAttributes[i], cstAttr[i]);
        }
      }*/
      else if(cstAttributesNames[i].equalsIgnoreCase("currentState")){
        cl.gridx=0;
        cl.gridy=i;
        pAttributes.add(new JLabel(cstAttributesNames[i] + " : "), cl);
        if(!reuseTextFields || tcCstAttributes[i] == null)
          tcCstAttributes[i] = createTextComponent(TEXTFIELDWIDTH);
        
        setJText(tcCstAttributes[i], cstAttr[i]);
        //tcCstAttributes[i].setEnabled(false);
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("taskFK")){
        cl.gridx=0;
        cl.gridy=i;
        pAttributes.add(new JLabel(cstAttributesNames[i] + " : "), cl);
        if(!reuseTextFields || tcCstAttributes[i] == null)
          tcCstAttributes[i] = createTextComponent(TEXTFIELDWIDTH);
        
        setJText(tcCstAttributes[i], Integer.toString(taskID));
        tcCstAttributes[i].setEnabled(false);
      }
      else if(!cstAttributesNames[i].equalsIgnoreCase("jobPars") &&
              !cstAttributesNames[i].equalsIgnoreCase("jobOutputs") &&
              !cstAttributesNames[i].equalsIgnoreCase("jobLogs")){
        cl.gridx=0;
        cl.gridy=i;
        pAttributes.add(new JLabel(cstAttributesNames[i] + " : "), cl);
        if(!reuseTextFields || tcCstAttributes[i] == null)
          tcCstAttributes[i] = createTextComponent(TEXTFIELDWIDTH);
        
        if(cstAttr[i]!=null && !cstAttr[i].equals("")){
          Debug.debug("Setting cstAttr["+i+"]: "+cstAttr[i], 3);
          setJText(tcCstAttributes[i], cstAttr[i]);
        }
      }      
      cl.gridx=1;
      cl.gridy=i;
      if(!cstAttributesNames[i].equalsIgnoreCase("jobXML") &&
          !cstAttributesNames[i].equalsIgnoreCase("jobPars") &&
          !cstAttributesNames[i].equalsIgnoreCase("jobOutputs") &&
          !cstAttributesNames[i].equalsIgnoreCase("jobLogs")){
        pAttributes.add(tcCstAttributes[i], cl);
      }
      if(cstAttributesNames[i].equalsIgnoreCase(jobDefIdentifier)){
        // when creating, zap loaded jobDefinitionID
        if(!editing){
          setJText((JComponent) tcCstAttributes[i],"");
        }
        ((JTextComponent) tcCstAttributes[i]).setEnabled(false);
      }
    }
  }

  private void setEnabledAttributes(boolean enabled){
    for(int i =0; i<cstAttributesNames.length; ++i){
      if(cstAttributesNames[i].equalsIgnoreCase("jobXML")){
      }
      else if(!cstAttributesNames[i].equalsIgnoreCase("jobTransFK") &&
              !cstAttributesNames[i].equalsIgnoreCase(jobDefIdentifier) &&
              !cstAttributesNames[i].equalsIgnoreCase("taskFK") &&
              tcCstAttributes[i]!=null){
        tcCstAttributes[i].setEnabled(enabled);
      }
    }
    if(jobXmlPanel!=null){
      jobXmlPanel.setVisible(enabled);
      jobXmlPanel.updateUI();
    }
    // the create/update button on the CreateEditDialog panel
    ((JButton) ((JPanel) this.getParent().getComponent(1)).getComponent(3)).setEnabled(enabled);
    updateUI();
  }

  private void setValuesInAttributePanel(){
    
    Debug.debug("Setting values...", 3);

    boolean jobParsOk = false;
    boolean jobOutputsOk = false;
    boolean jobLogsOk = false;
    String jobXml = "";

    for(int i =0; i<cstAttributesNames.length; ++i){     
      if(cstAttributesNames[i].equalsIgnoreCase("jobTransFK")){
        ((JTextComponent) tcCstAttributes[i]).setEnabled(false);
        setJText(tcCstAttributes[i], jobTransFK);
      }
      // With the new schema jobPars, jobOutputs and jobLogs are in separate DB fields.
      // With the old schema they were in the same field and different XML sections.
      // Wrap <jobDef> around the three sections to emulate old schema...
      else if(cstAttributesNames[i].equalsIgnoreCase("jobPars") /*&& editing*/){
        jobXml += tcCstAttributes[i];
        jobParsOk = true;
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("jobOutputs") /*&& editing*/){
        jobXml += tcCstAttributes[i];
        jobOutputsOk = true;
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("jobLogs") /*&& editing*/){
        jobXml += tcCstAttributes[i];
        jobLogsOk = true;
      }
      if(jobParsOk && jobOutputsOk && jobLogsOk){
        cstAttributesNames[i] = "jobXML";
        cstAttr[i] = "<jobDef>"+jobXml+"</jobDef>";
      }
      if(cstAttributesNames[i].equalsIgnoreCase("jobXML") /*&& editing*/){
        Debug.debug("Setting jobXML", 2);
        tcCstAttributes[i].removeAll();
        GridBagConstraints cv = new GridBagConstraints();
        cv.ipady = 10;
        cv.weighty = 0.5;
        cv.anchor = GridBagConstraints.NORTHWEST;
        cv.fill = GridBagConstraints.VERTICAL;
        cv.weightx = 0.5;
        cv.gridx = 0;
        cv.gridy = 0;
        createJobXmlPanel();
        if(jobXmlPanel!=null){
          tcCstAttributes[i].add(jobXmlPanel,cv);
        }
        jobParsOk = false;
      }
    }
  }

  private void createJobXmlPanel(){
    Debug.debug("createJobXmlPanel, "+
        jobTransFK+", "+oldJobTransFK +" "+jobXmlPanel, 3);
    // reuse panel values if possible
    if(!editing && jobXmlPanel!=null &&
        (jobTransFK==null || jobTransFK.equals("-1") ||
        jobTransFK.equals(oldJobTransFK))){
      Debug.debug("NOT making new jobXmlPanel", 3);
      return;
    }
    else{
      Debug.debug("making new jobXmlPanel", 3);
      makeJobXmlPanel(false);
      oldJobTransFK = jobTransFK;
      Debug.debug("Seting oldJobTransFK to "+oldJobTransFK, 3);
    }
  }
  
  private String getSignature(){
    String signature = "";
    
    if(!jobTransFK.equals("-1") &&
        transformations.values.length!=0){
      if(table==null || table.getSelectedRow()<0 || !editing){
        Debug.debug("getting signature from jobTrans..."+transformations.values.length+
            " "+jobTransFK, 3);
        for(int i=0; i<transformations.values.length; ++i){
          if(transformations.getValue(i, "jobTransId").equals(jobTransFK)){
            signature = transformations.getValue(i, "formalPars");
            if (signature == null) Debug.debug("got signature: null from jobTrans",3); else
              Debug.debug("got signature from jobTrans: "+signature, 3);
            break;
          }
        }      
      }
      else{
        boolean jobParsOk = false;
        boolean jobOutputsOk = false;
        boolean jobLogsOk = false;
        String jobXml = "";
        for(int i=0; i<cstAttributesNames.length; ++i){

          if(cstAttributesNames[i].equalsIgnoreCase("jobPars") /*&& editing*/){
            jobXml += cstAttr[i];
            jobParsOk = true;
          }
          else if(cstAttributesNames[i].equalsIgnoreCase("jobOutputs") /*&& editing*/){
            jobXml += cstAttr[i];
            jobOutputsOk = true;
          }
          else if(cstAttributesNames[i].equalsIgnoreCase("jobLogs") /*&& editing*/){
            jobXml += cstAttr[i];
            jobLogsOk = true;
          }
          if(jobParsOk && jobOutputsOk && jobLogsOk){
            cstAttributesNames[i] = "jobXML";
            cstAttr[i] = "<jobDef>"+jobXml+"</jobDef>";
            jobParsOk = false;
          }
          
          if(cstAttributesNames[i].equalsIgnoreCase("jobXML")){
            signature = cstAttr[i];
            Debug.debug("got signature from jobDefinition: "+signature, 3);
          }
        }
      }
    }
    if(signature==null || signature.equals("")){
      Debug.debug("Ended here because "+jobTransFK+ " : "+editing+ " : "+table.getSelectedRow()+
          //" : "+transformations.values.length+
          " : "+table +" : "+transformations, 3);
      jobTransFK = "";
      // When jobTransFK is not set, the signature is obtained from the taskTransFK.
      // This should not happen...
      DBRecord taskTransRecord = dbPluginMgr.getTaskTransRecord(taskID);
      if (taskTransRecord == null ) {
        Debug.debug("getSignature: taskTransRecord is null!", 2);
        signature = "";
      }
      else{
        try{
          signature = taskTransRecord.getValue("formalPars").toString();
        }
        catch(Throwable e){
          Debug.debug("getSignature: formalPars is null!", 2);
          signature = "";
        }
      }
      if (signature == null){
          Debug.debug("got signature: null", 3);
          signature = "";
      }
      else
        Debug.debug("got signature from taskTrans: "+signature, 3);
    }
    return signature;
  }
  
  private void makeJobXmlPanel(boolean clearFields){
    
    Debug.debug("jobTransFK: "+jobTransFK, 3);
    Debug.debug("oldJobTransFK: "+oldJobTransFK, 3);
    
    String signature = getSignature();
    int jobXmlIndex;
    
    if(signature!=null && !signature.equals("")){
      xmlParsNode = XmlNode.parseString(signature, 0);
      if(xmlParsNode!=null){
        Debug.debug("Setting new jobXmlPanel", 3);
        xmlParsNode.fillJPanel(clearFields, editing);
        if(!editing){
          int logPanelNr = xmlParsNode.nodes.size()-1;
          XmlNode leafNode = (XmlNode)xmlParsNode.nodes.elementAt(logPanelNr);
          XmlNode testNode;
          // We attach the log fields to the last panel, which is
          // 'normal', i.e. with 2 components, i.e. not an output or
          // input files panel. Not too elegant if the last panel
          // is an output/input panel...
          if(leafNode.jpanel.getName().equals("OUTPUT") ||
              leafNode.jpanel.getName().equals("INPUT")){
            int i = xmlParsNode.nodes.size()-1;
            for(i=xmlParsNode.nodes.size()-1; i>=0; --i){
              testNode = (XmlNode)xmlParsNode.nodes.elementAt(i);
              if(!testNode.jpanel.getName().equals("OUTPUT") &&
                  !testNode.jpanel.getName().equals("INPUT")){
                leafNode = testNode;
                break;
              }
            }
            if(i==0 && (leafNode.jpanel.getName().equals("OUTPUT") ||
                leafNode.jpanel.getName().equals("INPUT"))){
              Debug.debug("ERROR! There needs to be at least one regular XML field.", 1);
            }
          }
          leafNode.addLogJFields();
        } 
        jobXmlPanel = xmlParsNode.jpanel;
      }
      else{
        Debug.debug(
            "The signature could not be parsed. Adding field with raw XML",
            3);
        jobXmlPanel = new JPanel();
        jobXmlPanel.add(new JTextField(signature,TEXTFIELDWIDTH));
      }
    }
    else{
      jobXmlPanel = new JPanel();
    }
    jobXmlPanel.setName("jobPars");
  }

   /**
   * Action Events
   */

  private void cbJobTransNameSelection_actionPerformed(){
    if(!loaded) return;
    
    if(cbJobTransNameSelection == null ||
        cbJobTransNameSelection.getSelectedItem() == null){
      if(cbJobTransNameSelection.getItemCount()>0){
        jobTransName = cbJobTransNameSelection.getItemAt(0).toString();
      }
      else{
        Debug.debug("No jobTransName selected...", 3);
        return;
      }
    }
    
    jobTransName = cbJobTransNameSelection.getSelectedItem().toString();
    /*
     Using DBPluginMgr object which was passed when function initGUI(..) was called
     */
    Debug.debug("Initializing version panel for jobTransName "+jobTransName, 3);
    initVersionPanel();
    setEnabledAttributes(false);
    pAttributes.updateUI();
  }

  private void cbVersionSelection_actionPerformed(){
    // TODO: these two variables are there to prevent
    // this method from being called when the main panel
    // is initialized or the versionPanel is initialized.
    // There must be another way!
    if(!loaded && versionInit) return;
   
    if(cbVersionSelection == null){
      return;
    }
    
    //oldJobTransFK = jobTransFK;
    //Debug.debug("Seting oldJobTransFK to "+oldJobTransFK, 3);

    if(cbVersionSelection!=null && cbVersionSelection.getSelectedItem()!=null){
      if(!versionInit){
        version = cbVersionSelection.getSelectedItem().toString();
        Debug.debug("Set version from selection "+version, 3);
      }
    }
    else{
      if(cbVersionSelection!=null && cbVersionSelection.getItemCount()==1){
        version = cbVersionSelection.getItemAt(0).toString();
      }
      else{
        Debug.debug("No version selected...", 3);
        //return;
      }
    }
    /*
     Using DBPluginMgr object which was passed when function initGUI(..) was called
     */
    // Set jobTransFK
    Debug.debug("jobTransName, version: "+jobTransName+", "+version, 3);
    for(int i=0; i<transformations.values.length; ++i){
      Debug.debug("Checking jobTransFK "+transformations.getValue(i,"jobTransID"), 3);
      Debug.debug("  "+transformations.getValue(i,"jobTransName"), 3);
      Debug.debug("  "+transformations.getValue(i,"version"), 3);
      if(transformations.getValue(i,"jobTransName")!=null &&
         transformations.getValue(i,"version")!=null &&
         transformations.getValue(i,"jobTransName").equals(jobTransName) &&
         transformations.getValue(i,"version").equals(version)){
        Debug.debug("Setting jobTransFK to "+transformations.getValue(i,"jobTransID"), 3);
        jobTransFK = transformations.getValue(i,"jobTransID");
        break;
      }
     }
    pAttributes.updateUI();
    if(!versionInit){
      setEnabledAttributes(true);
    }
    setValuesInAttributePanel();
  }


  /**
   * public methods
   */

  public void clear(){
    /**
     * Called by : JobDefinition.button_ActionPerformed()
     */

    Vector textFields = getNonIdTextFields();

    for(int i =0; i<textFields.size(); ++i)
      setJText((JComponent) textFields.get(i),"");
    
    GridBagConstraints cv = new GridBagConstraints();
    cv.fill = GridBagConstraints.VERTICAL;
    cv.weightx = 0.5;
    cv.gridx = 0;
    cv.gridy = 0;         
    cv.ipady = 10;
    cv.weighty = 0.5;
    cv.anchor = GridBagConstraints.NORTHWEST;
    makeJobXmlPanel(true);
    jobXmlContainer.removeAll();
    if(jobXmlPanel!=null){
      jobXmlContainer.add(jobXmlPanel,cv);
    }
    cv.gridx = 0;
    cv.gridy = 1;
    updateUI();
  }


  public void create(final boolean showResults, final boolean editing) {
    /**
     * Called when button Create is clicked in JobDefinition
     */

    Debug.debug("create",  1);
    
    //String signature = getSignature();
    //if(signature!=null && !signature.equals("")){
     // xmlParsNode.fillXML(editing);
     // Debug.debug("xmlParsNode: "+xmlParsNode.xmlstring, 3);
    //}
    
    for(int i=0; i< cstAttr.length; ++i){
      Debug.debug("setting " + cstAttributesNames[i],  3);
      cstAttr[i] = getJTextOrEmptyString(tcCstAttributes[i], xmlParsNode, editing);
      Debug.debug("to " + cstAttr[i],  3);
    }

      oldTcCstAttributes = tcCstAttributes;     
      oldJobTransFK = jobTransFK;
      Debug.debug("Seting oldJobTransFK to "+oldJobTransFK, 3);

  
    Debug.debug("creating new JobDefCreator",  3);
    
    new JobDefCreator(dbName,
                      taskMgr,
                      ((Integer)(sFrom.getValue())).intValue(),
                      ((Integer)(sTo.getValue())).intValue(),
                      showResults,
                      tcConstant,
                      cstAttr,
                      cstAttributesNames,
                      editing
                      );

  }

  /**
   * Private methods
   */

  private void addConstant(){
    if(tcConstant.size() == 26)
      return;
    
    JTextField tf = new JTextField(CFIELDWIDTH);
    char name = (char) ('A' + (char)tcConstant.size());
    int cstByRow = 4;

    int row = tcConstant.size() / cstByRow;
    int col = tcConstant.size() % cstByRow;
    
    tcConstant.add(tf);
    
    GridBagConstraints cc = new GridBagConstraints();

    cc.gridx = col*2;
    cc.gridy = row;   
    pConstants.add(new JLabel("  " + new String(new char[]{name}) + " : "),cc);
    
    cc.gridx = col*2+1;
    cc.gridy = row;   
    pConstants.add(tf,cc);
    
    pConstants.updateUI();
  }




  private boolean save(){
    Vector v = getTextFields();
    String [] values = new String[v.size()+1];

    values[0] = ""+tcConstant.size();

    for(int i=1; i<values.length; ++i){
      values[i] = ((JTextComponent) v.get(i-1)).getText();
      if(values[i].length() == 0)
        values[i] = " ";
    }
    String user = dbPluginMgr.getUserLabel();
    if(!dbPluginMgr.saveDefVals(taskID, values, user)){
      Debug.debug("ERROR: Could not save values: "+values, 1);
      return false;
    }
    return true;
  }

  private void load(){
    
    String user = dbPluginMgr.getUserLabel();

    String [] defValues = dbPluginMgr.getDefVals(taskID, user);

    if(defValues ==null || defValues.length == 0)
      return;

    try{
      int nbConst = new Integer(defValues[0]).intValue();
      if(nbConst != tcConstant.size()){
        tcConstant.removeAllElements();
        pConstants.removeAll();
        for(int i=0; i<nbConst; ++i)
          addConstant();
      }
    }catch(NumberFormatException nfe){
      nfe.printStackTrace();
    }

    Enumeration tfs = getTextFields().elements();

    int i=1;
    while(tfs.hasMoreElements() && i<defValues.length){
      JTextComponent tf = (JTextComponent) tfs.nextElement();
      setJText(tf, defValues[i].trim());
      ++i;
    }
  }


  private Vector getTextFields(){
    Vector v = new Vector();

    v.addAll(tcConstant);

    for(int i=0; i<tcCstAttributes.length; ++i)
      v.add(tcCstAttributes[i]);

    return v;
  }

  private Vector getNonIdTextFields(){
    Vector v = new Vector();

    v.addAll(tcConstant);

    for(int i=0; i<tcCstAttributes.length; ++i){
      if(!cstAttributesNames[i].equalsIgnoreCase(jobDefIdentifier) &&
          !cstAttributesNames[i].equalsIgnoreCase("jobTransFK") &&
          !cstAttributesNames[i].equalsIgnoreCase("taskFK")){
        v.add(tcCstAttributes[i]);
      }
    }

    return v;
  }

  private JTextComponent createTextComponent(){
    JTextArea ta = new JTextArea();
    ta.setBorder(new JTextField().getBorder());
    ta.setWrapStyleWord(true);
    ta.setLineWrap(true);
    return ta;
  }
  
  private JTextComponent createTextComponent(int cols){
    JTextField tf = new JTextField("", cols);
    return tf;
  }
  
  private JTextComponent createTextComponent(String str){
    int length;
    if(str.length()>10){
      length = str.length()-5;
    }
    else{
      length = 6;
    }
    JTextArea ta = new JTextArea(str, 1, length);
    ta.setBorder(new JTextField().getBorder());
    ta.setWrapStyleWord(true);
    ta.setLineWrap(true);
    return ta;
  }
  
  private static String getJTextOrEmptyString(JComponent comp,
      XmlNode node, boolean editing){
    if(comp==null){
      Debug.debug("WARNING: JComponent is null", 3);
      return "";
    }
    String name = "";
    String label = "";
    String text = "";
    String [] ses;
    JComponent com;
    if(comp.getClass().isInstance(new JTextArea())||
        comp.getClass().isInstance(new JTextField())){
      text =  ((JTextComponent) comp).getText();
    }
    else if(comp.getClass().isInstance(new JComboBox())){
      if(((JComboBox) comp).getSelectedItem()==null){
        text = "";
      }
      else{
        text = ((JComboBox) comp).getSelectedItem().toString();
      }
    }
    // the jobXML panel
    else if(comp.getClass().isInstance(new JPanel())){
      for(int i=0; i<comp.getComponentCount(); ++i){
        // TODO: Here we seem to be missing jobOutputs in the case where
        // there is only one output/input file. As in e.g.
        // JobTransforms-0.2.19-share/ctb.g4sim.partgen.trf
        if(((JPanel) comp.getComponent(i)).getName() != null &&
            ((JPanel) comp.getComponent(i)).getName().equalsIgnoreCase("jobPars")){
          Debug.debug("Filling XML", 3);
          node.fillXML(editing);
          text = node.xmlstring;
          break;
        }
      }
      // merge in the input, ouput and stdout/stderr
      text = text.replaceFirst("</jobDef>\n","");
      node.inputsXmlstring = node.inputsXmlstring.replaceAll("</jobInputs>\n<jobInputs>\n","");
      node.outputsXmlstring = node.outputsXmlstring.replaceAll("</jobOutputs>\n<jobOutputs>\n","");
      node.logsXmlstring = node.logsXmlstring.replaceAll("</jobLogs>\n<jobLogs>\n","");
      text += node.inputsXmlstring;
      text += node.outputsXmlstring;
      text += node.logsXmlstring;
      if(text.indexOf("</jobDef>")<0 && text.indexOf("<jobDef>")>-1){
        text += "</jobDef>\n";
      }
    }
    else{
      Debug.debug("WARNING: unsupported component type "+comp.getClass().toString(), 1);
    }
    return text;
  }
  
  private static String setJText(JComponent comp, String text){
    if(comp.getClass().isInstance(new JTextArea()) ||
        comp.getClass().isInstance(new JTextField())){
      ((JTextComponent) comp).setText(text);
    }
    else if(/*text!=null && !text.equals("") && */comp.getClass().isInstance(new JComboBox())){
      ((JComboBox) comp).setSelectedItem(text);
    }
    return text;
  }
}
