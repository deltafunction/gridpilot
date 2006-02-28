package gridpilot;

import gridpilot.Debug;
import gridpilot.DatasetMgr;
import gridpilot.Database.DBResult;
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

  private DatasetMgr taskMgr;

  private String jobTransName;
  private String version;
  private JPanel pCounter = new JPanel();
  private JPanel pConstants = new JPanel();
  public JPanel pAttributes = new JPanel();
  private JScrollPane spAttributes = new JScrollPane();
  private JPanel pButtons = new JPanel();
  private JComboBox cbJobTransNameSelection = null;
  private JComboBox cbVersionSelection = null;
  private String [] jobTransNames;
  private String [] versions;
  public String jobTransFK = "-1";
  private String jobDefinitionID = "-1";
  private Table table;
  private JSpinner sFrom = new JSpinner(new SpinnerNumberModel(1, 1, 999999, 1));
  private JSpinner sTo = new JSpinner(new SpinnerNumberModel(1, 1, 999999, 1));
  public String [] cstAttributesNames;
  public JComponent [] tcCstAttributes;
  private JComponent [] tcCstJobDefAttributes;
  public boolean reuseTextFields = true;
  public Vector tcConstant = new Vector(); // contains all text components
  public String [] cstAttr = null;
  public boolean editing = false;
  public JPanel jobXmlContainer = new JPanel(new GridBagLayout());
  private DBResult transformations;
  private boolean loaded = false;
  private boolean versionInit = false;
  private String dbName;
  private DBPluginMgr dbPluginMgr = null;
  private String jobTransNameColumn = "jobTrans";
  public int taskID = -1;
  private String taskName;
  
  public static JComponent [] oldTcCstAttributes;
  public static String oldJobTransFK = "-1";
  private static int TEXTFIELDWIDTH = 32;
  private static int CFIELDWIDTH = 8;
  
  public String jobDefIdentifier;
  private String jobTransIdentifier;
  
  public JobDefCreationPanel(/*this is in case DBPanel was opened from the menu and_taskMgr is null*/String _dbName,
      DatasetMgr _taskMgr, Table _table,
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
      taskID = taskMgr.getDatasetID();
      taskName = taskMgr.getDatasetName();
    }
    else{
      taskID = -1;
      dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
      taskName = "";
    }

    
    jobTransIdentifier = GridPilot.getClassMgr().getConfigFile().getValue(
        dbPluginMgr.getDBName(),
    "transformation table identifier");

    jobDefIdentifier = dbPluginMgr.getJobDefIdentifier(dbPluginMgr.getDBName());

    //cstAttributesNames = JobDefinition.Fields;
    cstAttributesNames = dbPluginMgr.getFieldNames(
        GridPilot.getClassMgr().getConfigFile().getValue(
            dbPluginMgr.getDBName(),
            "job definition table name"));
    
    Debug.debug("cstAttributesNames: "+Util.arrayToString(cstAttributesNames), 3);
    
    cstAttr = new String[cstAttributesNames.length];
    
    transformations = dbPluginMgr.getTransformations();
    
    jobTransNameColumn = dbPluginMgr.getTransNameColumn();
    
    // When editing, fill cstAttr from db
    if(table.getSelectedRow()>-1 && editing){
      // Find jobdDefinitionID from db
      for(int i=0; i<table.getColumnNames().length; ++i){
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
      // Find jobTransFK from db
      jobTransFK = dbPluginMgr.getTransformationID(Integer.parseInt(jobDefinitionID));
      Debug.debug("Set jobTransFK from db: "+jobTransFK, 2);
      // Get job definition from db
      DBRecord jobDef = dbPluginMgr.getJobDefinition(Integer.parseInt(jobDefinitionID));
      for(int i=0; i<cstAttributesNames.length; ++i){
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

  public void initGUI(){

    setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white,new Color(165, 163, 151)),taskName));
    
    spAttributes.setPreferredSize(new Dimension(550, 500));
    spAttributes.setMinimumSize(new Dimension(550, 500));
    
    setLayout(new GridBagLayout());
    removeAll();
    
    if(!reuseTextFields || tcCstAttributes == null ||
        tcCstAttributes.length != cstAttributesNames.length){
      Debug.debug("Creating new tcCstAttributes, "+
          tcCstAttributes+", "+(tcCstAttributes==null ? "":Integer.toString(tcCstAttributes.length)),
              3);
      tcCstAttributes = new JComponent[cstAttributesNames.length];
    }

    dbPluginMgr.initAttributePanel(cstAttributesNames,
        cstAttr,
        tcCstAttributes,
        pAttributes,
        jobXmlContainer);
    spAttributes.getViewport().add(pAttributes, null);
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

    dbPluginMgr.setValuesInAttributePanel(cstAttributesNames,
        cstAttr,
        tcCstAttributes);
    
    if(!editing){
      dbPluginMgr.setEnabledAttributes(false,
          cstAttributesNames,
          tcCstAttributes);
    }
    
    updateUI();
    
    loaded = true;
    
    }

  private void initJobTransNamePanel(){
    /**
     * Creates a combobox jobTransNamePanel which with user can select transformation.
     **/
   Debug.debug("initJobTransNamePanel with jobTransFK "+jobTransFK, 3);    
    String jtName = "-1";
    boolean ok = true;
    Vector vec = new Vector();
    
    if(transformations.values.length>0){
      if(transformations.getValue(0,jobTransNameColumn)!=null){
        Debug.debug("Adding transformation "+
            transformations.getValue(0,jobTransNameColumn), 3);
        vec.add(transformations.getValue(0,jobTransNameColumn));
        if(transformations.getValue(0,jobTransIdentifier).equals(jobTransFK)){
          jtName = transformations.getValue(0,jobTransNameColumn);
        }
      }
      else{
        Debug.debug("WARNING: name null for transformation 0", 2);
      }
    }
    
    transformations = dbPluginMgr.getTransformations();
    jobTransNames = new String[transformations.values.length];
    for(int i=0; i<transformations.values.length; ++i){
      jobTransNames[i] = transformations.getValue(i, jobTransNameColumn);
    }

    // Find jobTransName of jobTransFK
    if(transformations.values.length > 1){
      for(int i=1; i<transformations.values.length; ++i){
        ok = true;
        Debug.debug("Checking transformation with jobTransID "+
            transformations.getValue(i,jobTransIdentifier), 3);
        if(transformations.getValue(i,jobTransIdentifier).equals(jobTransFK) &&
            transformations.getValue(i,jobTransNameColumn)!=null){
          jtName = transformations.getValue(i,jobTransNameColumn);
        }
        // Avoid duplicates
        for(int j=0; j<vec.size(); ++j){
          if(transformations.getValue(i,jobTransNameColumn) != null &&
              transformations.getValue(i,jobTransNameColumn).equals(
              vec.get(j))){
            ok = false;
            break;
          }
        }
        if(ok){
          if(transformations.getValue(i,jobTransNameColumn) != null){
            Debug.debug("Adding transformation "+
                transformations.getValue(i,jobTransNameColumn), 3);
            vec.add(transformations.getValue(i,jobTransNameColumn));
          }    
          else{
            //Debug.debug("WARNING: name null for transformation "+i, 2);
          }
        }
      }
    }
          
    if(vec.size()==0){
      Debug.debug("WARNING: No transformations found ", 2);
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

  private void initVersionPanel(){
    /**
     * Initialises a panel with combobox which with user can select transformation
     */
    versionInit = true;
    Debug.debug("initVersionPanel with jobTransFK "+jobTransFK, 3);
    String imp = "";
    String jtVersion = "-1";
    Vector vec = new Vector();
    if(transformations.values.length>0){
      // When editing, find version of original jobTransFK
      for(int i=0; i<transformations.values.length; ++i){
        if(transformations.getValue(i,jobTransIdentifier).equals(jobTransFK)){
          jtVersion = transformations.getValue(i,"version");
          break;
        }
      }
      versions = dbPluginMgr.getVersions(jobTransName);
    }
    else{
      Debug.debug("WARNING: No transformations found.", 1);
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
     * 
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

  public String getSignature(){
    String signature = "";
    
    Debug.debug("Entering getSignature with "+
        jobTransFK+
        " : "+editing+ " : "+table.getSelectedRow()+
        " : "+transformations.values.length+
        " : "+table, 3);

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
            //cstAttributesNames[i] = "jobXML";
            cstAttr[i] = "<jobDef>"+jobXml+"</jobDef>";
            jobParsOk = false;
            signature = "<jobDef>"+jobXml+"</jobDef>";
          }
          
          if(cstAttributesNames[i].equalsIgnoreCase("jobXML")){
            Debug.debug("cstAttributesNames: "+Util.arrayToString(cstAttributesNames), 3);
            signature = cstAttr[i];
            Debug.debug("got signature from jobDefinition: "+signature, 3);
          }
        }
      }
    }
    /*if(signature==null || signature.equals("")){
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
      if(signature == null){
          Debug.debug("got signature: null", 3);
          signature = "";
      }
      else
        Debug.debug("got signature from taskTrans: "+signature, 3);
    }*/
    if(signature == null){
      Debug.debug("null signature", 3);
      signature = "";
    }
    return signature;
  }
  
  private void cbJobTransNameSelection_actionPerformed(){
    if(!loaded) return;
    
    if(cbJobTransNameSelection == null ||
        cbJobTransNameSelection.getSelectedItem() == null){
      if(cbJobTransNameSelection.getItemCount()>0){
        jobTransName = cbJobTransNameSelection.getItemAt(0).toString();
      }
      else{
        Debug.debug("No transformation selected...", 3);
        return;
      }
    }
    
    jobTransName = cbJobTransNameSelection.getSelectedItem().toString();
    Debug.debug("Initializing version panel for transformation "+jobTransName, 3);
    initVersionPanel();
    dbPluginMgr.setEnabledAttributes(false,
        cstAttributesNames,
        tcCstAttributes);
    //pAttributes.updateUI();
    updateUI();
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
      }
    }
    // Set jobTransFK
    Debug.debug("name, version: "+jobTransName+", "+version, 3);
    for(int i=0; i<transformations.values.length; ++i){
      Debug.debug("Checking jobTransFK "+transformations.getValue(i,jobTransIdentifier), 3);
      Debug.debug("  "+transformations.getValue(i,jobTransNameColumn), 3);
      Debug.debug("  "+transformations.getValue(i,"version"), 3);
      if(transformations.getValue(i,jobTransNameColumn)!=null &&
         transformations.getValue(i,"version")!=null &&
         transformations.getValue(i,jobTransNameColumn).equals(jobTransName) &&
         transformations.getValue(i,"version").equals(version)){
        Debug.debug("Setting jobTransFK to "+transformations.getValue(i,jobTransIdentifier), 3);
        jobTransFK = transformations.getValue(i,jobTransIdentifier);
        break;
      }
     }
    //pAttributes.updateUI();
    updateUI();
    if(!versionInit){
      dbPluginMgr.setEnabledAttributes(true,
          cstAttributesNames,
          tcCstAttributes);
    }
    dbPluginMgr.setValuesInAttributePanel(cstAttributesNames,
        cstAttr,
        tcCstAttributes);
  }

  public void create(final boolean showResults, final boolean editing){
    /**
     * Called when button Create is clicked in JobDefinition
     */

    Debug.debug("create",  1);
    
    for(int i=0; i< cstAttr.length; ++i){
      Debug.debug("setting " + cstAttributesNames[i],  3);
      cstAttr[i] = dbPluginMgr.getJTextOrEmptyString(cstAttributesNames[i],
          tcCstAttributes[i], editing);
      Debug.debug("to " + cstAttr[i],  3);
    }

    oldTcCstAttributes = tcCstAttributes;     
    oldJobTransFK = jobTransFK;
    Debug.debug("Seting oldJobTransFK to "+oldJobTransFK, 3);
    
    for(int i =0; i<cstAttr.length; ++i){
      if(cstAttributesNames[i].equals("jobXML")){
        if(cstAttr[i]==null || cstAttr[i].equals("null") || cstAttr[i].equals("")){
          cstAttr[i] = "";
        }
        else{
          if(!editing && cstAttr[i].indexOf("</jobDef>")<0 && cstAttr[i].indexOf("<jobDef>")<0){
            cstAttr[i] = "<jobDef>"+cstAttr[i]+"</jobDef>";
          }
        }
        break;
      }
    }
    
    Debug.debug("creating new JobDefCreator", 3);  
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

  public JTextComponent createTextComponent(){
    JTextArea ta = new JTextArea();
    ta.setBorder(new JTextField().getBorder());
    ta.setWrapStyleWord(true);
    ta.setLineWrap(true);
    return ta;
  }
  
  public JTextComponent createTextComponent(int cols){
    JTextField tf = new JTextField("", cols);
    return tf;
  }
  
  public JTextComponent createTextComponent(String str){
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
  
  public static String setJText(JComponent comp, String text){
    if(comp.getClass().isInstance(new JTextArea()) ||
        comp.getClass().isInstance(new JTextField())){
      ((JTextComponent) comp).setText(text);
    }
    else if(/*text!=null && !text.equals("") && */comp.getClass().isInstance(new JComboBox())){
      ((JComboBox) comp).setSelectedItem(text);
    }
    return text;
  }
  
  public void clearPanel(){
    dbPluginMgr.clearPanel(
        cstAttributesNames,
        tcCstAttributes,
        jobXmlContainer,
        tcConstant
        );
  }
}
