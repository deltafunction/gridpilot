package gridpilot;

import gridpilot.Debug;
import gridpilot.TaskMgr;
import gridpilot.Database.DBResult;
import gridpilot.simplexmlnode;
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
 * One instance of this class is created by TaskPanel
 *
 */


public class JobDefCreationPanel extends CreateEditPanel {

  /*
   DBPluginMgr to be used is stored here by initGUI(..). Still, not all functions use this global
   variable (just to make sure no conflicts happen).
   */
  //DBPluginMgr dbPluginMgr;
  TaskMgr taskMgr;

  private String homePackage;
  private String implementation;

  private JPanel pCounter = new JPanel();
  private JPanel pConstants = new JPanel();
  private JPanel pAttributes = new JPanel();
  private JScrollPane spAttributes = new JScrollPane();
  private static JPanel jobXmlPanel;
  private JPanel pButtons = new JPanel();
  
  private JComboBox cbHomePackageSelection = null;
  private JComboBox cbImplementationSelection = null;
  private String [] homePackages;
  private String [] implementations;

  private String jobTransFK = "-1";
  
  private DBVectorTable table;
  

  private JSpinner sFrom = new JSpinner(new SpinnerNumberModel(1, 1, 999999, 1));
  private JSpinner sTo = new JSpinner(new SpinnerNumberModel(1, 1, 999999, 1));


  private String [] cstAttributesNames;
  private JComponent [] tcCstAttributes;
  private JComponent [] tcCstJobDefAttributes;
  private boolean reuseTextFields = true;
  private Vector tcConstant = new Vector(); // contains all text components
  private String [] cstAttr = null;
  
  private boolean editing = false;
  
  private static simplexmlnode xmlParsNode;
  
  private JPanel jobXmlContainer = new JPanel(new GridBagLayout());
  
  private DBResult transformations;
  private boolean loaded = false;
  private boolean implementationInit = false;
  private int TEXTFIELDWIDTH = 16;
  private int CFIELDWIDTH = 8;
  
  
  /**
   * Constructor for creating new records
   */

 /*public JobDefCreationPanel(TaskMgr _taskMgr){
    
    editing = false;
    
    taskMgr=_taskMgr;
    
    cstAttributesNames = taskMgr.getVectorTableModel().columnNames;
    cstAttr = new String[cstAttributesNames.length];
    
    transformations = taskMgr.getDBPluginMgr().getAllJobTransRecords(taskMgr.getTaskIdentifier());
    
    Debug.debug("Creating new job record for task "+taskMgr.getTaskName()+
        " with "+transformations.values.length+" transformations.", 3);
    
    // Fill cstAttr with ""
    for(int i=0; i < cstAttributesNames.length; ++i){
      cstAttr[i] = "";
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
  }*/

  /**
   * Constructor for editing record
   */

  public JobDefCreationPanel(TaskMgr _taskMgr, DBVectorTable _table,
      boolean _editing){
    
    //editing = true;
    
    editing = _editing;
    
    taskMgr=_taskMgr;
    table = _table;

    cstAttributesNames = taskMgr.getVectorTableModel().columnNames;
    cstAttr = new String[cstAttributesNames.length];
    
    transformations = taskMgr.getDBPluginMgr().getAllJobTransRecords(taskMgr.getTaskIdentifier());

    Debug.debug("Editing job record for task "+taskMgr.getTaskName()+". Rows: "+
        table.getRowCount()+
        ". Number of transformations: "+
       (transformations!=null ? transformations.values.length : 0), 3);

    // Fill cstAttr from table
    if(table.getSelectedRow()>-1 && editing){
      for(int i=0; i < table.getColumnCount(); ++i){
        cstAttr[i] = table.getValueAt(table.getSelectedRow(),i).toString();
        Debug.debug("Filling in "+cstAttr[i], 3);
        if(cstAttributesNames[i].equals("jobTransFK")){
          jobTransFK = cstAttr[i];
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
        Color.white,new Color(165, 163, 151)),taskMgr.getTaskName()));
    
    spAttributes.setPreferredSize(new Dimension(550, 500));

    setLayout(new GridBagLayout());
    removeAll();

    initAttributePanel();
    initHomePackagePanel();
    initImplementationPanel();
    
    GridBagConstraints ct = new GridBagConstraints();
    ct.fill = GridBagConstraints.VERTICAL;
    //ct.anchor = GridBagConstraints.CENTER;
    ct.insets = new Insets(2,2,2,2);
    
    ct.gridx = 0;
    ct.gridy = 0;   
    ct.gridwidth=1;
    ct.gridheight=1;
    
    if(!editing){
      initArithmeticPanel();
      ct.gridx = 0;
      ct.gridy = 0;         
      add(cbHomePackageSelection,ct);
      
      ct.gridx = 1;
      ct.gridy = 0;         
      add(cbImplementationSelection,ct);
      
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
      add(pConstants,ct);    
            
      ct.gridx = 0;
      ct.gridy = 4;
      ct.gridwidth=3;
      add(spAttributes, ct);
    }
    else{
      ct.gridx = 0;
      ct.gridy = 0;
      add(cbHomePackageSelection,ct);
      ct.gridx = 1;
      ct.gridy = 0;         
      add(cbImplementationSelection,ct);
      ct.gridx = 0;
      ct.gridy = 1;
      ct.gridwidth=2;
      add(spAttributes,ct);
    }

    setValuesInAttributePanel();
    updateUI();
    
    loaded = true;
    
    }

  /**
   * Creates a combobox homePackagePanel which with user can select transformation.
   **/
  private void initHomePackagePanel(){
    Debug.debug("initHomePackagePanel with jobTransFK "+jobTransFK, 3);    
    String jtHomePack = "-1";
    boolean ok = true;
    Vector vec = new Vector();
    
    if(transformations.values.length > 0){
      if(transformations.getValue(0,"jobTransID").equals(jobTransFK) &&
          transformations.getValue(0,"homePackage")!=null){
        homePackage = transformations.getValue(0,"homePackage");
      }
      if(transformations.getValue(0,"homePackage")!=null){
        Debug.debug("Adding homePackage "+
            transformations.getValue(0,"homePackage"), 3);
        vec.add(transformations.getValue(0,"homePackage"));
      }
      else{
        Debug.debug("WARNING: homePackage null for transformation 0", 2);
      }
     }
    // When editing, find homePackage of original jobTransFK
    if(transformations.values.length > 1){
      for(int i=1; i<transformations.values.length; ++i){
        ok = true;
        if(transformations.getValue(i,"jobTransID").equals(jobTransFK) &&
            transformations.getValue(i,"homePackage")!=null){
          jtHomePack = transformations.getValue(i,"homePackage");
        }
        // Avoid duplicates
        for(int j=0; j<vec.size(); ++j){
          if(transformations.getValue(i,"homePackage") != null &&
              transformations.getValue(i,"homePackage").equals(
              vec.get(j))){
            ok = false;
            break;
          }
        }
        if(ok && transformations.getValue(i,"homePackage") != null){
          Debug.debug("Adding homePackage "+
              transformations.getValue(i,"homePackage"), 3);
          vec.add(transformations.getValue(i,"homePackage"));
        }
        else{
          Debug.debug("WARNING: homePackage null for transformation "+i, 2);
        }
      }
    }
          
    if(vec.size()>0){
      homePackages = new String [vec.size()];
      for(int i = 0; i < vec.size(); ++i){
        homePackages[i] = vec.get(i).toString();
      }    
    }
    else{
      Debug.debug("WARNING: No homePackages found for transformations belonging to task"+
          taskMgr.getTaskName()+". Displaying all homePackages...", 2);
      homePackages = taskMgr.getDBPluginMgr().getHomePackages();
      transformations = taskMgr.getDBPluginMgr().getAllJobTransRecords(-1);
    }

    if(cbHomePackageSelection == null){
      cbHomePackageSelection = new JComboBox(); 
      cbHomePackageSelection.addActionListener(new java.awt.event.ActionListener(){
        public void actionPerformed(java.awt.event.ActionEvent e){
          cbHomePackageSelection_actionPerformed();
      }});
    }
    else{
      cbHomePackageSelection.removeAllItems();
    }
    
    if(homePackages.length == 0){  
      homePackage = null;
      cbHomePackageSelection.setEnabled(false);
    }
    if(homePackages.length == 1){  
      homePackage = homePackages[0];
      cbHomePackageSelection.setEnabled(false);
    }
    if(homePackages.length > 0){
      //cbTransformationSelection = new JComboBox();  
      for(int i=0; i<homePackages.length; ++i){
        cbHomePackageSelection.addItem(homePackages[i]);
      }
      cbHomePackageSelection.setEnabled(true);
    }
    
    // Set the selection
    if(homePackages.length > 1 && cbHomePackageSelection.getClass().isInstance(new JComboBox())){
      for(int i=0; i<homePackages.length; ++i){
        if(homePackages[i].equals(jtHomePack)){
          homePackage = jtHomePack;
          ((JComboBox) cbHomePackageSelection).setSelectedIndex(i);
        }
      }
    }
  }

  /**
   * Initialises a panel with combobox which with user can select transformation
   */
  private void initImplementationPanel(){
    implementationInit = true;
    Debug.debug("initImplementationPanel with jobTransFK "+jobTransFK, 3);
    String imp = "";
    String jtImplementation = "-1";
    Vector vec = new Vector();
    boolean ok = true;
    if(transformations.values.length > 0){
      //if(transformations.getValue(0,"jobTransID").equals(jobTransFK)){
        imp = transformations.getValue(0,"implementation");
      //}
      if(transformations.getValue(0,"homePackage")!=null &&
          transformations.getValue(0,"homePackage").equals(homePackage)){
        vec.add(imp);
      }
    }
    if(transformations.values.length > 1){
      // When editing, find implementation of original jobTransFK
      for(int i=1; i<transformations.values.length; ++i){
        ok= true;
        imp = transformations.getValue(i,"implementation");
        if(transformations.getValue(i,"jobTransID").equals(jobTransFK)){
          jtImplementation = transformations.getValue(i,"implementation");
        }
        // Avoid duplicates
        for(int j=0; j<vec.size(); ++j){
          if(transformations.getValue(i,"implementation").equals(
              vec.get(j))){
            ok = false;
            break;
          }
        }
        if(ok && transformations.getValue(i,"homePackage")!=null &&
            transformations.getValue(i,"homePackage").equals(homePackage)){
          vec.add(imp);
        }
      }
    }
    
    implementations = new String [vec.size()];
    for(int i = 0; i < vec.size(); ++i){
      implementations[i] = (vec.toArray())[i].toString();
    }    

    if(vec.size()>0){
      implementations = new String [vec.size()];
      for(int i = 0; i < vec.size(); ++i){
        implementations[i] = vec.get(i).toString();
      }    
    }
    else{
      Debug.debug("WARNING: No implementations found for transformations belonging to task"+
          taskMgr.getTaskName()+" with homePackage "+homePackage+
          ". Displaying all implementations of homePackage...", 2);
      implementations = taskMgr.getDBPluginMgr().getImplementations(homePackage);
    }
    
    Debug.debug("Number of implementations: "+implementations.length, 3);

    if(cbImplementationSelection == null){
      cbImplementationSelection = new JComboBox();
      cbImplementationSelection.addActionListener(new java.awt.event.ActionListener(){
        public void actionPerformed(java.awt.event.ActionEvent e){
          cbImplementationSelection_actionPerformed();
      }});
    }
    else{
      cbImplementationSelection.removeAllItems();
    }

    if(implementations.length == 0){
      implementation = null;
      cbImplementationSelection.setEnabled(false);
    }
    if(implementations.length == 1){
      implementation = implementations[0];
      cbImplementationSelection.setEnabled(false);
    }
    
    if(implementations.length > 0){
      for(int i=0; i<implementations.length; ++i){
        cbImplementationSelection.addItem(implementations[i]);
      }
      cbImplementationSelection.setEnabled(true);
    }
    
    // Set the selection
    if(implementations.length > 1 && cbImplementationSelection.getClass().isInstance(new JComboBox())){
      for(int i=0; i<implementations.length; ++i){
        Debug.debug("Trying to set implementation, "+implementations[i]+" : "+
            jtImplementation, 3);
        if(implementations[i].equals(jtImplementation)){
          implementation = jtImplementation;
          ((JComboBox) cbImplementationSelection).setSelectedIndex(i);
          break;
        }
      }
    }
    implementationInit = false;
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
    cb.gridx = 0;
    cb.gridy = 1;         
    pButtons.add(bSave, cb);
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

    if(!reuseTextFields || tcCstAttributes == null || tcCstAttributes.length != cstAttributesNames.length)
      tcCstAttributes = new JComponent[cstAttributesNames.length];
    
    for(int i =0; i<cstAttributesNames.length; ++i){
      
      if(cstAttributesNames[i].equals("jobTransFK")){
        cl.gridx=0;
        cl.gridy=i;
        pAttributes.add(new JLabel("jobTransFK" + " : "), cl);
        if(!reuseTextFields || tcCstAttributes[i] == null)
          tcCstAttributes[i] = createTextComponent(TEXTFIELDWIDTH);
        
        ((JTextComponent) tcCstAttributes[i]).setEnabled(false);
      }
      else if(cstAttributesNames[i].equals("jobXML")){
        Debug.debug("Setting jobXML panel", 3);
        cl.gridx=0;
        cl.gridy=i;
        //pAttributes.add(new JLabel("jobXML" + " : "),cl);
        //JPanel jobXmlContainer = new JPanel(new GridBagLayout());
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
      }
      else if(cstAttributesNames[i].equals("ipConnectivity")){
        cl.gridx=0;
        cl.gridy=i;
        pAttributes.add(new JLabel("ipConnectivity" + " : "), cl);
        tcCstAttributes[i] = new JComboBox();
        ((JComboBox) tcCstAttributes[i]).addItem("yes");
        ((JComboBox) tcCstAttributes[i]).addItem("no");
        setJText(tcCstAttributes[i], cstAttr[i]);
       }
      else if(cstAttributesNames[i].equals("jobDefinitionID")){
        cl.gridx=0;
        cl.gridy=i;
        pAttributes.add(new JLabel(cstAttributesNames[i] + " : "), cl);
        if(!reuseTextFields || tcCstAttributes[i] == null)
          tcCstAttributes[i] = createTextComponent(TEXTFIELDWIDTH);
        
        setJText(tcCstAttributes[i], cstAttr[i]);
        tcCstAttributes[i].setEnabled(false);
      }
      else{
        cl.gridx=0;
        cl.gridy=i;
        pAttributes.add(new JLabel(cstAttributesNames[i] + " : "), cl);
        if(!reuseTextFields || tcCstAttributes[i] == null)
          tcCstAttributes[i] = createTextComponent(TEXTFIELDWIDTH);
        
        Debug.debug("Setting cstAttr["+i+"]: "+cstAttr[i], 3);
        setJText(tcCstAttributes[i], cstAttr[i]);
      }      
      cl.gridx=1;
      cl.gridy=i;
      if(!cstAttributesNames[i].equals("jobXML")){
        pAttributes.add(tcCstAttributes[i], cl);
      }
    }
  }


  private void setValuesInAttributePanel(){
    
    Debug.debug("Setting values...", 3);

    for(int i =0; i<cstAttributesNames.length; ++i){     
      if(cstAttributesNames[i].equals("jobTransFK")){
        ((JTextComponent) tcCstAttributes[i]).setEnabled(false);
        setJText(tcCstAttributes[i], jobTransFK);
      }
      else if(cstAttributesNames[i].equals("jobXML") /*&& editing*/){
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
      }
      else{
      }
    }
  }

  private void createJobXmlPanel(){
    String signature = "";
    int jobXmlIndex;
    
    if(!jobTransFK.equals("-1") && table!=null &&
        transformations.values.length!=0){
      if(table==null || table.getSelectedRow()<0 || !editing){
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
        DBResult allJobDefinitions = taskMgr.getDBPluginMgr().getAllJobDefinitions(taskMgr.taskID);
        for(int i=0; i<table.getColumnNames().length; ++i){
          if(table.getColumnNames()[i].equals("jobXML")){
            signature = table.getValueAt(table.getSelectedRow(),i).toString();
            Debug.debug("got signature from jobDefinition: "+signature, 3);
          }
        }
      }
    }
    if(signature==null || signature.equals("")){
      Debug.debug("Ended here because "+jobTransFK+ " : "+table +" : "+
          transformations, 3);
      jobTransFK = "";
      // When jobTransFK is not set, the signature is obtained from the taskTransFK.
      // This should not happen...
      DBRecord taskTransRecord = taskMgr.getDBPluginMgr().getTaskTransRecord(taskMgr.taskID);
      if (taskTransRecord == null ) {
        Debug.debug2("createJobXmlPanel: taskTransRecord is null!");
        signature = "";
      }
      else{
        signature = taskTransRecord.getValue("formalPars");
      }
      if (signature == null){
          Debug.debug("got signature: null", 3);
          signature = "";
      }
      else
        Debug.debug("got signature from taskTrans: "+signature, 3);
    }
    
    if(signature!=null && !signature.equals("")){
      xmlParsNode = simplexmlnode.parseString(signature, 0);
      xmlParsNode.fillJPanel();
      Debug.debug("Setting new jobXmlPanel", 3);
      jobXmlPanel = xmlParsNode.jpanel;
      jobXmlPanel.setName("jobPars");
    }
    else{
      jobXmlPanel = new JPanel();
    }
    simplexmlnode.jpanelLogAdded = false;
  }

   /**
   * Action Events
   */

  private void cbHomePackageSelection_actionPerformed(){
    if(!loaded) return;
    
    if(cbHomePackageSelection == null ||
        cbHomePackageSelection.getSelectedItem() == null){
      if(cbHomePackageSelection.getItemCount()>0){
        homePackage = cbHomePackageSelection.getItemAt(0).toString();
      }
      else{
        Debug.debug("No homePackage selected...", 3);
        return;
      }
    }
    
    homePackage = cbHomePackageSelection.getSelectedItem().toString();
    /*
     Using DBPluginMgr object which was passed when function initGUI(..) was called
     */
    Debug.debug("Initializing implemenation panel for homePackage "+homePackage, 3);
    initImplementationPanel();
    pAttributes.updateUI();
  }

  private void cbImplementationSelection_actionPerformed(){
    // TODO: these two variables are there to prevent
    // this method from being called when the main panel
    // is initialized or the implementationPanel is initialized.
    // There must be another way!
   if(!loaded && !implementationInit) return;
   
   if(cbImplementationSelection == null ||
        cbImplementationSelection.getSelectedItem() == null){
      if(cbImplementationSelection.getItemCount()>0){
        implementation = cbImplementationSelection.getItemAt(0).toString();
      }
      else{
        Debug.debug("No implementation selected...", 3);
        return;
      }
    }
    implementation = cbImplementationSelection.getSelectedItem().toString();
    /*
     Using DBPluginMgr object which was passed when function initGUI(..) was called
     */
    // Set jobTransFK
    Debug.debug("homePackage, implementation: "+homePackage+","+implementation, 3);
    for(int i=0; i<transformations.values.length; ++i){
      Debug.debug("Checking jobTransFK "+transformations.getValue(i,"jobTransID"), 3);
      Debug.debug("  "+transformations.getValue(i,"homePackage"), 3);
      Debug.debug("  "+transformations.getValue(i,"implementation"), 3);
      if(transformations.getValue(i,"homePackage")!=null &&
         transformations.getValue(i,"implementation")!=null &&
         transformations.getValue(i,"homePackage").equals(homePackage) &&
         transformations.getValue(i,"implementation").equals(implementation)){
        Debug.debug("Setting jobTransFK to "+transformations.getValue(i,"jobTransID"), 3);
        jobTransFK = transformations.getValue(i,"jobTransID");
        break;
      }
     }
    //initAttributePanel();
    pAttributes.updateUI();
    //jobXmlContainer.remove(jobXmlPanel);
    //createJobXmlPanel();
    //add(jobXmlPanel);
    setValuesInAttributePanel();
    //updateUI();
  }


  /**
   * public methods
   */

  public void clear(){
    /**
     * Called by : JobDefinition.button_ActionPerformed()
     */

    Vector textFields = getTextFields();

    for(int i =0; i<textFields.size(); ++i)
      setJText((JComponent) textFields.get(i),"");
  }


  public void create(final boolean showResults, final boolean editing) {
    /**
     * Called when button Create is clicked in JobDefinition
     */

    Debug.debug("create",  1);
    
    simplexmlnode.jpanelLogAdded = false;

    for(int i=0; i< cstAttr.length; ++i){
      Debug.debug("setting " + cstAttributesNames[i],  3);
      cstAttr[i] = getJTextOrEmptyString(tcCstAttributes[i]);
      Debug.debug("to " + cstAttr[i],  3);
    }

    Debug.debug("creating new JobDefCreator",  3);
    
    new JobDefCreator(taskMgr,
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
    if(!taskMgr.getDBPluginMgr().saveDefVals(taskMgr.getTaskIdentifier(), values)){
      Debug.debug("ERROR: Could not save values: "+values, 1);
      return false;
    }
    return true;
  }

  private void load(){

    String [] defValues = taskMgr.getDBPluginMgr().getDefVals(taskMgr.getTaskIdentifier());

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
  
  private static String getJTextOrEmptyString(JComponent comp){
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
        if(((JPanel) comp.getComponent(i)).getName() != null &&
            ((JPanel) comp.getComponent(i)).getName().equals("jobPars")){
          Debug.debug("Filling XML", 3);
          xmlParsNode.fillXML();
          text += xmlParsNode.xmlstring;
        }
      }
      text += xmlParsNode.filesXmlstring;
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
    else if(comp.getClass().isInstance(new JComboBox())){
      ((JComboBox) comp).setSelectedItem(text);
    }
    return text;
  }
}
