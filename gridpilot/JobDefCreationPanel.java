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
  private static JPanel stdoutErrXmlPanel;
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
  
  private DBResult transformations;
  private int TEXTFIELDWIDTH = 8;
  
  
  /**
   * Constructor for creating new records
   */

  public JobDefCreationPanel(TaskMgr _taskMgr){
    
    editing = false;
    
    taskMgr=_taskMgr;

    cstAttributesNames = taskMgr.getVectorTableModel().columnNames;
    cstAttr = new String[cstAttributesNames.length];
    
    transformations = taskMgr.getDBPluginMgr().getAllJobTransRecords(taskMgr.getTaskIdentifier());
    
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
  }

  /**
   * Constructor for editing record
   */

  public JobDefCreationPanel(TaskMgr _taskMgr, DBVectorTable _table){
    
    editing = true;
    
    taskMgr=_taskMgr;
    table = _table;

    cstAttributesNames = taskMgr.getVectorTableModel().columnNames;
    cstAttr = new String[cstAttributesNames.length];
    
    transformations = taskMgr.getDBPluginMgr().getAllJobTransRecords(taskMgr.getTaskIdentifier());

    // Fill cstAttr from table
    for(int i=0; i < table.getColumnCount(); ++i){
      cstAttr[i] = table.getValueAt(table.getSelectedRow(),i).toString();
      if(cstAttributesNames[i].equals("jobTransFK")){
        jobTransFK = cstAttr[i];
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
    ct.anchor = GridBagConstraints.CENTER;
    ct.insets = new Insets(5,5,5,5);
    
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
    }

  /**
   * Creates a combobox homePackagePanel which with user can select transformation.
   **/
  private void initHomePackagePanel(){
    
    String jtHomePack = "-1";
    boolean ok = true;
    Vector vec = new Vector();
    
    if(transformations.values.length > 0){
      if(transformations.getValue(0,"jobTransID").equals(jobTransFK)){
        homePackage = transformations.getValue(0,"homePackage");
      }
      vec.add(transformations.getValue(0,"homePackage"));
     }
    // When editing, find homePackage of original jobTransFK
    if(transformations.values.length > 1){
      for(int i=1; i<transformations.values.length; ++i){
        if(transformations.getValue(i,"jobTransID").equals(jobTransFK)){
          jtHomePack = transformations.getValue(i,"homePackage");
        }
        for(int j=0; j<vec.size(); ++j){
          if(transformations.getValue(i,"homePackage").equals(
              vec.get(j))){
            ok = false;
            break;
          }
        }
        if(ok){
          vec.add(transformations.getValue(i,"homePackage"));
        }
      }
    }
          
    homePackages = new String [vec.size()];
    for(int i = 0; i < vec.size(); ++i){
      homePackages[i] = (vec.toArray())[i].toString();
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

    String imp = "";
    Vector vec = new Vector();
    boolean ok = true;
    if(transformations.values.length > 0){
      if(transformations.getValue(0,"jobTransID").equals(jobTransFK)){
        imp = transformations.getValue(0,"implementation");
      }
      if(transformations.getValue(0,"homePackage").equals(homePackage)){
        vec.add(transformations.getValue(0,"implementation"));
      }
    }
    if(transformations.values.length > 1){
      for(int i=1; i<transformations.values.length; ++i){
        if(transformations.getValue(i,"jobTransID").equals(jobTransFK)){
          imp = transformations.getValue(i,"implementation");
        }
        for(int j=0; j<vec.size(); ++j){
          if(transformations.getValue(i,"implementation").equals(
              vec.get(j))){
            ok = false;
            break;
          }
        }
        if(ok && transformations.getValue(i,"homePackage").equals(homePackage)){
          vec.add(transformations.getValue(i,"implementation"));
        }
      }
    }
    
    implementations = new String [vec.size()];
    for(int i = 0; i < vec.size(); ++i){
      implementations[i] = (vec.toArray())[i].toString();
    }    

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
        if(implementations[i].equals(imp)){
          implementation = imp;
          ((JComboBox) cbImplementationSelection).setSelectedIndex(i);
        }
      }
    }
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
        cl.gridx=0;
        cl.gridy=i;
        pAttributes.add(new JLabel("jobXML" + " : "),cl);
        JPanel jobXmlContainer = new JPanel(new GridBagLayout());
        GridBagConstraints cv = new GridBagConstraints();
        cv.fill = GridBagConstraints.VERTICAL;
        cv.weightx = 0.5;
        cv.gridx = 0;
        cv.gridy = 0;         
        cv.ipady = 10;
        cv.weighty = 0.5;
        cv.anchor = GridBagConstraints.NORTHWEST;
        createJobXmlPanel();
        jobXmlContainer.add(jobXmlPanel,cv);
        cv.gridx = 0;
        cv.gridy = 1;
        //createStdOutErrXmlPanel();
        //jobXmlContainer.add(stdoutErrXmlPanel,cv);
        tcCstAttributes[i] = jobXmlContainer;
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
      else{
        cl.gridx=0;
        cl.gridy=i;
        pAttributes.add(new JLabel(cstAttributesNames[i] + " : "), cl);
        if(!reuseTextFields || tcCstAttributes[i] == null)
          tcCstAttributes[i] = createTextComponent(TEXTFIELDWIDTH);
        
        setJText(tcCstAttributes[i], cstAttr[i]);
      }      
      cl.gridx=1;
      cl.gridy=i;
      pAttributes.add(tcCstAttributes[i], cl);
    }
  }


  private void setValuesInAttributePanel(){

    for(int i =0; i<cstAttributesNames.length; ++i){     
      if(cstAttributesNames[i].equals("jobTransFK")){
        ((JTextComponent) tcCstAttributes[i]).setEnabled(false);
        setJText(tcCstAttributes[i], jobTransFK);
      }
      else if(cstAttributesNames[i].equals("jobXML")){
          tcCstAttributes[i].removeAll();
          GridBagConstraints cv = new GridBagConstraints();
          cv.ipady = 10;
          cv.weighty = 0.5;
          cv.anchor = GridBagConstraints.NORTHWEST;
          cv.fill = GridBagConstraints.VERTICAL;
          cv.weightx = 0.5;
          cv.gridx = 0;
          cv.gridy = 0;
          tcCstAttributes[i].add(jobXmlPanel,cv);
          //cv.gridx = 0;
          //cv.gridy = 1;         
          //tcCstAttributes[i].add(stdoutErrXmlPanel,cv);
      }
      else{
      }
    }
  }

  private void createJobXmlPanel(){
    String signature = "";
    int jobXmlIndex;
    
    if(jobTransFK.equals("-1")){
      // When creating new records the signature is obtained from the taskTransFK 
      DBRecord taskTransRecord = taskMgr.getDBPluginMgr().getTaskTransRecord(taskMgr.taskID);
      signature = taskTransRecord.getValue("formalPars");
      Debug.debug("got signature: "+signature,3);      
    }
    else{
      DBResult allJobDefinitions = taskMgr.getDBPluginMgr().getAllJobDefinitions(taskMgr.taskID);
      for(int i=0; i<table.getColumnNames().length; ++i){
        if(table.getColumnNames()[i].equals("jobXML")){
          jobXmlIndex = i;
        }
      }
      for(int i =0; i<allJobDefinitions.values.length; ++i){
        if(allJobDefinitions.getValue(i,"jobTransFK").equals(jobTransFK)){
          signature = allJobDefinitions.getValue(i, "jobXML");
        }
      }
      Debug.debug("got signature: "+signature,3);
      /*for(int j=0; j<allJobDefinitions.values.length; ++j){
        if(allJobDefinitions.getValue(j, "jobTransID").equals(jobTransFK)){
          signature = allJobDefinitions.getValue(j, "jobXML");
          Debug.debug("got signature: "+signature,3);
          break;
        }
      }*/
    }
    
    if(signature != ""){
      xmlParsNode = simplexmlnode.parseString(signature, 0);
      xmlParsNode.fillJPanel();
      jobXmlPanel = xmlParsNode.jpanel;
      jobXmlPanel.setName("jobPars");
    }
  }

   private void createStdOutErrXmlPanel(){

     stdoutErrXmlPanel = new JPanel(new GridBagLayout());
     stdoutErrXmlPanel.setName("stdOutErrXML");
     JTextField tf = new JTextField();
     JLabel jLabel = new JLabel();
     GridBagConstraints ch = new GridBagConstraints();
     ch.fill = GridBagConstraints.HORIZONTAL;
     ch.weightx = 0.0;
     ch.ipadx = 0;
     
     ch.gridx = 0;
     ch.gridy = 0;
     jLabel = new JLabel("  stdout : ");
     jLabel.setName("stdout");
     stdoutErrXmlPanel.add(jLabel,ch);
     
     ch.gridx = 0;
     ch.gridy = 1;
     stdoutErrXmlPanel.add(new JLabel("  LFN : "),ch);
     ch.gridx = 1;
     ch.gridy = 1;
     tf = new JTextField("",TEXTFIELDWIDTH);
     tf.setName("LFN");
     stdoutErrXmlPanel.add(tf,ch);     
     ch.gridx = 2;
     ch.gridy = 1;
     stdoutErrXmlPanel.add(new JLabel("  collection : "),ch);
     ch.gridx = 3;
     ch.gridy = 1; 
     tf = new JTextField("",TEXTFIELDWIDTH);
     tf.setName("logCol");
     stdoutErrXmlPanel.add(tf,ch);
     
     ch.gridx = 0;
     ch.gridy = 2;
     stdoutErrXmlPanel.add(new JLabel("  dataset : "),ch);
     ch.gridx = 1;
     ch.gridy = 2;         
     tf = new JTextField("",TEXTFIELDWIDTH);
     tf.setName("dataset");
     stdoutErrXmlPanel.add(tf,ch);
     ch.gridx = 2;
     ch.gridy = 2;         
     stdoutErrXmlPanel.add(new JLabel("  SE hint : "),ch);
     ch.gridx = 3;
     ch.gridy = 2;         
     tf = new JTextField("",TEXTFIELDWIDTH);
     tf.setName("SEList");
     stdoutErrXmlPanel.add(tf,ch);

     ch.gridx = 0;
     ch.gridy = 3; 
     jLabel = new JLabel("  stderr : ");
     jLabel.setName("stderr");
     stdoutErrXmlPanel.add(jLabel,ch);
     
     ch.gridx = 0;
     ch.gridy = 4;         
     stdoutErrXmlPanel.add(new JLabel("  LFN : "),ch);
     ch.gridx = 1;
     ch.gridy = 4;         
     tf = new JTextField("",TEXTFIELDWIDTH);
     tf.setName("LFN");
     stdoutErrXmlPanel.add(tf,ch);
     ch.gridx = 2;
     ch.gridy = 4;         
     stdoutErrXmlPanel.add(new JLabel("  collection : "),ch);
     ch.gridx = 3;
     ch.gridy = 4;         
     tf = new JTextField("",TEXTFIELDWIDTH);
     tf.setName("logCol");
     stdoutErrXmlPanel.add(tf,ch);
     
     ch.gridx = 0;
     ch.gridy = 5;         
     stdoutErrXmlPanel.add(new JLabel("  dataset : "),ch);
     ch.gridx = 1;
     ch.gridy = 5;         
     tf = new JTextField("",TEXTFIELDWIDTH);
     tf.setName("dataset");
     stdoutErrXmlPanel.add(tf,ch);
     ch.gridx = 2;
     ch.gridy = 5;         
     stdoutErrXmlPanel.add(new JLabel("  SE hint : "),ch);
     ch.gridx = 3;
     ch.gridy = 5;         
     tf = new JTextField("",TEXTFIELDWIDTH);
     tf.setName("SEList");
     stdoutErrXmlPanel.add(tf,ch);
   }
  
  
  /**
   * Action Events
   */

  private void cbHomePackageSelection_actionPerformed(){
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
    initImplementationPanel();
    pAttributes.updateUI();
  }

  private void cbImplementationSelection_actionPerformed(){
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
      if(transformations.getValue(i,"homePackage").equals(homePackage) &&
          transformations.getValue(i,"implementation").equals(implementation)){
        Debug.debug("Setting jobTransFK to "+transformations.getValue(i,"jobTransID"), 3);
        jobTransFK = transformations.getValue(i,"jobTransID");
        break;
      }
     }
    setValuesInAttributePanel();
    pAttributes.updateUI();
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

    for(int i=0; i< cstAttr.length; ++i){
      Debug.debug("setting " + cstAttr[i],  3);
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
    
    JTextField tf = new JTextField(TEXTFIELDWIDTH);
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
      text =  ((JComboBox) comp).getSelectedItem().toString();
    }
    // the jobXML panel
    else if(comp.getClass().isInstance(new JPanel())){
      for(int i=0; i<comp.getComponentCount(); ++i){
        if(((JPanel) comp.getComponent(i)).getName() != null &&
            ((JPanel) comp.getComponent(i)).getName().equals("jobPars")){
          xmlParsNode.fillXML();
          text += xmlParsNode.xmlstring;
        }
        else if(false && ((JPanel) comp.getComponent(i)).getName() != null &&
            ((JPanel) comp.getComponent(i)).getName().equals("stdOutErrXML")){
          text += "<"+"jobLogs"+">";
          for(int j=0; j<((JPanel) comp.getComponent(i)).getComponentCount(); ++j){
            com = ((JComponent) ((JPanel) comp.getComponent(i)).getComponent(j));
            if(com.getClass().isInstance(new JLabel()) && ((JLabel) com).getName()!=null){
              if(!name.equals("")){
                text += "</"+"fileInfo"+">";
              }
              text += "  <"+"fileInfo"+">";
              name = ((JLabel) com).getName();
              text += "    <stream>"+name+"</stream>\n";
            }
            if(com.getClass().isInstance(new JTextField())){
               label = ((JTextComponent) com).getName();
               if(label.equals("dataset")){
                 text += "    <dataset>\n      <name>"+((JTextComponent) com).getText()+
                 "      </name>\n    </dataset>\n";
               }
               else if(label.equals("SEList")){
                 ses = GridPilot.split(((JTextComponent) com).getText());
                 text += "    <SEList>\n";
                 for(int k=0; k<ses.length; ++k){
                   text += "      <SE>"+ses[k]+"</SE>\n";
                 }
                 text += "    </SEList>";
               }
               else{
                 text += "    <"+label+">\n";
                 text += ((JTextComponent) com).getText();
                 text += "\n    </"+label+">\n";
               }
            }
          }
          text += "  </"+"fileInfo"+">\n";
          text += "</"+"jobLogs"+">";
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
