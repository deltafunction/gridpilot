package gridpilot;

import gridfactory.common.DBRecord;
import gridfactory.common.DBResult;
import gridfactory.common.Debug;

import javax.swing.*;
import javax.swing.border.*;
import javax.swing.text.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * This panel creates records in the DB table.
 * It's shown inside the CreateEditDialog.
 */
public class ExecutableCreationPanel extends CreateEditPanel{

  private DBPluginMgr dbPluginMgr;
  private JPanel pAttributes = new JPanel();
  private JScrollPane spAttributes = new JScrollPane();
  private boolean editing = false;
  private MyJTable table;
  private String executableID = "-1";
  private String [] cstAttributesNames;
  private String [] cstAttr = null;
  private String executableIdentifier;
  private boolean reuseTextFields = true;
  private DBPanel panel = null;
  private JPanel pRuntimeEnvironment = new JPanel();
  private String runtimeEnvironmentName = null;
  private JComboBox cbRuntimeEnvironmentSelection;
  private GridBagConstraints ct = new GridBagConstraints();
  private DBRecord executable = null;
  private DBResult runtimeEnvironments = null;
  private String [] executableFields = null;
  private JButton bEditRuntimeEnvironment;

  private static final long serialVersionUID = 1L;
  private static int TEXTFIELDWIDTH = 32;
  private int comboBoxIndex;
  private JComboBox argsComboBox;

  private ArrayList<JComponent> detailFields = new ArrayList<JComponent>();
  private HashMap<String, JComponent> labels = new HashMap<String, JComponent>();
  private HashMap<String, JComponent> textFields = new HashMap<String, JComponent>();
  private String[] detailFieldNames;
  private HashMap<String, String> descriptions;

  public JComponent [] tcCstAttributes;
  private String[] executableRuntimeReference;
  private String[] datasetExecutableReference;
  private String[] datasetExecutableVersionReference;

  /**
   * Constructor
   */
  public ExecutableCreationPanel(DBPluginMgr _dbPluginMgr,
      DBPanel _panel, boolean _editing, String _executableID){
    dbPluginMgr = _dbPluginMgr;
    editing = _editing;
    panel = _panel;
    table = panel==null?null:panel.getTable();
    executableID = _executableID;
    executableIdentifier =
      MyUtil.getIdentifierField(dbPluginMgr.getDBName(), "executable");
    executableFields = dbPluginMgr.getFieldNames("executable");
    cstAttributesNames = dbPluginMgr.getFieldNames("executable");    
    runtimeEnvironments = dbPluginMgr.getRuntimeEnvironments();
    Debug.debug("Got field names: "+MyUtil.arrayToString(cstAttributesNames),3);
    Debug.debug("Number of runtimeEnvironments found: "+runtimeEnvironments.values.length+
        "; "+MyUtil.arrayToString(runtimeEnvironments.fields),3);
    cstAttr = new String[cstAttributesNames.length];
    executableRuntimeReference = MyUtil.getExecutableRuntimeReference(dbPluginMgr.getDBName());
    datasetExecutableReference =
      MyUtil.getDatasetExecutableReference(dbPluginMgr.getDBName());
    datasetExecutableVersionReference =
      MyUtil.getDatasetExecutableVersionReference(dbPluginMgr.getDBName());
    // Find executable ID from table
    if((executableID==null || executableID.equals("")) && table!=null && table.getSelectedRow()>-1){
      for(int i=0; i<table.getColumnNames().length; ++i){
        Object fieldVal = table.getUnsortedValueAt(table.getSelectedRow(),i);
        Debug.debug("Column name: "+table.getColumnNames().length+":"+i+" "+table.getColumnName(i), 3);
        if(fieldVal!=null && table.getColumnName(i).equalsIgnoreCase(executableIdentifier)){
          executableID = fieldVal.toString();
          break;
        }
      }
    }
    if((executableID!=null && !executableID.equals("")) &&
        editing){
      Debug.debug("Editing...", 3);
      if(executableID==null || executableID.equals("-1") ||
          executableID.equals("")){
        Debug.debug("ERROR: could not find executableID.", 1);
      }
      // Fill cstAttr from db
      executable = dbPluginMgr.getExecutable(executableID);
      for(int i=0; i<cstAttributesNames.length; ++i){
        if(editing){
          Debug.debug("filling " + cstAttributesNames[i],  3);
          if(executable.getValue(cstAttributesNames[i])!=null){
            cstAttr[i] = executable.getValue(cstAttributesNames[i]).toString();
            if(cstAttributesNames[i].equalsIgnoreCase(executableRuntimeReference[1])){
              runtimeEnvironmentName = cstAttr[i];
            }
          }
          else{
            cstAttr[i] = "";
          }
          Debug.debug("to " + cstAttr[i],  3);
        }
      }
    }
  }

  private void initButtons(){
    bEditRuntimeEnvironment = MyUtil.mkButton("search.png", "Look up", "Look up runtime environment record");
  }

  /**
   * GUI initialization
   */
  public void initGUI(){
    
    Debug.debug("Initializing GUI", 3);
    
    initButtons();

    setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white,new Color(165, 163, 151)), 
        (executableID==null||executableID.equals("-1")?"new executable":"executable "+executableID)));
    
    //spAttributes.setPreferredSize(new Dimension(650, 280));
    //spAttributes.setMinimumSize(new Dimension(650, 500));
    
    setLayout(new GridBagLayout());
    removeAll();

    initRuntimeEnvironmentPanel(Integer.parseInt(executableID));

    //initAttributePanel();
    
    ct.fill = GridBagConstraints.VERTICAL;
    ct.insets = new Insets(2,2,2,2);
    
    ct.gridx = 0;
    ct.gridy = 0;   
    ct.gridwidth=1;
    ct.gridheight=1;
    
    ct.gridx = 0;
    ct.gridy = 1;
    ct.gridwidth=2;
    add(spAttributes,ct);

    Debug.debug("Initializing panel", 3);
    initExecutableCreationPanel();
    if(editing){
      Debug.debug("Editing...", 3);
      editExecutable(Integer.parseInt(executableID), runtimeEnvironmentName);
    }
    else{
      // Disable identifier field when creating
      Debug.debug("Disabling identifier field", 3);
      for(int i =0; i<cstAttributesNames.length; ++i){
        if(cstAttributesNames[i].equalsIgnoreCase(executableIdentifier)){
          MyUtil.setJEditable(tcCstAttributes[i], false);
        }
        else if(runtimeEnvironmentName!=null && !runtimeEnvironmentName.equals("") &&
            cstAttributesNames[i].equalsIgnoreCase(executableRuntimeReference[1])){
          MyUtil.setJText(tcCstAttributes[i], runtimeEnvironmentName);
        }
        if(isDetail(cstAttributesNames[i])){
          detailFields.add(tcCstAttributes[i]);
          if(labels.containsKey(cstAttributesNames[i])){
            detailFields.add(labels.get(cstAttributesNames[i]));
          }
        }
      }
    }
    showDetails(false);
    updateUI();
   }


  /**
   * Called initially.
   * Initializes text fields with attributes for executable.
   */
  private void initExecutableCreationPanel(){

    // Panel Attributes
    pAttributes.setLayout(new GridBagLayout());
    pAttributes.removeAll();

    spAttributes.getViewport().add(pAttributes, null);

    add(spAttributes,
        new GridBagConstraints(0, 3, 3, 1, 0.9, 0.9,
            GridBagConstraints.CENTER,
            GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));

    initAttributePanel();
    
  }
  
  private String[] getRuntimeEnvironmentNames(){
    String [] ret = new String[runtimeEnvironments.values.length];
    for(int i=0; i<runtimeEnvironments.values.length; ++i){
      ret[i] = runtimeEnvironments.getValue(i, executableRuntimeReference[0]).toString(); 
      Debug.debug("name is "+ret[i], 3);
    }
    // This is to ensure only unique elements
    // TODO: for some reason this doesn't seam to work
    Arrays.sort(ret);
    Vector<String> vec = new Vector<String>();
    if(runtimeEnvironments.values.length>0){
      vec.add(ret[0]);
    }
    if(runtimeEnvironments.values.length>1){
      for(int i=1; i<runtimeEnvironments.values.length; ++i){
        //Debug.debug("Comparing "+ret[i]+" <-> "+ret[i-1],3);
        if(!ret[i].equalsIgnoreCase(ret[i-1])){
          Debug.debug("Adding "+ret[i],3);
            vec.add(ret[i]);
        }
      }
    }
    String[] arr = new String[vec.size()];
    for(int i=0; i<vec.size(); ++i){
      arr[i]=vec.elementAt(i).toString();
    } 
    return arr;
  }

  // TODO: hash by first letter if number of entries exceeds ~20
  private void initRuntimeEnvironmentPanel(int datasetID){
    
    pRuntimeEnvironment.removeAll();
    pRuntimeEnvironment.setLayout(new FlowLayout());

    String [] runtimeEnvironmentNames = getRuntimeEnvironmentNames();
    Arrays.sort(runtimeEnvironmentNames);

    if(runtimeEnvironmentNames.length==0){
      pRuntimeEnvironment.add(new JLabel("No runtime environments found."));
      bEditRuntimeEnvironment.setEnabled(false);
    }
    else if(runtimeEnvironmentNames.length==1){
      runtimeEnvironmentName = runtimeEnvironmentNames[0];
      pRuntimeEnvironment.add(new JLabel("Runtime environment : " + runtimeEnvironmentName));
    }
    else{
      cbRuntimeEnvironmentSelection = new JComboBox();
      cbRuntimeEnvironmentSelection.addItem("");
      for(int i=0; i<runtimeEnvironmentNames.length; ++i){
        cbRuntimeEnvironmentSelection.addItem(runtimeEnvironmentNames[i]);
      }
      pRuntimeEnvironment.add(new JLabel("Runtime environment: "), null);
      pRuntimeEnvironment.add(cbRuntimeEnvironmentSelection, null);

      cbRuntimeEnvironmentSelection.addActionListener(
        new java.awt.event.ActionListener(){
          public void actionPerformed(java.awt.event.ActionEvent e){
            cbRuntimeSelection_actionPerformed();
          }
        }
      ); 
    }
    ct.gridx = 0;
    ct.gridy = 0;
    ct.gridwidth=1;
    ct.gridheight=1;
    add(pRuntimeEnvironment, ct);
    
    bEditRuntimeEnvironment.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        try{
          lookupRuntimeEnvironments();
        } 
        catch(Exception e1){
          e1.printStackTrace();
        }
      }
    }
    );
    ct.gridx = 1;
    ct.gridy = 0;
    ct.gridwidth=1;
    ct.gridheight=1;
    if(GridPilot.ADVANCED_MODE){
      add(bEditRuntimeEnvironment, ct);
    }

    updateUI();
  }

  /**
   * Open new pane with corresponding runtime environments.
   * @throws InvocationTargetException 
   * @throws InterruptedException 
   */
  private void lookupRuntimeEnvironments() throws InterruptedException, InvocationTargetException{
    if(runtimeEnvironmentName==null || runtimeEnvironmentName.equals("")){
      return;
    }
    setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
    GridPilot.getClassMgr().getGlobalFrame().requestFocusInWindow();
    GridPilot.getClassMgr().getGlobalFrame().setVisible(true);
    Thread t = new Thread(){
      public void run(){
        try{
          // Create new panel with jobDefinitions.         
          DBPanel dbPanel = new DBPanel();
          dbPanel.initDB(dbPluginMgr.getDBName(), "runtimeEnvironment");
          dbPanel.initGUI();
          String nameField =
            MyUtil.getNameField(dbPluginMgr.getDBName(), "runtimeEnvironment");
          dbPanel.setConstraint(nameField, runtimeEnvironmentName, 0);
          dbPanel.searchRequest(true, false);           
          GridPilot.getClassMgr().getGlobalFrame().addPanel(dbPanel);
        }
        catch(Exception e){
          Debug.debug("Couldn't create panel for dataset " + "\n" +
                             "\tException\t : " + e.getMessage(), 2);
          e.printStackTrace();
        }
        finally{
          try{
            setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
          }
          catch(Exception ee){
          }
        }
      }
    };
    if(SwingUtilities.isEventDispatchThread()){
      t.run();
    }
    else{
      SwingUtilities.invokeAndWait(t);
    }
  }
  
  private void cbRuntimeSelection_actionPerformed(){
    if(cbRuntimeEnvironmentSelection.getSelectedItem()==null){
      return;
    }
    else{
      runtimeEnvironmentName = cbRuntimeEnvironmentSelection.getSelectedItem().toString();
    }
    editExecutable(Integer.parseInt(executableID), runtimeEnvironmentName);
  }
  
  private void initVars() {
    // identifier, name, version, runtimeEnvironmentName, arguments, outputFiles, executableFile,
    // comment, created, lastModified, inputFiles
    detailFieldNames = new String [] {executableIdentifier, /*datasetExecutableReference[0], datasetExecutableVersionReference[0],*/
        executableRuntimeReference[1], /*"arguments", "outputFiles", "executableFile", */"comment", "created", "lastModified"/*,
        "inputFiles"*/};
    descriptions = new HashMap<String, String>();
    descriptions.put(executableIdentifier, "Unique identifier of this executable");
    descriptions.put(datasetExecutableReference[0], "Name of this executable");
    descriptions.put(datasetExecutableVersionReference[0], "Version of this executable");
    descriptions.put(executableRuntimeReference[1], "Runtime environment used by this executable");
    descriptions.put("arguments", "Name(s) of argument(s) that must be given to the executable file");
    descriptions.put("outputFiles", "Names of output files produced by this executable");
    descriptions.put("executableFile", "Name of the file (script or binary) that will actually be run");
    descriptions.put("comment", "Optional: comment describing the executable");
    descriptions.put("created", "Creation date of this record");
    descriptions.put("lastModified", "Last modification date of this record");
    descriptions.put("inputFiles", "Optional: Input file(s) of the executable");
    String key;
    HashMap<String, String> locaseDescriptions = new HashMap<String, String>();
    for(Iterator<String>it=descriptions.keySet().iterator(); it.hasNext();){
      key = it.next();
      locaseDescriptions.put(key.toLowerCase(), descriptions.get(key));
    }
    for(Iterator<String>it=locaseDescriptions.keySet().iterator(); it.hasNext();){
      key = it.next();
      descriptions.put(key, locaseDescriptions.get(key));
    }
  }

  private void initAttributePanel(){
    
    initVars();
    
    if(!reuseTextFields || tcCstAttributes==null ||
        tcCstAttributes.length!=cstAttributesNames.length){
      tcCstAttributes = new JComponent[cstAttributesNames.length];
    }
    int row = 0;
    for(int i=0; i<cstAttributesNames.length; ++i, ++row){
      
      if(cstAttributesNames[i].equalsIgnoreCase("initLines") ||
          cstAttributesNames[i].equalsIgnoreCase("comment") ||
          GridPilot.ADVANCED_MODE && cstAttributesNames[i].equalsIgnoreCase(executableRuntimeReference[1])){
        if(!reuseTextFields || tcCstAttributes[i]==null){
          tcCstAttributes[i] = MyUtil.createTextArea(TEXTFIELDWIDTH);
        }
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("arguments")){
        if(!reuseTextFields || tcCstAttributes[i]==null){
          comboBoxIndex = i;
          argsComboBox = new JComboBox();
          initArgsComboBox();
          tcCstAttributes[i] = argsComboBox;
        }
        tcCstAttributes[i].setToolTipText("Arguments known by GridPilot:\n"+
           MyUtil.arrayToString(JobCreator.AUTO_FILL_ARGS, " "));
      }
      else{
        if(!reuseTextFields || tcCstAttributes[i]==null || !tcCstAttributes[i].isEnabled()){
          tcCstAttributes[i] = new JTextField("", TEXTFIELDWIDTH);
        }
      }
      
      if(cstAttributesNames[i].equalsIgnoreCase("executableFile")){
        JPanel executableFileCheckPanel = MyUtil.createCheckPanel1(
            (Window) SwingUtilities.getWindowAncestor(this),
            cstAttributesNames[i], (JTextComponent) tcCstAttributes[i], true, true, false, false);
        pAttributes.add(executableFileCheckPanel,
            new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(5, 22, 5, 5), 0, 0));
        labels.put(cstAttributesNames[i], executableFileCheckPanel);
        textFields.put(cstAttributesNames[i], tcCstAttributes[i]);
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("inputFiles")){
        JPanel inputFilesCheckPanel = MyUtil.createCheckPanel1(
            (Window) SwingUtilities.getWindowAncestor(this),
            cstAttributesNames[i], (JTextComponent) tcCstAttributes[i], false, true, false, false, true);
        pAttributes.add(inputFilesCheckPanel,
            new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(5, 22, 5, 5), 0, 0));
        labels.put(cstAttributesNames[i], inputFilesCheckPanel);
        textFields.put(cstAttributesNames[i], tcCstAttributes[i]);
      }
      else{
        JLabel jLabel = new JLabel(cstAttributesNames[i]);
        pAttributes.add(jLabel,
            new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
            GridBagConstraints.CENTER, GridBagConstraints.BOTH,
            new Insets(5, 25, 5, 5), 0, 0));
        labels.put(cstAttributesNames[i], jLabel);
        textFields.put(cstAttributesNames[i], tcCstAttributes[i]);
      }
      
      if(cstAttributesNames[i].equalsIgnoreCase(executableRuntimeReference[1])){
        Debug.debug("Setting selection to "+runtimeEnvironmentName, 3);
        if(cbRuntimeEnvironmentSelection!=null &&
            runtimeEnvironmentName!=null && !runtimeEnvironmentName.equals("")){
           cbRuntimeEnvironmentSelection.setSelectedItem(runtimeEnvironmentName);
          cbRuntimeEnvironmentSelection.updateUI();
        }
        // Since we now allow multiple runtimeEnvironment dependencies,
        // allow manual editing.
        // TODO: improve the GUI for selecting runtimeEnvironments - the list is too long
        // and multiple selections should be allowed.
        //MyUtil.setJEditable(tcCstAttributes[i], false);
      }
      
    }
    
    for(int i=0; i<cstAttributesNames.length; ++i){
      
      if(!GridPilot.ADVANCED_MODE && cstAttributesNames[i].equalsIgnoreCase(executableRuntimeReference[1])){
        MyUtil.setJEditable(tcCstAttributes[i], false);
      }
      if(cstAttributesNames[i].equalsIgnoreCase("created") ||
         cstAttributesNames[i].equalsIgnoreCase("lastModified")){
        MyUtil.setJEditable(tcCstAttributes[i], false);
      }
      if(isDetail(cstAttributesNames[i])){
        detailFields.add(tcCstAttributes[i]);
        if(labels.containsKey(cstAttributesNames[i])){
          detailFields.add(labels.get(cstAttributesNames[i]));
        }
      }
      else if(labels.containsKey(cstAttributesNames[i]) && textFields.containsKey(cstAttributesNames[i])){
        // First add the non-detail fields
        pAttributes.add(labels.get(cstAttributesNames[i]),
            new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(5, 25, 5, 5), 0, 0));
        pAttributes.add(textFields.get(cstAttributesNames[i]),
            new GridBagConstraints(1, row, 3, 1, 1.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
                new Insets(5, 5, 5, 5), 0, 0));
        ++row;
      }
    }
    for(int i=0; i<cstAttributesNames.length; ++i){
      // Then the detail fields
      if(isDetail(cstAttributesNames[i]) && labels.containsKey(cstAttributesNames[i])){
        pAttributes.add(labels.get(cstAttributesNames[i]),
            new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(5, 25, 5, 5), 0, 0));
        pAttributes.add(textFields.get(cstAttributesNames[i]),
            new GridBagConstraints(1, row, 3, 1, 1.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
                new Insets(5, 5, 5, 5), 0, 0));
        ++row;
      }
      addToolTipText(cstAttributesNames[i]);
    }
  }

  private void initArgsComboBox() {
    argsComboBox.removeAllItems();
    argsComboBox.addItem("");
    for(int ii=0; ii<JobCreator.AUTO_FILL_ARGS.length; ++ii){
      argsComboBox.addItem(JobCreator.AUTO_FILL_ARGS[ii]);
    }
    argsComboBox.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        if(argsComboBox.getSelectedIndex()<0){
          Debug.debug("Setting text to "+argsComboBox.getSelectedItem(), 3);
          argsComboBox.removeItemAt(0);
          argsComboBox.insertItemAt(argsComboBox.getSelectedItem(), 0);
          return;
        }
        else if(argsComboBox.getSelectedIndex()==0){
          Debug.debug("Leaving text at "+argsComboBox.getItemAt(0), 3);
          return;
        }
        String orig = (String) argsComboBox.getItemAt(0);
        String newItem = (String) argsComboBox.getSelectedItem();
        Debug.debug("Appending item " + newItem+" to "+orig, 3);
        argsComboBox.removeItemAt(0);
        argsComboBox.insertItemAt(orig+(orig.equals("")?"":" ")+newItem, 0);
        argsComboBox.setSelectedIndex(0);
      }
    });
    argsComboBox.setEditable(true);
    argsComboBox.updateUI();
  }

  /**
   *  Edit a executable
   */
  public void editExecutable(int executableID, String runtimeEnvironmentName){
    for(int i =0; i<tcCstAttributes.length; ++i){
      for(int j=0; j<executableFields.length;++j){
        if(executableFields[j].toString().equalsIgnoreCase(
            cstAttributesNames[i].toString()) &&
            !executableFields[j].toString().equals("")){
          if(tcCstAttributes[i]==null || !tcCstAttributes[i].isEnabled() &&
              MyUtil.getJTextOrEmptyString(tcCstAttributes[i]).length()==0){            
            if(cstAttributesNames[i].equalsIgnoreCase("initLines") ||
                cstAttributesNames[i].equalsIgnoreCase("comment")){
              tcCstAttributes[i] = MyUtil.createTextArea(TEXTFIELDWIDTH);
            }
            else{
              tcCstAttributes[i] = new JTextField("", TEXTFIELDWIDTH);
            }
            pAttributes.add(tcCstAttributes[i],
                new GridBagConstraints(
                    1,i/*row*/, 3, 1, 1.0, 0.0,
                    GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL,
                    new Insets(5, 5, 5, 5), 0, 0));
          }
          if(editing && !cstAttributesNames[i].equalsIgnoreCase(executableRuntimeReference[1])){
            try{
              MyUtil.setJText(tcCstAttributes[i], executable.values[j].toString().trim());
                Debug.debug(i+": "+cstAttributesNames[i].toString()+"="+
                    executableFields[j]+". Setting to "+MyUtil.getJTextOrEmptyString(tcCstAttributes[i]),3);
            }
            catch(java.lang.Exception e){
              Debug.debug("Attribute not found, "+e.getMessage(),1);
            }
          }
          break;
        }
      }
      if(cstAttributesNames[i].equalsIgnoreCase(executableIdentifier)){
        if(!editing){
          try{
            Debug.debug("Clearing identifier",3);
            MyUtil.setJText(tcCstAttributes[i], "");
          }
          catch(java.lang.Exception e){
            Debug.debug("Attribute not found, "+e.getMessage(),1);
          }
        }
      }
      else if(cstAttributesNames[i].equalsIgnoreCase(datasetExecutableVersionReference[0])){
        if(!editing){
          try{
            Debug.debug("Setting default version",3);
            MyUtil.setJText(tcCstAttributes[i], "0.0");
          }
          catch(java.lang.Exception e){
            Debug.debug("Attribute not found, "+e.getMessage(),1);
          }
        }
      }
      else if(cbRuntimeEnvironmentSelection!=null &&
          runtimeEnvironmentName!=null && !runtimeEnvironmentName.trim().equals("") &&
          cstAttributesNames[i].equalsIgnoreCase(executableRuntimeReference[1])){
        if(GridPilot.ADVANCED_MODE){
          tcCstAttributes[i].updateUI();
          String existingRte = MyUtil.getJTextOrEmptyString(tcCstAttributes[i]);
          if(!existingRte.trim().matches(runtimeEnvironmentName) && !existingRte.matches(".*[\\s\\n\\r]+"+runtimeEnvironmentName+"[\\s\\n\\r]*.*")){
            MyUtil.setJText(tcCstAttributes[i],
                (existingRte!=null&&!existingRte.trim().equals("")?existingRte+" ":"")+runtimeEnvironmentName.trim());
          }
        }
        else{
          MyUtil.setJText(tcCstAttributes[i], runtimeEnvironmentName);
        }
      }
    }
  }

  public void clearPanel(){
    Vector<JComponent> textFields = getTextFields();
    for(int i =0; i<textFields.size(); ++i)
    if(!(cstAttributesNames[i].equalsIgnoreCase(executableIdentifier))){
      MyUtil.setJText(textFields.get(i), "");
    }
    tcCstAttributes[comboBoxIndex] = argsComboBox;
    pAttributes.updateUI();
    pAttributes.validate();
 }


  public void create(final boolean showResults, final boolean editing){

    final String [] cstAttr = new String[tcCstAttributes.length];

    for(int i=0; i<cstAttr.length; ++i){
      cstAttr[i] = MyUtil.getJTextOrEmptyString(tcCstAttributes[i]).trim();
    }

  Debug.debug("create executable",  1);

  ExecutableCreator tc = new ExecutableCreator(
        SwingUtilities.getWindowAncestor(this),
        dbPluginMgr,
        showResults,
        cstAttr,
        cstAttributesNames,
        editing);
    
    if(tc.anyCreated){
      panel.refresh();
    }
  }

  private Vector<JComponent> getTextFields(){
    Vector<JComponent> v = new Vector<JComponent>();
    for(int i=0; i<tcCstAttributes.length; ++i)
      v.add(tcCstAttributes[i]);
    return v;
  }
  
  public void showDetails(boolean show){
    for(Iterator<JComponent> it=detailFields.iterator(); it.hasNext(); ){
      it.next().setVisible(show);
    }
  }

  private boolean isDetail(String fieldName){
    return MyUtil.arrayContainsIgnoreCase(detailFieldNames, fieldName);
  }

  private void addToolTipText(String fieldName){
    if(!labels.containsKey(fieldName) || !descriptions.containsKey(fieldName.toLowerCase())){
      return;
    }
    Debug.debug("Setting tool tip text of "+fieldName+" --> "+descriptions.get(fieldName), 2);
    labels.get(fieldName).setToolTipText(descriptions.get(fieldName.toLowerCase()));
  }

}
