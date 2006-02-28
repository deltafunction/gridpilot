package gridpilot;

import gridpilot.Debug;
import gridpilot.Database.DBRecord;
import gridpilot.ConfirmBox;

import javax.swing.*;
import javax.swing.border.*;
import javax.swing.event.*;
import java.awt.*;

import javax.swing.text.*;

import java.util.*;

/**
 * This panel creates records in the DB table. It's shown inside the CreateEditDialog.
 *
 */


public class DatasetCreationPanel extends CreateEditPanel{

  private JPanel pAttributes = new JPanel();
  private JScrollPane spAttributes = new JScrollPane();
  private String datasetID = "-1";
  private DBPanel panel;
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
  private boolean loaded = false;
  private DBPluginMgr dbPluginMgr = null;
  
  private static int TEXTFIELDWIDTH = 32;
  private static int CFIELDWIDTH = 8;
  
  private String transformation = "";
  private String version = "";
  private int [] datasetIDs = new int [] {-1};

  private JPanel pTransformation = new JPanel();
  private JPanel pVersion = new JPanel();

  private JComboBox cbTargetDBSelection;
  private JComboBox cbTransformationSelection;
  private JComboBox cbTransVersionSelection;
  
  private boolean transformationChosen = false;
  private boolean versionChosen = false;
  
  private Map keyValues = new HashMap();

  private StatusBar statusBar;

  private String targetDB = null;

  /**
   * Constructor
   */
  public DatasetCreationPanel(DBPluginMgr _dbPluginMgr, DBPanel _panel, boolean _editing){
    
    editing = _editing;
    panel = _panel;
    dbPluginMgr = _dbPluginMgr;
    
    table = panel.getTable();
    
    statusBar = GridPilot.getClassMgr().getStatusBar();
    
    cstAttributesNames = dbPluginMgr.getFieldNames("dataset");
    cstAttr = new String[cstAttributesNames.length];
    
    // Find identifier index
    int identifierIndex = -1;
    String identifier = dbPluginMgr.getIdentifier(dbPluginMgr.getDBName(), "dataset");
    for(int i=0; i<table.getColumnNames().length; ++i){
      Debug.debug("Column name: "+table.getColumnNames().length+":"+i+" "+table.getColumnName(i), 3);
      if(table.getColumnName(i).equalsIgnoreCase(identifier)){
        identifierIndex = i;
        break;
      }
      if(identifierIndex ==-1){
        Debug.debug("ERROR: could not find index of dataset, "+identifier, 1);
      }
    }
    
    // Find dataset id from table
    if(table.getSelectedRow()>-1 && editing){
      datasetID = table.getUnsortedValueAt(table.getSelectedRow(), identifierIndex).toString();
      if(datasetID==null || datasetID.equals("-1") ||
          datasetID.equals("")){
        Debug.debug("ERROR: could not find datasetID in table!", 1);
      }
      Debug.debug("Editing dataset "+dbPluginMgr.getDataset(Integer.parseInt(datasetID)).getValue("name").toString()+". Rows: "+
          ".", 3);
      // Fill cstAttr from db
      DBRecord dataset = dbPluginMgr.getDataset(Integer.parseInt(datasetID));
    }

    // Dataset(s) selected and not editing - creating with input dataset(s)
    if(table.getSelectedRows().length>0 && !editing){
      // Find input datasets
      int [] selectedIDs = panel.getSelectedIdentifiers();
      datasetIDs = new int [selectedIDs.length];
      for(int i=0; i<selectedIDs.length; ++i){
        datasetIDs[i] = Integer.parseInt(
            table.getUnsortedValueAt(i, identifierIndex).toString());
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

    String datasetName = "";
    if(dbPluginMgr.getDataset(Integer.parseInt(datasetID))!=null &&
        dbPluginMgr.getDataset(Integer.parseInt(datasetID)).getValue("name")!=null){
      dbPluginMgr.getDataset(Integer.parseInt(datasetID)).getValue("name").toString();
    }
    
    setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white,new Color(165, 163, 151)), datasetName));
    
    spAttributes.setPreferredSize(new Dimension(550, 500));
    spAttributes.setMinimumSize(new Dimension(550, 500));
    
    setLayout(new GridBagLayout());
    removeAll();

    initAttributePanel();
    
    GridBagConstraints ct = new GridBagConstraints();
    ct.fill = GridBagConstraints.VERTICAL;
    ct.insets = new Insets(2,2,2,2);
    
    ct.gridx = 0;
    ct.gridy = 0;   
    ct.gridwidth=1;
    ct.gridheight=1;
    
    if(!editing){
      initArithmeticPanel();
      
      ct.gridx = 2;
      ct.gridy = 0;
      ct.gridwidth=1;
      ct.gridheight=1;
      add(spAttributes, ct);
    }
    else{
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

  private void initArithmeticPanel(){

    /**
     * Initialises text fields with attributes
     */

    if(!reuseTextFields)
      sFrom.setValue(new Integer(1));
    if(!reuseTextFields)
      sTo.setValue(new Integer(1));

  }


  private void initAttributePanel(){
    
    GridBagConstraints cl = new GridBagConstraints();
    cl.fill = GridBagConstraints.VERTICAL;
    cl.gridx = 1;
    cl.gridy = 0;         
    cl.anchor = GridBagConstraints.NORTHWEST;

    pAttributes.setLayout(new GridBagLayout());
    pAttributes.removeAll();

    spAttributes.getViewport().add(pAttributes, cl);
    
    // when creating, zap loaded dataset id
    /*if(!editing){
      for(int i =0; i<tcCstAttributes.length; ++i){
        if(cstAttributesNames[i].equalsIgnoreCase("datasetID")){
          setJText((JComponent) tcCstAttributes[i],"");
          ((JComponent) tcCstAttributes[i]).setEnabled(false);
        }
      }
    }*/

    if(!reuseTextFields || tcCstAttributes == null ||
        tcCstAttributes.length != cstAttributesNames.length){
      Debug.debug("Creating new tcCstAttributes, "+
          tcCstAttributes+", "+(tcCstAttributes==null ? "":Integer.toString(tcCstAttributes.length)),
              3);
      tcCstAttributes = new JComponent[cstAttributesNames.length];
    }
    
    for(int i =0; i<cstAttributesNames.length; ++i){
      
      if(cstAttributesNames[i].equalsIgnoreCase("status")){
        cl.gridx=0;
        cl.gridy=i;
        pAttributes.add(new JLabel(cstAttributesNames[i] + " : "), cl);
        if(!reuseTextFields || tcCstAttributes[i] == null)
          tcCstAttributes[i] = createTextComponent(TEXTFIELDWIDTH);
        
        setJText(tcCstAttributes[i], cstAttr[i]);
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("actualPars")){
        cl.gridx=0;
        cl.gridy=i;
        JTextArea textArea = new JTextArea(10, TEXTFIELDWIDTH);
        textArea.setLineWrap(true);
        textArea.setWrapStyleWord(true);
        textArea.setEditable(true);
        pAttributes.add(new JLabel(cstAttributesNames[i] + " : "), cl);
        if(!reuseTextFields || tcCstAttributes[i] == null)
          tcCstAttributes[i] = textArea;
        
        setJText(tcCstAttributes[i], cstAttr[i]);
      }
      /*else if(cstAttributesNames[i].equalsIgnoreCase("status")){
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
      else{
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
      if(!cstAttributesNames[i].equalsIgnoreCase("formalPars")){
        pAttributes.add(tcCstAttributes[i], cl);
      }
      if(cstAttributesNames[i].equalsIgnoreCase("identifier")){
        // when creating, zap loaded dataset id
        if(!editing){
          setJText((JComponent) tcCstAttributes[i],"");
        }
        ((JTextComponent) tcCstAttributes[i]).setEnabled(false);
      }
    }
  }

  private void setEnabledAttributes(boolean enabled){
    for(int i =0; i<cstAttributesNames.length; ++i){
      if(cstAttributesNames[i].equalsIgnoreCase("formalPars")){
      }
      else if(!cstAttributesNames[i].equalsIgnoreCase("identifier")){
        tcCstAttributes[i].setEnabled(enabled);
      }
    }
  }

  private void setValuesInAttributePanel(){
    
    Debug.debug("Setting values...", 3);

    for(int i =0; i<cstAttributesNames.length; ++i){
      if(cstAttributesNames[i].equalsIgnoreCase("jobXML") /*&& editing*/){
          tcCstAttributes[i].removeAll();
          GridBagConstraints cv = new GridBagConstraints();
          cv.ipady = 10;
          cv.weighty = 0.5;
          cv.anchor = GridBagConstraints.NORTHWEST;
          cv.fill = GridBagConstraints.VERTICAL;
          cv.weightx = 0.5;
          cv.gridx = 0;
          cv.gridy = 0;
      }
      else{
      }
    }
  }
  
   /**
   * public methods
   */

  public void clear(){

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
    cv.gridx = 0;
    cv.gridy = 1;
    updateUI();
  }

  public void create(final boolean showResults){

    Debug.debug("create",  1);
    
    if(cbTargetDBSelection.getSelectedItem().toString()!=null &&
         !cbTargetDBSelection.getSelectedItem().toString().equals("") &&
         panel.getSelectedIdentifiers().length>0){
      targetDB = cbTargetDBSelection.getSelectedItem().toString();
    }

    final String [] cstAttr = new String[tcCstAttributes.length];

    for(int i=0; i< cstAttr.length; ++i){
      if(cstAttributesNames[i].equalsIgnoreCase("datasetFK")){
        cstAttr[i] = datasetID;
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("transformation")){
        cstAttr[i] = transformation;
      }   
      else if(cstAttributesNames[i].equalsIgnoreCase("transVersion")){
        cstAttr[i] = version;
      }
      else{
        cstAttr[i] = Util.getJTextOrEmptyString(tcCstAttributes[i]);
        Debug.debug("createDataset: cstAttr["+i+"]: "+cstAttr[i],  3);
      }
    }

    Debug.debug("create dataset",  1);

    new DatasetCreator(dbPluginMgr,
                       showResults,
                       cstAttr,
                       cstAttributesNames,
                       datasetIDs,
                       targetDB,
                       transformation,
                       version);

  }

  /**
   *  Edit a dataset
   */

  public void editDataset(int datasetIdentifier){
    
    Debug.debug("editDataset: " + Integer.toString(datasetIdentifier) + " " + table,3); 
    Debug.debug("Got field names: "+Util.arrayToString(cstAttributesNames),3);

    int row = 0;

    //// Constants attributes

    Database.DBRecord res = dbPluginMgr.getDataset(datasetIdentifier);
    
    for(int i =0; i<tcCstAttributes.length; ++i){
      for(int j=0; j<res.fields.length;++j){
        if(res.fields[j].toString().equalsIgnoreCase(cstAttributesNames[i].toString())){
          if(tcCstAttributes[i] == null || !tcCstAttributes[i].isEnabled() &&
             Util.getJTextOrEmptyString(tcCstAttributes[i]).length()==0 /*&& 
             res.fields[j].toString().equalsIgnoreCase("identifier")*/
             ){
            tcCstAttributes[i] = createTextComponent();
            pAttributes.add(tcCstAttributes[i], new GridBagConstraints(1, i/*row*/, 3, 1, 1.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
          }
          Debug.debug(cstAttributesNames[i].toString()+"="+res.fields[j]+". Setting to "+
              res.values[j].toString(),3);
          try{
            setJText(tcCstAttributes[i], res.values[j].toString());
          }
          catch(java.lang.Exception e){
            Debug.debug("Attribute not found, "+e.getMessage(),1);
          }
          // TODO: Make primary key fields inactive
          break;
        }
      }
      if(cstAttributesNames[i].equalsIgnoreCase("identifier") || cstAttributesNames[i].equalsIgnoreCase("datasetFK") ||
            cstAttributesNames[i].equalsIgnoreCase("transformation") || cstAttributesNames[i].equalsIgnoreCase("transVersion") ||
            cstAttributesNames[i].equalsIgnoreCase("reconName") || cstAttributesNames[i].equalsIgnoreCase("logicalDatasetName")){          
        if(!cstAttributesNames[i].equalsIgnoreCase("reconName") &&
           !cstAttributesNames[i].equalsIgnoreCase("identifier") &&
           datasetIdentifier!=-1){
          Util.setJEditable(tcCstAttributes[i], false);
        }
        //else{
        //  tcCstAttributes[i].setBackground(Color.white);
        //}
        //tcCstAttributes[i].setEnabled(false);
        keyValues.put(cstAttributesNames[i].toString(), Integer.toString(i));
        if(cstAttributesNames[i].equalsIgnoreCase("datasetFK")){
          try{
            setJText(tcCstAttributes[i], Integer.toString(datasetIdentifier));
          }
          catch(java.lang.Exception e){Debug.debug("Attribute not found, "+e.getMessage(),1);}
        }
        if(cstAttributesNames[i].equalsIgnoreCase("identifier") && res.values.length < 1){
          try{
            Debug.debug("Clearing identifier",3);
            setJText(tcCstAttributes[i], "");
          }
          catch(java.lang.Exception e){
            Debug.debug("Attribute not found, "+e.getMessage(),1);
          }
        }
      }
    } 
  }

  /**
   *  Delete datasets. Returns HashSet of identifier strings.
   */

  public HashSet deleteDatasets(int [] datasetIdentifiers){
    boolean skip = false;
    boolean okAll = false;
    int choice = 3;
    HashSet deleted = new HashSet();
    JCheckBox cbCleanup = null;
    for(int i=datasetIdentifiers.length-1; i>=0; --i){
      if(datasetIdentifiers[i]!=-1){
        if(!okAll){
          ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame()/*,"",""*/); 
          cbCleanup = new JCheckBox("Delete child records", true);
          
          if(i<1){
            try{
              choice = confirmBox.getConfirm("Confirm delete",
                                   "Really delete "+table+" # "+datasetIdentifiers[i]+"?",
                                new Object[] {"OK", "Skip", cbCleanup});
            }
            catch(java.lang.Exception e){Debug.debug("Could not get confirmation, "+e.getMessage(),1);}
          }
          else{
            try{
              choice = confirmBox.getConfirm("Confirm delete",
                                   "Really delete "+table+" # "+datasetIdentifiers[i]+"?",
                                new Object[] {"OK", "Skip", "OK for all", "Skip all", cbCleanup});
              }catch(java.lang.Exception e){Debug.debug("Could not get confirmation, "+e.getMessage(),1);}
          }
    
          switch(choice){
          case 0  : skip = false; break;  // OK
          case 1  : skip = true ; break;  // Skip
          case 2  : skip = false; okAll = true ;break;  // OK for all
          case 3  : skip = true ; return deleted; // Skip all
          default : skip = true;    // other (closing the dialog). Same action as "Skip"
          }
        }
        if(!skip || okAll){
          Debug.debug("deleting dataset # " + datasetIdentifiers[i], 2);
          if(dbPluginMgr.deleteDataset(datasetIdentifiers[i], cbCleanup.isSelected())){
            deleted.add(Integer.toString(datasetIdentifiers[i]));
            statusBar.setLabel("Dataset # " + datasetIdentifiers[i] + " deleted.");
          }
          else{
            Debug.debug("WARNING: dataset "+datasetIdentifiers[i]+" could not be deleted",1);
            statusBar.setLabel("Dataset # " + datasetIdentifiers[i] + " NOT deleted.");
          }
        }
      }
      else{
        Debug.debug("WARNING: dataset undefined and could not be deleted",1);
      }
    }
    return deleted;
  }

  //// Private methods

  private String[] getTransNames(){
    Database.DBResult res = dbPluginMgr.getTransformations();
    Debug.debug("Number of transformations found: "+res.values.length,3);
    String [] ret = new String[res.values.length];
    for(int i=0; i<res.values.length; ++i){
      ret[i] = res.getValue(i,"name"); 
    }
    // This is to ensure only unique elements
    // TODO: for some reason this doesn't seam to work
    Arrays.sort(ret);
    Vector vec = new Vector();
    if(res.values.length>0){
      vec.add(ret[0]);
    }
    if(res.values.length>1){
      for(int i=1; i<res.values.length; ++i){
        Debug.debug("Comparing "+ret[i]+" <-> "+ret[i-1],3);
        if(!ret[i].equals(ret[i-1])){
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
  
  private String[] getTransVersions(String transformation){
    Debug.debug("Finding version for transformation "+transformation, 3);
    Database.DBResult res = dbPluginMgr.getTransformations();
    String [] names = new String[res.values.length];
    String [] versions = new String[res.values.length];
    for(int i=0; i<res.values.length; ++i){
      names[i] = res.getValue(i,"name"); 
      versions[i] = res.getValue(i,"version"); 
    }
    // This is to ensure only unique elements
    // TODO: for some reason this doesn't seam to work
    Vector vec = new Vector();
    if(res.values.length>0){
      for(int i=0; i<versions.length; ++i){
        if(names[i].equals(transformation)){
          boolean found = false;
          for(int j=0; j<vec.size(); ++j){
            Debug.debug("Comparing "+versions[i]+" <-> "+vec.get(j),3);
            if(versions[i].equals(vec.get(j))){
              found = true;
              break;
            }
          }
          if(!found){
            Debug.debug("Adding "+versions[i],3);
            vec.add(versions[i]);
          }
        }
      }
    }
    String[] arr = new String[vec.size()];
    for(int i=0; i<vec.size(); ++i){
      arr[i]=vec.elementAt(i).toString();
    } 
    return arr;
  }

  private void initTransformationPanel(int datasetIdentifier){
    boolean found = false;
    
    Debug.debug("Finding transformations...",3);

    pTransformation.removeAll();
    pTransformation.setLayout(new FlowLayout());

    String [] transformations = getTransNames();

    if(transformations.length == 0){
      pTransformation.add(new JLabel("No transformations found."));
      transformationChosen = false;
    }
    else if(transformations.length == 1){
      transformation = transformations[0];
      pTransformation.add(new JLabel("Transformation:" + transformation));
      initTransVersionPanel(datasetIdentifier, transformation);
      transformationChosen = true;
    }
    else{
      cbTransformationSelection = new JComboBox();
      for(int i=0; i<transformations.length; ++i){
          cbTransformationSelection.addItem(transformations[i]);
      }
      pTransformation.add(new JLabel("Transformation:"), null);
      pTransformation.add(cbTransformationSelection, null);

      cbTransformationSelection.addActionListener(
        new java.awt.event.ActionListener(){
          public void actionPerformed(java.awt.event.ActionEvent e){
            cbTransformationSelection_actionPerformed();
          }
        }
      );
      transformationChosen = false;
    }
    add(pTransformation,   new GridBagConstraints(2, 0, 1, 1, 0.0, 0.01
        ,GridBagConstraints.NORTH, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    updateUI();
  }

  private void initTransVersionPanel(int datasetIdentifier, String transformation){
    boolean found = false;

    Debug.debug("Finding versions...",3);

    pVersion.removeAll();
    pVersion.setLayout(new FlowLayout());

    String [] versions = getTransVersions(transformation);
    Debug.debug("Number of versions found: "+versions.length,3);

    if(versions.length == 0){

      pVersion.add(new JLabel("No versions found."));
      versionChosen = false;

    }
    else if(versions.length == 1){

      version = versions[0];
      pVersion.add(new JLabel("Version:" + version));

        versionChosen = true;
        editDataset(Integer.parseInt(datasetID));
    }
    else{
      cbTransVersionSelection = new JComboBox();

      for(int i=0; i<versions.length; ++i){
        Debug.debug("Adding version "+versions[i],3);
        cbTransVersionSelection.addItem(versions[i]);
      }

      pVersion.add(new JLabel("Version:"), null);
      pVersion.add(cbTransVersionSelection, null);

      cbTransVersionSelection.addActionListener(
        new java.awt.event.ActionListener(){
          public void actionPerformed(java.awt.event.ActionEvent e){
            cbTransVersionSelection_actionPerformed();
          }
        }
      );
      versionChosen = false;
    }

    add(pVersion,   new GridBagConstraints(3, 0, 1, 1, 0.0, 0.01
      ,GridBagConstraints.NORTHWEST, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));

    updateUI();
  }

  private void cbTransformationSelection_actionPerformed(){
    if(cbTransformationSelection.getSelectedItem() == null){
        //transformation = cbTransformationSelection.getItemAt(0).toString();
        return;
    }
    else{
        transformation = cbTransformationSelection.getSelectedItem().toString();
    }
    transformationChosen = true;
    initTransVersionPanel(Integer.parseInt(datasetID), transformation);
    try{
        setJText(tcCstAttributes[Integer.parseInt(keyValues.get("transformation").toString())], transformation);
    }
    catch(java.lang.Exception e){Debug.debug("Attribute not found, "+e.getMessage(),1);}
  }

  private void cbTransVersionSelection_actionPerformed(){
    if(cbTransVersionSelection.getSelectedItem() == null){
      //transformation = cbTransVersionSelection.getItemAt(0).toString();
      return;
    }
    else{
      version = cbTransVersionSelection.getSelectedItem().toString();
    }
    versionChosen = true;
    editDataset(Integer.parseInt(datasetID));
    try{
      setJText(tcCstAttributes[Integer.parseInt(keyValues.get("transVersion").toString())], version);
    }
    catch(java.lang.Exception e){Debug.debug("Attribute not found, "+e.getMessage(),1);}
    try{
      setJText(tcCstAttributes[Integer.parseInt(keyValues.get("logicalDatasetName").toString())],
          dbPluginMgr.getTargetDatasetName(targetDB, dbPluginMgr.getDatasetName(datasetIDs[0]),
              transformation, version));
    }
    catch(java.lang.Exception e){Debug.debug("Attribute not found, "+e.getMessage(),1);}
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
      if(!cstAttributesNames[i].equalsIgnoreCase("identifier") &&
          !cstAttributesNames[i].equalsIgnoreCase("transformation") &&
          !cstAttributesNames[i].equalsIgnoreCase("datasetFK")){
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
  
  private static String getJTextOrEmptyString(JComponent comp, boolean editing){
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
