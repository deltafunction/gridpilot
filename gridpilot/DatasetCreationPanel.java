package gridpilot;

import gridfactory.common.DBRecord;
import gridfactory.common.DBResult;
import gridfactory.common.Debug;

import javax.swing.*;
import javax.swing.border.*;
import javax.swing.text.JTextComponent;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * This panel creates records in the DB table. It's shown inside the CreateEditDialog.
 *
 */
public class DatasetCreationPanel extends CreateEditPanel{

  private static final long serialVersionUID = 1L;
  private JPanel pAttributes = new JPanel();
  private JScrollPane spAttributes = new JScrollPane();
  private String datasetID = "-1";
  private String datasetIdentifier = "identifier";
  private DBPanel panel;
  private MyJTable table;
  private String [] cstAttributesNames;
  private JComponent [] tcCstAttributes;
  private boolean reuseTextFields = true;
  private Vector<JTextComponent> tcConstant = new Vector<JTextComponent>(); // contains all text components
  private String [] cstAttr = null;
  private boolean editing = false;
  private DBPluginMgr dbPluginMgr = null;
  private DBPluginMgr targetDBPluginMgr = null;
  private static int TEXTFIELDWIDTH = 32;
  private String executableName = "";
  private String executableVersion = "";
  private String [] datasetIDs = new String [] {"-1"};
  private JPanel pExe = new JPanel();
  private JPanel pExecutable = new JPanel();
  private JPanel pTargetDBs = new JPanel();
  private JLabel jlTargetDBSelection = null;
  private JPanel pVersion = new JPanel();
  private JComboBox cbTargetDBSelection;
  private JComboBox cbExecutableSelection;
  private JComboBox cbInputDBSelection;
  private JComboBox cbExeVersionSelection;
  private String targetDB = null;
  private DBRecord dataset = null;
  private DBResult executables = null;
  private String [] jobDefDatasetReference;
  private String [] datasetExecutableReference;
  private String [] datasetExecutableVersionReference;
  private JButton jbEditExe;
  private String datasetName = "";
  private String title = "";
  private DBPanel targetPanel;
  private ArrayList<JComponent> detailFields = new ArrayList<JComponent>();
  private HashMap<String, JComponent> labels = new HashMap<String, JComponent>();
  private HashMap<String, JComponent> textFields = new HashMap<String, JComponent>();
  private ArrayList<String> detailFieldNames;
  private HashMap<String, String> descriptions;
  private String inputDB = null;
  private JComponent datasetChooser;
  private JLabel instructionsLabel;

  public boolean editable = true;

  /**
   * Constructor
   */
  public DatasetCreationPanel(DBPluginMgr _dbPluginMgr, DBPanel _panel, boolean _editing){
    
    editing = _editing;
    panel = _panel;
    dbPluginMgr = _dbPluginMgr;
    targetDBPluginMgr = dbPluginMgr;
    table = panel.getTable();
    cstAttributesNames = dbPluginMgr.getFieldNames("dataset");
    cstAttr = new String[cstAttributesNames.length];
    executables = dbPluginMgr.getExecutables();
    editable = true;

    setFieldNames(dbPluginMgr.getDBName());
    
    // Find identifier index
    int identifierIndex = -1;
    for(int i=0; i<table.getColumnNames().length; ++i){
      Debug.debug("Column name: "+table.getColumnNames().length+":"+i+" "+table.getColumnName(i), 3);
      if(table.getColumnName(i).equalsIgnoreCase(datasetIdentifier)){
        identifierIndex = i;
        break;
      }
    }
    if(identifierIndex==-1){
      Debug.debug("ERROR: could not find index of dataset, "+datasetIdentifier, 1);
    }
    
    // Find dataset id from table
    if(table.getSelectedRows().length==1 && editing){
      datasetID = table.getUnsortedValueAt(table.getSelectedRow(), identifierIndex).toString();
      if(datasetID==null || datasetID.equals("-1") ||
          datasetID.equals("")){
        Debug.debug("ERROR: could not find datasetID in table!", 1);
        return;
      }
      Debug.debug("Getting dataset. "+editing, 1);     
      dataset = dbPluginMgr.getDataset(datasetID);
    }
    // Creating; set empty dataset
    else{
      //TODO
    }

    // Dataset(s) selected and not editing - creating with input dataset(s)
    if(table.getSelectedRows().length>0 && !editing){
      // Find input datasets
      datasetIDs = panel.getSelectedIdentifiers();
      Debug.debug("Creating with input datasets "+identifierIndex+":"+datasetID+": "+
          MyUtil.arrayToString(datasetIDs), 3);
    } 
  }

  private void setFieldNames(String dbName) {
    datasetIdentifier = MyUtil.getIdentifierField(dbName, "dataset");    
    jobDefDatasetReference = MyUtil.getJobDefDatasetReference(dbName);
    datasetExecutableReference =
      MyUtil.getDatasetExecutableReference(dbName);
    datasetExecutableVersionReference =
      MyUtil.getDatasetExecutableVersionReference(dbName);
    
    try{
      for(int i=0; i<cstAttributesNames.length; ++i){
        if(isDetail(cstAttributesNames[i])){
          detailFields.add(tcCstAttributes[i]);
          if(labels.containsKey(cstAttributesNames[i])){
            detailFields.add(labels.get(cstAttributesNames[i]));
          }
        }
      }
    }
    catch(Exception e){
      e.printStackTrace();
    }
    
    initVars();

  }

  private void initButtons(){
    jbEditExe = MyUtil.mkButton("search.png", GridPilot.ADVANCED_MODE?"Look up":"Edit",
        GridPilot.ADVANCED_MODE?"Look up executable record":"Edit executable record");
  }

  /**
   * GUI initialisation
   */
  public void initGUI(){
    initButtons();
    setLayout(new BorderLayout());
    pExe.setLayout(new FlowLayout());
     removeAll();
    
    if(editing){
      try{
        datasetName = dbPluginMgr.getDatasetName(datasetID);
      }
      catch(Exception e){
        // nothing
      }
    }
    else if(GridPilot.DB_NAMES!=null && GridPilot.DB_NAMES.length!=0 &&
        datasetIDs!=null && datasetIDs.length!=0 &&
        (datasetIDs.length!=1 || !datasetIDs[0].equals(null) && !datasetIDs[0].equals("-1"))){
      if(datasetIDs.length==1){
        if(datasetIDs[0].equals(null) || datasetIDs[0].equals("-1")){
          title = "Define new dataset";
        }
        else{
          title = "Define target dataset for id ";
        }
      }
      else{
        title = "Define target datasets for ids ";
      }
      for(int i=0; i<datasetIDs.length; ++i){
        if(i>0){
          title += ", ";
        }
        title += datasetIDs[i];
        if(i>5){
          title += " ... ";
          break;
        }
      }
      initTargetDBsPanel();
      pExe.add(pTargetDBs);
    }

    setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white, new Color(165, 163, 151)), title));
    
    //spAttributes.setPreferredSize(new Dimension(590, 500));
    //spAttributes.setMaximumSize(new Dimension(600, 500));
    //spAttributes.setMinimumSize(new Dimension(300, 300));
    
    pExe.add(pExecutable);
    
    cbInputDBSelection = new JComboBox();
    cbInputDBSelection.addActionListener(
        new java.awt.event.ActionListener(){
          public void actionPerformed(java.awt.event.ActionEvent e){
            cbInputDBSelection_actionPerformed();
          }
        }
    );

    //initVars();
    initAttributePanel();
    setValues();
    checkValues();
    
    if(GridPilot.DB_NAMES==null || GridPilot.DB_NAMES.length==0 ||
        datasetIDs==null || datasetIDs.length==0 ||
        (datasetIDs.length==1 && (datasetIDs[0].equals(null) || datasetIDs[0].equals("-1")))){
      initExecutablePanel();
      initExeVersionPanel();
      setComboBoxValues();
    }
    
    instructionsLabel = new JLabel(createDatasetInstructionsLabelString());
    JPanel instructionsPanel = new JPanel();
    instructionsPanel.add(instructionsLabel);
    
    JPanel pTop = new JPanel(new BorderLayout());
    pTop.add(pExe, BorderLayout.NORTH);
    pTop.add(instructionsPanel, BorderLayout.CENTER);   
    add(pTop, BorderLayout.NORTH);
    add(spAttributes, BorderLayout.CENTER);
    
    if(GridPilot.ADVANCED_MODE){
      jbEditExe.addActionListener(new java.awt.event.ActionListener(){
        public void actionPerformed(ActionEvent e){
          try{
            lookupExecutable();
          }
          catch(Exception e1){
            e1.printStackTrace();
          }
        }
      }
      );
    }
    else{
      jbEditExe.addActionListener(new java.awt.event.ActionListener(){
        public void actionPerformed(ActionEvent e){
          try{
            editExecutable();
          }
          catch(Exception e1){
            e1.printStackTrace();
          }
        }
      }
      );
    }
    
    showDetails(false);
    
  }
  
  private void initVars() {
    detailFieldNames = new ArrayList<String>();
    Collections.addAll(detailFieldNames,
       new String [] {datasetIdentifier, datasetExecutableReference[1], datasetExecutableVersionReference[1],
       "created", "lastModified", "metaData", "runNumber", "totalEvents", "totalFiles"});
    descriptions = new HashMap<String, String>();
    descriptions.put("outputLocation", "URL of the directory where the files of this application/dataset are kept");
    descriptions.put(jobDefDatasetReference[0], "Name of this application/dataset");
    descriptions.put(datasetIdentifier, "Unique identifier");
    descriptions.put(datasetExecutableReference[1], "Optional: Executable used by this application/dataset");
    descriptions.put(datasetExecutableVersionReference[1], "Optional: Version of the executable");
    descriptions.put("created", "Creation date of this record");
    descriptions.put("lastModified", "Last modification date of this record");
    descriptions.put("metaData", "Optional: data describing the application/dataset");
    descriptions.put("runNumber", "Optional: number used to keep track of datasets");
    descriptions.put("totalEvents", "Optional: number of events of this dataset");
    descriptions.put("totalFiles", "Optional: total number of files. If not given or -1, inferred from number of input files " +
    		"when creating jobs");
    descriptions.put("inputDataset", "Input dataset");
    descriptions.put("inputDB", "Name of database holding the input dataset");
    /*
    detailFieldNames.add("inputDataset");
    detailFieldNames.add("inputDB");
    descriptions.put("inputDataset", "Optional: input dataset");
    descriptions.put("inputDB", "Optional: name of database holding the input dataset");
    */
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

  private void checkValues(){
    for(int i=0; i<cstAttributesNames.length; ++i){
      // in case the database does not support lookup on dataset id (like DQ2);
      // then the values will be null and we have to take those we can directly from tableResults.
      // If we cannot get all values, we will not allow editing.
      if(editing && (cstAttr[i]==null /*|| cstAttr[i].equals("")*/)){
        try{
          MyJTable tableResults = panel.getTable();
          if(tableResults.getSelectedRows().length==1){
            boolean ok = false;
            for(int k=0; k<tableResults.getColumnCount(); ++k){
              if(tableResults.getColumnName(k).equalsIgnoreCase(cstAttributesNames[i])){
                cstAttr[i] = tableResults.getUnsortedValueAt(tableResults.getSelectedRow(), k).toString();
                ok = true;
                break;
              }
            }
            if(!ok){
              Debug.debug("Dataset has no "+cstAttributesNames[i]+", "+cstAttr[i], 2);
              if(datasetName==null){
              }
              editable = false;
            }
          }
          else{
            Debug.debug("Dataset has no "+cstAttributesNames[i]+", "+cstAttr[i]+" and no row is selected.", 2);
            editable = false;
          }
        }
        catch(Exception e){
          editable = false;
          e.printStackTrace();
        }
      }
    }
    title = "Edit dataset " + 
    (datasetName.length()>32 ? datasetName.substring(0, 28)+"..." : datasetName);
  }

  private void initAttributePanel(){
    pAttributes.setLayout(new GridBagLayout());
    pAttributes.removeAll();
    spAttributes.getViewport().add(pAttributes);
    if(!reuseTextFields || tcCstAttributes==null ||
        tcCstAttributes.length!=cstAttributesNames.length){
      Debug.debug("Creating new tcCstAttributes, "+tcCstAttributes+", "+
          (tcCstAttributes==null ? "":Integer.toString(tcCstAttributes.length)),
              3);
      tcCstAttributes = new JComponent[cstAttributesNames.length];
    }
    int row = 0;
    for(int i=0; i<cstAttributesNames.length; ++i){
      if(cstAttributesNames[i].equalsIgnoreCase("metaData")){
        if(!reuseTextFields || tcCstAttributes[i]==null){
          tcCstAttributes[i] = MyUtil.createTextArea(TEXTFIELDWIDTH);
        }
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("inputDB")){
        tcCstAttributes[i] = cbInputDBSelection;
        // Start with empty entry
        ((JComboBox) tcCstAttributes[i]).addItem("");
        for(int ii=0; ii<GridPilot.DB_NAMES.length; ++ii){
          cbInputDBSelection.addItem(GridPilot.DB_NAMES[ii]);
        }
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("inputDataset")){
        tcCstAttributes[i] = new DatasetChooser(pAttributes);
        datasetChooser = tcCstAttributes[i];
      }
      else{
        if(!editing && !reuseTextFields || tcCstAttributes[i]==null){
          tcCstAttributes[i] = new JTextField("", TEXTFIELDWIDTH);
        }
      }
      if(cstAttr[i]!=null && !cstAttr[i].equals("")){
        Debug.debug("Setting cstAttr["+i+"]: "+cstAttr[i], 3);
        MyUtil.setJText(tcCstAttributes[i], cstAttr[i]);
      }
      Debug.debug("Adding cstAttributesNames["+i+"], "+cstAttributesNames[i]+
          " "+tcCstAttributes[i].getClass().toString(), 3);
      if(cstAttributesNames[i].equalsIgnoreCase("outputLocation")){
        JPanel outputLocationCheckPanel = MyUtil.createCheckPanel1(
            (Window) SwingUtilities.getWindowAncestor(this),
            cstAttributesNames[i], (JTextComponent) tcCstAttributes[i], true, true, true, false);
        pAttributes.add(outputLocationCheckPanel,
            new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(5, 21, 5, 5), 0, 0));
        pAttributes.add(tcCstAttributes[i],
            new GridBagConstraints(1, row, 3, 1, 0.0, 0.0,
            GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
        labels.put(cstAttributesNames[i], outputLocationCheckPanel);
        outputLocationCheckPanel.setToolTipText(descriptions.get(cstAttributesNames[i].toLowerCase()));
        ++row;
      }
      else{
        JLabel jLabel = new JLabel(cstAttributesNames[i]);
        labels.put(cstAttributesNames[i], jLabel);
        textFields.put(cstAttributesNames[i], tcCstAttributes[i]);
      }
      // when creating, zap loaded dataset id
      if(!editing && cstAttributesNames[i].equalsIgnoreCase(datasetIdentifier)){
        MyUtil.setJText((JComponent) tcCstAttributes[i], "");
        MyUtil.setJEditable(tcCstAttributes[i], false);
      }
    }
    
    for(int i=0; i<cstAttributesNames.length; ++i){
      if(cstAttributesNames[i].equalsIgnoreCase(datasetExecutableReference[1]) ||
         cstAttributesNames[i].equalsIgnoreCase(datasetExecutableVersionReference[1]) ||
         cstAttributesNames[i].equalsIgnoreCase("created") ||
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
  
  private boolean isDetail(String fieldName){
    if(detailFieldNames==null){
      return false;
    }
    return MyUtil.arrayContainsIgnoreCase(detailFieldNames.toArray(new String[detailFieldNames.size()]), fieldName);
  }
  
  private void addToolTipText(String fieldName){
    if(!labels.containsKey(fieldName) || !descriptions.containsKey(fieldName.toLowerCase())){
      return;
    }
    Debug.debug("Setting tool tip text of "+fieldName+" --> "+descriptions.get(fieldName), 2);
    labels.get(fieldName).setToolTipText(descriptions.get(fieldName.toLowerCase()));
  }

  private void setComboBoxValues(){
    for(int i=0; i<cstAttributesNames.length; ++i){
      if(cstAttributesNames[i].equalsIgnoreCase(datasetExecutableReference[1])){
        if(cbExecutableSelection!=null &&
            executableName!=null && executableName!=null){
          cbExecutableSelection.setSelectedItem(executableName);
        }
      }
      else if(cstAttributesNames[i].equalsIgnoreCase(datasetExecutableVersionReference[1])){
        if(executableVersion!=null && cbExeVersionSelection!=null){
          cbExeVersionSelection.setSelectedItem(executableVersion);
        }
      }
    }
  }
  
  /*
   * Set special values.
   */
  private void setValuesInAttributePanel(){
    
    Debug.debug("Setting values "+executableName+":"+executableVersion+":"+executableName+":"+executableVersion, 3);

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
      else if(executableName!=null &&
          cstAttributesNames[i].equalsIgnoreCase(datasetExecutableReference[1])){
        MyUtil.setJText(tcCstAttributes[i], executableName);
      }
      else if(executableVersion!=null && !executableVersion.equals("") &&
          cstAttributesNames[i].equalsIgnoreCase(datasetExecutableVersionReference[1])){
        MyUtil.setJText(tcCstAttributes[i], executableVersion);
      }
    }
  }
  
  public void clearPanel(){
    Vector<JComponent> textFields;
    if(editing){
      textFields = getFields(); 
    }
    else{
      textFields = getNonIdTextFields();
    }
    for(int i =0; i<textFields.size(); ++i){
      MyUtil.setJText((JComponent) textFields.get(i), "");
    }
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

  public void create(final boolean showResults, final boolean editing){

    if(editing){
      Debug.debug("edit dataset", 1);
    }
    else{
      Debug.debug("create dataset", 1);
    }
    
    final String [] cstAttr = new String[tcCstAttributes.length];
    
    if(editing){
      for(int i=0; i<cstAttr.length; ++i){
        cstAttr[i] = MyUtil.getJTextOrEmptyString(tcCstAttributes[i]);
        Debug.debug("createDataset: cstAttr["+i+"]: "+cstAttr[i], 3);
      }
      DatasetUpdater dsu = new DatasetUpdater(
          SwingUtilities.getWindowAncestor(this),
          dbPluginMgr,
          showResults,
          cstAttr,
          cstAttributesNames,
          datasetID,
          datasetName
          );
      // TODO: refresh results on panel showing datasets from the db - if such a panel is shown
      if(dsu.anyCreated && panel.getDBName().equalsIgnoreCase(dbPluginMgr.getDBName())){
        panel.refresh();
      }
    }
    else{
      if(cbTargetDBSelection!=null && cbTargetDBSelection.getSelectedItem()!=null &&
          !cbTargetDBSelection.getSelectedItem().toString().equals("") &&
          panel.getSelectedIdentifiers().length>0){
        targetDB = cbTargetDBSelection.getSelectedItem().toString();
      }
      for(int i=0; i<cstAttr.length; ++i){
        cstAttr[i] = MyUtil.getJTextOrEmptyString(tcCstAttributes[i]);
        Debug.debug("createDataset: cstAttr["+i+"]: "+cstAttr[i], 3);
      }
      DatasetCreator dsc = new DatasetCreator(
          SwingUtilities.getWindowAncestor(this),
          statusBar,
          dbPluginMgr,
          showResults,
          cstAttr,
          cstAttributesNames,
          datasetIDs,
          targetDB
          );
      if(dsc.anyCreated && dbPluginMgr.getDBName().equals(targetDB)){
        panel.refresh();
      }
    }
  }

  /**
   *  Edit or create a dataset
   */
  private void setValues(){
    
    Debug.debug("setValues: " + datasetID +
        " " + executableName + " " + executableVersion, 3); 
    Debug.debug("Got field names: "+
        MyUtil.arrayToString(cstAttributesNames), 3);

    if(editing){
      // set values of fields
      Debug.debug("Got dataset record. "+dataset.fields.length+":"+
          tcCstAttributes.length, 3);
      for(int i=0; i<tcCstAttributes.length; ++i){
        for(int j=0; j<dataset.fields.length;++j){
          //Debug.debug("Checking dataset field, "+dataset.fields[j].toString()+"<->"+cstAttributesNames[i].toString(), 3);
          if(dataset.fields[j].toString().equalsIgnoreCase(cstAttributesNames[i].toString())){
              try{
                Debug.debug(cstAttributesNames[i].toString()+"="+dataset.fields[j]+". Setting to "+
                    dataset.values[j].toString(), 3);
                MyUtil.setJText(tcCstAttributes[i], dataset.values[j].toString());
                cstAttr[i] = dataset.values[j].toString();
                
                if(cstAttributesNames[i].equalsIgnoreCase(datasetExecutableReference[1])){
                  executableName = dataset.values[j].toString();
                }
                else if(cstAttributesNames[i].equalsIgnoreCase(datasetExecutableVersionReference[1])){
                  executableVersion = dataset.values[j].toString();
                }
                else if(cstAttributesNames[i].equalsIgnoreCase("inputDB")){
                  inputDB = dataset.values[j].toString();
                  cbInputDBSelection_actionPerformed();
                }
              }
              catch(java.lang.Exception e){
                Debug.debug("Field not found or set, "+e.getMessage(), 1);
              }
            break;
          }
        }
        // make identifier and executable foreign key inactive
        if(cstAttributesNames[i].equalsIgnoreCase(datasetIdentifier) ||
              cstAttributesNames[i].equalsIgnoreCase(datasetExecutableReference[1]) ||
              cstAttributesNames[i].equalsIgnoreCase(datasetExecutableVersionReference[1]) ||
              cstAttributesNames[i].equalsIgnoreCase("created") ||
              cstAttributesNames[i].equalsIgnoreCase("lastModified")){
          try{
            if(datasetID==null || datasetID.equals("-1")){
              Debug.debug("Clearing identifier", 3);
              MyUtil.setJText(tcCstAttributes[i], "");
            }
            MyUtil.setJEditable(tcCstAttributes[i], false);
          }
          catch(java.lang.Exception e){
            Debug.debug("Field not found, "+e.getMessage(), 1);
          }
        }
        else{
          MyUtil.setJEditable(tcCstAttributes[i], true);
        }
      }
    }
    setValuesInAttributePanel();
  }

  private String[] getExeNames(){
    String [] ret = new String[executables.values.length];
    Debug.debug("number of executables: "+executables.values.length, 3);
    Debug.debug("fields: "+MyUtil.arrayToString(executables.fields), 3);
    for(int i=0; i<executables.values.length; ++i){
      Debug.debug("#"+i, 3);
      Debug.debug("name: "+executables.getValue(i, datasetExecutableReference[0]), 3);
      Debug.debug("values: "+MyUtil.arrayToString(executables.values[i]), 3);
      ret[i] = executables.getValue(i, datasetExecutableReference[0]).toString(); 
    }
    // This is to ensure only unique elements
    Arrays.sort(ret);
    Vector<String> vec = new Vector<String>();
    if(executables.values.length>0){
      vec.add(ret[0]);
    }
    if(executables.values.length>1){
      for(int i=1; i<executables.values.length; ++i){
        //Debug.debug("Comparing "+ret[i]+" <-> "+ret[i-1],3);
        if(!ret[i].equals(ret[i-1])){
          Debug.debug("Adding "+ret[i], 3);
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
  
  private void initExecutablePanel(){
    
    Debug.debug("Finding executables...", 3);

    pExecutable.removeAll();
    pExecutable.setLayout(new FlowLayout());

    // if there's no executable table, just skip
    String [] exeNames = {};
    try{
      exeNames = getExeNames();
    }
    catch(Exception e){
      Debug.debug("Could not get executables, probably no executable table.", 1);
    }

    if(exeNames.length==0){
      pExecutable.add(new JLabel("No executables found."));
      initExeVersionPanel();
      jbEditExe.setEnabled(false);
    }
    else if(exeNames.length==1){
      executableName = exeNames[0];
      pExecutable.add(new JLabel("Executable:   " + executableName));
      initExeVersionPanel();
    }
    else{
      cbExecutableSelection = new JComboBox();
      // Start with empty entry
      cbExecutableSelection.addItem("");
      for(int i=0; i<exeNames.length; ++i){
        cbExecutableSelection.addItem(exeNames[i]);
      }
      pExecutable.add(new JLabel("Executable:"), null);
      pExecutable.add(cbExecutableSelection, null);

      cbExecutableSelection.addActionListener(
        new java.awt.event.ActionListener(){
          public void actionPerformed(java.awt.event.ActionEvent e){
            cbExecutableSelection_actionPerformed();
          }
        }
      );
    }
  }

  private void initExeVersionPanel(){

    pExe.add(pVersion);

    Debug.debug("Finding versions...", 3);

    pVersion.removeAll();
    pVersion.setLayout(new FlowLayout());

    String [] versions = {};
    
    try{
      versions = targetDBPluginMgr.getVersions(executableName);
    }
    catch(Exception e){
    }
    if(versions==null){
      versions = new String [] {};
    }
    
    Debug.debug("Number of versions found: "+versions.length, 3);

    if(versions.length==0){
      try{
        pExe.remove(jbEditExe);
      }
      catch(Exception e){
      }
      if(editing){
        pVersion.add(new JLabel("No versions found."));
      }
      setValuesInAttributePanel();
    }
    else if(versions.length==1){
      executableVersion = versions[0];
      pVersion.add(new JLabel(executableVersion));
      //setValues();
      setValuesInAttributePanel();
      pExe.add(jbEditExe);
    }
    else{
      cbExeVersionSelection = new JComboBox();

      for(int i=0; i<versions.length; ++i){
        Debug.debug("Adding version "+versions[i], 3);
        cbExeVersionSelection.addItem(versions[i]);
      }

      pVersion.add(cbExeVersionSelection, null);

      cbExeVersionSelection.addActionListener(
        new java.awt.event.ActionListener(){
          public void actionPerformed(java.awt.event.ActionEvent e){
            cbExeVersionSelection_actionPerformed();
          }
        }
      );
    }
    pExe.updateUI();
  }
  
  protected void editExecutable(){
    if(executableName==null || executableName.equals("") ||
        executableVersion==null || executableVersion.equals("")){
      return;
    }
    setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
    String executableID = dbPluginMgr.getExecutableID(executableName, executableVersion);
    CreateEditDialog pDialog = new CreateEditDialog(
       new ExecutableCreationPanel(dbPluginMgr, null, true, executableID),
       true, true, true, false, true);
    pDialog.setTitle(GridPilot.getRecordDisplayName("Executable"));
    setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
  }

  /**
   * Open new pane with corresponding executables.
   * @throws InvocationTargetException 
   * @throws InterruptedException 
   */
  private void lookupExecutable() throws InterruptedException, InvocationTargetException{
    if(executableName==null || executableName.equals("") ||
        executableVersion==null || executableVersion.equals("")){
      return;
    }
    
    setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
    
    MyResThread t1 = new MyResThread(){
      public void run(){
        try{
          GridPilot.getClassMgr().getGlobalFrame().requestFocusInWindow();
          //GridPilot.getClassMgr().getGlobalFrame().setVisible(true);
          // Create new panel with jobDefinitions.         
          targetPanel = new DBPanel();
          targetPanel.initDB(targetDBPluginMgr.getDBName(), "executable");
          targetPanel.initGUI();
        }
        catch(Exception e){
          e.printStackTrace();
        }
      }
    };
    if(SwingUtilities.isEventDispatchThread()){
      t1.run();
    }
    else{
      SwingUtilities.invokeAndWait(t1);
    }
    
    MyResThread t2 = new MyResThread(){
      public void run(){
        try{
          targetPanel.initDB(targetDBPluginMgr.getDBName(),
              "executable");
          String idField =
            MyUtil.getIdentifierField(targetDBPluginMgr.getDBName(), "executable");
          String id = targetDBPluginMgr.getExecutableID(executableName,
              executableVersion);
          targetPanel.getSelectPanel().setConstraint(idField, id, 0);
          targetPanel.searchRequest(true, false);           
          GridPilot.getClassMgr().getGlobalFrame().addPanel(targetPanel);
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
      t2.run();
    }
    else{
      SwingUtilities.invokeLater(t2);
    }
  }

  private void cbInputDBSelection_actionPerformed(){
    if(datasetChooser==null){
      return;
    }
    inputDB = cbInputDBSelection.getSelectedItem().toString();
    if(inputDB==null || inputDB.trim().equals("")){
      return;
    }
    Debug.debug("Input DB: "+inputDB, 2);
    ((DatasetChooser) datasetChooser).setDB(inputDB);
  }

  private void cbExecutableSelection_actionPerformed(){
    if(cbExecutableSelection.getSelectedItem()==null){
      return;
    }
    else{
      executableName = cbExecutableSelection.getSelectedItem().toString();
    }
    try{
      pExe.remove(jbEditExe);
    }
    catch(Exception e){
    }
    initExeVersionPanel();
  }

  private void cbExeVersionSelection_actionPerformed(){
    if(cbExeVersionSelection.getSelectedItem()==null){
      return;
    }
    else{
      executableVersion = cbExeVersionSelection.getSelectedItem().toString();
    }
    try{
      pExe.remove(jbEditExe);
    }
    catch(Exception e){
    }
    //setValues();
    setValuesInAttributePanel();
    pExe.add(jbEditExe);
    pExe.validate();
  }

  private Vector<JComponent> getFields(){
    Vector<JComponent> v = new Vector<JComponent>();

    v.addAll(tcConstant);

    for(int i=0; i<tcCstAttributes.length; ++i)
      v.add(tcCstAttributes[i]);

    return v;
  }

  private Vector<JComponent> getNonIdTextFields(){
    Vector<JComponent> v = new Vector<JComponent>();

    v.addAll(tcConstant);

    for(int i=0; i<tcCstAttributes.length; ++i){
      if(!cstAttributesNames[i].equalsIgnoreCase(datasetIdentifier)){
        v.add(tcCstAttributes[i]);
      }
    }

    return v;
  }

  private void initTargetDBsPanel(){
    
    Debug.debug("Finding target dataset databases...",3);
  
    pTargetDBs.removeAll();
    pTargetDBs.setLayout(new FlowLayout());
  
    cbTargetDBSelection = new JComboBox();
    
    cbTargetDBSelection.addItem("");
    for(int i=0; i<GridPilot.DB_NAMES.length; ++i){
      // If this DB has no job definition table, there's no point in
      // allowing the creation of datasets with an input dataset in it.
      try{
        if(!GridPilot.getClassMgr().getDBPluginMgr(GridPilot.DB_NAMES[i]).isJobRepository()){
          continue;
        }
      }
      catch(Exception e){
        continue;
      }
      cbTargetDBSelection.addItem(GridPilot.DB_NAMES[i]);
    }

    jlTargetDBSelection = new JLabel("DB:");
    pTargetDBs.add(jlTargetDBSelection, null);
    pTargetDBs.add(cbTargetDBSelection, null);
    
    cbTargetDBSelection.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(java.awt.event.ActionEvent e){
        cbTargetDBSelection_actionPerformed();
    }});
  
  }

  /**
   * Action Events
   */
  
  /**
   * Modify attributes according to target database selected:
   * map source fields to target fields if possible.
   */
  private void cbTargetDBSelection_actionPerformed(){
    if(cbTargetDBSelection.getSelectedItem()==null){
      return;
    }
    // only do anything if there are input dataset(s) selected
    Debug.debug("datasetIDs[0]="+datasetIDs[0], 3);
    if(datasetIDs[0].equals(null) || datasetIDs[0].equals("-1")){
      return;
    }
    boolean detailsVisible = detailsShown();
    String [] sourceFields = cstAttributesNames;
    String [] sourceAttr = new String[cstAttributesNames.length];
    for(int i=0; i<tcCstAttributes.length; ++i){
      sourceAttr[i] = MyUtil.getJTextOrEmptyString(tcCstAttributes[i]);
    }
    targetDB = cbTargetDBSelection.getSelectedItem().toString();
    targetDBPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(targetDB);
    setFieldNames(targetDBPluginMgr.getDBName());
    String datasetNameField = MyUtil.getNameField(targetDBPluginMgr.getDBName(), "dataset");
    String [] targetFields = targetDBPluginMgr.getFieldNames("dataset");
    String [] targetAttr = new String[targetFields.length];
    for(int j=0; j<targetFields.length; ++j){
      targetAttr[j] = "";
      //Do the mapping.
      for(int k=0; k<sourceFields.length; ++k){
        if(!sourceFields[k].equalsIgnoreCase("inputDataset") &&
            !sourceFields[k].equalsIgnoreCase("inputDB") &&
            sourceFields[k].equalsIgnoreCase(targetFields[j])){
          targetAttr[j] = sourceAttr[k];
          break;
        }
      }
      // Get values from source dataset in question, excluding
      // executableName and any other filled-in values.
      // Construct name for new target dataset.
      if(targetFields[j].equalsIgnoreCase(datasetExecutableReference[1]) ||
          targetFields[j].equalsIgnoreCase(datasetExecutableVersionReference[1])){
      }
      else if(datasetIDs!=null && datasetIDs.length==1 && targetFields[j].equalsIgnoreCase(datasetNameField)){
        targetAttr[j] = dbPluginMgr.createTargetDatasetName(null,
            targetDB, dbPluginMgr.getDatasetName(datasetIDs[0]), 0, 
            executableName, executableVersion);
      }
      //else if(targetFields[j].equalsIgnoreCase("runNumber")){
      //  targetAttr[j] = dbPluginMgr.getRunNumber(datasetIDs[0]);
      //}
      else if(targetFields[j].equalsIgnoreCase("InputDataset")){
        targetAttr[j] = dbPluginMgr.getDatasetName(datasetIDs[0]);
      }
      else if(targetFields[j].equalsIgnoreCase("InputDB")){
        targetAttr[j] = dbPluginMgr.getDBName();
      }
      else if(targetFields[j].equalsIgnoreCase(datasetIdentifier) ||
          targetFields[j].equalsIgnoreCase("percentageValidatedFiles") ||
          targetFields[j].equalsIgnoreCase("percentageFailedFiles ") ||
          //targetFields[j].equalsIgnoreCase("totalFiles") ||
          //targetFields[j].equalsIgnoreCase("totalEvents") ||
          targetFields[j].equalsIgnoreCase("averageEventSize") ||
          targetFields[j].equalsIgnoreCase("totalDataSize") ||
          targetFields[j].equalsIgnoreCase("averageCPUTime") ||
          targetFields[j].equalsIgnoreCase("totalCPUTime") ||
          targetFields[j].equalsIgnoreCase("created") ||
          targetFields[j].equalsIgnoreCase("lastModified") ||
          targetFields[j].equalsIgnoreCase("lastStatusUpdate") ||
          targetFields[j].equalsIgnoreCase("metaData")){
        targetAttr[j] = "";
      }
      // For unset fields:
      // see if attribute is in target dataset and set. If not, ignore.
      else if(targetAttr[j]==null || targetAttr[j].equals("")){
        boolean fieldPresent = false;
        DBRecord inputDataset0 = dbPluginMgr.getDataset(datasetIDs[0]);
        for(int l=0; l<sourceFields.length; ++l){
          if(targetFields[j].equalsIgnoreCase(sourceFields[l])){
            fieldPresent = true;
            break;
          }
        }
        if(fieldPresent){
          try{
            if(inputDataset0.getValue(targetFields[j])!=null){
              targetAttr[j] = inputDataset0.getValue(targetFields[j]).toString();
            }
          }
          catch(Exception e){
            e.printStackTrace();
          }
        }
        // Default to -1 for totalFiles and totalEvents
        if(!editing && (targetFields[j].equalsIgnoreCase("totalFiles") || targetFields[j].equalsIgnoreCase("totalEvents"))&&
            (targetAttr[j]==null || targetAttr[j].equals(""))){
          targetAttr[j] = "-1";
        }
      }
    }
    
    cstAttributesNames = targetFields;
    cstAttr = targetAttr;
    tcCstAttributes = null;
    Debug.debug("initAttributePanel", 3);
    initAttributePanel();
    
    for(int i=0; i<tcCstAttributes.length; ++i){
      Debug.debug("Setting "+targetFields[i]+"->"+targetAttr[i], 3);
      MyUtil.setJText(tcCstAttributes[i], targetAttr[i]);
      if((cstAttributesNames[i].equalsIgnoreCase("runNumber") ||
          cstAttributesNames[i].equalsIgnoreCase("InputDB") ||
          cstAttributesNames[i].equalsIgnoreCase("InputDataset") ||
          cstAttributesNames[i].equalsIgnoreCase(datasetExecutableReference[1]) ||
          cstAttributesNames[i].equalsIgnoreCase(datasetExecutableVersionReference[1]))){
        try{
          MyUtil.setJEditable(tcCstAttributes[i], false);
        }
        catch(java.lang.Exception e){
          Debug.debug("Attribute not found, "+e.getMessage(),1);
        }
      }
    }

    executables = targetDBPluginMgr.getExecutables();    
    Debug.debug("initAttributePanel done, setting values, "+
        targetAttr.length+", "+tcCstAttributes.length, 3);
    initExecutablePanel();
        
    if(!detailsVisible){
      showDetails(false);
    }
    else{
      if(!editing && checkForInputDataset()){
        instructionsLabel.setVisible(true);
      }
    }
    
    pExe.updateUI();
    
  }
  
  private boolean detailsShown(){
    return detailFields.iterator().next().isVisible();
  }
  
  public void showDetails(boolean show){
    JComponent comp;
    for(Iterator<JComponent> it=detailFields.iterator(); it.hasNext(); ){
      comp = it.next();
      Debug.debug((show?"showing ":"hiding ")+MyUtil.getJTextOrEmptyString(comp), 2);
      comp.setVisible(show);
    }
    if(!editing && show && checkForInputDataset()){
      instructionsLabel.setVisible(true);
    }
    else{
      instructionsLabel.setVisible(false);
    }
  }
  
  private boolean checkForInputDataset(){
    Debug.debug("Checking if targetDBPluginMgr contains inputDataset. "+
        (targetDBPluginMgr==null?"null":targetDBPluginMgr.getDBName()), 2);
    if(targetDBPluginMgr==null){
      return false;
    }
    String [] datasetFieldArray = targetDBPluginMgr.getFieldnames("dataset");  
    boolean ret = MyUtil.arrayContainsIgnoreCase(datasetFieldArray, "inputDataset");
    Debug.debug("targetDBPluginMgr "+(ret?"contains ":"")+"inputDataset", 2);
    return ret;
  }
  
  public String createDatasetInstructionsLabelString(){
    String instructionLabelString = "";
    instructionLabelString += "sanitized/raw input dataset name: $n/$r";
    instructionLabelString += ", number of input files: $f";
    instructionLabelString += ", number of input events: $e";
    return instructionLabelString;
  }

}
