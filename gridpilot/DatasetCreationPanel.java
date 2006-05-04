package gridpilot;

import gridpilot.Debug;
import gridpilot.Database.DBRecord;

import javax.swing.*;
import javax.swing.border.*;

import java.awt.*;
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
  private Table table;
  private String [] cstAttributesNames;
  private JComponent [] tcCstAttributes;
  private boolean reuseTextFields = true;
  private Vector tcConstant = new Vector(); // contains all text components
  private String [] cstAttr = null;
  private boolean editing = false;
  private DBPluginMgr dbPluginMgr = null;
  private static int TEXTFIELDWIDTH = 32;
  private String transformationName = "";
  private String transformationVersion = "";
  private int [] datasetIDs = new int [] {-1};
  private JPanel pTransformation = new JPanel();
  private JPanel pTargetDBs = new JPanel();
  private JLabel jlTargetDBSelection = null;
  private JPanel pVersion = new JPanel();
  private JComboBox cbTargetDBSelection;
  private JComboBox cbTransformationSelection;
  private JComboBox cbTransVersionSelection;
  private String targetDB = null;
  private DBRecord dataset = null;
  private GridBagConstraints ct = new GridBagConstraints();
  private Database.DBResult transformations = null;

  /**
   * Constructor
   */
  public DatasetCreationPanel(DBPluginMgr _dbPluginMgr, DBPanel _panel, boolean _editing){
    
    editing = _editing;
    panel = _panel;
    dbPluginMgr = _dbPluginMgr;
    table = panel.getTable();
    cstAttributesNames = dbPluginMgr.getFieldNames("dataset");
    cstAttr = new String[cstAttributesNames.length];
    datasetIdentifier = dbPluginMgr.getIdentifierField(
       dbPluginMgr.getDBName(), "dataset");
    transformations = dbPluginMgr.getTransformations();
    
    // Find identifier index
    int identifierIndex = -1;
    for(int i=0; i<table.getColumnNames().length; ++i){
      Debug.debug("Column name: "+table.getColumnNames().length+":"+i+" "+table.getColumnName(i), 3);
      if(table.getColumnName(i).equalsIgnoreCase(datasetIdentifier)){
        identifierIndex = i;
        break;
      }
    }
    if(identifierIndex ==-1){
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
      // Fill cstAttr from db
      Debug.debug("Getting dataset. "+editing, 1);     
      dataset = dbPluginMgr.getDataset(Integer.parseInt(datasetID));
    }
    // Creating; set empty dataset
    else{
      //TODO
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
  }

  /**
   * GUI initialisation
   */
  public void initGUI(){
    String datasetName = "New dataset";
    if(editing){
      try{
        datasetName = dbPluginMgr.getDatasetName(Integer.parseInt(datasetID));
      }
      catch(Exception e){
        // nothing
      }
    }
    else if(GridPilot.dbs!=null && GridPilot.dbs.length!=0 &&
        datasetIDs!=null && datasetIDs.length!=0 &&
        (datasetIDs.length!=1 || datasetIDs[0]!=-1)){
      initTargetDBsPanel();
    }

    setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white,new Color(165, 163, 151)), datasetName));
    
    spAttributes.setPreferredSize(new Dimension(550, 500));
    spAttributes.setMinimumSize(new Dimension(550, 500));
    
    setLayout(new GridBagLayout());
    removeAll();
    
    initTransformationPanel(Integer.parseInt(datasetID));

    initAttributePanel();
    
    ct.fill = GridBagConstraints.VERTICAL;
    ct.insets = new Insets(2,2,2,2);
    
    ct.gridx = 0;
    ct.gridy = 1;   
    ct.gridwidth=2;
    ct.gridheight=1;
    add(spAttributes,ct);
    
    updateUI();
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
    
    if(!reuseTextFields || tcCstAttributes==null ||
        tcCstAttributes.length != cstAttributesNames.length){
      Debug.debug("Creating new tcCstAttributes, "+
          tcCstAttributes+", "+(tcCstAttributes==null ? "":Integer.toString(tcCstAttributes.length)),
              3);
      tcCstAttributes = new JComponent[cstAttributesNames.length];
    }
    
    for(int i =0; i<cstAttributesNames.length; ++i){
      
      if(cstAttributesNames[i].equalsIgnoreCase("actualPars") ||
          cstAttributesNames[i].equalsIgnoreCase("transFormalPars")){
        cl.gridx=0;
        cl.gridy=i;
        JTextArea textArea = new JTextArea(10, TEXTFIELDWIDTH);
        textArea.setLineWrap(true);
        textArea.setWrapStyleWord(true);
        textArea.setEditable(true);
        if(!reuseTextFields || tcCstAttributes[i]==null){
          tcCstAttributes[i] = textArea;
        }
        Util.setJText(tcCstAttributes[i], cstAttr[i]);
      }
      else{
        if(!editing && !reuseTextFields ||
            tcCstAttributes[i]==null){
          tcCstAttributes[i] = new JTextField("", TEXTFIELDWIDTH);
        }
        if(cstAttr[i]!=null && !cstAttr[i].equals("")){
          Debug.debug("Setting cstAttr["+i+"]: "+cstAttr[i], 3);
          Util.setJText(tcCstAttributes[i], cstAttr[i]);
        }
      }      
      cl.gridx=0;
      cl.gridy=i;
      pAttributes.add(new JLabel(cstAttributesNames[i] + " : "), cl);
      cl.gridx=1;
      cl.gridy=i;
      Debug.debug("Adding cstAttributesNames["+i+"], "+cstAttributesNames[i]+
          " "+tcCstAttributes[i].getClass().toString(), 3);
      pAttributes.add(tcCstAttributes[i], cl);//,

      // when creating, zap loaded dataset id
      if(!editing && cstAttributesNames[i].equalsIgnoreCase(datasetIdentifier)){
        Util.setJText((JComponent) tcCstAttributes[i], "");
        Util.setJEditable(tcCstAttributes[i], false);
      }
    }
    
    editDataset(Integer.parseInt(datasetID), transformationName, transformationVersion);
    
    for(int i =0; i<cstAttributesNames.length; ++i){
      if(cstAttributesNames[i].equalsIgnoreCase("transformationName")){
        Util.setJEditable(tcCstAttributes[i], false);
        if(transformationName!=null && transformationName!=null){
          cbTransformationSelection.setSelectedItem(transformationName);
        }
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("transformationVersion")){
        Util.setJEditable(tcCstAttributes[i], false);
        if(transformationVersion!=null && cbTransVersionSelection!=null){
          cbTransVersionSelection.setSelectedItem(transformationVersion);
        }
      }
    }
  }

  /*
   * Set special values - like values from xml tags
   */
  private void setValuesInAttributePanel(String transformation,
      String version){
    
    Debug.debug("Setting values "+transformation+":"+version+":"+transformationName+":"+transformationVersion, 3);

    if(transformation!=null && !transformation.equals("")){
      transformationName = transformation;
    }
    if(version!=null && !version.equals("")){
      transformationVersion = version;
    }
    
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
      else if(transformationName!=null && !transformationName.equals("") &&
          cstAttributesNames[i].equalsIgnoreCase("transformationName")){
        Util.setJText(tcCstAttributes[i], transformationName);
      }
      else if(transformationVersion!=null && !transformationVersion.equals("") &&
          cstAttributesNames[i].equalsIgnoreCase("transformationVersion")){
        Util.setJText(tcCstAttributes[i], transformationVersion);
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("targetDatabase")){
        // TODO
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("inputDataset")){
        // TODO
      }
      else{
        // nothing
      }
    }
  }
  
  public void clearPanel(){
    Vector textFields;
    if(editing){
      textFields = getFields(); 
    }
    else{
      textFields = getNonIdTextFields();
    }
    for(int i =0; i<textFields.size(); ++i){
      Util.setJText((JComponent) textFields.get(i),"");
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
      Debug.debug("edit dataset",  1);
    }
    else{
      Debug.debug("create dataset",  1);
    }
    
    final String [] cstAttr = new String[tcCstAttributes.length];
    
    if(editing){
      for(int i=0; i<cstAttr.length; ++i){
        cstAttr[i] = Util.getJTextOrEmptyString(tcCstAttributes[i]);
        Debug.debug("createDataset: cstAttr["+i+"]: "+cstAttr[i],  3);
      }
      new DatasetUpdater(
          dbPluginMgr,
          showResults,
          cstAttr,
          cstAttributesNames,
          Integer.parseInt(datasetID)
          );
      panel.refresh();
    }
    else{
      if(cbTargetDBSelection!=null && cbTargetDBSelection.getSelectedItem()!=null &&
          !cbTargetDBSelection.getSelectedItem().toString().equals("") &&
          panel.getSelectedIdentifiers().length>0){
        targetDB = cbTargetDBSelection.getSelectedItem().toString();
      }
      for(int i=0; i<cstAttr.length; ++i){
        cstAttr[i] = Util.getJTextOrEmptyString(tcCstAttributes[i]);
        Debug.debug("createDataset: cstAttr["+i+"]: "+cstAttr[i],  3);
      }
      new DatasetCreator(
          dbPluginMgr,
          showResults,
          cstAttr,
          cstAttributesNames,
          datasetIDs,
          targetDB
          );
      panel.refresh();
    }
  }

  /**
   *  Edit or create a dataset
   */
  public void editDataset(int datasetID,
      String transformation, String version){
    
    Debug.debug("editDataset: " + Integer.toString(datasetID) +
        " " + transformation + " " + version, 3); 
    Debug.debug("Got field names: "+
        Util.arrayToString(cstAttributesNames), 3);

    if(editing){
      // set values of fields
      Debug.debug("Got dataset record. "+dataset.fields.length+":"+
          tcCstAttributes.length, 3);   
      for(int i =0; i<tcCstAttributes.length; ++i){
        for(int j=0; j<dataset.fields.length;++j){
          //Debug.debug("Checking dataset field, "+dataset.fields[j].toString()+"<->"+cstAttributesNames[i].toString(), 3);
          if(dataset.fields[j].toString().equalsIgnoreCase(cstAttributesNames[i].toString())){
              try{
                Debug.debug(cstAttributesNames[i].toString()+"="+dataset.fields[j]+". Setting to "+
                    dataset.values[j].toString(),3);
                Util.setJText(tcCstAttributes[i], dataset.values[j].toString());
                
                if(cstAttributesNames[i].equalsIgnoreCase("transformationName")){
                  transformationName = dataset.values[j].toString();
                }
                else if(cstAttributesNames[i].equalsIgnoreCase("transformationVersion")){
                  transformationVersion = dataset.values[j].toString();
                }

              }
              catch(java.lang.Exception e){
                Debug.debug("Field not found or set, "+e.getMessage(),1);
              }
            break;
          }
        }
        // make identifier and transformation foreign key inactive
        if(cstAttributesNames[i].equalsIgnoreCase(datasetIdentifier) ||
              cstAttributesNames[i].equalsIgnoreCase("transformationName") ||
              cstAttributesNames[i].equalsIgnoreCase("transformationVersion")){          
          if(!cstAttributesNames[i].equalsIgnoreCase(datasetIdentifier) ||
              datasetID!=-1){
            Util.setJEditable(tcCstAttributes[i], false);
          }
          else{
            try{
              Debug.debug("Clearing identifier",3);
              Util.setJText(tcCstAttributes[i], "");
              Util.setJEditable(tcCstAttributes[i], true);
            }
            catch(java.lang.Exception e){
              Debug.debug("Field not found, "+e.getMessage(),1);
            }
          }
        }
        else{
          Util.setJEditable(tcCstAttributes[i], true);
        }
      }
    }
    setValuesInAttributePanel(transformation, version);
  }

  private String[] getTransNames(){
    String [] ret = new String[transformations.values.length];
    for(int i=0; i<transformations.values.length; ++i){
      ret[i] = transformations.getValue(i, "name").toString(); 
      Debug.debug("name is "+ret[i], 3);
    }
    // This is to ensure only unique elements
    // TODO: for some reason this doesn't seam to work
    Arrays.sort(ret);
    Vector vec = new Vector();
    if(transformations.values.length>0){
      vec.add(ret[0]);
    }
    if(transformations.values.length>1){
      for(int i=1; i<transformations.values.length; ++i){
        //Debug.debug("Comparing "+ret[i]+" <-> "+ret[i-1],3);
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
  
  private void initTransformationPanel(int datasetID){
    
    Debug.debug("Finding transformations...",3);

    pTransformation.removeAll();
    pTransformation.setLayout(new FlowLayout());

    String [] transformations = getTransNames();

    if(transformations.length==0){
      pTransformation.add(new JLabel("No transformations found."));
    }
    else if(transformations.length==1){
      transformationName = transformations[0];
      pTransformation.add(new JLabel("Transformation:" + transformationName));
      initTransVersionPanel(datasetID, transformationName);
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
    }
    
    ct.gridx = 0;
    ct.gridy = 0;
    ct.gridwidth=1;
    ct.gridheight=1;
    add(pTransformation, ct);

    updateUI();
  }

  private void initTransVersionPanel(int datasetID, String transformation){

    Debug.debug("Finding versions...",3);

    pVersion.removeAll();
    pVersion.setLayout(new FlowLayout());

    String [] versions = dbPluginMgr.getVersions(transformation);
    Debug.debug("Number of versions found: "+versions.length,3);

    if(versions.length==0){
      pVersion.add(new JLabel("No versions found."));
    }
    else if(versions.length==1){
      transformationVersion = versions[0];
      pVersion.add(new JLabel(/*"Version : " +*/ transformationVersion));
      //editDataset(datasetID, transformation, transformationVersion);
      setValuesInAttributePanel(transformation, transformationVersion);
    }
    else{
      cbTransVersionSelection = new JComboBox();

      for(int i=0; i<versions.length; ++i){
        Debug.debug("Adding version "+versions[i],3);
        cbTransVersionSelection.addItem(versions[i]);
      }

      //pVersion.add(new JLabel("Version : "), null);
      pVersion.add(cbTransVersionSelection, null);

      cbTransVersionSelection.addActionListener(
        new java.awt.event.ActionListener(){
          public void actionPerformed(java.awt.event.ActionEvent e){
            cbTransVersionSelection_actionPerformed();
          }
        }
      );
    }

    ct.gridx = 1;
    ct.gridy = 0;
    ct.gridwidth=1;
    ct.gridheight=1;
    add(pVersion, ct);

    updateUI();
  }

  private void cbTransformationSelection_actionPerformed(){
    if(cbTransformationSelection.getSelectedItem()==null){
        return;
    }
    else{
        transformationName = cbTransformationSelection.getSelectedItem().toString();
    }
    initTransVersionPanel(Integer.parseInt(datasetID), transformationName);
  }

  private void cbTransVersionSelection_actionPerformed(){
    if(cbTransVersionSelection.getSelectedItem()==null){
      return;
    }
    else{
      transformationVersion = cbTransVersionSelection.getSelectedItem().toString();
    }
    editDataset(Integer.parseInt(datasetID), transformationName, transformationVersion);
  }

  private Vector getFields(){
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
      if(!cstAttributesNames[i].equalsIgnoreCase(datasetIdentifier)){
        v.add(tcCstAttributes[i]);
      }
    }

    return v;
  }

  /*private Vector getIdTextFields(){
    Vector v = new Vector();

    v.addAll(tcConstant);

    for(int i=0; i<tcCstAttributes.length; ++i){
      if(cstAttributesNames[i].equalsIgnoreCase(datasetIdentifier)){
        v.add(tcCstAttributes[i]);
      }
    }

    return v;
  }*/

  private void initTargetDBsPanel(){
    
    Debug.debug("Finding target dataset databases...",3);
  
    pTargetDBs.removeAll();
    pTargetDBs.setLayout(new FlowLayout());
  
    cbTargetDBSelection = new JComboBox();
    
    cbTargetDBSelection.addItem("");
    for(int i=0; i<GridPilot.dbs.length; ++i){
      cbTargetDBSelection.addItem(GridPilot.dbs[i]);
     }

    jlTargetDBSelection = new JLabel("DB:");
    pTargetDBs.add(jlTargetDBSelection, null);
    pTargetDBs.add(cbTargetDBSelection, null);
    
    cbTargetDBSelection.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(java.awt.event.ActionEvent e){
        cbTargetDBSelection_actionPerformed();
    }});
  
    add(pTargetDBs, new GridBagConstraints(1, 0, 1, 1, 0.0, 0.01
        ,GridBagConstraints.NORTH, GridBagConstraints.BOTH,
        new Insets(0, 0, 0, 0), 0, 0));
      
    updateUI();
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
    if(datasetIDs[0]>0){
      return;
    }
    String [] sourceFields = cstAttributesNames;
    String [] sourceAttr = new String[cstAttributesNames.length];
    for(int i=0; i<tcCstAttributes.length; ++i){
      sourceAttr[i] = Util.getJTextOrEmptyString(tcCstAttributes[i]);
    }
    targetDB = cbTargetDBSelection.getSelectedItem().toString();
    String [] targetFields = GridPilot.getClassMgr().getDBPluginMgr(
        targetDB).getFieldNames("dataset");
    String [] targetAttr = new String[targetFields.length];
    for(int j=0; j<targetFields.length; ++j){ 
      targetAttr[j] = "";
      //Do the mapping.
      for(int k=0; k<sourceFields.length; ++k){
        if(sourceFields[k].equalsIgnoreCase(targetFields[j])){
          targetAttr[j] = sourceAttr[k];
          break;
        }
      }
      // Get values from source dataset in question, excluding
      // transformationName and any other filled-in values.
      // Construct name for new target dataset.
      if(targetFields[j].equalsIgnoreCase("transformationName") ||
          targetFields[j].equalsIgnoreCase("transformationVersion")){
      }
      else if(targetFields[j].equalsIgnoreCase(
          dbPluginMgr.getNameField(targetDB, "dataset"))){
        targetAttr[j] = dbPluginMgr.getTargetDatasetName(
            targetDB, dbPluginMgr.getDatasetName(datasetIDs[0]),
            transformationName, transformationVersion);
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
      else if(targetFields[j].equalsIgnoreCase("identifier") ||
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
          targetFields[j].equalsIgnoreCase("lastStatusUpdate")){
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
      }
    }
    
    cstAttributesNames = targetFields;
    tcCstAttributes = null;
    Debug.debug("initAttributePanel", 3);
    initAttributePanel();
    Debug.debug("initAttributePanel done, setting values, "+targetAttr.length+", "+tcCstAttributes.length, 3);
    for(int i=0; i<tcCstAttributes.length; ++i){
      Debug.debug("Setting "+targetFields[i]+"->"+targetAttr[i], 3);
      Util.setJText(tcCstAttributes[i], targetAttr[i]);
      if((cstAttributesNames[i].equalsIgnoreCase("runNumber") ||
          cstAttributesNames[i].equalsIgnoreCase("InputDataset") ||
          cstAttributesNames[i].equalsIgnoreCase("InputDB") ||
          cstAttributesNames[i].equalsIgnoreCase("transformationName") ||
          cstAttributesNames[i].equalsIgnoreCase("transVersion"))){         
        try{
          Util.setJEditable(tcCstAttributes[i], false);
        }
        catch(java.lang.Exception e){
          Debug.debug("Attribute not found, "+e.getMessage(),1);
        }
      }
    }
  }

}
