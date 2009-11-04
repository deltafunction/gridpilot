package gridpilot;

import gridfactory.common.Debug;

import javax.swing.*;
import javax.swing.border.*;
import java.awt.*;

import javax.swing.text.*;

import java.util.*;

/**
 * This panel creates job definitions in the selected database.
 * It is shown inside a CreateEditDialog.
 *
 */
public class JobCreationPanel extends CreateEditPanel{

  private static final long serialVersionUID = 1L;
  private DBPluginMgr dbPluginMgr = null;
  private String [] datasetIDs = new String [] {"-1"};
  private JPanel pDataset = new JPanel();
  private JPanel pAttributes = new JPanel();
  private JScrollPane spAttributes = new JScrollPane();
  private String [] cstAttributesNames;
  private String [] jobParamNames;
  private String [] outputMapNames;
  private JTextComponent [] tcCstAttributes;
  private JTextComponent [] tcJobParam;
  private JTextComponent [][] tcOutputMap;
  private JTextComponent [] tcStdOutput;
  private boolean reuseTextFields = true;
  private Vector<JComponent> tcConstant = new Vector<JComponent>(); // contains all text components
  private String dbName = null;
  private ArrayList<JComponent> detailFields = new ArrayList<JComponent>();
  private ArrayList<JComponent> datasetFields = null;
  private HashMap metaData = null;
  // TODO: make this configurable?
  private String [] stdOutputNames = {"stdout", "stderr"};
  
  private static String LOCAL_NAME_LABEL = " : Local name : ";
  private static String REMOTE_NAME_LABEL = " -> Destination : ";
  private static String LABEL_END = " : ";
  private boolean closeWhenDone;

  // TODO: use JobMgr, move some functionality from here to there.
  
  /**
   * Constructor
   */
  public JobCreationPanel(DBPluginMgr _dbPluginMgr, String [] columnNames, String [] _datasetIDs,
      boolean _closeWhenDone){
    
    dbPluginMgr = _dbPluginMgr;
    dbName = dbPluginMgr.getDBName();
    closeWhenDone = _closeWhenDone;
    
    String jobDefinitionIdentifier = MyUtil.getIdentifierField(
        dbPluginMgr.getDBName(), "jobDefinition");
    String [] datasetFieldArray = dbPluginMgr.getFieldnames("dataset");
    
    for(int i=0; i<datasetFieldArray.length; ++i){
      datasetFieldArray[i] = datasetFieldArray[i].toLowerCase();
    }
    datasetFields = new ArrayList(Arrays.asList(datasetFieldArray));        
    // Find identifier index
    int identifierIndex = -1;
    for(int i=0; i<columnNames.length; ++i){
      Debug.debug("Column name: "+columnNames.length+":"+i+" "+columnNames[i], 3);
      if(columnNames[i].equalsIgnoreCase(jobDefinitionIdentifier)){
        identifierIndex = i;
        break;
      }
    }
    if(identifierIndex ==-1){
      Debug.debug("ERROR: could not find index of dataset, "+jobDefinitionIdentifier, 1);
    }

    // Dataset(s) selected and not editing - creating from dataset(s)
    if(_datasetIDs.length>0){
      Debug.debug("Creating job(s) for datasets "+MyUtil.arrayToString(_datasetIDs), 3);
    } 

    initGUI(_datasetIDs);
  }

  /**
   * GUI initialisation with dataset ids
   */
  public void initGUI(String [] _datasetIDs){
    
    datasetIDs = _datasetIDs;

    setLayout(new GridBagLayout());
    removeAll();
    String title="";
    if(datasetIDs.length==1){
      if(datasetIDs[0].equals("-1")){
        title = "Define new job record";
      }
      else{
        title = "Define job records for dataset ";
      }
    }
    else{
      title = "Define job records for datasets ";
    }
    if(datasetIDs.length>1 || !datasetIDs[0].equals("-1")){
      for(int i=0; i<datasetIDs.length; ++i){
        if(i>0){
          title += " ";
        }
        title += dbPluginMgr.getDatasetName(datasetIDs[i]);
        if(title.length()>100){
          title += " ... ";
          break;
        }
      }
    }
    setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white,new Color(165, 163, 151)),title));
    
    Debug.debug("Initializing job creation for dataset(s) "+MyUtil.arrayToString(datasetIDs), 2);
    
    initJobCreationPanel();
  }

  /**
   * Initialises text fields with attributes for job definition
   */
  private void initJobCreationPanel(){
   
    pDataset.setLayout(new GridBagLayout());
    pDataset.removeAll();
    String instructionLabelString = "dataset name: $n";
    if(datasetFields.contains("runnumber")){
      instructionLabelString += ", run number: $r";
    }
    if(datasetFields.contains("beamenergy")){
      instructionLabelString += ", energy: $e";
    }
    if(datasetFields.contains("outputlocation")){
      instructionLabelString += ", output destination: $o";
    }
    if(datasetFields.contains("inputdataset")){
      instructionLabelString += ", input file name(s): $f";
      instructionLabelString += ", input file URL(s): $u";
      instructionLabelString += ", input path: $p";
    }
    instructionLabelString += ", iterator: $i";
    detailFields.add(new JLabel(instructionLabelString));
    pDataset.add((JLabel) detailFields.get(detailFields.size()-1),
        new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
            GridBagConstraints.CENTER,
            GridBagConstraints.NONE, new Insets(10, 10, 10, 10), 0, 0));
    add(pDataset,
        new GridBagConstraints(0, 1, 1, 1, 0.0, 0.01,
            GridBagConstraints.NORTH,
            GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    pAttributes.setLayout(new GridBagLayout());
    pAttributes.removeAll();
    spAttributes.getViewport().add(pAttributes, null);
    add(spAttributes,
        new GridBagConstraints(0, 3, 2, 1, 0.9, 0.9,
            GridBagConstraints.CENTER,
            GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    initAttributePanel();
  }

  private void initAttributePanel(){
    
    cstAttributesNames = GridPilot.FIXED_JOB_ATTRIBUTES;
    ArrayList jobDefinitionFields = new ArrayList(Arrays.asList(dbPluginMgr.getFieldnames("jobDefinition")));    
    
    String executableID = dbPluginMgr.getExecutableID(
        dbPluginMgr.getDatasetExecutableName(datasetIDs[0]),
        dbPluginMgr.getDatasetExecutableVersion(datasetIDs[0]));
    jobParamNames = dbPluginMgr.getExecutableJobParameters(executableID);
    outputMapNames = dbPluginMgr.getExecutableOutputs(executableID);

    Debug.debug("Fixed job attributes: "+MyUtil.arrayToString(cstAttributesNames), 3);
    if(!reuseTextFields || tcCstAttributes==null ||
        tcCstAttributes.length != cstAttributesNames.length){
      tcCstAttributes = new JTextComponent[cstAttributesNames.length];
    }
    if(!reuseTextFields || tcJobParam==null || tcJobParam.length!=jobParamNames.length){
      tcJobParam = new JTextComponent[jobParamNames.length];
    }
    if(!reuseTextFields || tcStdOutput==null ||
        tcStdOutput.length!=stdOutputNames.length){
      tcStdOutput = new JTextComponent[stdOutputNames.length];
    }
    if(!reuseTextFields || tcOutputMap==null || tcOutputMap.length!=outputMapNames.length){
      tcOutputMap = new JTextComponent[outputMapNames.length] [2] ;
    }

    int row = 0;

    // Constant attributes
    Debug.debug("Job attributes: "+MyUtil.arrayToString(cstAttributesNames), 3);
    detailFields.add(new JLabel("Fixed job parameters"));
    pAttributes.add((JLabel) detailFields.get(detailFields.size()-1),
        new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
            GridBagConstraints.CENTER,
            GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));
    ++row;
    
    // take out attributes that are not in the schema and issue a warning
    ArrayList realAttributes = new ArrayList();
    for(int i=0; i<cstAttributesNames.length; ++i, ++row){
      if(jobDefinitionFields.contains(cstAttributesNames[i].toLowerCase())){
        realAttributes.add(cstAttributesNames[i]);
      }
      else{
        GridPilot.getClassMgr().getLogFile().addMessage("WARNING: Field "+" "+cstAttributesNames[i]+" NOT in schema.");
      } 
    }
    cstAttributesNames = new String[realAttributes.size()];
    for(int i=0; i<realAttributes.size(); ++i){
      cstAttributesNames[i] = realAttributes.get(i).toString();
    }
    
    JLabel [] constantAttributeLabels = new JLabel [cstAttributesNames.length];
    boolean isInMetadata = false;
    // metadata information from the metadata field of the dataset.
    // We display nothing, jobCreator will take care of filling in
    // - IF the field is left empty
    String metaDataString = (String) dbPluginMgr.getDataset(datasetIDs[0]).getValue("metaData");
    metaData = MyUtil.parseMetaData(metaDataString);    
    for(int i=0; i<cstAttributesNames.length; ++i, ++row){
      isInMetadata = false;
      constantAttributeLabels[i] = new JLabel(cstAttributesNames[i] + LABEL_END);
      pAttributes.add(constantAttributeLabels[i],
          new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.BOTH, new Insets(5, 25, 5, 5), 0, 0));

      if(!reuseTextFields || tcCstAttributes[i]==null){
        tcCstAttributes[i] = MyUtil.createTextArea();
      }
      if(metaData!=null && metaData.containsKey(cstAttributesNames[i].toLowerCase())){
        isInMetadata = true;
      }
      if(cstAttributesNames[i].equalsIgnoreCase("name")){
        // Set the name of the job description to <dataset name>.<number>
        detailFields.add(tcCstAttributes[i]);
        detailFields.add(constantAttributeLabels[i]);
        tcCstAttributes[i].setText("$n.${i:5}");
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("number")){
        detailFields.add(tcCstAttributes[i]);
        detailFields.add(constantAttributeLabels[i]);
        tcCstAttributes[i].setText("${i:5}");
      }
      else if(cstAttributesNames[i].equalsIgnoreCase(JobCreator.EVENT_MIN) ||
          cstAttributesNames[i].equalsIgnoreCase(JobCreator.EVENT_MAX) ||
          cstAttributesNames[i].equalsIgnoreCase(JobCreator.N_EVENTS)){
        detailFields.add(tcCstAttributes[i]);
        detailFields.add(constantAttributeLabels[i]);
      }
      else if(datasetFields.contains(cstAttributesNames[i].toLowerCase())){
        detailFields.add(tcCstAttributes[i]);
        detailFields.add(constantAttributeLabels[i]);
        tcCstAttributes[i].setText("$"+datasetFields.indexOf(cstAttributesNames[i].toLowerCase()));
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("created") ||
          cstAttributesNames[i].equalsIgnoreCase("lastModified")){
        MyUtil.setJEditable(tcCstAttributes[i], false);
      }
      // TODO: disable also fields filled out by GridPilot and runtime fields
      else if(datasetFields.contains(cstAttributesNames[i].toLowerCase())){
        detailFields.add(tcCstAttributes[i]);
        detailFields.add(constantAttributeLabels[i]);
        tcCstAttributes[i].setText("$"+datasetFields.indexOf(cstAttributesNames[i].toLowerCase()));
      }
      
      // Override field values by metadata settings
      if(isInMetadata){
        tcCstAttributes[i].setText((String) metaData.get(cstAttributesNames[i].toLowerCase()));
      }

      pAttributes.add(tcCstAttributes[i], new GridBagConstraints(1, row, 3, 1, 1.0, 0.0,
          GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
          new Insets(5, 5, 5, 5), 0, 0));
    }
    
    // Job parameters
    detailFields.add(new JLabel("Executable parameters"));
    pAttributes.add((JLabel) detailFields.get(detailFields.size()-1),
        new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
            GridBagConstraints.CENTER,
            GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));
    ++row;

    JLabel [] jobAttributeLabels = new JLabel[jobParamNames.length];

    for(int i=0; i<jobParamNames.length; ++i, ++row){
      isInMetadata = false;
      if(metaData!=null && metaData.containsKey(jobParamNames[i].toLowerCase())){
        isInMetadata = true;
      }
      jobAttributeLabels[i] = new JLabel(jobParamNames[i] + LABEL_END);
      pAttributes.add(jobAttributeLabels[i],
          new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.BOTH, new Insets(5, 25, 5, 5), 0, 0));
      if(!reuseTextFields || tcJobParam[i]==null){
        tcJobParam[i] = MyUtil.createTextArea();
      }
      // The following fields will be set by JobCreator.
      // They are rather specific to HEP, but if they are
      // not in the jobDefinition schema, it is no problem;
      // this functionality will simply not be used.
      // Special abbreviations are provided for:
      // min, max and number of events, input and output file names,
      // random seed, beam energy and particle.
      // Other fields that match dataset fields will be set to
      // $0, $1, ... according to their place in the schema
      // and these variables will be parsed accordingly by JobCreator.
      if(jobParamNames[i].equalsIgnoreCase("nEvents") ||
          jobParamNames[i].equalsIgnoreCase("eventMin") ||
          jobParamNames[i].equalsIgnoreCase("eventMax") ||
          jobParamNames[i].equalsIgnoreCase("inputFileURLs") ||
          jobParamNames[i].equalsIgnoreCase("inputFileNames")){
        detailFields.add(jobAttributeLabels[i]);
        detailFields.add(tcJobParam[i]);
        tcJobParam[i].setText("");
        // Override field values by metadata settings
        if(isInMetadata){
          tcJobParam[i].setText((String) metaData.get(jobParamNames[i].toLowerCase()));
        }
      }
      // The metaData field of the job definition can be used to store
      // default settings for the fields of jobCreation.
      else if(metaData!=null && metaData.containsKey(jobParamNames[i].toLowerCase())){
        detailFields.add(jobAttributeLabels[i]);
        detailFields.add(tcJobParam[i]);
        tcJobParam[i].setText((String) metaData.get(jobParamNames[i].toLowerCase()));
      }
      else if(jobParamNames[i].equalsIgnoreCase("castorInput")){
        detailFields.add(jobAttributeLabels[i]);
        detailFields.add(tcJobParam[i]);
        tcJobParam[i].setText(".");
      }
      else if(jobParamNames[i].equalsIgnoreCase("randNum") ||
      		jobParamNames[i].equalsIgnoreCase("randNumber") ||
            jobParamNames[i].equalsIgnoreCase("randomNumber")){
        detailFields.add(jobAttributeLabels[i]);
        detailFields.add(tcJobParam[i]);
        tcJobParam[i].setText("${i:5}");
      }
      else if(jobParamNames[i].equalsIgnoreCase("outputFileName")){
        String extension = ".root";
        if(outputMapNames!=null && !outputMapNames.equals("")){
          String [] fullNameStrings = MyUtil.split(outputMapNames[0], "\\.");
          if(fullNameStrings.length>0){
            extension = "."+fullNameStrings[fullNameStrings.length-1];
          }
        }
        detailFields.add(jobAttributeLabels[i]);
        detailFields.add(tcJobParam[i]);
        tcJobParam[i].setText("$n.${i:5}"+extension);
      }
      else if(jobParamNames[i].equalsIgnoreCase("runNumber")){
      // get from dataset
      detailFields.add(jobAttributeLabels[i]);
      detailFields.add(tcJobParam[i]);
      tcJobParam[i].setText("$r"); 
      }
      else if(jobParamNames[i].equalsIgnoreCase("beamEnergy")){
        // get from dataset
        detailFields.add(jobAttributeLabels[i]);
        detailFields.add(tcJobParam[i]);
        tcJobParam[i].setText("$e");
      }
      else if(jobParamNames[i].equalsIgnoreCase("beamParticle")){
        // get from dataset
        detailFields.add(jobAttributeLabels[i]);
        detailFields.add(tcJobParam[i]);
        tcJobParam[i].setText("$p");
      }

      pAttributes.add(tcJobParam[i],
          new GridBagConstraints(1, row, 3, 1, 1.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
    }

    // OutputMapping
    // Read from metadata if there
    isInMetadata = false;
    String [] outputMapValues = null;
    if(metaData!=null && metaData.containsKey("outfilemapping")){
      try{
        String om = metaData.get("outfilemapping").toString();
        String [] outMap = MyUtil.splitUrls(om);
        String [] outputMapNames = new String [outMap.length/2];
        outputMapValues = new String [outMap.length/2];
        Debug.debug("Got outfilemapping: "+MyUtil.arrayToString(outMap, "-->"), 2);
        for(int i=0; i<outMap.length/2; ++i){
          Debug.debug(i+":"+outMap.length, 2);
          outputMapNames[i] = outMap[2*i];
          outputMapValues[i] = outMap[2*i+1];
        }
        isInMetadata = true;
      }
      catch(Exception e) {
         e.printStackTrace();
      }
    }
    pAttributes.add(new JLabel("Output mapping"),
        new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
            GridBagConstraints.CENTER,
            GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));
    ++row;

    String extension = "";
    String [] fullNameStrings;
    for(int i=0; i<outputMapNames.length; ++i, ++row){
      pAttributes.add(new JLabel(outputMapNames[i] + LOCAL_NAME_LABEL),
          new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.BOTH, new Insets(5, 25, 5, 5), 0, 0));

      if(!reuseTextFields || tcOutputMap[i][0]==null){
        tcOutputMap[i][0] = MyUtil.createTextArea();
      }
      tcOutputMap[i][0].setText(outputMapNames[i]);
      tcOutputMap[i][0].setEnabled(false);
      
      Debug.debug("Finding extension from "+outputMapNames[i], 3);
      fullNameStrings = MyUtil.split(outputMapNames[i], "\\.");
      if(fullNameStrings.length>0){
        extension = "."+fullNameStrings[fullNameStrings.length-1];
        Debug.debug("Extension: "+extension, 3);
      }

      pAttributes.add(tcOutputMap[i][0],
          new GridBagConstraints(1, row, 1, 1, 1.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));

      pAttributes.add(new JLabel(REMOTE_NAME_LABEL),
          new GridBagConstraints(2, row, 1, 1, 0.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));

      if(!reuseTextFields || tcOutputMap[i][1]==null){
        tcOutputMap[i][1] = MyUtil.createTextArea();
      }
      
      if(isInMetadata){
        tcOutputMap[i][1].setText(outputMapValues[i].replaceFirst("\"\"", ""));
      }
      else{
        tcOutputMap[i][1].setText("$o/$n.${i:5}"+extension);
      }
      tcOutputMap[i][1].setEnabled(false);

      pAttributes.add(tcOutputMap[i][1],
          new GridBagConstraints(3, row, 1, 1, 1.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
    }

    // Log files
    if(stdOutputNames.length>0){
      pAttributes.add(new JLabel("Log files"),
          new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));
      ++row;
    }

    for(int i=0; i<stdOutputNames.length; ++i, ++row){
      pAttributes.add(new JLabel(stdOutputNames[i] + LABEL_END),
          new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.BOTH, new Insets(5, 25, 5, 5), 0, 0));

      if(!reuseTextFields || tcStdOutput[i]==null){
        tcStdOutput[i] = MyUtil.createTextArea();
      }

      if(metaData!=null && metaData.containsKey(stdOutputNames[i])){
        tcStdOutput[i].setText((String) metaData.get(stdOutputNames[i]));
      }
      else{
        tcStdOutput[i].setText("$o/$n.${i:5}."+stdOutputNames[i]);
      }
      tcStdOutput[i].setEnabled(false);

      pAttributes.add(tcStdOutput[i],
          new GridBagConstraints(1, row, 3, 1, 1.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
    }
    for(int i=0; i<detailFields.size(); ++i){
      ((JComponent) detailFields.get(i)).setVisible(false);
    }
  }

  public void clear(){
    Vector textFields = getTextFields();
    for(int i =0; i<textFields.size(); ++i){
      ((JTextComponent) textFields.get(i)).setText("");
    }
  }


  public void create(final boolean showResults, boolean editing) {
    
    setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));

    if(editing){
      Debug.debug("edit job definition", 1);
    }
    else{
      Debug.debug("create job definition", 1);
    }
    if(editing){
      return;
    }

    final String [] cstAttr = new String[tcCstAttributes.length];
    final String [] jobParam = new String[tcJobParam.length];
    final String [][] outMap = new String[tcOutputMap.length][2];
    final String [] stdOut = new String[tcStdOutput.length];


    for(int i=0; i<cstAttr.length; ++i){
      cstAttr[i] = tcCstAttributes[i].getText();
    }
    for(int i=0; i<jobParam.length; ++i){
      jobParam[i] = tcJobParam[i].getText();
    }
    for(int i=0; i<outMap.length; ++i){
      outMap[i][0] = tcOutputMap[i][0].getText();
      outMap[i][1] = tcOutputMap[i][1].getText();
    }
    for(int i=0; i<stdOut.length; ++i){
      stdOut[i] = tcStdOutput[i].getText();
    }
          
    new JobCreator(statusBar,
                   dbName,
                   datasetIDs,
                   showResults,
                   tcConstant,
                   cstAttr,
                   jobParam,
                   outMap,
                   stdOut,
                   cstAttributesNames,
                   jobParamNames,
                   outputMapNames,
                   stdOutputNames,
                   closeWhenDone,
                   SwingUtilities.getWindowAncestor(this));
  }

  private Vector getTextFields(){
    Vector v = new Vector();
    v.addAll(tcConstant);
    for(int i=0; i<tcCstAttributes.length; ++i){
      v.add(tcCstAttributes[i]);
    }
    for(int i=0; i<tcJobParam.length ; ++i){
      v.add(tcJobParam[i]);
    }
    for(int i=0; i<tcOutputMap.length ; ++i){
      v.add(tcOutputMap[i][0]);
      v.add(tcOutputMap[i][1]);
    }
    for(int i=0; i<tcStdOutput.length ; ++i){
      v.add(tcStdOutput[i]);
    }
    Debug.debug(v.toString(),3);
    return v;
  }
  
  public void showDetails(boolean show){
    for(Iterator it=detailFields.iterator(); it.hasNext(); ){
      ((JComponent) it.next()).setVisible(show);
    }
    for(int i=0; i<tcOutputMap.length; ++i){
      tcOutputMap[i][1].setEnabled(show);
    }
    for(int i=0; i<tcStdOutput.length; ++i){
      tcStdOutput[i].setEnabled(show);
    }
  }
  
  public void saveSettings(){
    HashMap settings = new HashMap();
    // get all field:values from the form
    String field = null;
    String value = null;
    Vector outMapping = new Vector();
    for(int i=0; i<pAttributes.getComponentCount()-1; ++i){
      field = null;
      value = null;
      if(pAttributes.getComponent(i).getClass().isInstance(new JLabel()) &&
          (pAttributes.getComponent(i+1).getClass().isInstance(new JTextField()) ||
              pAttributes.getComponent(i+1).getClass().isInstance(MyUtil.createTextArea()))){
        field = ((JLabel) pAttributes.getComponent(i)).getText().replaceFirst(LABEL_END+"$", "");
        Debug.debug("field: "+field, 3);
        value = MyUtil.getJTextOrEmptyString((JComponent) pAttributes.getComponent(i+1));
        // outputMapping, <>:<local name>:<>:<remote name>
        if(((JLabel) pAttributes.getComponent(i)).getText().endsWith(LOCAL_NAME_LABEL)){
          field = null;
          outMapping.add(value);
        }
        else if(((JLabel) pAttributes.getComponent(i)).getText().equals(REMOTE_NAME_LABEL)){
          field = null;
          if(value!=null && value.equals("")){
            outMapping.add("\"\"");
          }
          else{
            outMapping.add(value);
          }
        }
        // Normal, field\::value
        if(field!=null && !field.equals("") && value!=null/* && !value.equals("")*/){
          settings.put(field, value);
        }
      }
      if(!outMapping.isEmpty()){
        value = MyUtil.arrayToString(outMapping.toArray(), " ");
        if(!value.trim().equals("")){
          settings.put("outFileMapping", value);
        }
      }
    }
    String origMetadata = (String) dbPluginMgr.getDataset(datasetIDs[0]).getValue("metaData");
    String nonFieldValueMetadata = MyUtil.getMetadataComments(origMetadata);
    String newMetadata = nonFieldValueMetadata;
    String key = null;
    for(Iterator it=settings.keySet().iterator(); it.hasNext();){
      key = (String) it.next();
      newMetadata += key + ": " + settings.get(key) + "\n";
    }
    Debug.debug("Saving jobDefinition defaults in dataset: "+newMetadata, 1);
    // It should be safe to use null for datasetName, as ATLAS is the only DB that uses it.
    dbPluginMgr.updateDataset(datasetIDs[0], null, new String [] {"metaData"}, new String [] {newMetadata});
  }
  
}
