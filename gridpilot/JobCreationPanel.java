package gridpilot;

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
  private int [] datasetIDs = new int [] {-1};
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
  private Vector tcConstant = new Vector(); // contains all text components
  private String dbName = null;
  private ArrayList detailFields = new ArrayList();
  ArrayList datasetFields = null;
  // TODO: make this configurable?
  private String [] stdOutputNames = {"stdout", "stderr"};

  // TODO: use DatasetMgr, move some functionality from here to there.
  
  /**
   * Constructor
   */
  public JobCreationPanel(DBPluginMgr _dbPluginMgr, DBPanel panel){
    dbPluginMgr = _dbPluginMgr;
    dbName = dbPluginMgr.getDBName();
    Table table = panel.getTable();
    String jobDefinitionIdentifier = dbPluginMgr.getIdentifierField(
        "jobDefinition");
    datasetFields = new ArrayList(Arrays.asList(dbPluginMgr.getFieldnames("dataset")));        
    // Find identifier index
    int identifierIndex = -1;
    for(int i=0; i<table.getColumnNames().length; ++i){
      Debug.debug("Column name: "+table.getColumnNames().length+":"+i+" "+table.getColumnName(i), 3);
      if(table.getColumnName(i).equalsIgnoreCase(jobDefinitionIdentifier)){
        identifierIndex = i;
        break;
      }
    }
    if(identifierIndex ==-1){
      Debug.debug("ERROR: could not find index of dataset, "+jobDefinitionIdentifier, 1);
    }

    // Dataset(s) selected and not editing - creating from dataset(s)
    if(table.getSelectedRows().length>0){
      datasetIDs = panel.getSelectedIdentifiers();
      Debug.debug("Creating jobs for datasets "+Util.arrayToString(datasetIDs), 3);
    } 

    initGUI(datasetIDs);
  }

  /**
   * GUI initialisation with dataset ids
   */
  public void initGUI(int[] _datasetIDs){
    
    datasetIDs = _datasetIDs;

    setLayout(new GridBagLayout());
    removeAll();
    String title="";
    if(datasetIDs.length==1){
      if(datasetIDs[0]==-1){
        title = "Define new job record";
      }
      else{
        title = "Define job records for dataset ";
      }
    }
    else{
      title = "Define job records for datasets ";
    }
    if(datasetIDs.length>1 || datasetIDs[0] != -1){
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
    
    cstAttributesNames = GridPilot.fixedJobAttributes;
    ArrayList jobDefinitionFields = new ArrayList(Arrays.asList(dbPluginMgr.getFieldnames("jobDefinition")));    
    
    int transformationID = dbPluginMgr.getTransformationID(
        dbPluginMgr.getDatasetTransformationName(datasetIDs[0]),
        dbPluginMgr.getDatasetTransformationVersion(datasetIDs[0]));
    jobParamNames = dbPluginMgr.getTransJobParameters(transformationID);
    outputMapNames = dbPluginMgr.getTransOutputs(transformationID);

    Debug.debug("Fixed job attributes: "+Util.arrayToString(cstAttributesNames), 3);
    if(!reuseTextFields || tcCstAttributes == null ||
        tcCstAttributes.length != cstAttributesNames.length){
      tcCstAttributes = new JTextComponent[cstAttributesNames.length];
    }
    if(!reuseTextFields || tcJobParam == null || tcJobParam.length  != jobParamNames.length){
      tcJobParam = new JTextComponent[jobParamNames.length];
    }
    if(!reuseTextFields || tcStdOutput == null ||
        tcStdOutput.length!=stdOutputNames.length){
      tcStdOutput = new JTextComponent[stdOutputNames.length];
    }
    if(!reuseTextFields || tcOutputMap == null || tcOutputMap.length != outputMapNames.length){
      tcOutputMap = new JTextComponent[outputMapNames.length] [2] ;
    }

    int row = 0;

    // Constant attributes
    Debug.debug("Job attributes: "+Util.arrayToString(cstAttributesNames), 3);
    detailFields.add(new JLabel("Fixed job parameters"));
    pAttributes.add((JLabel) detailFields.get(detailFields.size()-1),
        new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
            GridBagConstraints.CENTER,
            GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));
    ++row;
    
    // take out attributes that are not in the schema and issue a warning
    ArrayList realAttibutes = new ArrayList();
    for(int i=0; i<cstAttributesNames.length; ++i, ++row){
      if(jobDefinitionFields.contains(cstAttributesNames[i].toLowerCase())){
        realAttibutes.add(cstAttributesNames[i]);
      }
      else{
        Debug.debug("WARNING: Field "+" "+cstAttributesNames[i]+" NOT in schema.", 1);
      } 
    }
    cstAttributesNames = new String[realAttibutes.size()];
    for(int i=0; i<realAttibutes.size(); ++i){
      cstAttributesNames[i] = realAttibutes.get(i).toString();
    }
    
    JLabel [] constantAttributeLabels = new JLabel [cstAttributesNames.length];
    for(int i=0; i<cstAttributesNames.length; ++i, ++row){
      constantAttributeLabels[i] = new JLabel(cstAttributesNames[i] + " : ");
      pAttributes.add(constantAttributeLabels[i],
          new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.BOTH, new Insets(5, 25, 5, 5), 0, 0));

      if(!reuseTextFields || tcCstAttributes[i] == null)
        tcCstAttributes[i] = createTextComponent();
      
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
      else if(datasetFields.contains(cstAttributesNames[i].toLowerCase())){
        detailFields.add(tcCstAttributes[i]);
        detailFields.add(constantAttributeLabels[i]);
        tcCstAttributes[i].setText("$"+datasetFields.indexOf(cstAttributesNames[i].toLowerCase()));
      }
      pAttributes.add(tcCstAttributes[i], new GridBagConstraints(1, row, 3, 1, 1.0, 0.0,
          GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
          new Insets(5, 5, 5, 5), 0, 0));
    }
    
    // Job parameters
    detailFields.add(new JLabel("Transformation job parameters"));
    pAttributes.add((JLabel) detailFields.get(detailFields.size()-1),
        new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
            GridBagConstraints.CENTER,
            GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));
    ++row;

    JLabel [] jobAttributeLabels = new JLabel[jobParamNames.length];
    for(int i=0; i<jobParamNames.length; ++i, ++row){
      jobAttributeLabels[i] = new JLabel(jobParamNames[i] + " : ");
      pAttributes.add(jobAttributeLabels[i],
          new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.BOTH, new Insets(5, 25, 5, 5), 0, 0));
      if(!reuseTextFields || tcJobParam[i] == null){
        tcJobParam[i] = createTextComponent();
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
          jobParamNames[i].equalsIgnoreCase("inputFileName")){
        detailFields.add(jobAttributeLabels[i]);
        detailFields.add(tcJobParam[i]);
        tcJobParam[i].setText("");
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
        detailFields.add(jobAttributeLabels[i]);
        detailFields.add(tcJobParam[i]);
        tcJobParam[i].setText("$n.${i:5}.root");
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
      else if( cstAttributesNames[i].equalsIgnoreCase("created") ||
          cstAttributesNames[i].equalsIgnoreCase("lastModified")){
        Util.setJEditable(tcCstAttributes[i], false);
      }
      // TODO: disable also fields filled out by GridPilot and runtime fields
      else if(datasetFields.contains(cstAttributesNames[i].toLowerCase())){
        detailFields.add(tcCstAttributes[i]);
        detailFields.add(constantAttributeLabels[i]);
        tcCstAttributes[i].setText("$"+datasetFields.indexOf(cstAttributesNames[i].toLowerCase()));
      }

      pAttributes.add(tcJobParam[i],
          new GridBagConstraints(1, row, 3, 1, 1.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
    }

    // OutputMapping
    pAttributes.add(new JLabel("Output mapping"),
        new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
            GridBagConstraints.CENTER,
            GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));
    ++row;

    String extension = "";
    String [] fullNameStrings;
    for(int i=0; i<outputMapNames.length; ++i, ++row){
      pAttributes.add(new JLabel(outputMapNames[i] + " : Local name : "),
          new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.BOTH, new Insets(5, 25, 5, 5), 0, 0));

      if(!reuseTextFields || tcOutputMap[i][0] == null){
        tcOutputMap[i][0] = createTextComponent();
      }
      tcOutputMap[i][0].setText(outputMapNames[i]);
      tcOutputMap[i][0].setEnabled(false);
      
      fullNameStrings = Util.split(outputMapNames[i], ".");
      if(fullNameStrings.length>0){
        extension = "."+fullNameStrings[fullNameStrings.length-1];
      }

      pAttributes.add(tcOutputMap[i][0],
          new GridBagConstraints(1, row, 1, 1, 1.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));

      pAttributes.add(new JLabel(" -> Remote name : "),
          new GridBagConstraints(2, row, 1, 1, 0.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));

      if(!reuseTextFields || tcOutputMap[i][1] == null)
        tcOutputMap[i][1] = createTextComponent();

      tcOutputMap[i][1].setText("$o/$n.${i:5}"+extension);
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
      pAttributes.add(new JLabel(stdOutputNames[i] + " : "),
          new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.BOTH, new Insets(5, 25, 5, 5), 0, 0));

      if(!reuseTextFields || tcStdOutput[i] == null)
        tcStdOutput[i] = createTextComponent();

      tcStdOutput[i].setText("$o/$n.${i:5}."+stdOutputNames[i]);
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
                   stdOutputNames);
  }

  private Vector getTextFields(){
    Vector v = new Vector();
    v.addAll(tcConstant);
    for(int i=0; i<tcCstAttributes.length; ++i)
      v.add(tcCstAttributes[i]);
    for(int i=0; i<tcJobParam.length ; ++i)
      v.add(tcJobParam[i]);
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

  private JTextComponent createTextComponent(){
    JTextArea ta = new JTextArea();
    ta.setBorder(new JTextField().getBorder());
    ta.setWrapStyleWord(true);
    ta.setLineWrap(true);
    return ta;
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
}
