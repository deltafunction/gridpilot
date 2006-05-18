package gridpilot;

import javax.swing.*;
import javax.swing.border.*;
import java.awt.*;
import javax.swing.text.*;
import java.util.*;

/**
 * This panel creates job definitions in the selected database.
 *
 */
public class JobCreationPanel extends CreateEditPanel{

  private DBPluginMgr dbPluginMgr = null;
  private StatusBar statusBar;
  private int [] datasetIDs = new int [] {-1};
  protected int partitionIdentifier;
  private String version;
  private JComboBox cbVersionSelection;
  private JComboBox cbReconNameSelection;
  private JPanel pDataset = new JPanel();
  private JPanel pConstants = new JPanel(new GridBagLayout());
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
  private boolean versionChosen = false;
  private boolean reconNameChosen = false;
  private Vector tcConstant = new Vector(); // contains all text components
  private String dbName = null;

  /**
   * Constructor
   */

  public JobCreationPanel(String _dbName){
    
    dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
    statusBar = GridPilot.getClassMgr().getStatusBar();
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
  }

  /**
   * Initialises text fields with attributes for job definition
   */
  private void initPartitionCreationPanel(){
   
    pDataset.setLayout(new GridBagLayout());
    pDataset.removeAll();
    pDataset.add(new JLabel("dataset name: $n, run number: $r, energy: $e, particle: $p, output destination: $o, iterator: $i"),
        new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0
        ,GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(10, 10, 10, 10), 0, 0));
    add(pDataset, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.01
        ,GridBagConstraints.NORTH, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    pAttributes.setLayout(new GridBagLayout());
    pAttributes.removeAll();
    spAttributes.getViewport().add(pAttributes, null);
    add(spAttributes,   new GridBagConstraints(0, 3, 2, 1, 0.9, 0.9
        ,GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    initAttributePanel();
  }

  private void initAttributePanel(){
    
    cstAttributesNames = dbPluginMgr.getConstantJobAttributes();
    
    jobParamNames = dbPluginMgr.getTransJobParameters(
        dbPluginMgr.getDatasetTransformationName(datasetIDs[0]),
        dbPluginMgr.getDatasetTransformationVersion(datasetIDs[0]));
    outputMapNames = dbPluginMgr.getTransOutputs(
        dbPluginMgr.getDatasetTransformationName(datasetIDs[0]),
        dbPluginMgr.getDatasetTransformationVersion(datasetIDs[0]));

    if(!reuseTextFields || tcCstAttributes == null || tcCstAttributes.length != cstAttributesNames.length)
      tcCstAttributes = new JTextComponent[cstAttributesNames.length];

    if(!reuseTextFields || tcJobParam == null || tcJobParam.length  != jobParamNames.length)
      tcJobParam = new JTextComponent[jobParamNames.length];

    if(!reuseTextFields || tcOutputMap == null || tcOutputMap.length != outputMapNames.length){
      tcOutputMap = new JTextComponent[outputMapNames.length] [2] ;
    }

    if(!reuseTextFields || tcStdOutput == null ||
        tcStdOutput.length != AtCom.stdOutputNames.length){
      tcStdOutput = new JTextComponent[AtCom.stdOutputNames.length];
    }

    int row = 0;

    // Constant attributes
    for(int i =0; i<cstAttributesNames.length; ++i, ++row){
      pAttributes.add(new JLabel(cstAttributesNames[i] + " : "),
          new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.BOTH, new Insets(5, 25, 5, 5), 0, 0));

      if(!reuseTextFields || tcCstAttributes[i] == null)
        tcCstAttributes[i] = createTextComponent();
      
      // These will be set by JobCreator
      if(cstAttributesNames[i].equalsIgnoreCase("eventMin") ||
         cstAttributesNames[i].equalsIgnoreCase("eventMax")||
         cstAttributesNames[i].equalsIgnoreCase("inputFileName")){
        tcCstAttributes[i].setEnabled(false);
        tcCstAttributes[i].setText("");
      }
      else if(cstAttributesNames[i].equalsIgnoreCase(dbPluginMgr.getNameField(dbName, "jobDefinition"))){
         tcCstAttributes[i].setEnabled(false);
         tcCstAttributes[i].setText("$n.${i:5}");
       }
      else if(cstAttributesNames[i].equalsIgnoreCase("number") ||
          // TODO: fill in what proddb uses for this.
          cstAttributesNames[i].equalsIgnoreCase("number")){
        tcCstAttributes[i].setEnabled(false);
        tcCstAttributes[i].setText("${i:5}");
      }
      pAttributes.add(tcCstAttributes[i], new GridBagConstraints(1, row, 3, 1, 1.0, 0.0,
          GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
          new Insets(5, 5, 5, 5), 0, 0));
    }
    
    // Job parameters
    pAttributes.add(new JLabel("Job parameters"), new GridBagConstraints(0, row, 1, 1, 0.0, 0.0
        ,GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));
    ++row;


    for(int i =0; i<jobParamNames.length; ++i, ++row){
      pAttributes.add(new JLabel(jobParamNames[i] + " : "), new GridBagConstraints(0, row, 1, 1, 0.0, 0.0
      ,GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 25, 5, 5), 0, 0));

      if(!reuseTextFields || tcJobParam[i] == null)
        tcJobParam[i] = createTextComponent();
      
      // These will be set by JobCreator
      if(jobParamNames[i].equalsIgnoreCase("NumEvents") ||
          jobParamNames[i].equalsIgnoreCase("StartAtEvent") ||
          jobParamNames[i].equalsIgnoreCase("evtNum") ||
          jobParamNames[i].equalsIgnoreCase("evtNumber")){
        tcJobParam[i].setEnabled(false);
        tcJobParam[i].setText("");
      }
      else if(jobParamNames[i].equalsIgnoreCase("castorInput")){
        tcJobParam[i].setEnabled(false);
        tcJobParam[i].setText(".");
      }
      else if(jobParamNames[i].equalsIgnoreCase("runNum") ||
      		jobParamNames[i].equalsIgnoreCase("runNumber")){
        // get from dataset
        tcJobParam[i].setEnabled(false);
        tcJobParam[i].setText("$r");
      }
      else if(jobParamNames[i].equalsIgnoreCase("randNum") ||
      		jobParamNames[i].equalsIgnoreCase("randNumber")){
        tcJobParam[i].setEnabled(false);
        tcJobParam[i].setText("${i:5}");
      }
      else if(jobParamNames[i].equalsIgnoreCase("outFileName")){
        tcJobParam[i].setEnabled(false);
        tcJobParam[i].setText("$n.${i:5}.root");
      }
      else if(jobParamNames[i].equalsIgnoreCase("inFileName")){
        tcJobParam[i].setEnabled(false);
        tcJobParam[i].setText("");
      }
      else if(jobParamNames[i].equalsIgnoreCase("energy")){
        // get from dataset
        tcJobParam[i].setEnabled(false);
        tcJobParam[i].setText("$e");
      }
      else if(jobParamNames[i].equalsIgnoreCase("particle")){
        // get from dataset
        tcJobParam[i].setEnabled(false);
        tcJobParam[i].setText("$p");
      }

      pAttributes.add(tcJobParam[i], new GridBagConstraints(1, row, 3, 1, 1.0, 0.0
          ,GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
    }

    //// OutputMapping

    pAttributes.add(new JLabel("Output mapping"), new GridBagConstraints(0, row, 1, 1, 0.0, 0.0
        ,GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));
    ++row;


    String extension = "";
    String [] fullNameStrings;
    for(int i =0; i<outputMapNames.length; ++i, ++row){
      pAttributes.add(new JLabel(outputMapNames[i] + " : Local name : "), new GridBagConstraints(0, row, 1, 1, 0.0, 0.0
      ,GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 25, 5, 5), 0, 0));

      if(!reuseTextFields || tcOutputMap[i][0] == null)
        tcOutputMap[i][0] = createTextComponent();
      
      tcOutputMap[i][0].setText(outputMapNames[i]);
      tcOutputMap[i][0].setEnabled(false);
      
      fullNameStrings = Utils.split(outputMapNames[i], ".");
      if(fullNameStrings.length>0){
        extension = "."+fullNameStrings[fullNameStrings.length-1];
      }

      pAttributes.add(tcOutputMap[i][0], new GridBagConstraints(1, row, 1, 1, 1.0, 0.0
          ,GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));

      pAttributes.add(new JLabel(" -> Remote name : "), new GridBagConstraints(2, row, 1, 1, 0.0, 0.0
          ,GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));

      if(!reuseTextFields || tcOutputMap[i][1] == null)
        tcOutputMap[i][1] = createTextComponent();

      tcOutputMap[i][1].setText("$o/$n.${i:5}"+extension);
      tcOutputMap[i][1].setEnabled(false);

      pAttributes.add(tcOutputMap[i][1], new GridBagConstraints(3, row, 1, 1, 1.0, 0.0
          ,GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
    }

//// Log files
    if(AtCom.stdOutputNames.length>0){
      pAttributes.add(new JLabel("Log files"), new GridBagConstraints(0, row, 1, 1, 0.0, 0.0
          ,GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));
      ++row;
    }

    for(int i=0; i<AtCom.stdOutputNames.length; ++i, ++row){
      pAttributes.add(new JLabel(AtCom.stdOutputNames[i] + " : "), new GridBagConstraints(0, row, 1, 1, 0.0, 0.0
      ,GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 25, 5, 5), 0, 0));

      if(!reuseTextFields || tcStdOutput[i] == null)
        tcStdOutput[i] = createTextComponent();

      tcStdOutput[i].setText("$o/$n.${i:5}."+AtCom.stdOutputNames[i]);
      tcStdOutput[i].setEnabled(false);

      pAttributes.add(tcStdOutput[i], new GridBagConstraints(1, row, 3, 1, 1.0, 0.0
          ,GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
    }

  }

  public void clear(){
    /**
     * Called by : JobDefinition.button_ActionPerformed()
     */

    if(!versionChosen && !reconNameChosen)
      return;

    Vector textFields = getTextFields();

    for(int i =0; i<textFields.size(); ++i)
      ((JTextComponent) textFields.get(i)).setText("");
  }


  public void createPartitions(final boolean showResults) {
    /**
     * Called when button Create is clicked in JobDefinition
     */

    if(!versionChosen && !reconNameChosen)
      return;

    Debug.debug("createPartition",  1);

    final String [] cstAttr = new String[tcCstAttributes.length];
    final String [] jobParam = new String[tcJobParam.length];
    final String [][] outMap = new String[tcOutputMap.length][2];
    final String [] stdOut = new String[tcStdOutput.length];


    for(int i=0; i< cstAttr.length; ++i)
      cstAttr[i] = tcCstAttributes[i].getText();
    for(int i=0; i< jobParam.length; ++i)
      jobParam[i] = tcJobParam[i].getText();
    for(int i=0; i< outMap.length; ++i){
      outMap[i][0] = tcOutputMap[i][0].getText();
      outMap[i][1] = tcOutputMap[i][1].getText();
    }
    for(int i=0; i< stdOut.length; ++i)
      stdOut[i] = tcStdOutput[i].getText();
          
    new JobCreator(false,datasetIDs, version,
                         showResults,
                         tcConstant,
                         cstAttr,
                         jobParam,
                         outMap,
                         stdOut,
                         cstAttributesNames,
                         jobParamNames,
                         outputMapNames,
                         AtCom.stdOutputNames
                         );

  }

  public void updatePartition(int identifier, final boolean showResults) {
	/**
	 * Called when button Update is clicked in JobDefinitionPanel
	 */

	final String [] cstAttr = new String[tcCstAttributes.length];

	for(int i=0; i< cstAttr.length; ++i){
	   cstAttr[i] = tcCstAttributes[i].getText();
	}

	Debug.debug("updatePartition",  1);

	new PartitionUpdater(
						 AtCom.amiPartition,
						 identifier,
						 showResults,
						 cstAttr,
						 cstAttributesNames
						  );

  }

  /**
   * Deletes selected partitions (logicalFiles) from AMI. 
   * Returns list of successfully deleted partitions.
   */
  public synchronized HashSet deletePartitions(int[] selectedPartitions, boolean showResults) {
	// TODO: Delete partitions only if not running.
	
	boolean skipAll = false;
	boolean skip = false;

	boolean showThis;
	int choice = 3;
  HashSet deleted = new HashSet();

	JProgressBar pb = new JProgressBar();
	pb.setMaximum(/*pb.getMaximum()+*/selectedPartitions.length);
	statusBar.setProgressBar(pb);
  showThis = showResults;
		  
	Debug.debug("Deleting "+selectedPartitions.length+" logical files",2);
  JCheckBox cbCleanup = null;
  for(int i=selectedPartitions.length-1; i>=0 && !skipAll; --i){
    
    if(skipAll){
      break;
    }
    
    if(showThis && !skipAll){
    	  
  	  ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame()/*,"",""*/);	
      cbCleanup = new JCheckBox("Cleanup runtime info", true);
  	  try{
  	    if(i>0){
  	      choice = confirmBox.getConfirm("Confirm delete",
             "Really delete logical file # "+selectedPartitions[i]+"?",
  		       new Object[] {"OK", "Skip", "OK for all", "Skip all", cbCleanup});
  	    }
  	    else{
  		    choice = confirmBox.getConfirm("Confirm delete",
  		     "Really delete logical file # "+selectedPartitions[i]+"?",
  				 new Object[] {"OK",  "Skip", cbCleanup});   	  	
  	    }
  	  }
      catch(java.lang.Exception e){Debug.debug("Could not get confirmation, "+e.getMessage(),1);}
  	    switch(choice){
  		    case 0  : skip = false;  break;  // OK
  		    case 1  : skip = true;   break; // Skip
  		    case 2  : skip = false;  showThis = false ; break;   //OK for all
  		    case 3  : skip = true;   showThis = false ; skipAll = true; break;// Skip all
  		    default : skip = true;   skipAll = true; break;// other (closing the dialog). Same action as "Skip all"
  	    }
  	  }
  	  if(!skipAll && !skip){
  	    Debug.debug("deleting logical file # " + selectedPartitions[i], 2);
  	    pb.setValue(pb.getValue()+1);
        if(dbPluginMgr.deletePart(selectedPartitions[i], cbCleanup.isSelected())){
          deleted.add(Integer.toString(selectedPartitions[i]));
          statusBar.setLabel("Logical file # " + selectedPartitions[i] + " deleted.");
        }
        else{
          statusBar.setLabel("Logical file # " + selectedPartitions[i] + " NOT deleted.");
          Debug.debug("WARNING: logical file "+selectedPartitions[i]+" could not be deleted",1);
        }   
  	  }
    }
    return deleted;
  }

  public int getPartitionIdentifier(){
    return partitionIdentifier;
  }

  /**
   * Private methods
   */

  private void addConstant(){
    if(tcConstant.size() == 26)
      return;

    JTextField tf = new JTextField();
    char name = (char) ('A' + (char)tcConstant.size());
    int cstByRow = 4;

    int row = tcConstant.size() / cstByRow;
    int col = tcConstant.size() % cstByRow;

    tcConstant.add(tf);

    pConstants.add(new JLabel(new String(new char[]{name}) + " : "),
                   new GridBagConstraints(col*2, row, 1, 1, 0.0, 0.0
                   ,GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));

    pConstants.add(tf, new GridBagConstraints(col*2+1, row, 1, 1, 1.0, 0.0
        ,GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));

    pConstants.updateUI();
  }




  private void save(){
    Vector v = getTextFields();
    String [] values = new String[v.size()+1];

    values[0] = ""+tcConstant.size();

    for(int i=1; i<values.length; ++i){
      values[i] = ((JTextComponent) v.get(i-1)).getText();
      if(values[i].length() == 0)
        values[i] = " ";
    }
    if(AtCom.threePanes){
      // TODO: fixme: this is a quick hack to avoid overwriting each others defaults.
      // The datasetID field is misused for recon_detail names
      for(int i=0; i<reconNames.length; ++i){
        dbPluginMgr.saveDefVals("", reconNames[i], values);
      }
   }
    else{
      for(int i=0; i<datasetIDs.length; ++i){
        dbPluginMgr.saveDefVals(Integer.toString(datasetIDs[i]), version, values);
      }
    }
  }

  private void editPartition(){
	cstAttributesNames = dbPluginMgr.getFieldNames(AtCom.amiPartition);
	
	Debug.debug("Got field names: "+AtCom.arrayToString(cstAttributesNames," "),3);

	int row = 0;

	//// Constants attributes

	String arg = "";
	String iden = "";
	AMITable res = null;
		iden = "identifier";
		arg="select * from "+AtCom.amiPartition+" where "+iden+"='"+
			 partitionIdentifier+"'";
		res = dbPluginMgr.sqlRequest(arg);
	if(res.values.length != 1){
		Debug.debug("Cannot get "+AtCom.amiPartition+" with "+iden+" "+partitionIdentifier,1);
		try{
			AtCom.getClassMgr().getJobDefinitionPanel().bUpdate.setEnabled(false);
			AtCom.getClassMgr().getJobDefinitionPanel().bCreate.setEnabled(true);
		}catch(java.lang.Exception e){Debug.debug("pPartitionCreation not initialized, "+e.getMessage(),1);}
	}
	else{
		try{
			AtCom.getClassMgr().getJobDefinitionPanel().bUpdate.setEnabled(true);
			AtCom.getClassMgr().getJobDefinitionPanel().bCreate.setEnabled(false);
		}catch(java.lang.Exception e){Debug.debug("pPartitionCreation not initialized, "+e.getMessage(),1);}
	}
   	
	for(int i =0; i<tcCstAttributes.length; ++i){
		for(int j=0; j<res.fields.length;++j){
			if(res.fields[j].toString().equalsIgnoreCase(cstAttributesNames[i].toString())){
				if(tcCstAttributes[i] == null || !tcCstAttributes[i].isEnabled() &&
				 tcCstAttributes[i].getText().length() == 0){
					tcCstAttributes[i] = createTextComponent();
					 pAttributes.add(tcCstAttributes[i], new GridBagConstraints(1, i/*row*/, 3, 1, 1.0, 0.0
						,GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
				}
				Debug.debug(cstAttributesNames[i].toString()+"="+res.fields[j]+". Setting to "+res.values[0][j].toString(),3);
				try{
					tcCstAttributes[i].setText(res.values[0][j].toString());
				}catch(java.lang.Exception e){Debug.debug("Attribute not found, "+e.getMessage(),1);}
				// TODO: Make primary key fields inactive
				break;
			}
		}
		if(cstAttributesNames[i].equalsIgnoreCase("identifier")){					
			tcCstAttributes[i].setEditable(false);
			tcCstAttributes[i].setBackground(Color.lightGray);
			//tcCstAttributes[i].setEnabled(false);
			//keyValues.put(cstAttributesNames[i].toString(), Integer.toString(i));
			if(res.values.length != 1){
				try{
					Debug.debug("Clearing identifier",3);
					tcCstAttributes[i].setText("");
				}catch(java.lang.Exception e){Debug.debug("Attribute not found, "+e.getMessage(),1);}
			}
		}
	}
  }


  private void load(){

    String [] defValues = null;

    if(AtCom.threePanes){
     // TODO: fixme: this is a quick hack to avoid overwriting each others defaults.
     // The datasetID field is misused for recon_detail names
     defValues = dbPluginMgr.getDefVals("", reconNames[0]);
   }
   else{
     defValues = dbPluginMgr.getDefVals(Integer.toString(datasetIDs[0]), version);
   }

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
      tf.setText(defValues[i].trim());
      ++i;
    }
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
    for(int i=0; i<tcStdOutput.length ; ++i)
      v.add(tcStdOutput[i]);

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
}
