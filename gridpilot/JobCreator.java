package gridpilot;

import java.util.*;

import javax.swing.*;

import gridpilot.Database.DBResult;

import java.awt.*;

/**
 * Creates the job definitions
 */
public class JobCreator{

  private static final long serialVersionUID=1L;
  
  private int [] datasetIdentifiers;
  private boolean showResults;
  private Vector constants;
  private String [] cstAttr;
  private String [] jobParam;
  private String [][] outMap;
  private String [] stdOut;
  private String [] resCstAttr;
  private String [] resJobParam;
  private String [][] resOutMap;
  private String [] resStdOut;
  private String [] cstAttrNames;
  private String [] jobParamNames;
  private String [] outMapNames;
  private String [] stdOutNames;
  private Vector vPartition = new Vector();
  private Vector vCstAttr = new Vector();
  private Vector vJobParam = new Vector();
  private Vector vOutMap = new Vector();
  private Vector vStdOut = new Vector();
  private JProgressBar pb = new JProgressBar();
  private Object semaphoreDBCreate = new Object();
  private DBPluginMgr dbPluginMgr = null;
  private Object[] showResultsOptions = {"OK", "Skip", "OK for all", "Skip all"};
  private Object[] showResultsOptions1 = {"OK", "Skip"};
  private String dbName;
  private StatusBar statusBar;

  public JobCreator(StatusBar _statusBar,
                    String _dbName,
                    int [] _datasetIdentifiers,
                    boolean _showResults,
                    Vector _constants,
                    String [] _cstAttr,
                    String [] _jobParam,
                    String [][] _outMap,
                    String [] _stdOut,
                    String [] _cstAttrNames,
                    String [] _jobParamNames,
                    String [] _outMapNames,
                    String [] _stdOutNames){
    
    super();

    statusBar = _statusBar;
	dbName = _dbName;
    datasetIdentifiers = _datasetIdentifiers;
    showResults = _showResults;
    constants = _constants;
    cstAttr = _cstAttr;
    jobParam = _jobParam;
    outMap = _outMap;
    stdOut = _stdOut;

    cstAttrNames =  _cstAttrNames;
    jobParamNames = _jobParamNames;
    outMapNames = _outMapNames;
    stdOutNames = _stdOutNames;

    resCstAttr = new String[cstAttr.length];
    resJobParam = new String[jobParam.length];
    resOutMap = new String [outMap.length][2];
    resStdOut  = new String[stdOut.length];
    dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);

    createJobDefs();
  }

  private void createJobDefs(){
    try{
      removeConstants();
    }
    catch(SyntaxException se){
      String msg = "Syntax error  : \n" + se.getMessage() + "\nCannot create job definition";
      String title = "Syntax error";
      MessagePane.showMessage(msg, title);
      return;
    }


    int firstPartition = 1;
    int lastPartition  = 1;

    if(firstPartition > lastPartition){
      GridPilot.getClassMgr().getLogFile().addMessage(
          "first value (from) cannot be greater then last value (To) : " +
          firstPartition + ">" + lastPartition);
      return;
    }

    boolean skipAll = false;
    boolean skip = false;
    boolean showThis = true;
    int [][] eventSplits = null;

    int len = 0;
    //  When using recon_detail, version is not known
    len = datasetIdentifiers.length;
    for(int currentDataset=0; currentDataset<len; ++currentDataset){

      int partitionCount = 0;
      
      // NOTICE: here we assume that the fields totalFiles and totalEvents are
      // present in dataset. If they are not, automatic splitting will not be done,
      // that is, the fields eventMin, eventMax and nEvents will not be set in the
      // jobDefinition record.
      eventSplits = dbPluginMgr.getEventSplits(datasetIdentifiers[currentDataset]);
      if(eventSplits!=null && eventSplits.length>1){
        lastPartition = firstPartition+eventSplits.length-1;
      }
      else{
      	Debug.debug("ERROR: Could not get event splitting.", 1);
        statusBar.setLabel("ERROR: Could not get event splitting.");
      	return;
      }

      for(int currentPartition = firstPartition ; currentPartition <= lastPartition && !skipAll; ++currentPartition){

        showThis = showResults;
        // NOTICE: here we assume that the following fields are present in dataset:
        // beamEnergy, beamParticle, outputLocation.
        // If they are not, the automatic generation of the names will not include
        // this information.
        try{
          evaluateAll(currentDataset, currentPartition,
              dbPluginMgr.getDatasetName(datasetIdentifiers[currentDataset]),
              dbPluginMgr.getRunNumber(datasetIdentifiers[currentDataset]),
              dbPluginMgr.getDataset(datasetIdentifiers[currentDataset]).getValue("beamEnergy").toString(),
              dbPluginMgr.getDataset(datasetIdentifiers[currentDataset]).getValue("beamParticle").toString(),
              dbPluginMgr.getDataset(datasetIdentifiers[currentDataset]).getValue("outputLocation").toString(),
                 eventSplits);
        }
        catch(Exception ex){
          String msg = "";
          String title = "";
          if(ex instanceof ArithmeticException){
            msg = "Arithmetic error in partition " + currentPartition+" : \n" +
                  ex.getMessage() +
                  "\n\nDo you want to continue job definition creation ?";
            title = "Arithmetic error";
          }
          else{
            if(ex instanceof SyntaxException){
              msg = "Syntax error in partition " + currentPartition+" : \n" +
                    ex.getMessage() +
                    "\n\nDo you want to continue job definition creation ?";
              title = "Syntax error";
            }
            else{ // should not happen
              msg = "Unexpected " + ex.getClass().getName() + " : " + ex.getMessage() +
              "\n\nDo you want to continue job definition creation ?";
              title = "Unexpected exception";
              ex.printStackTrace();
              GridPilot.getClassMgr().getLogFile().addMessage("Job definition creation", ex);
            }
          }

          int choice = JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(), msg, title,
              JOptionPane.YES_NO_OPTION);
          if(choice == JOptionPane.NO_OPTION){
            showThis = false;
            skip = true;
            skipAll = true;
          }
          else{
            showThis = false;
            skip = true;
          }
        }

        int choice;
        if(showThis){
          if(lastPartition - currentPartition + len - currentDataset > 1){       
            choice = showResult(currentPartition, resCstAttr, resJobParam, resOutMap, resStdOut,
            showResultsOptions);
          }
          else{
            choice = showResult(currentPartition, resCstAttr, resJobParam, resOutMap, resStdOut,
            showResultsOptions1);
          }

          switch(choice){
            case 0  : skip = false;  break;  // OK
            case 1  : skip = true;   break; // Skip
            case 2  : skip = false;  showResults = false ; break;   //OK for all
            case 3  : skip = true;   skipAll = true; // Skip all
            default : skip = true;   skipAll = true; // other (closing the dialog). Same action as "Skip all"
          }
        }
        if(!skip){
          Debug.debug("creating/updating job definition # " + currentPartition+
              " in dataset # "+datasetIdentifiers[currentDataset], 2);
          vPartition.add(new Integer(currentPartition));
          vCstAttr.add(resCstAttr.clone());
          vJobParam.add(resJobParam.clone());

          // clone() doesn't work on String [][]
          String [][] tmp = new String[resOutMap.length][2];
          for(int i=0; i<tmp.length; ++i){
            tmp[i][0] = resOutMap[i][0];
            tmp[i][1] = resOutMap[i][1];
          }

          vOutMap.add(tmp);
          vStdOut.add(resStdOut.clone());
          ++partitionCount;

        }
      }


      if(!skipAll){

        pb.setMaximum(pb.getMaximum()+partitionCount);
        statusBar.setProgressBar(pb);
        try{
          createDBJobDefinitions(currentDataset);
        }
        catch(java.lang.Exception e){Debug.debug("Failed creating partition from "+
            currentDataset+" : "+e.getMessage(),3);}
        statusBar.removeProgressBar(pb);
        //statusBar.removeLabel();
      }
    }
  }

  private void createDBJobDefinitions(int idNum) throws Exception{
    String transName = null;
    String transVersion = null;
    int id = -1;
    
    synchronized(semaphoreDBCreate){
      while(!vPartition.isEmpty()){
        int part = ((Integer) vPartition.remove(0)).intValue();
        resCstAttr = (String [] ) vCstAttr.remove(0);
        resJobParam = (String [] ) vJobParam.remove(0);
        resOutMap = (String [][]) vOutMap.remove(0);
        resStdOut  = (String []) vStdOut.remove(0);

        statusBar.setLabel("Creating job definition # " + part + " ...");
        pb.setValue(pb.getValue()+1);

        transName = dbPluginMgr.getDatasetTransformationName(datasetIdentifiers[idNum]);
        transVersion = dbPluginMgr.getDatasetTransformationVersion(datasetIdentifiers[idNum]);
        id = datasetIdentifiers[idNum];
        Debug.debug("Got transformation: "+transName+":"+transVersion+" <-- "+
            datasetIdentifiers[idNum], 3);
        Debug.debug("stdout/stderr length "+resStdOut.length, 2);
        
        if(!dbPluginMgr.createJobDefinition(
                              dbPluginMgr.getDatasetName(id),
                              cstAttrNames,
                              resCstAttr,
                              resJobParam,
                              resOutMap,
                              resStdOut!=null && resStdOut.length>0 && resStdOut[0]!=null ? resStdOut[0] : "",
                              resStdOut!=null && resStdOut.length>1 && resStdOut[1]!=null ? resStdOut[1] : ""
                              )){
          if(!dbPluginMgr.getError().equals("")){
            Runnable showModalDialog = new Runnable(){
              public void run(){
                JFrame frame = new JFrame("Message");
                JLabel label = new JLabel(dbPluginMgr.getError());
                frame.getContentPane().add(label);
                frame.pack();
                frame.setVisible(true);
              }
            };
            SwingUtilities.invokeLater(showModalDialog);
          }
          if(JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(), "Job definition " + part +
              " cannot be created.\n\nClick Cancel to stop or OK to continue creating job definitions.", "", JOptionPane.OK_CANCEL_OPTION)==JOptionPane.CANCEL_OPTION){
            break;
          }
          statusBar.setLabel("Job definition # " + part + " : "+Integer.toString(idNum+1)+" NOT created.");
        }
        else{
          statusBar.setLabel("Job definition # " + part + " : "+Integer.toString(idNum+1)+" created.");
        }
      }
    }
  }

  private String evaluate(String ss, int var, String name, String number,
      String energy, String particle, String outputDest) throws ArithmeticException, SyntaxException {
    // expression format : ${<arithmExpr>[:length]}
    // arithmExpr : operator priority : (*,/,%), (+,-), left associative
    Debug.debug("parsing, "+ss+" : "+var+" : "+name+" : "+number+" : "+energy+" : "+particle+" : "+outputDest, 3);

    int pos = -1;
    int pos1 = -1;
    int pos2 = -1;
    int pos3 = -1;
    int pos4 = -1;
    int pos5 = -1;
    int totPos = -5;
    StringBuffer sss = new StringBuffer(ss);
    int counter = 0;
    while(true){
      ++counter;
      // Parse datasetName and runNumber
      pos1 = sss.indexOf("$n");
      if(pos1>=0){
        sss.replace(pos1, pos1+2, name);
      }
      pos2 = sss.indexOf("$r");
      if(pos2>=0){
        sss.replace(pos2, pos2+2, number);
      }
      pos3 = sss.indexOf("$e");
      if(pos3>=0){
        sss.replace(pos3, pos3+2, energy);
      }
      pos4 = sss.indexOf("$p");
      if(pos4>=0){
        sss.replace(pos4, pos4+2, particle);
      }
      pos5 = sss.indexOf("$o");
      if(pos5>=0){
      	if(sss.substring(pos5+2, pos5+3).equals("/") && outputDest.length()>0){
          if(outputDest.substring(outputDest.length()-1,
              outputDest.length()).equals("/")){
            sss.replace(pos5, pos5+2,outputDest.substring(0,
                outputDest.length()-1));    
          }
          else{
            sss.replace(pos5, pos5+2, outputDest);
          }
      	}
      	else{
          if(outputDest.length()>0 && outputDest.substring(outputDest.length()-1,
              outputDest.length()).equals("/")){       
            sss.replace(pos5, pos5+2, outputDest);
          }
          else{
            sss.replace(pos5, pos5+2, outputDest+"/");
          }
      	}
      }
      
      // Parse the fields of the dataset
      pos = sss.indexOf("$1") + sss.indexOf("$2")+ sss.indexOf("$3") +
            sss.indexOf("$4") + sss.indexOf("$5") + sss.indexOf("$6") +
            sss.indexOf("$7") + sss.indexOf("$8") + sss.indexOf("$9");
      if(pos>-9){
        String rep = "";
        String val = "";
        String [] fields = new String [] {};
        fields = dbPluginMgr.getFieldNames("dataset");
        for(int i=fields.length-1; i>=0; --i){
          rep = "$"+i;
          pos = sss.indexOf(rep);
          if(pos>=0){
            try{
              val = dbPluginMgr.getDataset(dbPluginMgr.getDatasetID(name)
              ).getValue(fields[i]).toString();
            }
            catch(Exception e){
              // nothing necessary...
            }
            if(val==null || val.equals("")){
              Debug.debug("WARNING: could not get field "+i+" from dataset", 1);
            }
            else{
              sss.replace(pos, pos+rep.length(), val);
            }
          }          
        }
      }
      totPos = pos1+pos2+pos3+pos4+pos5+pos;
      Debug.debug("evaluating, "+totPos+": "+sss, 3);
      if(pos1+pos2+pos3+pos4+pos5+pos<-13 || counter>10){
        break;
      }
    }        
    String s = new String(sss);     
    String result = "";
    int previousPos = 0;
    int currentPos;
    boolean end=false;
    do{
      currentPos = s.indexOf("${", previousPos);
      if(currentPos == -1){
        currentPos = s.length();
        end = true;
      }
      result += s.substring(previousPos, currentPos);
      if(end){
        break;
      }
      previousPos = currentPos + 2;
      int colon = s.indexOf(':', previousPos);
      if(colon == -1){
        colon = s.length();
      }
      int brace = s.indexOf('}', previousPos);
      if(brace == -1){
        throw new SyntaxException(s + " : misformed expression - } missing");
      }
      if(colon<brace){
        // with colon
        currentPos = colon;
        int value = evaluateExpression(s.substring(previousPos, currentPos), var);
        previousPos = currentPos +1;
        currentPos = s.indexOf('}', previousPos);
        if(currentPos == -1){
          throw new SyntaxException(s + " : misformed expression - } missing");
        }
        int l;
        try{
          l = new Integer(s.substring(previousPos, currentPos)).intValue();
        }
        catch(NumberFormatException nfe){
          throw new SyntaxException(s + " : " + s.substring(previousPos, currentPos) +
                                    " is not an integer");
        }

        result += format(value, l);
      }
      else{
        currentPos = brace;
        int value = evaluateExpression(s.substring(previousPos, currentPos), var);
        result += value;
      }
      previousPos = currentPos + 1;
    }
    while(true);

    return result.trim();
  }

  private String format(int val, int length){
    Debug.debug("format : "+val+", "+length, 1);
    StringBuffer res = new StringBuffer();
    res.setLength(length);
    int currentVal = val;
    for(int i=length-1; i>=0; --i){
      res.setCharAt(i, (char)('0'+(currentVal%10)));
      currentVal /=10;
    }
    return res.toString();
  }

  /**
   * Gets the value of the constant c, or null if this constant is not defined.
   */
  private String getConstantValue(char c){
    int index = (int) (c - 'A');
    if(index < 0 || index >=constants.size())
      return null;
    else
      return ((JTextField ) (constants.get(index))).getText();
  }

  /**
   * Returns the arithmetic value of the expression <code>s</code>, when
   * the variable 'i' has the value <code>var</code>
   * @throws ArithmeticException if the expression s in not syntaxically correct.
   */
  private int evaluateExpression(String s, int var) throws ArithmeticException{
    Debug.debug("evaluate : "+s,1);
    ArithmeticExpression ae = new ArithmeticExpression(s, 'i', var);
    int res=ae.getValue();
    return res;
  }

  /** The first of the output file namnes of a dataset.
   * This is used as input file for the jobs of other datasets.
   * So, NOTICE: the (root) output file should always be the FIRST in the
   * field 'outputs' in the transformation definition.
   */
  private String getTransOutFileName(String db, int datasetID){
    String outputFileNameStr = null;
    DBPluginMgr dbMgr = GridPilot.getClassMgr().getDBPluginMgr(
        db);
    outputFileNameStr = dbMgr.getTransOutputs(
        dbPluginMgr.getTransformationID(
        dbPluginMgr.getDatasetTransformationName(datasetID),
        dbPluginMgr.getDatasetTransformationVersion(datasetID))).toString();
    return Util.split(outputFileNameStr)[0];
  }

  private void evaluateAll(int currentDataset, int currentPartition, String name,
      String number, String energy, String particle, String outputDest,
      int [][] eventSplits)throws ArithmeticException, SyntaxException{
    
    String inputFiles = "";
    int evtMin = eventSplits[currentPartition-1][0];
    int evtMax = eventSplits[currentPartition-1][1];
    int readEvtMin;
    int readEvtMax;
    int [] inputJobDefIds = new int [] {};
    String inputDB = null;
    String inputDataset = null;
    DBPluginMgr inputMgr = null;
    int inputDatasetID = -1;
    DBResult inputJobDefRecords = null;
    String inputJobDefOutputFileName = null;
    // Construct input file names
    try{
      inputDB = dbPluginMgr.getDataset(
          datasetIdentifiers[currentDataset]).getValue("inputDB").toString();
      String inputDBIdentifierField = dbPluginMgr.getIdentifierField(inputDB, "jobDefinition");
      inputDataset = dbPluginMgr.getDataset(
          datasetIdentifiers[currentDataset]).getValue("inputDataset").toString();
      if(inputDataset!=null && !inputDataset.equals("") &&
      		inputDB!=null && !inputDB.equals("")){
        inputMgr = GridPilot.getClassMgr().getDBPluginMgr(
            inputDB);
        inputDatasetID = inputMgr.getDatasetID(inputDataset);
        inputJobDefRecords = inputMgr.getJobDefinitions(inputDatasetID, 
                new String [] {inputDBIdentifierField});
        inputJobDefIds = new int [inputJobDefRecords.values.length];
        for(int i=0; i<inputJobDefIds.length; ++i){
          inputJobDefIds[i] = Integer.parseInt(
            inputJobDefRecords.getValue(i, inputDBIdentifierField).toString());
        }
        Debug.debug("Input datasets for "+datasetIdentifiers[currentDataset]+
        		":"+inputDataset+":"+inputDB+": "+inputJobDefIds.length, 3);
      }
    }
    catch(Exception e){
      Debug.debug("No input dataset for "+datasetIdentifiers[currentDataset], 2);
      e.printStackTrace();
      return;
    }
    if(inputJobDefIds.length>0 &&
        Integer.parseInt(inputJobDefRecords.getValue(
            currentPartition-1, "eventMin").toString())==evtMin &&
        Integer.parseInt(inputJobDefRecords.getValue(
                currentPartition-1, "eventMax").toString())==
	    	evtMax){
      inputJobDefOutputFileName = getTransOutFileName(inputDB, datasetIdentifiers[currentDataset]);
      inputFiles += inputMgr.getJobDefOutRemoteName(
          inputJobDefIds[currentPartition-1], inputJobDefOutputFileName);
      Debug.debug("Adding input file "+inputJobDefIds[currentPartition-1]+
      		"-->"+inputFiles, 3);
    }
    else if(inputJobDefIds.length>0){
      for(int j=0; j<inputJobDefIds.length; ++j){
        readEvtMin = Integer.parseInt(inputJobDefRecords.getValue(
            j, "eventMin").toString());
        readEvtMax = Integer.parseInt(inputJobDefRecords.getValue(
            j, "eventMax").toString());
        Debug.debug("Range of events: "+evtMin+"-->"+evtMax, 2);
        Debug.debug("Range of input events: "+readEvtMin+"-->"+readEvtMax, 2);
        
        if(readEvtMin!=-1 && readEvtMax!=-1 && (
  					// This should catch all cases where event in input files are
  					// partitioned differently from input events (events in output files).
  					// TODO:  needs to be checked!
        		readEvtMax-readEvtMin>evtMax-evtMin &&
           (evtMin>=readEvtMin &&
            evtMin<=readEvtMax ||
            
            evtMin<=readEvtMin &&
            evtMax>=readEvtMax ||
                
            evtMax>=readEvtMin &&
            evtMax<=readEvtMax) ||

            readEvtMax-readEvtMin<evtMax-evtMin &&
            (readEvtMin>=evtMin &&
             readEvtMin<=evtMax ||
             
             readEvtMin<=evtMin &&
             readEvtMax>=evtMax ||
                 
             readEvtMax>=evtMin &&
             readEvtMax<=evtMax))
        ){
          if(j>0){
            inputFiles += " ";
          }
          inputFiles += inputMgr.getJobDefOutRemoteName(
              inputJobDefIds[j], inputJobDefOutputFileName);
          Debug.debug("Adding input file "+inputJobDefIds[j]+
          		"-->"+inputFiles, 3);
        }
        else{
        }
      }
    }
    String inputs = "";
    String [] fils;
    String [] inArr = Util.split(inputFiles);
    for(int j=0; j<inArr.length; ++j){
      if(j>0){
        inputs += " ";
      }
      fils = Util.split(inArr[j], "/");
      if(fils.length>0){
        inputs += fils[fils.length-1];
      }
      else{
        inputs += inArr[j];
      }
    }
    // jobDefinition fields
    
    // Add eventMin, eventMax and inputFileName if they are
    // present in the fields of jobDefinition, but not in the fixed
    // attributes.
    String [] jobDefFields = dbPluginMgr.getFieldNames("jobDefinition");
    for(int i=0; i<jobDefFields.length; ++i){
      jobDefFields[i] = jobDefFields[i].toLowerCase();
    }
    ArrayList jobdefinitionfields = new ArrayList(Arrays.asList(jobDefFields));    
    ArrayList jobattributenames = new ArrayList(Arrays.asList(cstAttrNames));
    ArrayList jobAttributeNames = new ArrayList(Arrays.asList(cstAttrNames));
    ArrayList jobAttributes = new ArrayList(Arrays.asList(cstAttr));
    for(int i=0; i<jobattributenames.size(); ++i){
      jobattributenames.set(i, jobattributenames.get(i).toString().toLowerCase());
    }
    if(!jobattributenames.contains("eventmin") &&
        jobdefinitionfields.contains("eventmin")){
      jobAttributeNames.add("eventMin");
      jobAttributes.add("0");
    }
    if(!jobattributenames.contains("eventmax") &&
        jobdefinitionfields.contains("eventmax")){
      jobAttributeNames.add("eventMax");
      jobAttributes.add("0");
    }
    if(!jobattributenames.contains("nevents") &&
        jobdefinitionfields.contains("nevents")){
      jobAttributeNames.add("nEvents");
      jobAttributes.add("0");
    }
    if(!jobattributenames.contains("inputfilename") &&
        jobdefinitionfields.contains("inputfilename")){
      jobAttributeNames.add("inputFileName");
      jobAttributes.add("");
    }
    cstAttrNames = new String [jobAttributeNames.size()];
    for(int i=0; i<jobAttributeNames.size(); ++i){
      cstAttrNames[i] = jobAttributeNames.get(i).toString();
    }
    cstAttr = new String [jobAttributes.size()];
    for(int i=0; i<jobAttributes.size(); ++i){
      Debug.debug("Setting attribute "+jobAttributes.get(i), 3);
      if(jobAttributes.get(i)!=null){
        cstAttr[i] = jobAttributes.get(i).toString();
      }
    }
    resCstAttr = new String[cstAttr.length];
    for(int i=0; i<resCstAttr.length; ++i){
      Debug.debug("parameter #"+i+":"+resCstAttr.length+":"+cstAttrNames[i], 3);
      Debug.debug("eventSplits: "+eventSplits.length, 3);
      if(cstAttrNames[i].equalsIgnoreCase("eventMin") &&
          eventSplits!=null && eventSplits.length>1){
        Debug.debug("setting event minimum", 3);
        resCstAttr[i] = Integer.toString(eventSplits[currentPartition-1][0]);
      }
      else if(cstAttrNames[i].equalsIgnoreCase("eventMax") &&
          eventSplits!=null && eventSplits.length>1){
        Debug.debug("setting event maximum", 3);
        resCstAttr[i] = Integer.toString(eventSplits[currentPartition-1][1]);
      }
      else if(cstAttrNames[i].equalsIgnoreCase("nevents") &&
          eventSplits!=null && eventSplits.length>1){
        Debug.debug("setting event number", 3);
        resCstAttr[i] = Integer.toString(eventSplits[currentPartition-1][1]-
                                         eventSplits[currentPartition-1][0]+1);
      }
      else if(cstAttrNames[i].equalsIgnoreCase("inputFileName") &&
          eventSplits!=null && eventSplits.length>1){
        // all files from input dataset containing the needed events
        Debug.debug("setting input files "+inputFiles, 3);
        resCstAttr[i] = inputFiles;
      }
      else{
        Debug.debug("evaluating", 3);
        resCstAttr[i] = evaluate(cstAttr[i], currentPartition, name, number,
            energy, particle, outputDest);
      }
    }
    // Job parameters
    for(int i=0; i<resJobParam.length; ++i){
      Debug.debug("param #"+i, 3);
      if((jobParamNames[i].equalsIgnoreCase("eventMin")) &&
          eventSplits!=null && eventSplits.length>1){
        resJobParam[i] = Integer.toString(eventSplits[currentPartition-1][0]);
      }
      else if((jobParamNames[i].equalsIgnoreCase("nEvents")) &&
          eventSplits!=null && eventSplits.length>1){
        resJobParam[i] = Integer.toString(eventSplits[currentPartition-1][1]-
            eventSplits[currentPartition-1][0]+1);
      }
      else if((jobParamNames[i].equalsIgnoreCase("inputFileName")) &&
          eventSplits!=null && eventSplits.length>1){
        resJobParam[i] = inputs;
      }
      // Fill in if it matches one of the jobDefinition fields
      else if(jobdefinitionfields.contains(jobParamNames[i].toLowerCase())){
        if(resCstAttr[jobdefinitionfields.indexOf(jobParamNames[i].toLowerCase())]==null ||
            resCstAttr[jobdefinitionfields.indexOf(jobParamNames[i].toLowerCase())].equals("")){
          resCstAttr[jobdefinitionfields.indexOf(jobParamNames[i].toLowerCase())] = resJobParam[i];
        }
      }
      else{
        resJobParam[i] = evaluate(jobParam[i], currentPartition, name, number,
            energy, particle, outputDest);
      }
    }
    for(int i=0; i<resOutMap.length; ++i){
      Debug.debug("param #"+i, 3);
      resOutMap[i][0] = evaluate(outMap[i][0], currentPartition, name, number,
          energy, particle, outputDest);
      resOutMap[i][1] = evaluate(outMap[i][1], currentPartition, name, number,
          energy, particle, outputDest);
    }
    for(int i=0; i<resStdOut.length; ++i){
      Debug.debug("param #"+i, 3);
      resStdOut[i] = evaluate(stdOut[i], currentPartition, name, number,
          energy, particle, outputDest);
    }
  }

  /**
   * Removes constants in every String.
   * @throws SyntaxException if a constant is not defined.
   */
  private void removeConstants() throws SyntaxException{
    for(int i=0; i< resCstAttr.length; ++i)
      cstAttr[i] = removeConstants(cstAttr[i]);
    for(int i=0; i< resJobParam.length; ++i)
      jobParam[i] = removeConstants(jobParam[i]);
    for(int i=0; i< resOutMap.length; ++i){
      outMap[i][0] = removeConstants(outMap[i][0]);
      outMap[i][1] = removeConstants(outMap[i][1]);
    }
    for(int i=0; i< resStdOut.length; ++i)
      stdOut[i] = removeConstants(stdOut[i]);
  }

  /**
   * Creates a String which contains the String s, in which each constant ($A, $B, etc)
   * has been replaced by the value of this constant.
   * @throws SyntaxException if a constant has been found but is not defined.
   */
  private String removeConstants(String s) throws SyntaxException{
    int begin = 0;
    int end;
    String res = "";

    while(true){
      end = s.indexOf('$', begin);
      if(end == -1)
        end = s.length();

      /**
       * s.substring(begin, end) = the substring from the last constant (or the begin)
       * to the last character before the next constant (or the last character of s)
       */
      res += s.substring(begin, end);
      if(end == s.length())
        break;
      if(end +1 < s.length() && s.charAt(end + 1) != '{' &&
          s.charAt(end + 1) != 'n' && s.charAt(end + 1) != 'r' &&
          s.charAt(end + 1) != 'e' && s.charAt(end + 1) != 'p' &&
          s.charAt(end + 1) != 'o' &&
          s.charAt(end + 1) != '1' && s.charAt(end + 1) != '2' &&
          s.charAt(end + 1) != '3' && s.charAt(end + 1) != '4' &&
          s.charAt(end + 1) != '5' && s.charAt(end + 1) != '6' &&
          s.charAt(end + 1) != '7' && s.charAt(end + 1) != '8' &&
          s.charAt(end + 1) != '9'){
        // a constant has been found : s.charAt(end) = '$', and s.charAt(end+1) =
        // the name of the constant
        String cstValue = getConstantValue(s.charAt(end+1));
        if(cstValue == null) // this constant is not defined
          throw new SyntaxException(s + " : Constant " + s.charAt(end + 1) + " unknown");
        res += cstValue;
        begin = end + 2; // skip $<constant name>
      }
      else{
        // an arithmetic expression has been found : adds the '$', and skips it
        res += '$';
        begin = end+1;
      }
    }
    return res;
  }
  
  private int showResult(int currentPartition, String [] resCstAttr, String [] resJobParam,
                         String [][] resOutMap, String [] resStdOut, Object[] showResultsOptions){

    JPanel pResult = new JPanel(new GridBagLayout());
    int row = 0;

    // Fixed parameters
    pResult.add(new JLabel("Fixed job parameters"),
        new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
            GridBagConstraints.CENTER,
            GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));
    ++row;

    for(int i=0; i<cstAttr.length; ++i, ++row){

      pResult.add(new JLabel(cstAttrNames[i] + " : "),
          new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.BOTH, new Insets(5, 25, 5, 5), 0, 0));
      
      pResult.add(new JLabel(resCstAttr[i]),
          new GridBagConstraints(1, row, 3, 1, 1.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
    }

    // Job parameters
    pResult.add(new JLabel("Transformation job parameters"),
        new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
            GridBagConstraints.CENTER,
            GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));
    ++row;

    for(int i=0; i<jobParam.length; ++i, ++row){
      
      pResult.add(new JLabel(jobParamNames[i] + " : "),
          new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.BOTH, new Insets(5, 25, 5, 5), 0, 0));
      
      pResult.add(new JLabel(resJobParam[i]),
          new GridBagConstraints(1, row, 3, 1, 1.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
    }

    // OutputMapping
    pResult.add(new JLabel("Output mapping"),
        new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
            GridBagConstraints.CENTER,
            GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));
    ++row;

    for(int i=0; i<outMap.length; ++i, ++row){
      
      pResult.add(new JLabel(outMapNames[i] + " : Local name : "),
          new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.BOTH, new Insets(5, 25, 5, 5), 0, 0));

      pResult.add(new JLabel(resOutMap[i][0]),
          new GridBagConstraints(1, row, 1, 1, 1.0, 0.0,GridBagConstraints.CENTER,
              GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));

      pResult.add(new JLabel(" -> Remote name : "),
          new GridBagConstraints(2, row, 1, 1, 0.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));

      pResult.add(new JLabel(resOutMap[i][1]),
          new GridBagConstraints(3, row, 1, 1, 1.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
    }

    // Output files
    if(stdOutNames.length>0){
      pResult.add(new JLabel("Output files"),
          new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));
      ++row;
    }

    for(int i=0; i<stdOut.length; ++i, ++row){
      pResult.add(new JLabel(stdOutNames[i] + " : "),
          new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.BOTH, new Insets(5, 25, 5, 5), 0, 0));
      pResult.add(new JLabel(resStdOut[i]),
          new GridBagConstraints(1, row, 3, 1, 1.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
    }

    JScrollPane sp = new JScrollPane(pResult);
    sp.setPreferredSize(new Dimension(500,
                                      (int)pResult.getPreferredSize().getHeight() +
                                      (int)sp.getHorizontalScrollBar().getPreferredSize().getHeight() + 5));

    JOptionPane op = new JOptionPane(sp,
                                     JOptionPane.QUESTION_MESSAGE,
                                     JOptionPane.YES_NO_CANCEL_OPTION,
                                     null,
                                     showResultsOptions,
                                     showResultsOptions[0]);

    JDialog dialog = op.createDialog(JOptionPane.getRootFrame(), "Job definition # "+currentPartition);
    dialog.setResizable(true);
    dialog.setVisible(true);
    dialog.dispose();


    Object selectedValue = op.getValue();

    if(selectedValue==null){
      return JOptionPane.CLOSED_OPTION;
    }
    for (int i=0; i<showResultsOptions.length; ++i){
      if(showResultsOptions[i]==selectedValue){
        return i;
      }
    }
    return JOptionPane.CLOSED_OPTION;
  }
}
