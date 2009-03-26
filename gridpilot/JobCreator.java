package gridpilot;

import gridfactory.common.DBResult;
import gridfactory.common.Debug;
import gridfactory.common.StatusBar;

import java.util.*;

import javax.swing.*;

import java.awt.*;
import java.io.IOException;

/**
 * Creates the job definitions
 */
public class JobCreator{

  private static final long serialVersionUID=1L;
  
  private String [] datasetIdentifiers;
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
  // Caches
  private HashMap datasetNameIds = new HashMap();
  private HashMap datasetIdFiles = new HashMap();
  
  private DBResult inputRecords = null;
  private String [] inputIds = null;
  
  private static String EVENT_MIN = "eventMin";
  private static String EVENT_MAX = "eventMax";
  private static String N_EVENTS = "nEvents";

  public JobCreator(StatusBar _statusBar,
                    String _dbName,
                    String [] _datasetIdentifiers,
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
    
    pb.setMaximum(0);

    createJobDefs();
  }

  private void createJobDefs(){
    try{
      removeConstants();
    }
    catch(SyntaxException se){
      String msg = "Syntax error  : \n" + se.getMessage() + "\nCannot create job definition";
      String title = "Syntax error";
      MyUtil.showLongMessage(msg, title);
      return;
    }

    int firstPartition = 1;
    int lastPartition  = -1;

    boolean skipAll = false;
    boolean skip = false;
    boolean showThis = true;
    int [][] eventSplits = null;

    int len = 0;
    //  When using recon_detail, version is not known
    len = datasetIdentifiers.length;
    for(int currentDataset=0; currentDataset<len; ++currentDataset){
      
      Debug.debug("Creating job definitions for dataset "+datasetIdentifiers[currentDataset], 2);

      int partitionCount = 0;
      
      // NOTICE: here we assume that the fields totalFiles and totalEvents are
      // present in dataset. If they are not, automatic splitting will not be done,
      // that is, the fields eventMin, eventMax and nEvents will not be set in the
      // jobDefinition record.
      eventSplits = dbPluginMgr.getEventSplits(datasetIdentifiers[currentDataset]);
      // First, see if we can split according to event specifications
      if(eventSplits!=null && eventSplits.length>1){
        lastPartition = firstPartition+eventSplits.length-1;
        Debug.debug("found first partition from event splitting: "+firstPartition, 1);
        Debug.debug("found last partition rom event splitting: "+lastPartition, 1);
      }      
      // If that didn't work, see if we can get the number of files
      // of the output dataset directly
      if(lastPartition<=0){
        String error = "WARNING: could not get event splitting.";
        GridPilot.getClassMgr().getLogFile().addInfo(error);
        statusBar.setLabel(error);
        //return;
        // try to get number of files from input dataset
        try{
          String totalOutputFiles = (String) dbPluginMgr.getDataset(
              datasetIdentifiers[currentDataset]).getValue("totalFiles");
          firstPartition = 1;
          lastPartition = Integer.parseInt(totalOutputFiles);
        }
        catch(Exception ee){
          error = "WARNING: Could not get number of files from " +
              "field totalFiles in dataset record.";
          GridPilot.getClassMgr().getLogFile().addMessage(error);
        }
      }    
      // If that didn't work, see if we can get the number of files
      // of the input dataset directly
      String inputDataset = null;
      DBPluginMgr inputMgr = null;
      String inputDB = null;
      if(lastPartition<=0){
        String error = "WARNING: could not get event splitting.";
        GridPilot.getClassMgr().getLogFile().addInfo(error);
        statusBar.setLabel(error);
      	//return;
        // try to get number of files from input dataset
        try{
          inputDB = (String) dbPluginMgr.getDataset(
              datasetIdentifiers[currentDataset]).getValue("inputDB");
          inputMgr = GridPilot.getClassMgr().getDBPluginMgr(
              inputDB);
          inputDataset = (String) dbPluginMgr.getDataset(
              datasetIdentifiers[currentDataset]).getValue("inputDataset");
          String totalInputFiles = (String) inputMgr.getDataset(
              inputDataset).getValue("totalFiles");
          firstPartition = 1;
          lastPartition = Integer.parseInt(totalInputFiles);
        }
        catch(Exception ee){
          error = "ERROR: Could not get number of input files from " +
              "field totalFiles in input dataset record.";
          GridPilot.getClassMgr().getLogFile().addMessage(error);
        }
      }
      // If that didn't work, see if we can get the number of files
      // of the input dataset by querying the dataset catalog
      if(lastPartition<=0){
        String error = "WARNING: number of files not in dataset record.";
        GridPilot.getClassMgr().getLogFile().addInfo(error);
        statusBar.setLabel(error);
        //DBResult inputFiles = inputMgr.getFiles(inputMgr.getDatasetID(inputDataset));
        if(!datasetNameIds.containsKey(inputDataset)){
          datasetNameIds.put(inputDataset, inputMgr.getDatasetID(inputDataset));
        }
        String inputDatasetID = (String) datasetNameIds.get(inputDataset);
        if(!datasetIdFiles.containsKey(inputDatasetID)){
          datasetIdFiles.put(inputDatasetID, inputMgr.getFiles(inputDatasetID));
        }
        DBResult inputFiles = (DBResult) datasetIdFiles.get(inputDatasetID);
        firstPartition = 1;
        lastPartition = inputFiles.values.length;
      }
      // If that didn't work, see if we can get the number of files
      // of the input dataset by querying the file catalog
      if(lastPartition<=0){
        String error = "ERROR: Could not get number of input files from input dataset.";
        GridPilot.getClassMgr().getLogFile().addMessage(error);
        return;
        //firstPartition = 1;
        //lastPartition  = 1;
      }
      Debug.debug("first partition: "+firstPartition, 1);
      Debug.debug("last partition: "+lastPartition, 1);
      
      if(firstPartition>lastPartition){
        GridPilot.getClassMgr().getLogFile().addMessage(
            "ERROR: first value (from) cannot be greater then last value (To) : " +
            firstPartition + ">" + lastPartition);
        return;
      }

      for(int currentPartition=firstPartition; currentPartition<=lastPartition && !skipAll; ++currentPartition){

        statusBar.setLabel("Preparing job definition # " + currentPartition + "...");

        showThis = showResults;
        // NOTICE: here we assume that the following fields are present in dataset:
        // beamEnergy, beamParticle, outputLocation.
        // If they are not, the automatic generation of the names will not include
        // this information.
        Debug.debug("evaluating all", 3);
        try{
          evaluateAll(currentDataset, currentPartition,
              dbPluginMgr.getDatasetName(datasetIdentifiers[currentDataset]),
              dbPluginMgr.getRunNumber(datasetIdentifiers[currentDataset]),
              // beamEnergy and beamParticle are optional, non-general attributes.
              (String) dbPluginMgr.getDataset(datasetIdentifiers[currentDataset]).getValue("beamEnergy"),
              (String) dbPluginMgr.getDataset(datasetIdentifiers[currentDataset]).getValue("beamParticle"),
              (String) dbPluginMgr.getDataset(datasetIdentifiers[currentDataset]).getValue("outputLocation"),
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
        Debug.debug("done evaluating all", 3);
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
            case 0  : skip = false;  break; // OK
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
    String id = "-1";
    
    synchronized(semaphoreDBCreate){
      while(!vPartition.isEmpty()){
        int part = ((Integer) vPartition.remove(0)).intValue();
        resCstAttr = (String [] ) vCstAttr.remove(0);
        resJobParam = (String [] ) vJobParam.remove(0);
        resOutMap = (String [][]) vOutMap.remove(0);
        resStdOut  = (String []) vStdOut.remove(0);

        statusBar.setLabel("Creating job definition # " + part + "...");
        pb.setValue(pb.getValue()+1);

        transName = dbPluginMgr.getDatasetTransformationName(datasetIdentifiers[idNum]);
        transVersion = dbPluginMgr.getDatasetTransformationVersion(datasetIdentifiers[idNum]);
        id = datasetIdentifiers[idNum];
        Debug.debug("Got transformation: "+transName+":"+transVersion+" <-- "+
            datasetIdentifiers[idNum], 3);
        Debug.debug("stdout/stderr length "+resStdOut.length, 2);
        Debug.debug("cstAttrNames --> "+MyUtil.arrayToString(cstAttrNames), 3);
        Debug.debug("resCstAttr --> "+MyUtil.arrayToString(resCstAttr), 3);
        Debug.debug("resJobParam --> "+MyUtil.arrayToString(resCstAttr), 3);
        
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
            Runnable showDialog = new Runnable(){
              public void run(){
                JFrame frame = new JFrame("Message");
                JLabel label = new JLabel(dbPluginMgr.getError());
                frame.getContentPane().add(label);
                frame.pack();
                frame.setVisible(true);
              }
            };
            SwingUtilities.invokeLater(showDialog);
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
    Debug.debug("Evaluating, "+ss+" : "+var+" : "+name+" : "+number+" : "+energy+" : "+particle+" : "+outputDest, 3);

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
              val = (String) dbPluginMgr.getDataset(dbPluginMgr.getDatasetID(name)
              ).getValue(fields[i]);
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
    Debug.debug("Done evaluating", 3);
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
    if(index<0 || index>=constants.size()){
      return null;
    }
    else{
      return ((JTextField ) (constants.get(index))).getText();
    }
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
  
  /**
   * The first of the output file names of the transformation
   * used to produce a dataset.
   * This is used as input file for the jobs of other datasets.
   * So, NOTICE: the (root) output file should always be the FIRST in the
   * field 'outputs' in the transformation definition.
   */

  private void evaluateAll(int currentDataset, int currentPartition, String name,
      String number, String energy, String particle, String outputDest,
      int [][] eventSplits)throws ArithmeticException, SyntaxException,
      IOException{
    
    int evtMin = -1;
    int evtMax = -1;
    try{
      evtMin = eventSplits[currentPartition-1][0];
      evtMax = eventSplits[currentPartition-1][1];
    }
    catch(Exception e){
    }
    String inputDB = null;
    String inputDataset = null;
    DBPluginMgr inputMgr = null;
    boolean fileCatalogInput = false;
    // Construct input file names
    inputDB = (String) dbPluginMgr.getDataset(
        datasetIdentifiers[currentDataset]).getValue("inputDB");
    if(inputDB!=null && !inputDB.equals("")){
      inputMgr = GridPilot.getClassMgr().getDBPluginMgr(
          inputDB);
      inputDataset = (String )dbPluginMgr.getDataset(
          datasetIdentifiers[currentDataset]).getValue("inputDataset");
    }
    if(inputDataset!=null && !inputDataset.equalsIgnoreCase("")){
      fileCatalogInput = evaluateAllWithInput(currentDataset, inputMgr, inputDataset, inputDB);
    }
    boolean eventsPresent = true;
    // just a quick check to avoid exceptions
    if(inputRecords!=null &&(
        inputRecords.getValue(0, EVENT_MIN)==null ||
        inputRecords.getValue(0, EVENT_MAX)==null ||
        inputRecords.getValue(0, EVENT_MIN).equals("") ||
        inputRecords.getValue(0, EVENT_MAX).equals("") ||
        inputRecords.getValue(0, EVENT_MIN).equals("''") ||
        inputRecords.getValue(0, EVENT_MAX).equals("''") ||
        inputRecords.getValue(0, EVENT_MIN).equals("no such field") ||
        inputRecords.getValue(0, EVENT_MAX).equals("no such field"))){
      eventsPresent = false;
      Debug.debug("No event information in dataset "+datasetIdentifiers[currentDataset], 2);
    }
    String inputFiles = "";
    if(inputIds!=null && inputIds.length>0){
      inputFiles = findInputFiles(evtMin, evtMax, currentDataset, currentPartition,
          inputMgr, inputDataset, fileCatalogInput, eventsPresent);
    }
    // construct the (short) file names for the job script arguments
    String inputs = "";
    String [] fils = null;
    String [] inArr = MyUtil.split(inputFiles);
    String addFils = "";
    for(int j=0; j<inArr.length; ++j){
      if(j>0){
        inputs += " ";
      }
      fils = MyUtil.split(inArr[j], "/");
      if(fils.length>0){
        addFils = fils[fils.length-1];
      }
      else{
        addFils = inArr[j];
      }
      fils = MyUtil.split(addFils, "\\\\");
      if(fils.length>0){
        inputs += fils[fils.length-1];
      }
      else{
        inputs += addFils;
      }
      Debug.debug("--->"+inputs, 2);
    }
    
    // jobDefinition fields
    ArrayList jobattributenames = fillInResCstAttr(eventSplits, currentPartition, name, number,
        energy, particle, outputDest, evtMin, evtMax, inputFiles);
    
    // Job parameters
    fillInJobParams(currentDataset, currentPartition, eventSplits, evtMin, evtMax, name, number,
        energy, particle, outputDest, jobattributenames, inputs);

    // if the destination is left empty on the creation panel and
    // we are using input files from a file catalog, name the output file
    // by simply appending ".out" to the input file name
    Debug.debug("Filling in outputs", 3);
    if(fileCatalogInput && !eventsPresent && outMap.length==1 &&
        (outMap[0][1]==null || outMap[0][1].equals("")) && inputs!=null &&
        MyUtil.split(inputs).length==1){
      String ifn = inputs;
      String [] fullNameStrings = MyUtil.split(ifn, "\\.");
      if(fullNameStrings.length>0){
        String extension = "."+fullNameStrings[fullNameStrings.length-1];
        resOutMap[0][1] = evaluate("$o/"+ifn.replaceFirst(extension, ".out"+extension),
            currentPartition, name, number, energy, particle, outputDest);
      }
      else{
        resOutMap[0][1] = evaluate("$o/"+ifn+".out",
            currentPartition, name, number, energy, particle, outputDest);
      }
      resOutMap[0][0] = evaluate(outMap[0][0], currentPartition, name, number,
          energy, particle, outputDest);
    }
    else{
      for(int i=0; i<resOutMap.length; ++i){
        Debug.debug("param #"+i, 3);
        resOutMap[i][0] = evaluate(outMap[i][0], currentPartition, name, number,
            energy, particle, outputDest);
        resOutMap[i][1] = evaluate(outMap[i][1], currentPartition, name, number,
            energy, particle, outputDest);
      }
    }
    for(int i=0; i<resStdOut.length; ++i){
      Debug.debug("param #"+i, 3);
      resStdOut[i] = evaluate(stdOut[i], currentPartition, name, number,
          energy, particle, outputDest);
    }
  }

  private boolean evaluateAllWithInput(int currentDataset, DBPluginMgr inputMgr, String inputDataset,
      String inputDB) throws IOException{
    boolean fileCatalogInput = false;
    String inputDatasetID = "-1";
    inputIds = new String [] {};
    if(inputMgr.isFileCatalog()){
      try{
        String inputDBFileIdentifierField = MyUtil.getIdentifierField(
            inputMgr.getDBName(), "file");
        if(inputDataset!=null && !inputDataset.equals("") &&
            inputDB!=null && !inputDB.equals("")){
          if(!datasetNameIds.containsKey(inputDataset)){
            datasetNameIds.put(inputDataset, inputMgr.getDatasetID(inputDataset));
          }
          inputDatasetID = (String) datasetNameIds.get(inputDataset);
          if(!datasetIdFiles.containsKey(inputDatasetID)){
            datasetIdFiles.put(inputDatasetID, inputMgr.getFiles(inputDatasetID));
          }
          inputRecords = (DBResult) datasetIdFiles.get(inputDatasetID);
          inputIds = new String[inputRecords.values.length];
          for(int i=0; i<inputIds.length; ++i){
            inputIds[i] = (String) inputRecords.getValue(i,
                inputDBFileIdentifierField);
          }
          Debug.debug("Input files for "+datasetIdentifiers[currentDataset]+
              ":"+inputDataset+":"+inputDB+": "+MyUtil.arrayToString(inputIds), 3);
        }
      }
      catch(Exception e){
        Debug.debug("No input dataset for "+datasetIdentifiers[currentDataset], 2);
        //e.printStackTrace();
        //return;
      }
    }
    if((inputRecords==null || inputRecords.values.length==0) && inputMgr.isJobRepository()){
      try{
        String inputDBJobDefIdentifierField = MyUtil.getIdentifierField(
            inputMgr.getDBName(), "jobDefinition");
        if(inputDataset!=null && !inputDataset.equals("") &&
            inputDB!=null && !inputDB.equals("")){
          if(!datasetNameIds.containsKey(inputDataset)){
            datasetNameIds.put(inputDataset, inputMgr.getDatasetID(inputDataset));
          }
          inputDatasetID = (String) datasetNameIds.get(inputDataset);
          inputRecords = inputMgr.getJobDefinitions(inputDatasetID, 
                  new String [] {inputDBJobDefIdentifierField, EVENT_MIN, EVENT_MAX},
                  null, null);
          inputIds = new String[inputRecords.values.length];
          for(int i=0; i<inputIds.length; ++i){
            inputIds[i] = (String) inputRecords.getValue(i, inputDBJobDefIdentifierField);
          }
          Debug.debug("Input job definitions for "+datasetIdentifiers[currentDataset]+
              ":"+inputDataset+":"+inputDB+": "+MyUtil.arrayToString(inputIds), 3);
        }
      }
      catch(Exception e){
        Debug.debug("No input dataset for "+datasetIdentifiers[currentDataset], 2);
        //e.printStackTrace();
        //return;
      }
    }
    else{
      fileCatalogInput = true;
    }
    if(inputRecords==null || inputRecords.values.length==0){
      throw new IOException ("Could not get input files, cannot proceed.");
    }
    return fileCatalogInput;
  }

  private String findInputFiles(int evtMin, int evtMax, int currentDataset, int currentPartition,
      DBPluginMgr inputMgr, String inputDataset, boolean fileCatalogInput, boolean eventsPresent) {
    String inputFiles = "";
    String inputFileName = null;
    int readEvtMin = -1;
    int readEvtMax = -1;
    if(eventsPresent){
      boolean inputFileFound = false;
      for(int j=0; j<inputIds.length; ++j){
        if(Integer.parseInt((String) inputRecords.getValue(j, EVENT_MIN))==evtMin &&
           Integer.parseInt((String) inputRecords.getValue(j, EVENT_MAX))==evtMax){
          //inputJobDefOutputFileName = getTransOutFileName(inputDB, datasetIdentifiers[currentDataset]);
          inputFileName = inputMgr.getOutputFiles(inputIds[j])[0];
          inputFiles += inputMgr.getJobDefOutRemoteName(inputIds[j], inputFileName);
          Debug.debug("Adding input file "+inputIds[j]+"-->"+inputFiles, 3);
          inputFileFound = true;
          break;
        }
      }
      if(!inputFileFound){
        for(int j=0; j<inputIds.length; ++j){
          readEvtMin = Integer.parseInt((String) inputRecords.getValue(j, EVENT_MIN));
          readEvtMax = Integer.parseInt((String) inputRecords.getValue(j, EVENT_MAX));
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
               readEvtMax<=evtMax))){
            if(j>0){
              inputFiles += " ";
            }
            inputFiles += inputMgr.getJobDefOutRemoteName(
                inputIds[j], inputFileName);
            Debug.debug("Adding input file "+inputIds[j]+
                    "-->"+inputFiles, 3);
          }
        }
      }
    }
    else{
      // just assume one-to-one input to output files
      if(fileCatalogInput){
        Debug.debug("Using input file from file catalog", 2);
        // The logical file name - we don't use it: I don't see the point
        // of giving lfc://some/path/some/file.root or mysql://somedb/file.root
        // to the CE, when we can check file availability right here.
        /*inputFileName = (String) inputMgr.getFile(datasetIdentifiers[currentDataset],
            inputIds[currentPartition-1]).getValue("lfn");*/
        // Use the first pfn returned.
        // Notice: if "find all PFNs" is checked all PFNs will be looked up,
        // slowing down job creation enormeously, and still only the first will be used.
        // So, it should NOT be checked.
        String inputFils = (String) inputMgr.getFile(inputDataset,
            inputIds[currentPartition-1], 1).getValue("pfns");
        String [] inputFilArr = null;
        try{
          inputFilArr = MyUtil.splitUrls(inputFils);
        }
        catch(Exception e){
          Debug.debug("WARNING: could not split as URLs, trying normal split", 1);
          e.printStackTrace();
          inputFilArr = MyUtil.split(inputFils);
        }
        inputFiles = inputFilArr[0];
      }
      else{
        inputFileName = inputMgr.getOutputFiles(
            inputIds[currentPartition-1])[0];
        inputFiles = inputMgr.getJobDefOutRemoteName(
            inputIds[currentPartition-1], inputFileName);
      }
      Debug.debug("--->"+inputFiles, 2);
    }
    return inputFiles;
  }

  private void fillInJobParams(int currentDataset, int currentPartition,
      int [][] eventSplits, int evtMin, int evtMax, String name, String number,
      String energy, String particle, String outputDest, ArrayList jobattributenames,
      String inputs) throws ArithmeticException, SyntaxException {
    Debug.debug("Filling in job parameters", 3);
    // metadata information from the metadata field of the dataset
    String metaDataString = (String) dbPluginMgr.getDataset(
        datasetIdentifiers[currentDataset]).getValue("metaData");
    HashMap metaData = MyUtil.parseMetaData(metaDataString);
    for(int i=0; i<resJobParam.length; ++i){
      Debug.debug("param #"+i+" : "+jobParamNames[i]+" -> "+
          metaData.containsKey(jobParamNames[i].toLowerCase())+ " : "+
          MyUtil.arrayToString(metaData.keySet().toArray()), 3);
      if((jobParam[i]==null || jobParam[i].equals("")) &&
          (jobParamNames[i].equalsIgnoreCase(EVENT_MIN)) &&
          eventSplits!=null && eventSplits.length>1){
        resJobParam[i] = Integer.toString(evtMin);
      }
      else if((jobParam[i]==null || jobParam[i].equals("")) &&
          (jobParamNames[i].equalsIgnoreCase(EVENT_MAX)) &&
          eventSplits!=null && eventSplits.length>1){
        resJobParam[i] = Integer.toString(evtMax);
      }
      else if((jobParam[i]==null || jobParam[i].equals("")) &&
          (jobParamNames[i].equalsIgnoreCase(N_EVENTS)) &&
          eventSplits!=null && eventSplits.length>1){
        resJobParam[i] = Integer.toString(evtMax-evtMin+1);
      }
      else if((jobParam[i]==null || jobParam[i].equals("")) &&
          (jobParamNames[i].equalsIgnoreCase("inputFileURLs"))){
        if(eventSplits!=null && eventSplits.length>1){
          resJobParam[i] = inputs;
        }
        else{
          resJobParam[i] = inputs;
        }
      }
      // Fill in if it matches one of the jobDefinition fields
      //else if(jobdefinitionFields.contains(jobParamNames[i].toLowerCase())){
      else if((jobParam[i]==null || jobParam[i].equals("")) &&
          jobattributenames.contains(jobParamNames[i].toLowerCase())){
        //int jobParamIndex = jobdefinitionFields.indexOf(jobParamNames[i].toLowerCase());
        int jobParamIndex = jobattributenames.indexOf(jobParamNames[i].toLowerCase());
        Debug.debug("Filling in job parameter "+jobParamNames[i]+":"+jobParamIndex+" from "+
            MyUtil.arrayToString(jobattributenames.toArray()), 3);
        if(resCstAttr[jobParamIndex]==null || resCstAttr[jobParamIndex].equals("")){
          //resCstAttr[jobdefinitionFields.indexOf(jobParamNames[i].toLowerCase())] = resJobParam[i];
          resCstAttr[jobParamIndex] = resJobParam[i];
        }
      }
      // Fill in metadata
      else if((jobParam[i]==null || jobParam[i].equals("")) &&
          metaData.containsKey(jobParamNames[i].toLowerCase())){
        resJobParam[i] = (String) metaData.get(jobParamNames[i].toLowerCase());
        Debug.debug("Matched metadata with job parameter: "+jobParamNames[i]+
            "-->"+resJobParam[i], 3);
      }
      else{
        resJobParam[i] = evaluate(jobParam[i], currentPartition, name, number,
            energy, particle, outputDest);
      }
    }
  }

  private ArrayList fillInResCstAttr(int [][] eventSplits, int currentPartition, String name, String number,
     String energy, String particle, String outputDest, int evtMin, int evtMax, String inputFiles)
     throws ArithmeticException, SyntaxException {
    // Add eventMin, eventMax and inputFileURLs if they are
    // present in the fields of jobDefinition, but not in the fixed
    // attributes.
    String [] jobDefFields = dbPluginMgr.getFieldNames("jobDefinition");
    for(int i=0; i<jobDefFields.length; ++i){
      jobDefFields[i] = jobDefFields[i].toLowerCase();
    }
    ArrayList jobdefinitionFields = new ArrayList(Arrays.asList(jobDefFields));    
    ArrayList jobattributenames = new ArrayList(Arrays.asList(cstAttrNames));
    ArrayList jobAttributeNames = new ArrayList(Arrays.asList(cstAttrNames));
    ArrayList jobAttributes = new ArrayList(Arrays.asList(cstAttr));
    for(int i=0; i<jobattributenames.size(); ++i){
      jobattributenames.set(i, jobattributenames.get(i).toString().toLowerCase());
    }
    if(!jobattributenames.contains(EVENT_MIN) &&
        jobdefinitionFields.contains(EVENT_MIN)){
      jobAttributeNames.add(EVENT_MIN);
      jobAttributes.add("0");
    }
    if(!jobattributenames.contains(EVENT_MAX) &&
        jobdefinitionFields.contains(EVENT_MAX)){
      jobAttributeNames.add(EVENT_MAX);
      jobAttributes.add("0");
    }
    if(!jobattributenames.contains(N_EVENTS) &&
        jobdefinitionFields.contains(N_EVENTS)){
      jobAttributeNames.add(N_EVENTS);
      jobAttributes.add("0");
    }
    if(!jobattributenames.contains("inputFileURLs") &&
        jobdefinitionFields.contains("inputFileURLs")){
      jobAttributeNames.add("inputFileURLs");
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
      Debug.debug("eventSplits: "+(eventSplits==null?-1:eventSplits.length), 3);
      if(cstAttrNames[i].equalsIgnoreCase(EVENT_MIN) &&
          eventSplits!=null && eventSplits.length>1){
        Debug.debug("setting evtMin to "+evtMin, 3);
        resCstAttr[i] = Integer.toString(evtMin);
      }
      else if(cstAttrNames[i].equalsIgnoreCase(EVENT_MAX) &&
          eventSplits!=null && eventSplits.length>1){
        Debug.debug("setting eventMax to "+evtMax, 3);
        resCstAttr[i] = Integer.toString(evtMax);
      }
      else if(cstAttrNames[i].equalsIgnoreCase(N_EVENTS) &&
          eventSplits!=null && eventSplits.length>1){
        Debug.debug("setting event number", 3);
        resCstAttr[i] = Integer.toString(evtMax-evtMin+1);
      }
      else if(cstAttrNames[i].equalsIgnoreCase("inputFileURLs")){
        if(eventSplits!=null && eventSplits.length>1){
          resCstAttr[i] = inputFiles;
        }
        else{
          resCstAttr[i] = inputFiles;
        }
        // all files from input dataset containing the needed events
        Debug.debug("setting input files "+inputFiles, 3);
      }
      else{
        resCstAttr[i] = evaluate(cstAttr[i], currentPartition, name, number,
            energy, particle, outputDest);
      }
    }
    return jobattributenames;
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

    Debug.debug("showing results for confirmation", 3);
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

    Debug.debug("creating dialog", 3);
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
