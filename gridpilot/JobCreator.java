package gridpilot;

import gridfactory.common.DBRecord;
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
  private Vector<Integer> vPartition = new Vector<Integer>();
  private Vector<String []> vCstAttr = new Vector<String []>();
  private Vector<String []> vJobParam = new Vector<String []>();
  private Vector<String [][]> vOutMap = new Vector<String [][]>();
  private Vector<String []> vStdOut = new Vector<String []>();
  private JProgressBar pb;
  private Object semaphoreDBCreate = new Object();
  private DBPluginMgr dbPluginMgr = null;
  private DBPluginMgr inputMgr = null;
  private String dbName;
  private StatusBar statusBar;
  // Caches
  private HashMap datasetNameIds = new HashMap();
  private HashMap datasetIdFiles = new HashMap();
  
  private DBResult inputFileRecords = null;
  private String [] inputFileIds = null;
  private DBResult inputJobDefRecords = null;
  private String [] inputJobDefIds = null;
  
  private boolean closeWhenDone;
  private Window parent;

  private String currentDatasetID;
  private String inputDatasetName;
  private String inputDatasetID;


  public static String EVENT_MIN = "eventMin";
  public static String EVENT_MAX = "eventMax";
  public static String N_EVENTS = "nEvents";

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
                    String [] _stdOutNames,
                    boolean _closeWhenDone,
                    Window _parent){
    
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
    closeWhenDone = _closeWhenDone;
    parent = _parent;

    resCstAttr = new String[cstAttr.length];
    resJobParam = new String[jobParam.length];
    resOutMap = new String [outMap.length][2];
    resStdOut  = new String[stdOut.length];
    dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);

    createAllJobDefs();
    
    if(closeWhenDone){
      parent.dispose();
    }
    
  }

  private void createAllJobDefs(){
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
    int [][] eventSplits = null;
    int [] split;
    //  When using recon_detail, version is not known
    for(int i=0; i<datasetIdentifiers.length; ++i){
      
      currentDatasetID = datasetIdentifiers[i];
      setInputMgrAndDataset();
      
      Debug.debug("Creating job definitions for dataset "+currentDatasetID, 2);
      // NOTICE: here we assume that the fields totalFiles and totalEvents are
      // present in dataset. If they are not, automatic splitting will not be done,
      // that is, the fields eventMin, eventMax and nEvents will not be set in the
      // jobDefinition record.
      eventSplits = dbPluginMgr.getEventSplits(currentDatasetID);
      // First, see if we can split according to event specifications
      if(eventSplits!=null && eventSplits.length>0 && eventSplits[0][0]>-1){
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
        split = getSplitFromOutputDataset();
        firstPartition = split[0];
        lastPartition = split[1];
      }    
      // If that didn't work, see if we can get the number of files
      // of the input dataset directly
      if(lastPartition<=0){
        String error = "WARNING: could not get event splitting.";
        GridPilot.getClassMgr().getLogFile().addInfo(error);
        statusBar.setLabel(error);
        split = getSplitFromInputDataset();
        firstPartition = split[0];
        lastPartition = split[1];
      }
      // If that didn't work, see if we can get the number of files
      // of the input dataset by querying the dataset catalog
      if(lastPartition<=0){
        String error = "WARNING: number of files not in dataset record.";
        GridPilot.getClassMgr().getLogFile().addInfo(error);
        statusBar.setLabel(error);
        split = getSplitFromInputCatalog();
        firstPartition = split[0];
        lastPartition = split[1];
      }
      // If that didn't work, bail out
      if(lastPartition<=0){
        String error = "ERROR: Could not get number of input files from input dataset.";
        GridPilot.getClassMgr().getLogFile().addMessage(error);
        return;
      }
      Debug.debug("first partition: "+firstPartition, 1);
      Debug.debug("last partition: "+lastPartition, 1);
      if(firstPartition>lastPartition){
        GridPilot.getClassMgr().getLogFile().addMessage(
            "ERROR: first value (from) cannot be greater then last value (To) : " +
            firstPartition + ">" + lastPartition);
        return;
      }
      int partitionCount = 0;
      int currentPartitionCount = 0;
      for(int currentPartition=firstPartition; currentPartition<=lastPartition; ++currentPartition){
        currentPartitionCount = prepareJobDefs(currentPartition, lastPartition, eventSplits, i);
        if(currentPartitionCount==-1){
          return;
        }
        partitionCount += currentPartitionCount;
      }
      pb = statusBar.createJProgressBar(0, partitionCount);
      statusBar.setProgressBar(pb);
      try{
        createDBJobDefinitions();
      }
      catch(java.lang.Exception e){Debug.debug("Failed creating partition from "+
          currentDatasetID+" : "+e.getMessage(),3);}
      //statusBar.removeProgressBar(pb);
      //statusBar.removeLabel();
    }
  }
  
  private void setInputMgrAndDataset() {
    String inputDB = (String) dbPluginMgr.getDataset(
        currentDatasetID).getValue("inputDB");
    if(inputDB!=null && !inputDB.equals("")){
      inputMgr = GridPilot.getClassMgr().getDBPluginMgr(inputDB);
      DBRecord dataset = dbPluginMgr.getDataset(currentDatasetID);
      inputDatasetName = (String) dataset.getValue("inputDataset");
      inputDatasetID = inputMgr.getDatasetID(inputDatasetName);
    }
    else{
      inputMgr = null;
      inputDatasetName = null;
      inputDatasetID = null;
    }
  }

  private int[] getSplitFromInputCatalog() {
    int firstPartition = 1;
    int lastPartition = -1;
    if(inputDatasetID==null){
      GridPilot.getClassMgr().getLogFile().addMessage("No input dataset found for "+
          currentDatasetID);
      return null;
    }
    if(!datasetNameIds.containsKey(inputDatasetName)){
      datasetNameIds.put(inputDatasetName, inputDatasetID);
    }
    if(!datasetIdFiles.containsKey(inputDatasetID)){
      datasetIdFiles.put(inputDatasetID, inputMgr.getFiles(inputDatasetID));
    }
    DBResult inputFiles = (DBResult) datasetIdFiles.get(inputDatasetID);
    lastPartition = inputFiles.values.length;
    return new int [] {firstPartition, lastPartition};
  }

  private int[] getSplitFromInputDataset() {
    int firstPartition = 1;
    int lastPartition = -1;
    // try to get number of files from input dataset
    DBRecord inputDataset;
    try{
      inputDataset = inputMgr.getDataset(inputMgr.getDatasetID(inputDatasetName));
      String totalInputFiles = (String) inputDataset.getValue("totalFiles");
      lastPartition = Integer.parseInt(totalInputFiles);
    }
    catch(Exception ee){
      String error = "ERROR: Could not get number of input files from " +
          "field totalFiles in input dataset record.";
      GridPilot.getClassMgr().getLogFile().addMessage(error);
    }
    return new int [] {firstPartition, lastPartition};
  }

  private int[] getSplitFromOutputDataset(){
    int firstPartition = 1;
    int lastPartition = -1;
    // try to get number of files from input dataset
    try{
      String totalOutputFiles = (String) dbPluginMgr.getDataset(currentDatasetID).getValue("totalFiles");
      lastPartition = Integer.parseInt(totalOutputFiles);
    }
    catch(Exception ee){
      String error = "WARNING: Could not get number of files from " +
          "field totalFiles in dataset record.";
      GridPilot.getClassMgr().getLogFile().addMessage(error);
    }
    return new int [] {firstPartition, lastPartition};
  }

  private int prepareJobDefs(int currentPartition, int lastPartition, int[][] eventSplits, int dsIndex) {
    statusBar.setLabel("Preparing job definition # " + currentPartition);
    int partitionCount = 0;
    boolean skipAll = false;
    boolean skip = false;
    boolean showThis = showResults;
    Debug.debug("evaluating all", 3);
    try{
      String currentDatasetOutputLocation =
        (String) dbPluginMgr.getDataset(currentDatasetID).getValue("outputLocation");
      evaluateAll(currentPartition,
                  currentDatasetOutputLocation,
                  eventSplits);
    }
    catch(Exception ex){
      boolean [] res = confirmContinue(ex, currentPartition);
      skip = res[0];
      skipAll = res[1];
      showThis = res[2];
    }
    Debug.debug("done evaluating all", 3);
    int choice;
    if(showThis){
      if(lastPartition - currentPartition + datasetIdentifiers.length - dsIndex > 1){       
        choice = showResult(currentPartition, resCstAttr, resJobParam, resOutMap, resStdOut,
           MyUtil.OK_ALL_SKIP_ALL_OPTION);
      }
      else{
        choice = showResult(currentPartition, resCstAttr, resJobParam, resOutMap, resStdOut,
           MyUtil.OK_SKIP_OPTION);
      }
      Debug.debug("Choice: "+choice, 2);
      switch(choice){
        case 0  : skip = false;  break; // OK
        case 1  : skip = true;   break; // Skip
        case 2  : skip = false;  showResults = false ; break; // OK for all
        case 3  : skip = true;   skipAll = true; break; // Skip all
        default : skip = true;   skipAll = true; // other (closing the dialog). Same action as "Skip all"
      }
    }
    if(!skip && !skipAll){
      Debug.debug("creating/updating job definition # " + currentPartition +  " in dataset # "+currentDatasetID, 2);
      vPartition.add(new Integer(currentPartition));
      vCstAttr.add(resCstAttr.clone());
      vJobParam.add(resJobParam.clone());
      String [][] tmp = new String[resOutMap.length][2];// clone() doesn't work on String [][]
      for(int i=0; i<tmp.length; ++i){
        tmp[i][0] = resOutMap[i][0];
        tmp[i][1] = resOutMap[i][1];
      }
      vOutMap.add(tmp);
      vStdOut.add(resStdOut.clone());
      ++partitionCount;
    }
    if(skipAll){
      return -1;
    }
    else{
      return partitionCount;
    }
  }

  private boolean[] confirmContinue(Exception ex, int currentPartition) {
    boolean skipAll = false;
    boolean skip = false;
    boolean showThis = showResults;
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
    return new boolean[]{skip, skipAll, showThis};
  }

  private void createDBJobDefinitions() throws Exception{
    String exeName = null;
    String exeVersion = null;
    String id = "-1";
    synchronized(semaphoreDBCreate){
      while(!vPartition.isEmpty()){
        int part = ((Integer) vPartition.remove(0)).intValue();
        resCstAttr = vCstAttr.remove(0);
        resJobParam = vJobParam.remove(0);
        resOutMap = vOutMap.remove(0);
        resStdOut  = vStdOut.remove(0);

        statusBar.setLabel("Creating job definition # " + part);
        statusBar.incrementProgressBarValue(pb, 1);

        exeName = dbPluginMgr.getDatasetExecutableName(currentDatasetID);
        exeVersion = dbPluginMgr.getDatasetExecutableVersion(currentDatasetID);
        id = currentDatasetID;
        Debug.debug("Got executable: "+exeName+":"+exeVersion+" <-- "+currentDatasetID, 3);
        Debug.debug("stdout/stderr length "+resStdOut.length, 2);
        Debug.debug("cstAttrNames --> "+MyUtil.arrayToString(cstAttrNames), 3);
        Debug.debug("resCstAttr --> "+MyUtil.arrayToString(resCstAttr), 3);
        Debug.debug("resJobParam --> "+MyUtil.arrayToString(resJobParam), 3);
        
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
              " cannot be created.\n\nClick Cancel to stop or OK to continue creating job definitions.", "",
              JOptionPane.OK_CANCEL_OPTION)==JOptionPane.CANCEL_OPTION){
            break;
          }
          statusBar.setLabel("Job definition # " + part + " NOT created.");
        }
        else{
          statusBar.setLabel("Job definition # " + part + " created.");
        }
      }
      statusBar.removeProgressBar(pb);
    }
  }

  private String evaluate(String ss, int var, String datasetName, String runNumber,
      String [] inputFileURLs, String [] inputFileNames, String outputDest,
      String inputSource)
     throws ArithmeticException, SyntaxException {
    // expression format : ${<arithmExpr>[:length]}
    // arithmExpr : operator priority : (*,/,%), (+,-), left associative
    Debug.debug("Evaluating, "+ss+" : "+var+" : "+datasetName+" : "+runNumber+" : "+
        MyUtil.arrayToString(inputFileURLs)+" : "+MyUtil.arrayToString(inputFileNames)+" : "+outputDest, 3);
    int pos = -1;
    int pos1 = -1;
    int pos2 = -1;
    int pos3 = -1;
    int pos4 = -1;
    int pos5 = -1;
    int pos6 = -1;
    int totPos = -6;
    StringBuffer sss = new StringBuffer(ss);
    int counter = 0;
    while(true){
      ++counter;
      // Fill in datasetName
      pos1 = sss.indexOf("$n");
      if(pos1>=0){
        sss.replace(pos1, pos1+2, datasetName);
      }
      // Fill in runNumber
      pos2 = sss.indexOf("$r");
      if(pos2>=0){
        sss.replace(pos2, pos2+2, runNumber);
      }
      // Fill in outputDestination
      pos3 = sss.indexOf("$o");
      if(pos3>=0){
      	if(sss.substring(pos3+2, pos3+3).equals("/") && outputDest.length()>0){
          if(outputDest.substring(outputDest.length()-1,
              outputDest.length()).equals("/")){
            sss.replace(pos3, pos3+2,outputDest.substring(0,
                outputDest.length()-1));    
          }
          else{
            sss.replace(pos3, pos3+2, outputDest);
          }
      	}
      	else{
          if(outputDest.length()>0 && outputDest.substring(outputDest.length()-1,
              outputDest.length()).equals("/")){       
            sss.replace(pos3, pos3+2, outputDest);
          }
          else{
            sss.replace(pos3, pos3+2, outputDest+"/");
          }
      	}
      }
      // Fill in inputFileNames
      pos4 = sss.indexOf("$f");
      if(pos4>=0){
        String [] names = new String[inputFileNames.length];
        for(int i=0; i<inputFileNames.length; ++i){
          names[i] = getBaseAndExtension(inputFileNames[i])[0];
        }
        sss.replace(pos4, pos4+2, MyUtil.arrayToString(names));
      }
      // Fill in inputFileURLs
      pos5 = sss.indexOf("$u");
      if(pos5>=0){
        sss.replace(pos5, pos5+2, MyUtil.arrayToString(inputFileURLs));
      }
      // Fill in the relative path of the first input file URL
      pos6 = sss.indexOf("$p");
      if(pos6>=0 && inputSource!=null){
        String path = inputFileURLs[0];
        if(inputSource!=null){
          if(MyUtil.isLocalFileName(path)){
            if(path.startsWith("file:")){
              path = path.replaceFirst("^file:/*"+MyUtil.clearFile(inputSource), "");
              path = path.replaceFirst("^file:/*"+myClearTildeLocally(MyUtil.clearFile(inputSource)), "");
            }
            else{
              path = path.replaceFirst("^"+MyUtil.clearFile(inputSource), "");
              path = path.replaceFirst("^"+myClearTildeLocally(MyUtil.clearFile(inputSource)), "");
            }
          }
          else{
            path = path.replaceFirst("^"+inputSource, "");
          }
        }
        // If the outputLocation of the input dataset does not match
        // the location of this input file, we cannot find a relative path.
        if(path.equals(inputFileURLs[0])){
          Debug.debug("Could not find relative path for "+path, 2);
          sss.replace(pos6, pos6+2, "");
        }
        else{
          path = path.replaceFirst(escapeRegChars(inputFileNames[0]), "");
          Debug.debug("Path: "+path, 2);
          sss.replace(pos6, pos6+2, path);
        }
      }
      // Fill in the fields of the dataset
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
              val = (String) dbPluginMgr.getDataset(dbPluginMgr.getDatasetID(datasetName)
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
      totPos = pos1+pos2+pos3+pos;
      Debug.debug("evaluating, "+totPos+": "+sss, 3);
      if(totPos<-11 || counter>10){
        break;
      }
    }
    return doEvaluate(var, new String(sss));
  }
  
  private String escapeRegChars(String str) {
    String ret = str;
    String [] regChars = {"(", ")", "{", "}", "[", "]",
                          "^", "$",
                          "*", "+", "?"};
    for(int i=0; i<regChars.length; ++i){
      ret = ret.replaceAll("\\"+regChars[i], "\\\\"+regChars[i]);
    }
    return ret;
  }

  private String myClearTildeLocally(String str){
    String ret = MyUtil.clearTildeLocally(str).replaceAll("\\\\", "\\\\\\\\");
    return ret;
  }
  
  private String doEvaluate(int var, String s) throws SyntaxException{
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
   * The first of the output file names of the executable
   * used to produce a dataset.
   * This is used as input file for the jobs of other datasets.
   * So, NOTICE: the (root) output file should always be the FIRST in the
   * field 'outputs' in the executable definition.
   */
  private void evaluateAll(int currentPartition, String outputDest, int [][] eventSplits)
     throws ArithmeticException, SyntaxException,
     IOException{
    
    String datasetName = dbPluginMgr.getDatasetName(currentDatasetID);
    String runNumber = dbPluginMgr.getRunNumber(currentDatasetID);
    
    int evtMin = -1;
    int evtMax = -1;
    try{
      evtMin = eventSplits[currentPartition-1][0];
      evtMax = eventSplits[currentPartition-1][1];
    }
    catch(Exception e){
    }
    String inputDataset = null;
    String inputSource = null;
    boolean fileCatalogInput = false;
    // Construct input file names
    if(inputMgr!=null){
      inputDataset = (String) dbPluginMgr.getDataset(currentDatasetID).getValue("inputDataset");
      inputSource = (String) inputMgr.getDataset(inputMgr.getDatasetID(inputDataset)).getValue("outputLocation");
    }
    if(inputDataset!=null && !inputDataset.equalsIgnoreCase("")){
      fileCatalogInput = evaluateAllWithInput();
    }
    boolean eventsPresent = true;
    // just a quick check to avoid exceptions
    if(inputJobDefRecords!=null && (
        inputJobDefRecords.getValue(0, EVENT_MIN)==null ||
        inputJobDefRecords.getValue(0, EVENT_MAX)==null ||
        inputJobDefRecords.getValue(0, EVENT_MIN).equals("") ||
        inputJobDefRecords.getValue(0, EVENT_MAX).equals("") ||
        inputJobDefRecords.getValue(0, EVENT_MIN).equals("''") ||
        inputJobDefRecords.getValue(0, EVENT_MAX).equals("''") ||
        inputJobDefRecords.getValue(0, EVENT_MIN).equals("no such field") ||
        inputJobDefRecords.getValue(0, EVENT_MAX).equals("no such field"))){
      eventsPresent = false;
      Debug.debug("No event information in dataset "+currentDatasetID+
          ":"+inputJobDefRecords.getValue(0, EVENT_MIN)+inputJobDefRecords.getValue(0, EVENT_MAX), 2);
    }
    String [] inputFileURLs = new String[]{};
    try{
      inputFileURLs = findInputFiles(evtMin, evtMax, currentPartition,
         fileCatalogInput, eventsPresent, inputJobDefIds!=null && inputJobDefIds.length>0);
    }
    catch(Exception e){
      e.printStackTrace();
    }
    // construct the (short) file names for the job script arguments
    Vector<String> inputFileNamesVec = new Vector<String>();
    if(inputFileURLs!=null){
      String [] fils = null;
      String addFils = "";
      for(int j=0; j<inputFileURLs.length; ++j){
        fils = MyUtil.split(inputFileURLs[j], "/");
        if(fils.length>0){
          addFils = fils[fils.length-1];
        }
        else{
          addFils = inputFileURLs[j];
        }
        fils = MyUtil.split(addFils, "\\\\");
        if(fils.length>0){
          inputFileNamesVec.add(fils[fils.length-1]);
        }
        else{
          inputFileNamesVec.add(addFils);
        }
        Debug.debug("Inputs: "+inputFileNamesVec, 2);
      }
    }
    String[] inputFileNames = inputFileNamesVec.toArray(new String[inputFileNamesVec.size()]);
    
    // jobDefinition fields
    ArrayList jobattributenames = fillInResCstAttr(eventSplits, currentPartition, datasetName, runNumber,
        outputDest, inputSource, evtMin, evtMax, inputFileURLs, inputFileNames);
    
    // Job parameters
    fillInJobParams(currentPartition, eventSplits, evtMin, evtMax, datasetName, runNumber,
        outputDest, inputSource, jobattributenames, inputFileURLs, inputFileNames);

    // If the destination is left empty on the creation panel and
    // we are using input files from a file catalog, name the output file
    // by simply using the extension from the output file or
    // (if it's the same as that of the input file)
    // appending ".out" to the input file name.
    if(outMap!=null && outMap.length>0){
      Debug.debug("Filling in outputs, "+fileCatalogInput+", "+!eventsPresent+", "+
          outMap.length+", "+outMap[0][1]+", "+inputFileNames, 3);
      if(fileCatalogInput && !eventsPresent && outMap.length==1 &&
          (outMap[0][1]==null || outMap[0][1].equals("")) && inputFileNames!=null &&
          inputFileURLs.length==1){
        String [] bn = getBaseAndExtension(inputFileNames[0].trim());
        String dest = "$o/$p"+bn[0]+bn[1];
        resOutMap[0][1] = evaluate(dest,
            currentPartition, datasetName, runNumber, inputFileURLs, inputFileNames, outputDest, inputSource);
        resOutMap[0][0] = evaluate(outMap[0][0], currentPartition, datasetName, runNumber,
            inputFileURLs, inputFileNames, outputDest, inputSource);
      }
      else{
        for(int i=0; i<resOutMap.length; ++i){
          resOutMap[i][0] = evaluate(outMap[i][0], currentPartition, datasetName, runNumber,
              inputFileURLs, inputFileNames, outputDest, inputSource);
          resOutMap[i][1] = evaluate(outMap[i][1], currentPartition, datasetName, runNumber,
              inputFileURLs, inputFileNames, outputDest, inputSource);
          Debug.debug("output mapping #"+i+":"+resOutMap[i][0]+"-->"+resOutMap[i][1], 3);
        }
      }
    }
    for(int i=0; i<resStdOut.length; ++i){
      resStdOut[i] = evaluate(stdOut[i], currentPartition, datasetName, runNumber,
          inputFileURLs, inputFileNames, outputDest, inputSource);
      Debug.debug("stdout/err #"+i+": "+resStdOut[i], 3);
    }
  }
  
  private String[] getBaseAndExtension(String file){
    String ifn = file;
    String [] fullInputNameStrings = MyUtil.split(ifn, "\\.");
    String ofn = outMap[0][0];
    String [] fullOutputNameStrings = MyUtil.split(ofn, "\\.");
    String extension = null;
    String extensionIn = null;
    String extensionOut = null;
    String base = ifn;
    if(fullOutputNameStrings.length>0){
      extensionOut = "."+fullOutputNameStrings[fullOutputNameStrings.length-1];
    }
    if(fullInputNameStrings.length>0){
      extensionIn = "."+fullInputNameStrings[fullInputNameStrings.length-1];
      base = ifn.replaceFirst("\\."+fullInputNameStrings[fullInputNameStrings.length-1]+"$", "");
    }
    if(extensionOut!=null && (extensionIn!=null || !extensionOut.equals(extensionIn))){
      extension = extensionOut;
    }
    else if(extensionIn!=null && extensionIn!=null){
      extension = ".out"+extensionOut;
    }
    else if(extensionIn!=null && extensionIn==null){
      extension = extensionOut;
    }
    else{
      extension = ".out";
    }
    Debug.debug("Base name and extension: "+base+" : "+extension, 3);
    return new String [] {base, extension};
  }

  private boolean evaluateAllWithInput() throws IOException{
    boolean fileCatalogInput = false;
    String inputDatasetID = "-1";
    inputFileIds = new String [] {};
    if(inputMgr!=null && inputMgr.isFileCatalog()){
      try{
        String inputDBFileIdentifierField = MyUtil.getIdentifierField(
            inputMgr.getDBName(), "file");
        if(inputDatasetName!=null){
          if(!datasetNameIds.containsKey(inputDatasetName)){
            datasetNameIds.put(inputDatasetName, inputMgr.getDatasetID(inputDatasetName));
          }
          inputDatasetID = (String) datasetNameIds.get(inputDatasetName);
          if(!datasetIdFiles.containsKey(inputDatasetID)){
            datasetIdFiles.put(inputDatasetID, inputMgr.getFiles(inputDatasetID));
          }
          inputFileRecords = (DBResult) datasetIdFiles.get(inputDatasetID);
          inputFileIds = new String[inputFileRecords.values.length];
          for(int i=0; i<inputFileIds.length; ++i){
            inputFileIds[i] = (String) inputFileRecords.getValue(i,
                inputDBFileIdentifierField);
          }
          Debug.debug("Input files for "+currentDatasetID+
              ":"+inputDatasetName+": "+MyUtil.arrayToString(inputFileIds), 3);
        }
      }
      catch(Exception e){
        Debug.debug("No input dataset for "+currentDatasetID, 2);
        //e.printStackTrace();
        //return;
      }
    }
    if(inputMgr!=null && inputMgr.isJobRepository()){
      try{
        String inputDBJobDefIdentifierField = MyUtil.getIdentifierField(
            inputMgr.getDBName(), "jobDefinition");
        if(inputDatasetName!=null ){
          if(!datasetNameIds.containsKey(inputDatasetName)){
            datasetNameIds.put(inputDatasetName, inputMgr.getDatasetID(inputDatasetName));
          }
          inputDatasetID = (String) datasetNameIds.get(inputDatasetName);
          inputJobDefRecords = inputMgr.getJobDefinitions(inputDatasetID, 
                  new String [] {inputDBJobDefIdentifierField, EVENT_MIN, EVENT_MAX, "outFileMapping"},
                  null, null);
          inputJobDefIds = new String[inputJobDefRecords.values.length];
          for(int i=0; i<inputJobDefIds.length; ++i){
            inputJobDefIds[i] = (String) inputJobDefRecords.getValue(i, inputDBJobDefIdentifierField);
          }
          Debug.debug("Input job definitions for "+currentDatasetID+
              ":"+inputDatasetName+": "+MyUtil.arrayToString(inputJobDefIds), 3);
        }
      }
      catch(Exception e){
        Debug.debug("No input dataset for "+currentDatasetID, 2);
        //e.printStackTrace();
        //return;
      }
    }
    else{
      fileCatalogInput = true;
    }
    if((inputFileRecords==null || inputFileRecords.values.length==0) &&
        (inputJobDefRecords==null || inputJobDefRecords.values.length==0)){
      throw new IOException ("Could not get input files, cannot proceed.");
    }
    return fileCatalogInput;
  }

  private String [] findInputFiles(int evtMin, int evtMax, int currentPartition,
      boolean fileCatalogInput, boolean eventsPresent, boolean inputJobDefsPresent) {
    Debug.debug("findInputFiles "+evtMin+":"+evtMax+":"+currentPartition+":"+fileCatalogInput+":"+eventsPresent+":"+inputJobDefsPresent, 3);
    Vector<String> inputFiles = new Vector<String>();
    String inputFileName = null;
    String pfnsField = MyUtil.getPFNsField(inputMgr.getDBName());
    int readEvtMin = -1;
    int readEvtMax = -1;
    if(eventsPresent && inputJobDefsPresent){
      boolean inputFileFound = false;
      for(int j=0; j<inputJobDefIds.length; ++j){
        Debug.debug("checking input record "+inputJobDefRecords.get(j), 3);
        if(Integer.parseInt((String) inputJobDefRecords.getValue(j, EVENT_MIN))==evtMin &&
           Integer.parseInt((String) inputJobDefRecords.getValue(j, EVENT_MAX))==evtMax){
          //inputJobDefOutputFileName = getTransOutFileName(inputDB, currentDatasetID);
          inputFileName = inputMgr.getOutputFiles(inputJobDefIds[j])[0];
          inputFiles.add(inputMgr.getJobDefOutRemoteName(inputJobDefIds[j], inputFileName));
          Debug.debug("Found input file "+inputJobDefIds[j]+"-->"+inputFiles, 3);
          inputFileFound = true;
          break;
        }
      }
      if(!inputFileFound){
        String outFileMapping;
        String [] outMap = null;
        for(int j=0; j<inputJobDefIds.length; ++j){
          outFileMapping = (String) inputJobDefRecords.getValue(j, "outFileMapping");
          try{
            outMap = MyUtil.splitUrls(outFileMapping);
          }
          catch(Exception e){
            e.printStackTrace();
          }
          readEvtMin = Integer.parseInt((String) inputJobDefRecords.getValue(j, EVENT_MIN));
          readEvtMax = Integer.parseInt((String) inputJobDefRecords.getValue(j, EVENT_MAX));
          Debug.debug("Range of events: "+evtMin+"-->"+evtMax, 2);
          Debug.debug("Range of input events: "+readEvtMin+"-->"+readEvtMax, 2);
          
          if(outMap!=null && readEvtMin!=-1 && readEvtMax!=-1 && (
                        // This should catch all cases where events in input files are
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
            inputFileName = outMap[0];
            inputFiles.add(inputMgr.getJobDefOutRemoteName(inputJobDefIds[j], inputFileName));
            Debug.debug("Adding input file "+inputJobDefIds[j]+"-->"+inputFiles, 3);
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
        /*inputFileName = (String) inputMgr.getFile(currentDatasetID,
            inputIds[currentPartition-1]).getValue("lfn");*/
        // Use the first pfn returned.
        // Notice: if "find all PFNs" is checked all PFNs will be looked up,
        // slowing down job creation enormously, and still only the first will be used.
        // So, it should NOT be checked.
        
        DBRecord inputFile = inputMgr.getFile(inputDatasetName, inputFileIds[currentPartition-1], DBPluginMgr.LOOKUP_PFNS_ONE);
        Debug.debug("Looked up input file "+inputFile, 2);
        String inputFils = (String) inputFile.getValue(pfnsField);
        inputFiles.add(inputFils);
        /*String [] inputFilArr = null;
        try{
          inputFilArr = MyUtil.splitUrls(inputFils);
        }
        catch(Exception e){
          Debug.debug("WARNING: could not split as URLs, trying normal split", 1);
          e.printStackTrace();
          inputFilArr = MyUtil.split(inputFils);
        }
        inputFiles = inputFilArr[0];*/
      }
      else{
        inputFileName = inputMgr.getOutputFiles(inputJobDefIds[currentPartition-1])[0];
        inputFiles.add(inputMgr.getJobDefOutRemoteName(inputJobDefIds[currentPartition-1], inputFileName));
      }
      Debug.debug("Input files ---> "+inputFiles, 2);
    }
    return inputFiles.toArray(new String[inputFiles.size()]);
  }

  private void fillInJobParams(int currentPartition,
      int [][] eventSplits, int evtMin, int evtMax, String name, String number,
      String outputDest, String inputSource, ArrayList jobattributenames,
      String [] inputFileURLs, String [] inputFileNames) throws ArithmeticException, SyntaxException {
    Debug.debug("Filling in job parameters", 3);
    // metadata information from the metadata field of the dataset
    String metaDataString = (String) dbPluginMgr.getDataset(currentDatasetID).getValue("metaData");
    HashMap metaData = MyUtil.parseMetaData(metaDataString);
    for(int i=0; i<resJobParam.length; ++i){
      Debug.debug("param #"+i+" : "+jobParamNames[i]+" -> "+
          metaData.containsKey(jobParamNames[i].toLowerCase())+ " : "+
          MyUtil.arrayToString(metaData.keySet().toArray()), 3);
      if((jobParam[i]==null || jobParam[i].equals("")) &&
          (jobParamNames[i].equalsIgnoreCase(EVENT_MIN)) && eventSplits!=null){
        resJobParam[i] = Integer.toString(evtMin);
      }
      else if((jobParam[i]==null || jobParam[i].equals("")) &&
          (jobParamNames[i].equalsIgnoreCase(EVENT_MAX)) && eventSplits!=null){
        resJobParam[i] = Integer.toString(evtMax);
      }
      else if((jobParam[i]==null || jobParam[i].equals("")) &&
          (jobParamNames[i].equalsIgnoreCase(N_EVENTS)) && eventSplits!=null){
        resJobParam[i] = Integer.toString(evtMax-evtMin+1);
      }
      else if((jobParam[i]==null || jobParam[i].equals("")) &&
          (jobParamNames[i].equalsIgnoreCase("inputFileNames"))){
        resJobParam[i] = MyUtil.removeQuotes(MyUtil.arrayToString(inputFileNames));
      }
      else if((jobParam[i]==null || jobParam[i].equals("")) &&
          (jobParamNames[i].equalsIgnoreCase("inputFileURLs"))){
        resJobParam[i] = MyUtil.arrayToString(inputFileURLs);
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
            inputFileURLs, inputFileNames, outputDest, inputSource);
      }
    }
  }

  private ArrayList fillInResCstAttr(int [][] eventSplits, int currentPartition, String datasetName, String runNumber,
     String outputDest, String inputSource, int evtMin, int evtMax, String [] inputFileURLs, String [] inputFileNames)
     throws ArithmeticException, SyntaxException {
    // Add eventMin, eventMax and inputFileURLs if they are
    // present in the fields of jobDefinition, but not in the fixed
    // attributes.
    String [] jobdeffields = dbPluginMgr.getFieldNames("jobDefinition");
    for(int i=0; i<jobdeffields.length; ++i){
      jobdeffields[i] = jobdeffields[i].toLowerCase();
    }
    ArrayList jobdefinitionfields = new ArrayList(Arrays.asList(jobdeffields));    
    ArrayList jobattributenames = new ArrayList(Arrays.asList(cstAttrNames));
    ArrayList jobAttributeNames = new ArrayList(Arrays.asList(cstAttrNames));
    ArrayList jobAttributes = new ArrayList(Arrays.asList(cstAttr));
    for(int i=0; i<jobattributenames.size(); ++i){
      jobattributenames.set(i, ((String) jobattributenames.get(i)).toLowerCase());
    }
    if(!jobattributenames.contains(EVENT_MIN.toLowerCase()) &&
        jobdefinitionfields.contains(EVENT_MIN.toLowerCase())){
      jobAttributeNames.add(EVENT_MIN);
      jobAttributes.add("0");
    }
    if(!jobattributenames.contains(EVENT_MAX.toLowerCase()) &&
        jobdefinitionfields.contains(EVENT_MAX.toLowerCase())){
      jobAttributeNames.add(EVENT_MAX);
      jobAttributes.add("0");
    }
    if(!jobattributenames.contains(N_EVENTS.toLowerCase()) &&
        jobdefinitionfields.contains(N_EVENTS.toLowerCase())){
      jobAttributeNames.add(N_EVENTS);
      jobAttributes.add("0");
    }
    if(!jobattributenames.contains("inputfileurls") &&
        jobdefinitionfields.contains("inputfileurls")){
      jobAttributeNames.add("inputFileURLs");
      jobAttributes.add("");
    }
    if(!jobattributenames.contains("inputfilenames") &&
        jobdefinitionfields.contains("inputfilenames")){
      jobAttributeNames.add("inputFileNames");
      jobAttributes.add("");
    }
    if(!jobattributenames.contains("depjobs") &&
        jobdefinitionfields.contains("depjobs")){
      jobAttributeNames.add("depJobs");
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
      if(cstAttrNames[i].equalsIgnoreCase(EVENT_MIN) && eventSplits!=null){
        Debug.debug("setting evtMin to "+evtMin, 3);
        resCstAttr[i] = Integer.toString(evtMin);
      }
      else if(cstAttrNames[i].equalsIgnoreCase(EVENT_MAX) && eventSplits!=null){
        Debug.debug("setting eventMax to "+evtMax, 3);
        resCstAttr[i] = Integer.toString(evtMax);
      }
      else if(cstAttrNames[i].equalsIgnoreCase(N_EVENTS) && eventSplits!=null){
        Debug.debug("setting event number", 3);
        resCstAttr[i] = Integer.toString(evtMax-evtMin+1);
      }
      else if(cstAttrNames[i].equalsIgnoreCase("inputFileURLs")){
        resCstAttr[i] = MyUtil.arrayToString(inputFileURLs);
        // all files from input dataset containing the needed events
        Debug.debug("setting input files "+MyUtil.arrayToString(inputFileURLs), 3);
      }
      else if(cstAttrNames[i].equalsIgnoreCase("depJobs") && inputFileURLs!=null &&
          inputFileURLs.length>0){
        String[] depJobs = null;
        try{
          depJobs = findDepJobs(inputFileURLs);
        }
        catch (Exception e) {
          e.printStackTrace();
        }
        Debug.debug("setting depJobs "+MyUtil.arrayToString(depJobs), 3);
        resCstAttr[i] = (depJobs==null || depJobs.length==0)?"":MyUtil.arrayToString(depJobs);
      }
      else{
        resCstAttr[i] = evaluate(cstAttr[i], currentPartition, datasetName, runNumber,
            inputFileURLs, inputFileNames, outputDest, inputSource);
      }
    }
    return jobattributenames;
  }

  private String[] findDepJobs(String[] inputFileURLs) throws Exception {
    if(inputMgr==null){
      return null;
    }
    Vector<String> depJobDefIDs = new Vector<String>();
    String jobDefIdField = MyUtil.getIdentifierField(dbName, "jobDefinition");
    DBResult jobDefRecs = inputMgr.getJobDefinitions(inputDatasetID, new String [] {jobDefIdField, "outFileMapping"}, null, null);
    DBRecord jobDefRec;
    String jobDefID;
    String outUrl;
    //String [] outNames;
    String outFileMapping;
    String [] outMap;
    Debug.debug("Looking for file/job dependencies: "+MyUtil.arrayToString(inputFileURLs), 3);
    for(Iterator<DBRecord> it=jobDefRecs.iterator(); it.hasNext();){
      jobDefRec = it.next();
      jobDefID = (String) jobDefRec.getValue(jobDefIdField);
      outFileMapping = (String) jobDefRec.getValue("outFileMapping");
      outMap = MyUtil.splitUrls(outFileMapping);
      for(int i=0; i<outMap.length/2; ++i){
        outUrl = outMap[2*i+1];
        Debug.debug("Checking for dep --> "+outUrl, 3);
        if(MyUtil.arrayContains(inputFileURLs, outUrl)){
          depJobDefIDs.add(jobDefID);
        }
      }
      // The code below works but is too slow.
      /*outNames = inputMgr.getOutputFiles(jobDefID);
      for(int i=0; i<outNames.length; ++i){
        outUrl = inputMgr.getJobDefOutRemoteName(jobDefID, outNames[i]);
        Debug.debug("Checking for dep --> "+outUrl, 3);
        if(MyUtil.arrayContains(inputFileURLs, outUrl)){
          depJobDefIDs.add(jobDefID);
        }
      }*/
    }
    return depJobDefIDs.toArray(new String [depJobDefIDs.size()]);
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
   * Creates a string which contains the string 's', in which each constant ($A, $B, etc)
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
          s.charAt(end + 1) != 'o' && s.charAt(end + 1) != 'f' &&
          s.charAt(end + 1) != 'i' && s.charAt(end + 1) != 'u' &&
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
  
  private int showResult(final int currentPartition, final String [] resCstAttr, final String [] resJobParam,
      final String [][] resOutMap, final String [] resStdOut, final int showOption){
    MyResThread rt = new MyResThread(){
      int ret;
      public void run(){
        ret = showResult0(currentPartition, resCstAttr, resJobParam, resOutMap, resStdOut, showOption);
      }
      public int getIntRes(){
        return ret;
      }
    };
    try{
      SwingUtilities.invokeAndWait(rt);
    }
    catch(Exception e){
      e.printStackTrace();
      return -1;
    }
    return rt.getIntRes();
  }
    
  private int showResult0(int currentPartition, String [] resCstAttr, String [] resJobParam,
                         String [][] resOutMap, String [] resStdOut, int showOption){

    Debug.debug("showing results for confirmation", 3);
    JPanel pResult = new JPanel(new GridBagLayout());
    int row = 0;

    // Fixed parameters
    pResult.add(new JLabel("Job parameters"),
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
    pResult.add(new JLabel("Executable arguments"),
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

      pResult.add(new JLabel(" -> Destination : "),
          new GridBagConstraints(2, row, 1, 1, 0.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.BOTH, new Insets(5, 5, 5, 5), 0, 0));

      pResult.add(new JLabel(resOutMap[i][1]),
          new GridBagConstraints(3, row, 1, 1, 1.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
    }

    // Stdout/stderr files
    if(stdOutNames.length>0){
      pResult.add(new JLabel("stdout and stderr"),
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

    Debug.debug("creating dialog", 3);
    return MyUtil.showResult(parent, sp, "Job definition # "+currentPartition, showOption,
        showOption==MyUtil.OK_SKIP_OPTION?"Cancel":"Skip");
    
  }
}
