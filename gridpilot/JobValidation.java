package gridpilot;

import gridfactory.common.ConfigFile;
import gridfactory.common.Debug;

import java.util.*;

/**
 * Executes validation on all done jobs. <p>
 * When JobControl asks for a job validation, this job is appended to the queue
 * <code>toValidatesJobs</code> after <code>delayBeforeValidation</code>.
 * After this delay, and if there is not too many validation threads, a new thread
 * is started for the job validation.
 *
 * <p><a href="JobValidation.java.html">see sources</a>
 *
 */

public class JobValidation{

  static final int ERROR = -1;

  private MyLogFile logFile;
  private ConfigFile configFile;
  private int maxSimultaneaousValidating = 3;
  private int currentSimultaneousValidating = 0;
  private Vector<MyJobInfo> toValidateJobs = new Vector<MyJobInfo>();
  private Vector<MyJobInfo> waitingJobs = new Vector<MyJobInfo>();
  /** Delay (minimum) between the moment when GridPilot finds out job is done, and the validation. */
  private int delayBeforeValidation = 5000;
  private java.util.Timer timer = new java.util.Timer();
  private String [] errorPatterns = null;
  private String [] errorAntiPatterns = null;
  private boolean validateOutput = false;
  
  public JobValidation(){
    logFile =  GridPilot.getClassMgr().getLogFile();
    configFile = GridPilot.getClassMgr().getConfigFile();
    loadValues();
  }

  public void loadValues(){
    String validateOutputStr = configFile.getValue("Computing systems", "Validate output");
    if(validateOutputStr!=null){
      try{
        validateOutput = validateOutputStr.trim().equalsIgnoreCase("yes") || validateOutputStr.trim().equalsIgnoreCase("true");
      }
      catch(Exception e){
        logFile.addMessage("Value of \"validate output\" "+
                           "cannot be parsed as boolean. Defaulting to "+validateOutput, e);
      }
    }
    else{
      logFile.addMessage(configFile.getMissingMessage("Computing systems", "delay before validation") + "\n" +
                         "Default value = " + delayBeforeValidation);
    }
    String delay = configFile.getValue("Computing systems", "Delay before validation");
    if(delay!=null){
      try{
        delayBeforeValidation = Integer.parseInt(delay);
      }
      catch(NumberFormatException nfe){
        logFile.addMessage("Value of \"delay before validation\" "+
                           "is not an integer in configuration file", nfe);
      }
    }
    else{
      logFile.addMessage(configFile.getMissingMessage("Computing systems", "delay before validation") + "\n" +
                         "Default value = " + delayBeforeValidation);
    }
    String maxVal = configFile.getValue("Computing systems", "Max simultaneous validating");
    if(maxVal!=null){
      try{
        maxSimultaneaousValidating = Integer.parseInt(maxVal);
      }
      catch(NumberFormatException nfe){
        logFile.addMessage("Value of \"max simultaneous validating\" "+
                           "is not an integer in configuration file", nfe);
      }
    }
    else{
      logFile.addMessage(configFile.getMissingMessage("Computing systems", "max simultaneous validating") + "\n" +
                         "Default value = " + maxSimultaneaousValidating);
    }
    errorPatterns = configFile.getValues("Computing systems", "validation error patterns");
    errorAntiPatterns = configFile.getValues("Computing systems", "validation error antipatterns");
  }

  /**
   * Adds the specified job into the job to validate queue after
   * <code>delayBeforeValidation</code> ms. <p>
   * After this delay, and if there is not too much pending validation, this job
   * validation is started immediately. <p>
   */
  public synchronized void validate(MyJobInfo job){
    Debug.debug("validate " + job.getName() + " at " + Calendar.getInstance().getTime().toString(), 2);
    waitingJobs.add(job);
    timer.schedule(new TimerTask(){
      public void run(){
        delayElapsed();
      }
    }, delayBeforeValidation);
  }

  private synchronized void delayElapsed(){
    Debug.debug("delayElapsed for " + waitingJobs.get(0).getName() + " at " + Calendar.getInstance().getTime().toString(), 2);
    if(waitingJobs.isEmpty()){
      Debug.debug("WARNING: waitingJobs empty", 2);
      return;
    }
    toValidateJobs.add(waitingJobs.remove(0));
    newValidation();
  }

  /**
   * Checks is any validation thread is pending. <p>
   */
  public synchronized boolean isValidating(){
    return !toValidateJobs.isEmpty();
  }

  /**
   * Checks if a validation can start, starts a new validation thread. <p>
   */
  private synchronized void newValidation(){

    if(toValidateJobs.isEmpty()){
      return;
    }
    if(currentSimultaneousValidating<maxSimultaneaousValidating){
      final MyJobInfo job = toValidateJobs.remove(0);
      ++currentSimultaneousValidating;
      new Thread(){
        public void run(){
          Debug.debug("Validating job "+job.getName(), 2);
          int dbStatus = doValidate(job);
          Integer resubmit = (Integer) GridPilot.getClassMgr().getGlobalFrame().getMonitoringPanel(
             ).getJobMonitoringPanel().getSAutoResubmit().getValue();
          // If we are running in automatic resubmission mode, there is no such thing as undecided.
          // Just set the job to failed.
          if(resubmit>0 && (dbStatus==DBPluginMgr.UNDECIDED || dbStatus==DBPluginMgr.UNEXPECTED)){
            dbStatus = DBPluginMgr.FAILED;
          }
          try{
            GridPilot.getClassMgr().getGlobalFrame().getMonitoringPanel().getStatusBar().setLabel("Validation of " + job.getName() + " done : "
                + DBPluginMgr.getStatusName(dbStatus) +
                " (" + (toValidateJobs.size() + waitingJobs.size())+ " jobs in the queue )");
          }
          catch(Exception e){
            e.printStackTrace();
          }
          endOfValidation(job, dbStatus);
        }
      }.start();
    }
    else{
      Debug.debug("WARNING: currentSimultaneousValidation >= maxSimultaneaousValidation : "+
          currentSimultaneousValidating+">="+maxSimultaneaousValidating, 1);
    }
  }

  /**
   * Called when a validation ends. <br>
   * update database, warns, and checks for a new validation. <p>
   */
  private void endOfValidation(MyJobInfo job, int dbStatus){
    if(dbStatus!=job.getDBStatus()){
      JobMgr jobMgr = GridPilot.getClassMgr().getJobMgr(job.getDBName());
      jobMgr.updateDBStatus(job, dbStatus);
    }
    if(dbStatus!=job.getDBStatus()){ // checks that updateDBStatus succeeded
      logFile.addMessage("update DB status failed after validation ; " +
                         "this job is set back updatable, and will be revalidated later " +
                         "(after redetection of this job end", job);
      job.setStatusError();
      job.setNeedsUpdate(true);
    }

    if(!GridPilot.getClassMgr().getDBPluginMgr(job.getDBName()).updateJobValidationResult(
        job.getIdentifier(), job.getValidationResult())){
      logFile.addMessage("DB updateJobValidationResult(" + job.getIdentifier() + ", " +
                         job.getValidationResult() +
                         ") failed", job);
    }
    --currentSimultaneousValidating;
    newValidation();
  }

  /**
   * Takes as parameter the array {String <stdout>[, String <stderr>]}.
   * Returns {String <error lines in stdout>[, String <error lines in stderr>]}.
   **/
  private String validate(String [] outs){
    String [] outArray = MyUtil.split(outs[0], "[\\n\\r]");
    Vector<String> outErrArray = new Vector<String>();
    boolean foundError = false;
    for(int i=0; i<outArray.length; ++i){
      for(int j=0; j<errorPatterns.length; ++j){
        foundError = false;
        if(outArray[i].indexOf(errorPatterns[j])>-1){
          foundError = true;
          for(int k=0; k<errorAntiPatterns.length; ++k){
            if(outArray[i].indexOf(errorAntiPatterns[k])>-1){
              foundError = false;
              break;
            }
          }
        }
        if(foundError){
          outErrArray.add(outArray[i]);
          break;
        }
      }
    }
    String [] errArray = new String [] {};
    if(outs.length==2 && outs[1]!=null){
      errArray = MyUtil.split(outs[1], "[\\n\\r]");
    }
    Vector<String> errErrArray = new Vector<String>();
    for(int i=0; i<errArray.length; ++i){
      foundError = true;
      for(int k=0; k<errorAntiPatterns.length; ++k){
        if(errArray[i].indexOf(errorAntiPatterns[k])>-1){
          foundError = false;
          break;
        }
      }
      if(foundError){
        errErrArray.add(errArray[i]);
      }
    }
    return MyUtil.arrayToString(outErrArray.toArray(), "\n")+
           MyUtil.arrayToString(errErrArray.toArray(), "\n");
  }

  /**
   * Starts the validation and sets this job DBStatus corresponding to the exit value. <p>
   * Called by {@link #newValidation()}
   */
  private int doValidate(MyJobInfo job){
    
    if(!validateOutput ){
      return DBPluginMgr.VALIDATED;
    }
    
    int exitValue;
    String [] outs = null;
    long beginTime = new Date().getTime();
    Debug.debug("is going to validate ("+currentSimultaneousValidating + ") " + job.getName() + "..." , 2);
    try{
      if((job.getOutTmp()==null || job.getOutTmp().length()==0) &&
         (job.getErrTmp()==null || job.getErrTmp().length()==0)){
         logFile.addMessage("Validation for job " + job.getName()  +
             ") cannot be run : this job doesn't have any outputs", job);
         return DBPluginMgr.UNDECIDED;
      }
      
      try{
        outs = GridPilot.getClassMgr().getCSPluginMgr().getCurrentOutput(job);
      }
      catch(Exception e){
        // TODO: we would get an Exception here if trying to resync, since stdout/stderr are gone on the server,
        // but since they were synced by updateStatus, it should be ok
        e.printStackTrace();
      }

      /*if(!LocalStaticShellMgr.existsFile(job.getStdOut()) &&
          !LocalStaticShellMgr.existsFile(job.getStdErr())){
        logFile.addMessage("Validation for job " + job.getName() + 
         " cannot be run : stdout or stderr does not exist", job);
        return DBPluginMgr.UNDECIDED;
      }*/
      
      if(outs==null || outs.length==0 || outs[0]==null){
        logFile.addMessage("Validation for job " + job.getName() + 
         " cannot be run : stdout does not exist", job);
        return DBPluginMgr.UNDECIDED;
      }
      
      if(job.getErrTmp()==null){
        outs = new String[] {outs[0]};
      }

      String errorMatches = validate(outs);
      job.setValidationResult(errorMatches);
      if(errorMatches.length()==0){
        exitValue = DBPluginMgr.VALIDATED;
        Debug.debug("Validation ended with : " + errorMatches, 2);
      }
      else{
        exitValue = DBPluginMgr.UNDECIDED;
        logFile.addMessage("Validation ended with : " + errorMatches);
      }

    }
    catch(Exception ioe){
      exitValue = ERROR;
      logFile.addMessage("IOException during job " + job.getName() + " validation :\n" +
                         "\tException\t: " + ioe.getMessage(), ioe);
    }

    int dbStatus;
    extractInfo(job, outs[0]);
    switch(exitValue){
      case DBPluginMgr.VALIDATED :
        Debug.debug("Validation : exit validated", 2);
        dbStatus = DBPluginMgr.VALIDATED;
        break;
      case DBPluginMgr.UNDECIDED :
        Debug.debug("job " + job.getName() + " Validation : exit undecided", 2);
        dbStatus = DBPluginMgr.UNDECIDED;
        break;
      case DBPluginMgr.FAILED :
        Debug.debug("job " + job.getName() + " Validation : exit failed", 2);
        dbStatus = DBPluginMgr.FAILED;
        break;
      case DBPluginMgr.UNEXPECTED :
        Debug.debug("job " + job.getName() + " Validation : exit with unexpected errors", 2);
        dbStatus = DBPluginMgr.UNEXPECTED;
        break;
      case ERROR :
        Debug.debug("Validation : Error", 3);
        dbStatus = DBPluginMgr.UNDECIDED;
        break;
      default :
        Debug.debug("exit ? : " + exitValue, 3);
        logFile.addMessage("Validation returned a wrong value\n");
        dbStatus = DBPluginMgr.UNDECIDED;
        break;
    }

    Debug.debug("Validation of job " + job.getName() + " took " +
                (new Date().getTime() - beginTime) + " ms with result : " +
                (exitValue==DBPluginMgr.VALIDATED ? "Validated" :
                exitValue==DBPluginMgr.UNDECIDED ? "Undecided" :
                exitValue==DBPluginMgr.UNEXPECTED ? "Unexpected" :
                exitValue==DBPluginMgr.FAILED ? "Failed" :
                "(Wrong value)") , 3);
   return dbStatus;
  }
  
  /**
   * Extracts some information from stdout of this job and tries to fill
   * in db fields. <br>
   * Recognized lines are lines of the form
   * METADATA: <attribute> = <value>
   * 
   * @return <code>true</code> if the extraction went ok, <code>false</code> otherwise.
   * 
   * (from AtCom1)
   */
  private boolean extractInfo(MyJobInfo job, String stdOut){
    StringTokenizer st = new StringTokenizer(stdOut.toString(), "\n");
    Vector<String> attributes = new Vector<String>();
    Vector<String> values = new Vector<String>();
    int lineNr = 0;
    int tagLen = gridfactory.common.jobrun.ForkScriptGenerator.METADATA_TAG.length();
    while(st.hasMoreTokens()){
      ++ lineNr;
      String line = st.nextToken();
      int indexIs = line.indexOf("=");
      if(!line.startsWith(
          gridfactory.common.jobrun.ForkScriptGenerator.METADATA_TAG+":") ||
          indexIs==-1){
        continue;
      }
      String attr = line.substring(tagLen+1, indexIs).trim();
      String val = line.substring(indexIs+1).trim();
      if(attr.length()==0 || val.length()==0){
        logFile.addMessage("ERROR: results of extraction inconsistent for job "
                           + job.getName() + " at line " + lineNr + " : \n" +
                           " Outputs : " + stdOut +
                           "    --> DB update not done");
        continue;
      }
      attributes.add(attr);
      values.add(val);
    }
    
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String [] jobDefFields = dbPluginMgr.getFieldNames("jobDefinition");
    for(int i=0; i<jobDefFields.length ; ++i){
      jobDefFields[i] = jobDefFields[i].toLowerCase();
    }
    HashSet<String> fieldsSet = new HashSet<String>();
    Collections.addAll(fieldsSet, jobDefFields);

    Debug.debug("attr : "+ attributes.toString() + "\nvalues : "+ values.toString(), 2);
    // We lump all non-existing attribute-values into the metaData field.
    HashMap<String, String> attrValMap = new HashMap<String, String>();
    attrValMap.put("metaData", "");
    for(int i=0; i<attributes.size(); ++i){
      if(fieldsSet.contains(attributes.get(i).toLowerCase()) &&
          !attributes.get(i).equalsIgnoreCase("metadata")){
        attrValMap.put(attributes.get(i), values.get(i));
      }
      else{
        String newMeta = attrValMap.get("metaData");
        if(!newMeta.equals("")){
          newMeta += "\n";
        }
        newMeta += attributes.get(i)+": "+values.get(i);
        attrValMap.put("metaData", newMeta);
      }
    }
    String [] attrArray = new String[attrValMap.size()];
    String [] valuesArray = new String[attrValMap.size()];
    String [] keys = attrValMap.keySet().toArray(new String[attrValMap.keySet().size()]);
    for(int i=0; i<attrValMap.size(); ++i){
      attrArray[i] = keys[i];
      valuesArray[i] = attrValMap.get(keys[i]);
    }

    if((attrArray.length>1 || !attrValMap.get("metaData").equals("")) &&
        !dbPluginMgr.updateJobDefinition(job.getIdentifier(), attrArray, valuesArray)){
      logFile.addMessage("Unable to update DB for job " + job.getName() + "\n"+
                         "attributes : " + MyUtil.arrayToString(attrArray) + "\n" +
                         "values : " + MyUtil.arrayToString(valuesArray), job);
      return false;
    }
    return true;
  }

}
