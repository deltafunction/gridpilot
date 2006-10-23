package gridpilot;

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

  static final int EXIT_VALIDATED = 1;
  static final int EXIT_UNDECIDED = 2;
  static final int EXIT_FAILED = 3;
  static final int EXIT_UNEXPECTED = 4;
  static final int ERROR = -1;

  private LogFile logFile;
  private ConfigFile configFile;
  private int maxSimultaneaousValidation = 3;
  private int currentSimultaneousValidation = 0;
  private Vector toValidateJobs = new Vector();
  private Vector waitingJobs = new Vector();
  /** Delay (minimum) between the moment when AtCom finds out job is done, and the validation*/
  private int delayBeforeValidation = 5000;
  private java.util.Timer timer = new java.util.Timer();
  private String [] errorPatterns = null;
  private String [] errorAntiPatterns = null;
  
  public JobValidation(){
    logFile =  GridPilot.getClassMgr().getLogFile();
    configFile = GridPilot.getClassMgr().getConfigFile();
    loadValues();
  }

  public void loadValues(){
    String delay = configFile.getValue("GridPilot", "delay before validation");
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
      logFile.addMessage(configFile.getMissingMessage("GridPilot", "delay before validation") + "\n" +
                         "Default value = " + delayBeforeValidation);
    }
    errorPatterns = configFile.getValues("GridPilot", "validation error patterns");
    errorAntiPatterns = configFile.getValues("GridPilot", "validation error anti patterns");
  }

  /**
   * Adds the specified job into the job to validate queue after
   * <code>delayBeforeValidation</code> ms. <p>
   * After this delay, and if there is not too much pending validation, this job
   * validation is started immediatly. <p>
   */
  public synchronized void validate(JobInfo job){
    Debug.debug("validate " + job.getName() + " at " + Calendar.getInstance().getTime().toString(), 2);
    waitingJobs.add(job);
    timer.schedule(new TimerTask(){
      public void run(){
        delayElapsed();
      }
    }, delayBeforeValidation);
  }

  private synchronized void delayElapsed(){
    Debug.debug("delayElapsed for " + ((JobInfo) waitingJobs.get(0)).getName() + " at " + Calendar.getInstance().getTime().toString(), 2);
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
    if(currentSimultaneousValidation<maxSimultaneaousValidation){
      final JobInfo job = (JobInfo) toValidateJobs.remove(0);
      ++currentSimultaneousValidation;
      new Thread(){
        public void run(){
          Debug.debug("Validating job "+job.getName(), 2);
          int dbStatus = doValidate(job);
          GridPilot.getClassMgr().getStatusBar().setLabel("Validation of " + job.getName() + " done : "
              + DBPluginMgr.getStatusName(dbStatus) +
              " (" + (toValidateJobs.size() + waitingJobs.size())+ " jobs in the queue )");
          endOfValidation(job, dbStatus);
        }
      }.start();
    }
    else{
      Debug.debug("WARNING: currentSimultaneousValidation >= maxSimultaneaousValidation : "+
          currentSimultaneousValidation+">="+maxSimultaneaousValidation, 3);
    }
  }

  /**
   * Called when a validation ends. <br>
   * update database, warns, and checks for a new validation. <p>
   */
  private void endOfValidation(JobInfo job, int dbStatus){
    if(!GridPilot.getClassMgr().getDBPluginMgr(job.getDBName()).updateJobStdoutErr(
        job.getJobDefId(), job.getValidationResult())){
      logFile.addMessage("DB updateJobStdoutErr(" + job.getJobDefId() + ", " +
                         job.getValidationResult() +
                         ") failed", job);
    }
    if(dbStatus!=job.getDBStatus()){
      DatasetMgr datasetMgr = GridPilot.getClassMgr().getDatasetMgr(job.getDBName(),
          GridPilot.getClassMgr().getDBPluginMgr(job.getDBName()).getJobDefDatasetID(
              job.getJobDefId()));
      datasetMgr.updateDBStatus(job, dbStatus);
    }
    if(dbStatus!=job.getDBStatus()){ // checks that updateDBStatus succeded
      logFile.addMessage("update DB status failed after validation ; " +
                         "this job is set back updatable, and will be revalidated later " +
                         "(after redetection of this job end", job);
      job.setInternalStatus(ComputingSystem.STATUS_ERROR);
      job.setNeedToBeRefreshed(true);
    }

    --currentSimultaneousValidation;
    newValidation();
  }

  /**
   * Takes as parameter the array {String <stdout>[, String <stderr>]}.
   * Returns {String <error lines in stdout>[, String <error lines in stderr>]}.
   **/
  private String validate(String [] outs){
    String [] outArray = Util.split(outs[0], "[\\n\\r]");
    Vector outErrArray = new Vector();
    boolean foundError = false;
    for(int i=0; i<outArray.length; ++i){
      for(int j=0; j<errorPatterns.length; ++j){
        foundError = false;
        if(outArray[i].contains(errorPatterns[j])){
          foundError = true;
          for(int k=0; k<errorAntiPatterns.length; ++k){
            if(outArray[i].contains(errorAntiPatterns[k])){
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
      errArray = Util.split(outs[1], "[\\n\\r]");
    }
    Vector errErrArray = new Vector();
    for(int i=0; i<errArray.length; ++i){
      foundError = true;
      for(int k=0; k<errorAntiPatterns.length; ++k){
        if(errArray[i].contains(errorAntiPatterns[k])){
          foundError = false;
          break;
        }
      }
      if(foundError){
        errErrArray.add(errArray[i]);
      }
    }
    return Util.arrayToString(outErrArray.toArray(), "\n")+
           Util.arrayToString(errErrArray.toArray(), "\n");
  }

  /**
   * Starts the validation and sets this job DBStatus corresponding to the exit value. <p>
   * Called by {@link #newValidation()}
   */
  private int doValidate(JobInfo job){
    int exitValue;
    String [] outs = null;
    long beginTime = new Date().getTime();
    Debug.debug("is going to validate ("+currentSimultaneousValidation + ") " + job.getName() + "..." , 2);
    try{
      if((job.getStdOut()==null || job.getStdOut().length()==0) &&
         (job.getStdErr()==null || job.getStdErr().length()==0)){
         logFile.addMessage("Validation script for job " + job.getName()  +
             ") cannot be run : this job doesn't have any outputs", job);
         return DBPluginMgr.UNDECIDED;
      }
      
      try{
        outs = GridPilot.getClassMgr().getCSPluginMgr().getCurrentOutputs(job);
      }
      catch(Exception e){
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
      
      if(job.getStdErr()==null){
        outs = new String[] {outs[0]};
      }

      String errorMatches = validate(outs);
      Debug.debug("validation script ended with : " + errorMatches, 3);
      job.setValidationResult(errorMatches);
      if(errorMatches.length()==0){
        exitValue = EXIT_VALIDATED;
      }
      else{
        exitValue = EXIT_UNDECIDED;
      }

    }
    catch(Exception ioe){
      exitValue = ERROR;
      logFile.addMessage("IOException during job " + job.getName() + " validation :\n" +
                         "\tException\t: " + ioe.getMessage(), ioe);
    }

    int dbStatus;
    switch(exitValue){
      case EXIT_VALIDATED :
        Debug.debug("Validation : exit validated", 2);
        dbStatus = DBPluginMgr.VALIDATED;
        extractInfo(job, outs[0]);
        break;
      case EXIT_UNDECIDED :
        Debug.debug("job " + job.getName() + " Validation : exit undecided", 2);
        dbStatus = DBPluginMgr.UNDECIDED;
        break;
      case EXIT_FAILED :
        Debug.debug("job " + job.getName() + " Validation : exit failed", 2);
        dbStatus = DBPluginMgr.FAILED;
        break;
      case EXIT_UNEXPECTED :
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
                (exitValue==EXIT_VALIDATED ? "Validated" :
                exitValue==EXIT_UNDECIDED ? "Undecided" :
                exitValue==EXIT_UNEXPECTED ? "Unexpected" :
                exitValue==EXIT_FAILED ? "Failed" :
                "(Wrong value)") , 3);
   return dbStatus;
  }
  
  /**
   * Extracts some information from stdout of this job and tries to fill
   * in db fields. <br>
   * Recognized lines are lines of the form
   * GRIDPILOT METADATA: <attribute> = <value>
   * 
   * @return <code>true</code> if the extraction went ok, <code>false</code> otherwise.
   * 
   * (from AtCom1)
   */
  private boolean extractInfo(JobInfo job, String stdOut){
    StringTokenizer st = new StringTokenizer(stdOut.toString(), "\n");
    Vector attributes = new Vector();
    Vector values = new Vector();
    int lineNr = 0;
    while(st.hasMoreTokens()){
      ++ lineNr;
      String line = st.nextToken();
      int indexIs = line.indexOf("=");
      if(!line.startsWith("GRIDPILOT METADATA:") || indexIs==-1){
        continue;
      }
      String attr = line.substring(19, indexIs).trim();
      String val = line.substring(indexIs+1);
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

    Debug.debug("attr : "+ attributes.toString() + "\nvalues : "+ values.toString(), 2);
    String [] attrArray = new String[attributes.size()];
    String [] valuesArray = new String[values.size()];
    for(int i=0; i< attrArray.length ; ++i){
      attrArray[i] = attributes.get(i).toString();
      valuesArray[i] = values.get(i).toString();
    }

    if(attrArray.length>0 &&
        !GridPilot.getClassMgr().getDBPluginMgr(job.getDBName()).updateJobDefinition(job.getJobDefId(), attrArray, valuesArray)){
      logFile.addMessage("Unable to update DB for job " + job.getName() + "\n"+
                         "attributes : " + Util.arrayToString(attrArray) + "\n" +
                         "values : " + Util.arrayToString(valuesArray), job);
      return false;
    }
    return true;
  }

}
