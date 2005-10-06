package gridpilot;

/**
 * <p>Title: AtCom</p>
 * <p>Description: An Atlas Commander</p>
 * <p>Copyright: Copyright (c) 2002</p>
 * <p>Company: CERN - EP/ATC</p>
 * @author  Vandy BERTEN (Vandy.Berten@cern.ch)
 * @version 1.2
 */


import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Executes the validation script on all done jobs. <p>
 * The validation script is run with arguments parameters job.StdOut and job.StdErr.<br>
 * This script is run with the following variables in its environment:  <ul>
 * <li>ATCOM_VALIDATED
 * <li>ATCOM_UNDECIDED
 * <li>ATCOM_FAILED
 * </ul>
 * One of these values should be returned. <br>
 * When JobControl asks for a job validation, this job is appended to the queue
 * <code>toValidatesJobs</code> after <code>delayBeforeValidation</code>.
 * After this delay, and if there is not too many validation threads, a new thread
 * is started for the job validation.
 *
 * <p><a href="JobValidation.java.html">see sources</a>
 *
 */

public class JobValidation {

  static final int EXIT_VALIDATED = 1;
  static final int EXIT_UNDECIDED = 2;
  static final int EXIT_FAILED = 3;
  static final int EXIT_UNEXPECTED = 4;
//  static final
  static final int ERROR = -1;

  private String [] env = {
    "ATCOM_VALIDATED=" + EXIT_VALIDATED,
    "ATCOM_UNDECIDED="+ EXIT_UNDECIDED,
    "ATCOM_FAILED="+ EXIT_FAILED,
    "ATCOM_UNEXPECTED="+ EXIT_UNEXPECTED};

  LogFile logFile;
  ConfigFile configFile;

  private int maxSimultaneaousValidation = 3;
  private int currentSimultaneousValidation = 0;

  private Vector toValidateJobs = new Vector();
  private Vector waitingJobs = new Vector();

  /** Delay (minimum) between the moment when AtCom finds out job is done, and the validation*/
  private int delayBeforeValidation = 5000;
  private java.util.Timer timer = new java.util.Timer();
  
  public JobValidation(){
    logFile =  GridPilot.getClassMgr().getLogFile();
    configFile = GridPilot.getClassMgr().getConfigFile();
    loadValues();
  }

  public void loadValues(){
    String delay = configFile.getValue("gridpilot", "delay before validation");
    if(delay != null){
      try{
        delayBeforeValidation = Integer.parseInt(delay);
      }catch(NumberFormatException nfe){
        logFile.addMessage("Value of \"delay before validation\" "+
                           "is not an integer in configuration file", nfe);
      }
    }
    else
      logFile.addMessage(configFile.getMissingMessage("gridpilot", "delay before validation") + "\n" +
                         "Default value = " + delayBeforeValidation);

  }

  /**
   * Adds the specified job into the job to validate queue after
   * <code>delayBeforeValidation</code> ms. <p>
   * After this delay, and if there is not too much pending validation, this job
   * validation is started immediatly. <p>
   *
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
      Debug.debug("!!!!! waitinJobs empty", 3);
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

    if(toValidateJobs.isEmpty())
      return;

    if(currentSimultaneousValidation < maxSimultaneaousValidation){
      final JobInfo job = (JobInfo) toValidateJobs.remove(0);
      ++currentSimultaneousValidation;
      new Thread(){
        public void run(){
          GridPilot.getClassMgr().getStatusBar().setLabel("Validating " + job.getName() + " ... " +
              "(" + (toValidateJobs.size() + waitingJobs.size())+ " jobs in the queue )");
          String validationScriptShortPath =
            GridPilot.getClassMgr().getDBPluginMgr(job.getDBName()).getJobTransValue(job.getJobDefId(), "valScript");
          String validationScriptFile = (new File(validationScriptShortPath)).getName();
          String validationScriptPath = Util.getFullPath(validationScriptShortPath);
          String validationScriptUrl = Util.getURL(validationScriptShortPath);
          String validationScript = null;
          // If url is given, the file will already have been downloaded to the
          // working directory. Currently NG is the only plugin supporting this
          Debug.debug("Checking CS "+job.getCSName(), 2);
          if(job.getCSName().equalsIgnoreCase("NG") &&
              validationScriptUrl!=null && validationScriptUrl.length()>0){
            int lastSlash = job.getStdOut().lastIndexOf("/");
            // stdout will always be one level above the working directory.
            // See job.setOutputs in SubmissionControl.java
            if(lastSlash>-1){
              File dir = new File(job.getStdOut().substring(0,job.getStdOut().lastIndexOf("/")));
              validationScript = dir.getAbsolutePath()+"/"+validationScriptFile;
            }
            else{
              validationScript = validationScriptFile;
            }
          }
          else{
            validationScript = validationScriptPath;
          }
          int amiStatus = validate(job, validationScript);
          GridPilot.getClassMgr().getStatusBar().setLabel("Validation of " + job.getName() + " done : "
              + DBPluginMgr.getStatusName(amiStatus) +
              " (" + (toValidateJobs.size() + waitingJobs.size())+ " jobs in the queue )");
          endOfValidation(job, amiStatus);
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
   * update database, warns JobControl, and checks for a new validation. <p>
   */
  private void endOfValidation(JobInfo job, int dbStatus){
    if(!GridPilot.getClassMgr().getDBPluginMgr(job.getDBName()).updateJobStdoutErr(
        job.getJobDefId(), job.getValidationStdOut(), job.getValidationStdErr()))
      logFile.addMessage("AMI updatePartJob(" + job.getJobDefId() + ", " +
                         job.getValidationStdOut() + ", " + job.getValidationStdErr() +
                         ") failed", job);

    if(dbStatus != job.getDBStatus()){
      TaskMgr taskMgr = GridPilot.getClassMgr().getTaskMgr(job.getDBName(),
          GridPilot.getClassMgr().getDBPluginMgr(job.getDBName()).getTaskId(
              job.getJobDefId()));
      taskMgr.updateDBStatus(job, dbStatus);
    }

    if(dbStatus != job.getDBStatus()){ // checks that updateAMIStatus succeded
      logFile.addMessage("update AMI status failed after validation ; " +
                         "this job is set back updatable, and will be revalidated later " +
                         "(after redetection of this job end", job);
      job.setAtComStatus(ComputingSystem.STATUS_ERROR);
      job.setNeedToBeRefreshed(true);
    }

    --currentSimultaneousValidation;
    newValidation();
  }


  /**
   * Starts the specified script, giving as argument job.StdOut and job.StdErr, creates files
   * from this script outputs, and set this job AMIStatus regarding to this script exit value. <p>
   * Called by {@link #newValidation()}
   */
  private int validate(JobInfo job, String validationScript){

    long beginTime = new Date().getTime();

    ShellMgr shell = GridPilot.getClassMgr().getCSPluginMgr().getShellMgr(job);

    Debug.debug("is going to validate ("+currentSimultaneousValidation + ") " + job.getName() + "..." , 2);

    try{
      //    if(! new File(validationScript).exists()){
      if(!shell.existsFile(validationScript)){
        logFile.addMessage("Validation script for job " + job.getName() + " (" +
                           validationScript + ") doesn't exist ; validation cannot be done");
        job.setValidationOutputs(null, null);
        return DBPluginMgr.UNDECIDED;
      }
    }
    catch(Exception e){
      Debug.debug("ERROR checking for validation script: "+e.getMessage(), 2);
      logFile.addMessage("ERROR checking for validation script: "+e.getMessage());
      //throw e;
    }


    int exitValue;
    String cmd = "";
    try{
      if((job.getStdOut() == null || job.getStdOut().length() == 0) &&
         (job.getStdErr() == null || job.getStdErr().length() == 0)){
         logFile.addMessage("Validation script for job " + job.getName() + " (" +
                          validationScript + ") cannot be run : this job doesn't have any outputs", job);
         job.setValidationOutputs(null, null);
         return DBPluginMgr.UNDECIDED;
      }

      //    if( ! new File(job.getStdOut()).exists() && ! new File(job.getStdErr()).exists()){
      if( ! shell.existsFile(job.getStdOut()) && ! shell.existsFile(job.getStdErr())){
        logFile.addMessage("Validation script for job " + job.getName() + " (" +
                           validationScript + ") cannot be run : stdout and stderr do not exist", job);
        job.setValidationOutputs(null, null);
        return DBPluginMgr.UNDECIDED;
      }
   /* }
    catch(IOException e){
      Debug.debug("ERROR checking for stdout: "+e.getMessage(), 2);
      logFile.addMessage("ERROR checking for stdout: "+e.getMessage());
      //throw e;
    }*/

      LogFile logFile = GridPilot.getClassMgr().getLogFile();
      /** -> not necessary anymore -> if job.getStdOut exists, its parents exists
      File f = new File(job.getStdOut());
      if(!f.getParentFile().exists()){
        logFile.addMessage("the parent directory of the output file doesn't exist ; " +
                           "this directory is re-created");
        f.getParentFile().mkdirs();
      }
  */
  
      String parent = job.getStdOut().substring(0, job.getStdOut().lastIndexOf('/') + 1);
      String validationStdOut = parent + "validationStdOut";
      String validationStdErr = parent + "validationStdErr";
  
  
      cmd = validationScript+" "+job.getStdOut()+" "+job.getStdErr();

    //try{
      StringBuffer stdOut = new StringBuffer();
      StringBuffer stdErr = new StringBuffer();
      Debug.debug("exec ... (" + cmd +")", 2);
      exitValue = shell.exec(cmd, stdOut, stdErr);
      Debug.debug("validation script ended with exit value : " + exitValue, 2);

      if(stdOut.length()!=0){
        shell.writeFile(validationStdOut, stdOut.toString(), false);
        job.setValidationStdOut(validationStdOut);
      }
      else
        job.setValidationStdOut(null);

      if(stdErr.length() !=0){
        shell.writeFile(validationStdErr, stdErr.toString(), false);
        job.setValidationStdErr(validationStdErr);
        exitValue = EXIT_UNDECIDED;
      }
      else
        job.setValidationStdErr(null);

    }catch(IOException ioe){
      exitValue = ERROR;
      logFile.addMessage("IOException during job " + job.getName() + " validation :\n" +
                         "\tCommand\t: " + cmd +"\n" +
                         "\tException\t: " + ioe.getMessage(), ioe);
    }

    int amiStatus;
    switch(exitValue){
      case EXIT_VALIDATED :
        Debug.debug("Validation : exit validated", 2);
        amiStatus = DBPluginMgr.VALIDATED;
        break;
      case EXIT_UNDECIDED :
        Debug.debug("job " + job.getName() + " Validation : exit undecided", 2);
        amiStatus = DBPluginMgr.UNDECIDED;
        break;
      case EXIT_FAILED :
        Debug.debug("job " + job.getName() + " Validation : exit failed", 2);
        amiStatus = DBPluginMgr.FAILED;
        break;
      case EXIT_UNEXPECTED :
        Debug.debug("job " + job.getName() + " Validation : exit with unexpected errors", 2);
        amiStatus = DBPluginMgr.UNEXPECTED;
        break;
      case ERROR :
        Debug.debug("Validation : Error", 3);
        amiStatus = DBPluginMgr.UNDECIDED;
        break;
      default :
        Debug.debug("exit ? : " + exitValue, 3);
        logFile.addMessage("Validation script (" +validationScript + ") returns a wrong value\n" +
                           " Command : " + cmd);
        amiStatus = DBPluginMgr.UNDECIDED;

      break;
    }

    Debug.debug("Validation of job " + job.getName() + " took " +
                (new Date().getTime() - beginTime) + " ms with result : " +
                (exitValue == EXIT_VALIDATED ? "Validated" :
                exitValue == EXIT_UNDECIDED ? "Undecided" :
                exitValue == EXIT_UNEXPECTED ? "Unexpected" :
                exitValue == EXIT_FAILED ? "Failed" :
                "(Wrong value)") , 3);
   return amiStatus;
  }
}
