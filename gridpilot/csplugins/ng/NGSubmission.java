package gridpilot.csplugins.ng;

import java.io.*;

import gridpilot.ConfigFile;
import gridpilot.DBPluginMgr;
import gridpilot.Debug;
import gridpilot.JobInfo;
import gridpilot.LogFile;
import gridpilot.GridPilot;
import gridpilot.Util;

/**
 * Submission class for the NorduGrid plugin.
 * <p><a href="NGSubmission.java.html">see sources</a>
 */

public class NGSubmission{
 
  private ConfigFile configFile;
  private LogFile logFile;
  private String systemName;
  private String submissionCmd;
  private String submissionParams;

  public NGSubmission(String _systemName){
    Debug.debug("Loading class NGSubmission", 3);
    configFile = GridPilot.getClassMgr().getConfigFile();
    logFile = GridPilot.getClassMgr().getLogFile();
    systemName = _systemName;
    
    submissionCmd = configFile.getValue(systemName, "Submission command");
    if(submissionCmd == null){
      logFile.addMessage(configFile.getMissingMessage(systemName, "Submission command"));
      return;
    }
    
    submissionParams = configFile.getValue(systemName, "Submission parameters");
    
    Debug.debug("cmd : " + submissionCmd, 3);
    Debug.debug("params : " + submissionParams, 3);

  }

  public boolean submit(JobInfo job){

    NGScriptGenerator scriptGenerator =  new NGScriptGenerator(systemName);
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String finalStdErr = dbPluginMgr.getStdOutFinalDest(job.getJobDefId());
    String finalStdOut = dbPluginMgr.getStdErrFinalDest(job.getJobDefId());
    // The default is now to create both stderr if either
    // final destination is specified for stderr or final destination is speficied for
    // neither stdout nor stderr
    boolean withStdErr = finalStdErr!=null && finalStdErr.trim().length()>0 ||
       (finalStdErr==null || finalStdErr.equals("")) &&
       (finalStdOut==null || finalStdOut.equals(""));

    Debug.debug("stdout/err: "+finalStdOut+":"+finalStdErr, 3);

    String wrapperPath;
    if(job.getStdOut()!=null){
      wrapperPath = new File(job.getStdOut()).getParentFile().getAbsolutePath() + "/";
      // If running on Windows...
      if(wrapperPath.startsWith("C:\\")){
        //wrapperPath = wrapperPath.substring(0, 2);
        wrapperPath = wrapperPath.replaceAll(new String("C:\\\\"), new String("/"));
        wrapperPath = wrapperPath.replaceAll(new String("\\\\"), new String("/"));
      }
    }
    else{
      wrapperPath = "./";
    }
    
    Debug.debug("Submitting in "+wrapperPath, 3);

    String xrslName = wrapperPath +  job.getName() + ".xrsl";
    String scriptName = wrapperPath +  job.getName() + ".job";
    //job.setName(xrslName);

    if(!scriptGenerator.createXRSL(job, scriptName, xrslName, !withStdErr)){
      logFile.addMessage("Cannot create scripts for job " + job.getName() +
                              " on " + systemName);
      return false;
    }


    //File workingDirectory = new File(wrapperPath);

    String NGJobId = submit(xrslName, wrapperPath/*workingDirectory*/);
    //String NGJobId = submit(xrslName, systemName);
    if(NGJobId==null){
      job.setJobStatus(NGComputingSystem.NG_STATUS_ERROR);
      return false;
    }
    else{
      job.setJobId(NGJobId);
      return true;
    }

  }

  private String submit(String xrslName, String workingDirectory){
  // private static String submit(String xrslName, String systemName){

   String [] cmd = {submissionCmd, "-f", xrslName, submissionParams};

   String NGJobId;
   // execution of ngsub
    try{
      StringBuffer stdOut = new StringBuffer();
      StringBuffer stdErr = new StringBuffer();

      GridPilot.getClassMgr().getJobControl().getShellMgr(systemName).exec(cmd, workingDirectory.toString(), stdOut, stdErr);

      if(stdErr.length()!=0 || stdOut.length() ==0 || stdOut.indexOf("gsiftp://") == -1){
        logFile.addMessage("Error when submitting job "+ xrslName + " :\n" +
                           "\tCommand\t: " + Util.arrayToString(cmd) + "\n" +
                           "\tStdOut\t: " + stdOut + "\n"+
                           "\tStdErr\t: " + stdErr + "\n"+
                           "\tWorkDir\t: " + workingDirectory.toString());
      //job.setStatus(NGComputingSystem.NG_STATUS_ERROR);
      }
      if(stdOut.length()!= 0){

        Debug.debug("Output : \n" + stdOut, 2);

        int begin = stdOut.indexOf("gsiftp://");
        if(begin == -1){
          logFile.addMessage("Job Id not found in output : \n" +
                                  "\tCommand\t:" + Util.arrayToString(cmd) + "\n" +
                                  "\tOutput\t:" + stdOut + "\n" +
                                  "\tError\t:" + stdErr);
          return null;
        }
        int end = stdOut.indexOf("\n", begin);
        if(end == -1)
          end = stdOut.length();


        NGJobId = stdOut.substring(begin, end);
        Debug.debug("NGJobId : " + NGJobId, 3);

        //job.setComputingSystemJobId(NGJobId);
        //job.setStatus(ComputingSystem.STATUS_SUBMITTED);
      }
      else{
        return null;
      }
      // Update in AMI -> will be done by  LSFComputingSystem

    }
    catch(IOException ioe){
      logFile.addMessage("IOException during job " + xrslName + " submission :\n" +
                         "\tCommand\t: " + Util.arrayToString(cmd) +"\n" +
                         "\tException\t: " + ioe.getMessage(), ioe);
      //job.setStatus(NGComputingSystem.NG_STATUS_ERROR);
      return null;
    }

    return NGJobId;
  }

}