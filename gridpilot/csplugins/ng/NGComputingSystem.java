package gridpilot.csplugins.ng;

import java.io.*;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.TimeZone;
import java.util.Vector;

import javax.swing.*;

import org.globus.gsi.GlobusCredential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.ietf.jgss.GSSCredential;
import org.nordugrid.gridftp.ARCGridFTPJob;
import org.nordugrid.gridftp.ARCGridFTPJobException;

import java.awt.*;
import java.awt.event.*;

import gridpilot.ComputingSystem;
import gridpilot.DBPluginMgr;
import gridpilot.Debug;
import gridpilot.JobInfo;
import gridpilot.LocalShellMgr;
import gridpilot.LogFile;
import gridpilot.GridPilot;
import gridpilot.Util;
import gridpilot.ConfigFile;

/**
 * Main class for the NorduGrid plugin. <br>
 * <p><a href="NGComputingSystem.java.html">see sources</a>
 */

public class NGComputingSystem implements ComputingSystem{

  public static final String NG_STATUS_ACCEPTED =  "ACCEPTED";
  public static final String NG_STATUS_PREPARING = "PREPARING" ;
  public static final String NG_STATUS_FINISHING = "FINISHING" ;
  public static final String NG_STATUS_FINISHED = "FINISHED" ;
  public static final String NG_STATUS_DELETED = "DELETED" ;
  public static final String NG_STATUS_CANCELLING = "CANCELLING";
  public static final String NG_STATUS_SUBMITTING = "SUBMITTING";
  public static final String NG_STATUS_INLRMSQ = "INLRMS: Q";
  public static final String NG_STATUS_INLRMSR = "INLRMS: R";
  public static final String NG_STATUS_INLRMSE = "INLRMS: E";

  public static final String NG_STATUS_FAILURE = "FAILURE";
  public static final String NG_STATUS_FAILED = "FAILED";
  public static final String NG_STATUS_ERROR = "ERROR";

  private NGScriptGenerator scriptGenerator;
  private NGSubmission ngSubmission;
  private NGUpdateStatus ngUpdateStatus;
  private Boolean gridProxyInitialized = Boolean.FALSE;
  private Timer timerProxy = new Timer(0, new ActionListener(){
    public void actionPerformed(ActionEvent e){
      Debug.debug("actionPerformed timeProxy", 3);
      gridProxyInitialized = Boolean.FALSE;
    }
  });
  protected LocalShellMgr shell;
  private static String csName;
  private static LogFile logFile;
  private String workingDir;

  public NGComputingSystem(String _csName){
    csName = _csName;
    scriptGenerator = new NGScriptGenerator(csName);
    ngSubmission = new NGSubmission(csName, workingDir);
    ngUpdateStatus = new NGUpdateStatus(csName);
    workingDir = GridPilot.getClassMgr().getConfigFile().getValue(csName, "working directory");
    if(workingDir==null || workingDir.equals("")){
      workingDir = "~";
    }
    if(workingDir.startsWith("~")){
      workingDir = System.getProperty("user.home")+workingDir.substring(1);
    }
    if(workingDir.endsWith("/") || workingDir.endsWith("\\")){
      workingDir = workingDir.substring(0, workingDir.length()-1);
    }
    logFile = GridPilot.getClassMgr().getLogFile();
    timerProxy.setRepeats(false);
    shell = new LocalShellMgr();
  }
  
  /*
   * Local directory to keep xrsl, shell script and temporary copies of stdin/stdout
   */
  protected String runDir(JobInfo job){
    return workingDir +"/"+job.getName();
  }

  public boolean submit(JobInfo job) {
    Debug.debug("submitting..."+gridProxyInitialized, 3);
    return ngSubmission.submit(job);
  }


  public void updateStatus(Vector jobs){
    for(int i=0; i<jobs.size(); ++i){
      updateStatus((JobInfo) jobs.get(i));
    }
  }

  private int updateStatus(JobInfo job){
    if(ngUpdateStatus.updateStatus(job)){
      Debug.debug("Updating status of job "+job.getName(), 2);
      if(job.getJobStatus()==null){
        Debug.debug("No status found for job "+job.getName(), 2);
        job.setInternalStatus(ComputingSystem.STATUS_WAIT);
      }
      else if(job.getJobStatus().equals(NG_STATUS_FINISHED)){
        if(getOutput(job)){
          job.setInternalStatus(ComputingSystem.STATUS_DONE);
        }
        else{
          job.setInternalStatus(ComputingSystem.STATUS_ERROR);
        }
      }
      if(job.getJobStatus().equals(NG_STATUS_FAILURE)){
        //getOutput(job);
        job.setInternalStatus(ComputingSystem.STATUS_FAILED);
      }
      if(job.getJobStatus().equals(NG_STATUS_ERROR)){
        getOutput(job);
        job.setInternalStatus(ComputingSystem.STATUS_ERROR);
      }
      if(job.getJobStatus().equals(NG_STATUS_DELETED)){
        job.setInternalStatus(ComputingSystem.STATUS_ERROR);
      }
      if(job.getJobStatus().equals(NG_STATUS_FAILED)){
        job.setInternalStatus(ComputingSystem.STATUS_ERROR);
      }
      if(job.getJobStatus().equals(NG_STATUS_INLRMSR)){
        job.setInternalStatus(ComputingSystem.STATUS_RUNNING);
      }
      job.setInternalStatus(ComputingSystem.STATUS_WAIT);
    }
    return job.getInternalStatus();
  }

  public boolean killJobs(Vector jobsToKill){
    JobInfo job = null;
    String jobID = null;
    int lastSlash = -1;
    String submissionHost = null;
    Vector errors = new Vector();
    for(Enumeration en=jobsToKill.elements(); en.hasMoreElements();){
      try{
        job = (JobInfo) en.nextElement();
        jobID = job.getJobId().substring(job.getJobId().lastIndexOf("/"));
        lastSlash = job.getJobId().lastIndexOf("/");
        if(lastSlash>-1){
          jobID = job.getJobId().substring(lastSlash + 1);
        }
        Debug.debug("Killing : " + job.getName() + ":" + job.getJobId(), 3);
        submissionHost = job.getHost();
        ARCGridFTPJob gridJob = new ARCGridFTPJob(submissionHost, jobID);
        GSSCredential credential = GridPilot.getClassMgr().getGridCredential();
        GlobusCredential globusCred = null;
        if(credential instanceof GlobusGSSCredentialImpl){
          globusCred = ((GlobusGSSCredentialImpl)credential).getGlobusCredential();
        }
        gridJob.addProxy(globusCred);
        gridJob.cancel();
       }
       catch(Exception ae){
         errors.add(ae.getMessage());
         logFile.addMessage("Exception during killing of " + job.getName() + ":" + job.getJobId() + ":\n" +
                            "\tException\t: " + ae.getMessage(), ae);
         ae.printStackTrace();
       }
    }    
    if(errors.size()!=0){
      return false;
    }
    else{
      return true;
    }
  }

  public void clearOutputMapping(JobInfo job){
    
    // Delete files that may have been copied to storage elements
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String[] outputMapping = dbPluginMgr.getOutputMapping(job.getJobDefId());
    String fileName;
    for(int i=0; i<outputMapping.length; ++i){
      fileName = Util.addFile(outputMapping[2*i+1]);
      deleteFile(csName, fileName);
    }

    // Delete stdout/err that may have been copied to final destination
    String finalStdOut = dbPluginMgr.getStdOutFinalDest(job.getJobDefId());
    String finalStdErr = dbPluginMgr.getStdErrFinalDest(job.getJobDefId());
    if(finalStdOut!=null && finalStdOut.trim().length()>0){
      if(!deleteFile(csName, finalStdOut)){
        logFile.addMessage("Cannot remove log " + finalStdOut, job);
      }
    }
    if(finalStdErr!=null && finalStdErr.trim().length()>0){
      if(!deleteFile(csName, finalStdErr)){
        logFile.addMessage("Cannot remove log " + finalStdErr, job);
      }
    }
    
    // Delete the local run directory
    String runDir = runDir(job);
    try{
      Debug.debug("Clearing "+runDir, 2);
      deleteFile(csName, runDir);
    }
    catch(Exception ioe){
      logFile.addMessage("Exception during clearOutputMapping of job " + job.getName()+ "\n" +
                                  "\tException\t: " + ioe.getMessage(), ioe);
    }

    // Kill the job and clean up
    try{
      String jobID = null;
      int lastSlash = job.getJobId().lastIndexOf("/");
      if(lastSlash>-1){
        jobID = job.getJobId().substring(lastSlash + 1);
      }
      String submissionHost = job.getHost();
      ARCGridFTPJob gridJob = new ARCGridFTPJob(submissionHost, jobID);
      GSSCredential credential = GridPilot.getClassMgr().getGridCredential();
      GlobusCredential globusCred = null;
      if(credential instanceof GlobusGSSCredentialImpl){
        globusCred = ((GlobusGSSCredentialImpl)credential).getGlobusCredential();
      }
      gridJob.addProxy(globusCred);
      gridJob.cancel();
      gridJob.clean();
    }
    catch(Exception ioe){
      Debug.debug("Exception during clearOutputMapping of job " + job.getName() + " :\n" +
                                  "\tException\t: " + ioe.getMessage()+ioe, 2);
    }
  }

  public void exit(){
  }
  
  private boolean getOutput(JobInfo job){
    
    String jobID = null;
    int lastSlash = job.getJobId().lastIndexOf("/");
    if(lastSlash>-1){
      jobID = job.getJobId().substring(lastSlash + 1);
    }
    String dirName =  runDir(job)+File.pathSeparatorChar+jobID;
    
    // After a crash some unfinished downloads may be around.
    // Move away before downloading.
    try{
      // current date and time
      SimpleDateFormat dateFormat = new SimpleDateFormat(GridPilot.dateFormatString);
      dateFormat.setTimeZone(TimeZone.getDefault());
      String dateString = dateFormat.format(new Date());
      shell.moveFile(dirName, dirName+"."+dateString);
    }
    catch(Exception ioe){
      logFile.addMessage("IOException during job " + job.getName() + " get output :\n" +
                                  "\tException\t: " + ioe.getMessage(), ioe);
      return false;
    }
    
    // Get the outputs
    try{
      Debug.debug("Getting : " + job.getName() + ":" + job.getJobId(), 3);
      String submissionHost = job.getHost();
      ARCGridFTPJob gridJob = new ARCGridFTPJob(submissionHost, jobID);
      GSSCredential credential = GridPilot.getClassMgr().getGridCredential();
      GlobusCredential globusCred = null;
      if(credential instanceof GlobusGSSCredentialImpl){
        globusCred = ((GlobusGSSCredentialImpl)credential).getGlobusCredential();
      }
      gridJob.addProxy(globusCred);
      gridJob.get(dirName);
     }
     catch(Exception ae){
       logFile.addMessage("Exception during get outputs of " + job.getName() + ":" + job.getJobId() + ":\n" +
                          "\tException\t: " + ae.getMessage(), ae);
       ae.printStackTrace();
     }
    
    // Rename stdout and stderr to the name specified in the job description,
    // and move them one level up
    if(job.getStdOut()!=null && !job.getStdOut().equals("")){      
      try{
        shell.copyFile(dirName+File.pathSeparator+"stdout", job.getStdOut());
      } 
      catch(Exception ioe){
        logFile.addMessage("IOException during job " + job.getName() + " getFullStatus :\n" +
                                    "\tException\t: " + ioe.getMessage(), ioe);
        return false;
      }
    }
    if(job.getStdErr() != null && !job.getStdErr().equals("")){
      try{
        shell.copyFile(dirName+File.pathSeparator, job.getStdErr());
      }
      catch(Exception ioe){
        logFile.addMessage("IOException during job " + job.getName() + " getOutput :\n" +
                                    "\tException\t: " + ioe.getMessage(), ioe);
        return false;
      }
    }
    return true;
  }

  public String getFullStatus(JobInfo job){
    String state = "";
    try{
      String jobID = null;
      int lastSlash = job.getJobId().lastIndexOf("/");
      if(lastSlash>-1){
        jobID = job.getJobId().substring(lastSlash + 1);
      }
      Debug.debug("Getting : " + job.getName() + ":" + job.getJobId(), 3);
      String submissionHost = job.getHost();
      ARCGridFTPJob gridJob = new ARCGridFTPJob(submissionHost, jobID);
      GSSCredential credential = GridPilot.getClassMgr().getGridCredential();
      GlobusCredential globusCred = null;
      if(credential instanceof GlobusGSSCredentialImpl){
        globusCred = ((GlobusGSSCredentialImpl)credential).getGlobusCredential();
      }
      gridJob.addProxy(globusCred);
      state = gridJob.state();
    }
    catch(Exception ioe){
      logFile.addMessage("IOException during job " + job.getName() + " getFullStatus :\n" +
                                  "\tException\t: " + ioe.getMessage(), ioe);
      return ioe.getMessage();
    }
    return state;
  }

  public String [] getCurrentOutputs(JobInfo job){

    String [] res = new String[2];

    if(job.getStdOut()!=null){
      // get stdout
      try{
        Debug.debug("Getting stdout of: " + job.getName() + ":" + job.getJobId(), 3);
        copyFile(csName, job.getJobId()+"/stdout", job.getStdOut());
        res[0] = shell.readFile(job.getStdOut());
       }
       catch(Exception ae){
         logFile.addMessage("Exception during get stdout of " + job.getName() + ":" + job.getJobId() + ":\n" +
                            "\tException\t: " + ae.getMessage(), ae);
         ae.printStackTrace();
         res[0] = "";
       }
    }
    // get stderr
    try{
      Debug.debug("Getting stderr of: " + job.getName() + ":" + job.getJobId(), 3);
      copyFile(csName, job.getJobId()+"/stderr", job.getStdErr());
      res[1] = shell.readFile(job.getStdErr());
     }
     catch(Exception ae){
       logFile.addMessage("Exception during get stderr of " + job.getName() + ":" + job.getJobId() + ":\n" +
                          "\tException\t: " + ae.getMessage(), ae);
       ae.printStackTrace();
       res[1] = "";
     }
     return res;
  }
  
  public boolean copyFile(String csName, String src, String dest){
    Debug.debug("Copying file "+ src + "->" + dest, 3);
    try{
      if(dest.startsWith("gsiftp://") || src.startsWith("gsiftp://") ||
          dest.startsWith("http://") || src.startsWith("http://")){
        Debug.debug("Using gridftp" + dest, 3);
        if(src.startsWith("/")){
          src = "file://"+src;
        }
        try{
          GSSCredential credential = GridPilot.getClassMgr().getGridCredential();
          GlobusCredential globusCred = null;
          if(credential instanceof GlobusGSSCredentialImpl){
            globusCred = ((GlobusGSSCredentialImpl)credential).getGlobusCredential();
          }
        }
        catch(Exception ioe){
          logFile.addMessage("Exception during copyFile :\n" +
                                      "\tException\t: " + ioe.getMessage(), ioe);
          return false;
        }
      }
      else{
        Debug.debug("Using cp" + dest, 3);
        return AtCom.getClassMgr().getShellMgr(csName).copyFile(src, dest);
      }
    }
    catch(IOException ioe){
      logFile.addMessage("IOException during copying of file " +
          src + "->" + dest +" : \n" +
          "\tSource\t: " + src + "\n" +
          "\tDestination\t: " + dest + "\n" +
          "\tException\t: " + ioe.getMessage(), ioe);
      Debug.debug("IOException during copying of file " +ioe.getMessage(), 3);
      return false;
    }
  }

  public boolean deleteFile(String csName, String dest){
    Debug.debug("Deleting file "+ dest, 3);
    try{
      if(dest.startsWith("gsiftp://")){
        Debug.debug("Using gridftp" + dest, 3);
        if(dest.startsWith("/")){
          dest = "file://"+dest;
        }
        String [] cmd = {"ngremove", dest};

        try{
          StringBuffer stdOut = new StringBuffer();
          StringBuffer stdErr = new StringBuffer();

          shell.exec(cmd, stdOut, stdErr);

          if(stdErr.length()!=0){
            logFile.addMessage("Could not delete " + dest +
                                    "\tCommand\t: " + Util.arrayToString(cmd) +
                                    "\tError\t: " + stdErr);
            return false;
          }
          else{
            return true;
          }

        }
        catch(IOException ioe){
          logFile.addMessage("Could not delete " + dest +
                                      "\tCommand\t: " + Util.arrayToString(cmd) +"\n" +
                                      "\tException\t: " + ioe.getMessage(), ioe);
          return false;
        }
      }
      else{
        Debug.debug("Using rm" + dest, 3);
        return shell.deleteFile(dest);
      }
    }
    catch(Exception ioe){
      logFile.addMessage("IOException during deletion of file : \n" +
          "\tDestination\t: " + dest + "\n" +
          "\tException\t: " + ioe.getMessage(), ioe);
      Debug.debug("IOException during deletion of file " +ioe.getMessage(), 3);
      return false;
    }
  }
  
  public boolean existsFile(String csName, String src){
    try{
      return shellMgr.existsFile(src);
    }
    catch(Exception ioe){
      logFile.addMessage("Exception during checking of file " +
          csName + " : \n" +
          "\tSource\t: " + src + "\n" +
          "\tException\t: " + ioe.getMessage(), ioe);
      Debug.debug("Exception during checking of file " +ioe.getMessage(), 3);
      return false;
    }
  }

  public String getUserInfo(String csName){
    String user = null;
    try{
      user = System.getProperty("user.name");
      /* remove leading whitespace */
      user = user.replaceAll("^\\s+", "");
      /* remove trailing whitespace */
      user = user.replaceAll("\\s+$", "");      
    }
    catch(Exception ioe){
      logFile.addMessage("Exception during getUserInfo\n" +
                                 "\tException\t: " + ioe.getMessage(), ioe);
    }
    if(user==null){
      Debug.debug("Job user null, getting from config file", 3);
       user = GridPilot.getClassMgr().getConfigFile().getValue("GridPilot", "user");
    }
    return user;
  }

  public boolean postProcess(JobInfo job){
    return copyToFinalDest(job);
  }

  public boolean preProcess(JobInfo job){
    // input files are already there
    return true;
  }

  public String[] getScripts(JobInfo job){
    String jobScriptFile = runDir(job)+"/"+job.getName()+commandSuffix;
      return new String [] {jobScriptFile};
  }

}