package gridpilot;

import javax.swing.*;

import java.awt.event.*;
import java.net.URL;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Vector;

import gridpilot.DBRecord;

/**
 * Controls the job submission. <p>
 * When submitting some jobs (giving logical file id or jobs already created),
 * these jobs are put in a queue (<code>toSubmitJobs</code>). Each
 * <code>timeBetweenSubmissions</code>, a Timer checks if there is some jobs in
 * this queue (this timer is stopped when the queue is empty, and restarted when
 * new jobs arrive). <br>
 * If there are any, the first job is removed from <code>toSubmitJobs</code> and
 * put in <code>submittingJobs</code>
 */
public class SubmissionControl{
  private Vector submittedJobs;
  private StatusBar statusBar;
  private JProgressBar pbSubmission;
  private boolean isProgressBarSet = false;
  private ConfigFile configFile;
  private LogFile logFile;
  private Table statusTable;
  private CSPluginMgr csPluginMgr;
  private Timer timer;
  private ImageIcon iconSubmitting;
  /** All jobs for which the submission is not made yet */
  private Vector toSubmitJobs = new Vector();
  /** All jobs for which the submission is in progress */
  private Vector submittingJobs = new Vector();
 /** Maximum number of simulaneous threads for submission. <br>
   * It is not the maximum number of running jobs on the Computing System */
  private int maxSimultaneousSubmissions = 5;
  /** Delay between the begin of two submission threads */
  private int timeBetweenSubmissions = 1000;
  private String isRand = null;

  public SubmissionControl(){
    submittedJobs = GridPilot.getClassMgr().getSubmittedJobs();
    statusTable = GridPilot.getClassMgr().getJobStatusTable();
    csPluginMgr = GridPilot.getClassMgr().getCSPluginMgr();
    
    statusBar = GridPilot.getClassMgr().getStatusBar();
    configFile = GridPilot.getClassMgr().getConfigFile();
    logFile = GridPilot.getClassMgr().getLogFile();

    timer = new Timer(0, new ActionListener(){
      public void actionPerformed(ActionEvent e){
        trigSubmission();
      }
    });
    loadValues();
    pbSubmission = new JProgressBar(0,0);
  }
  
  /**
   * Reloads some values from configuration file. <p>
   * These values are : <ul>
   * <li>{@link #defaultUserName}
   * <li>{@link #maxSimultaneousSubmissions}
   * <li>{@link #timeBetweenSubmissions} </ul><p>
   *
   */
  public void loadValues(){
    String tmp = configFile.getValue("Computing systems", "maximum simultaneous submissions");
    if(tmp != null){
      try{
        maxSimultaneousSubmissions = Integer.parseInt(tmp);
      }
      catch(NumberFormatException nfe){
        logFile.addMessage("Value of \"maximum simultaneoud submission\" "+
                                    "is not an integer in configuration file", nfe);
      }
    }
    else
      logFile.addMessage(configFile.getMissingMessage("Computing systems", "maximum simultaneous submissions") + "\n" +
                              "Default value = " + maxSimultaneousSubmissions);
    tmp = configFile.getValue("Computing systems", "time between submissions");
    if(tmp != null){
      try{
        timeBetweenSubmissions = Integer.parseInt(tmp);
      }
      catch(NumberFormatException nfe){
        logFile.addMessage("Value of \"time between submissions\" is not"+
                                    " an integer in configuration file", nfe);
      }
    }
    else{
      logFile.addMessage(configFile.getMissingMessage("Computing systems", "time between submissions") + "\n" +
                              "Default value = " + timeBetweenSubmissions);
    }
    Debug.debug("Setting time between submissions "+timeBetweenSubmissions, 3);
    timer.setInitialDelay(0);
    timer.setDelay(timeBetweenSubmissions);
    String resourcesPath = configFile.getValue("GridPilot", "resources");
    if(resourcesPath != null && !resourcesPath.endsWith("/"))
      resourcesPath += "/";
    URL imgURL=null;
    try{
      imgURL = GridPilot.class.getResource(GridPilot.resourcesPath + "submitting.png");
      iconSubmitting = new ImageIcon(imgURL);
    }
    catch(Exception e){
      logFile.addMessage("Could not find image "+ resourcesPath + "submitting.png");
      iconSubmitting = new ImageIcon();
    }
    isRand = configFile.getValue("Computing systems", "randomized submission");
    Debug.debug("isRand = " + isRand, 2);
  }

  /**
   * Creates the jobs for the specified job definitions and submits them on the specified
   * computing system. <p>
   * Each job is reserved, the job is created, a row is added to statusTable
   * and submit is called. <p>
   */
  public void submitJobDefinitions(/*vector of DBRecords*/Vector selectedJobs,
      String csName, DBPluginMgr dbPluginMgr){
    GridPilot.getClassMgr().getGlobalFrame().showMonitoringPanel();
    synchronized(submittedJobs){
      Vector newJobs = new Vector();
      // This label is not shown because this function is not called in a Thread.
      // It seems to be quite dangereous to call this function in a thread, because
      // if one does it, you can "load job from db" during reservation (when jobs
      // are not yet put in toSubmitJobs).
      statusBar.setLabel("Reserving. Please wait...");
      //statusBar.animateProgressBar();
      String jobDefIdentifier = Util.getIdentifierField(dbPluginMgr.getDBName(), "jobDefinition");
      for(int i=0; i<selectedJobs.size(); ++i){
        DBRecord jobDef = ((DBRecord) selectedJobs.get(i));
        String jobDefID = jobDef.getValue(jobDefIdentifier).toString();
        if(dbPluginMgr.reserveJobDefinition(jobDefID, "", csName)){
          // checks if this partition has not been monitored (and is Submitted,
          // otherwise reservation doesn't work)
          JobInfo job = null;
          boolean isjobMonitored = false;
          for(Iterator it=submittedJobs.iterator(); it.hasNext();){
            job = ((JobInfo) it.next());
            if(job.getJobDefId()==jobDefID){
              isjobMonitored = true;
              break;
            };
          }
          if(isjobMonitored==false){ // this job doesn't exist yet
            job = new JobInfo(
                jobDefID,
                dbPluginMgr.getJobDefName(jobDefID),
                csName,
                dbPluginMgr.getDBName()
                );
            job.setTableRow(submittedJobs.size());
            submittedJobs.add(job);
            job.setDBStatus(DBPluginMgr.SUBMITTED);
            job.setName(dbPluginMgr.getJobDefinition(jobDefID).getValue("name").toString());
            job.setCSName(csName);
          }
          else{
            // this job exists, get its computingSystem, keeps its row number,
            // and do not add to submittedJobs
            job.setCSName(csName);
            job.setDBStatus(DBPluginMgr.SUBMITTED);
          }
          newJobs.add(job);
          Debug.debug("job definition " + job.getJobDefId() + "("+job.getName()+") reserved", 2);
        }
        else{
          logFile.addMessage("cannot reserve job "+jobDefID);
          Debug.debug("job " + jobDefID + " cannot be reserved", 1);
        }
      }
      statusBar.setLabel("Reserving done.");
      //statusBar.removeLabel();
      //statusBar.stopAnimation();
      if(!newJobs.isEmpty()){
        statusBar.setLabel("Monitoring. Please wait...");
        //statusBar.animateProgressBar();
        // new rows in table
        statusTable.createRows(submittedJobs.size());
        //jobControl.initChanges();
        for(Iterator it = GridPilot.getClassMgr().getJobMgrs().iterator(); it.hasNext();){
          ((JobMgr) it.next()).initChanges();
        }
        // jobControl.updateJobsByStatus();
        statusBar.setLabel("Monitoring done.");
        //statusBar.stopAnimation();
        queue(newJobs);
      }
    }
  }

  /**
   * Submits the specified jobs on the given computing system. <p>
   */
  public void submitJobs(Vector jobs, String csName){
    synchronized(submittedJobs){
      Vector newJobs = new Vector();
      for(int i=0; i<jobs.size(); ++i){
        JobInfo job = (JobInfo) jobs.get(i);
        DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
        if(dbPluginMgr.reserveJobDefinition(job.getJobDefId(), "", csName)){
          job.setCSName(csName);
          newJobs.add(job);
          job.setDBStatus(DBPluginMgr.SUBMITTED);
          job.setInternalStatus(ComputingSystem.STATUS_WAIT);
          job.setJobId(null);
          job.setHost(null);
          job.setJobStatus(null);
          Debug.debug("logical file " + job.getJobDefId() + "("+job.getName()+") reserved", 2);
        }
        else{
          logFile.addMessage("cannot reserve logical file "+job.getJobDefId());
        }
      }
      statusBar.removeLabel();
      if(!newJobs.isEmpty()){
        // jobControl.updateJobsByStatus();
        queue(newJobs);
      }
    }
  }

  /**
   * Submits the given jobs, on the computing system given by job.getComputingSystem. <p>
   * These jobs are put in toSubmitJobs. <p>
   */
  private void queue(Vector jobs){
    
    if(jobs==null || jobs.size()==0){
      return;
    }
    
    if(isRand!=null && isRand.equalsIgnoreCase("yes")){
      jobs = Util.shuffle(jobs);
    }
    pbSubmission.setMaximum(pbSubmission.getMaximum() + jobs.size());
    pbSubmission.addMouseListener(new MouseAdapter(){
      public void mouseClicked(MouseEvent e){
        cancelSubmission();
      }
    });
    statusBar.setLabel("Adding to submission queue. Please wait...");
    //statusBar.animateProgressBar();
    pbSubmission.setToolTipText("Click here to cancel submission");
    if(!isProgressBarSet){
      statusBar.setProgressBar(pbSubmission);
      isProgressBarSet = true;
    }
    
    JobInfo job = (JobInfo) jobs.get(0);
    
    JobMgr jobMgr = GridPilot.getClassMgr().getJobMgr(job.getDBName());
    
    jobMgr.updateDBCells(jobs);
    jobMgr.updateJobCells(jobs);
    toSubmitJobs.addAll(jobs);
    if(!timer.isRunning()){
      timer.restart();
    }
    statusBar.setLabel("Adding done.");
    //statusBar.stopAnimation();
  }

  /**
   * Resubmits the specified jobs. <p>
   * These jobs do not need to be reserved. <br>
   * If a job is not Failed, it cannot be resubmitted. <br>
   * If some outputs exist, the user is asked for save them.
   * If the user chooses to not save them, outputs are deleted <p>
   */
  public void resubmit(Vector jobs){

    boolean askSave = false;
    boolean deleteFiles = false;
    GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar.setLabel("Cleaning up jobs...");
    for(int i=0; i<jobs.size() ; ++i){

      JobInfo job = (JobInfo) jobs.get(i);
      ShellMgr shell = null;
      try{
        shell = GridPilot.getClassMgr().getShellMgr(job);
      }
      catch(Exception e){
        Debug.debug("ERROR getting shell manager: "+e.getMessage(), 1);
      }

      if(job.getDBStatus()!=DBPluginMgr.FAILED &&
          job.getDBStatus()!=DBPluginMgr.UNEXPECTED &&
          job.getDBStatus()!=DBPluginMgr.ABORTED){
        java.awt.Frame frame = javax.swing.JOptionPane.getRootFrame();
        javax.swing.JOptionPane.showMessageDialog(frame, "Cannot resubmit job " +
                                                  job.getName() +
                                                  "\nIt is still running",
                                                  "Cannot resubmit job",
                                                  javax.swing.JOptionPane.
                                                  WARNING_MESSAGE);
        return;
      }

      boolean stdOutExists = false;
      boolean stdErrExists = false;
      try{
        stdOutExists = job.getStdOut() != null &&
        shell.existsFile(job.getStdOut());
      }
      catch(Exception e){
        Debug.debug("ERROR checking for stdout: "+e.getMessage(), 2);
        logFile.addMessage("ERROR checking for stdout: "+e.getMessage());
        //throw e;
      }
      try{
        stdErrExists = job.getStdErr() != null &&
        shell.existsFile(job.getStdErr());
      }
      catch(Exception e){
        Debug.debug("ERROR checking for stdout: "+e.getMessage(), 2);
        logFile.addMessage("ERROR checking for stdout: "+e.getMessage());
        //throw e;
      }

      if(!askSave){
        if(deleteFiles){
          if(stdOutExists){
            shell.deleteFile(job.getStdOut());
          }
          if(stdErrExists){
            shell.deleteFile(job.getStdErr());
          }
        }
      }
      else{
        if(stdOutExists || stdErrExists){
          Object[] options = {"Yes", "No", "No for all", "Cancel"};

          int choice = JOptionPane.showOptionDialog(JOptionPane.getRootFrame(),
              "Do you want to save job \n" +
              "outputs before resubmit it", "Save outputs for " + job.getName() + " ?",
              JOptionPane.YES_NO_CANCEL_OPTION,
              JOptionPane.QUESTION_MESSAGE, null,
              options, options[0]);

          switch(choice){
            case JOptionPane.CLOSED_OPTION:
              
            case 3://JOptionPane.CANCEL_OPTION:
              return;

            case 2://No for all
              askSave = false;
              //no break !!!
              
            case 1://JOptionPane.NO_OPTION:
              if(deleteFiles){
                if(stdOutExists)
                  if(!shell.deleteFile(job.getStdOut()))
                    logFile.addMessage("Cannot delete stdout", job);
                if(stdErrExists)
                  if(!shell.deleteFile(job.getStdErr()))
                    logFile.addMessage("Cannot delete stderr", job);
              }
              break;

            case 0://JOptionPane.YES_OPTION:
              JFileChooser f = new JFileChooser();
              f.setMultiSelectionEnabled(false);
              // stdout
              if(stdOutExists){
                if(f.showDialog(JOptionPane.getRootFrame(), "Save stdout")==
                    JFileChooser.APPROVE_OPTION){

                  java.io.File stdOut = f.getSelectedFile();
                  if(stdOut != null){
                    try{
                     shell.writeFile(stdOut.getPath(),
                        shell.readFile(job.getStdOut()), false);
                    }
                    catch(java.io.IOException ioe){
                      logFile.addMessage("Cannot rename " + job.getStdOut() +
                                         " in " + stdOut.getPath(), ioe);
                    }
                  }
                }
                else
                if(deleteFiles) //remove StdOut
                  shell.deleteFile(job.getStdOut());
              }
              // stderr
              if(stdErrExists){
                if(f.showDialog(JOptionPane.getRootFrame(), "Save stderr")==
                    JFileChooser.APPROVE_OPTION){
                  java.io.File stdErr = f.getSelectedFile();
                  if(stdErr != null){
                    try{
                      shell.writeFile(stdErr.getPath(),
                         shell.readFile(job.getStdErr()), false);
                    }
                    catch(java.io.IOException ioe){
                      logFile.addMessage("Cannot rename " + job.getStdErr() +
                                         " in " + stdErr.getPath(), ioe);
                    }
                  }
                }
                else
                if(deleteFiles) //remove StdErr
                  shell.deleteFile(job.getStdErr());
              }
              break;

            default:
              return;
          }
        }
      }
    }
    Vector submitables = new Vector();
    GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar.setLabel("Updating job status...");
    
    JobInfo job = null;
    JobMgr jobMgr = null;
    
    while(jobs.size()>0){
      job = (JobInfo) jobs.remove(0);
      jobMgr = GridPilot.getClassMgr().getJobMgr(job.getDBName());
      // first clean up old job
      jobMgr.getDBPluginMgr().cleanRunInfo(job.getJobDefId());
      GridPilot.getClassMgr().getCSPluginMgr().clearOutputMapping(job);
      // then set the status to Submitted
      // (this will not cause a cleanup, as setting the status to Defined would)
      jobMgr.updateDBStatus(job, DBPluginMgr.SUBMITTED);
      if(job.getDBStatus()!=DBPluginMgr.SUBMITTED){
        // updateDBStatus didn't work
        logFile.addMessage(
            "This job cannot be set Submitted -> this job cannot be resubmited",
            job);
      }
      else{
        job.setNeedToBeRefreshed(false);
        job.setJobId(null);
        job.setHost(null);
        job.setJobStatus(null);
        submitables.add(job);
      }
    }

    jobMgr.updateJobCells(submitables);
    // if all went well we can now submit
    GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar.setLabel("Submitting jobs...");
    queue(submitables);
  }

 /**
  * Checks if there are not too many active Threads (for submission), and there are
  * waiting jobs. If there are any, creates new submission threads
  */
  private synchronized void trigSubmission(){
    if(submittingJobs.size()<maxSimultaneousSubmissions && !toSubmitJobs.isEmpty()){
      // transfer job from toSubmitJobs to submittingJobs
      final JobInfo job = (JobInfo) toSubmitJobs.remove(0);
      submittingJobs.add(job);
      new Thread(){
        public void run(){
          submit(job);
        }
      }.start();
    }
    else{
      timer.stop();
    }
  }

  /**
   * Submits the specified job. <p>
   * Creates these job outputs, and calls the plugin submission method (via PluginMgr). <br>
   * This method is started in a thread. <p>
   */
  private void submit(final JobInfo job){
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    job.setName(dbPluginMgr.getJobDefName(job.getJobDefId()));
    statusTable.setValueAt(job.getCSName(), job.getTableRow(),
        JobMgr.FIELD_CS);
    //statusTable.setValueAt(job.getName(), job.getTableRow(),
    //                       JobMgr);
    Debug.debug("Submitting : " + job.getName()+" : "+statusTable.getRowCount()+
        " : "+job.getTableRow()+" : "+iconSubmitting, 3);
    statusTable.setValueAt(iconSubmitting, job.getTableRow(),
        JobMgr.FIELD_CONTROL);
    JobMgr jobMgr = GridPilot.getClassMgr().getJobMgr(job.getDBName());
    if(csPluginMgr.preProcess(job) && csPluginMgr.submit(job)){
      Debug.debug("Job " + job.getName() + " submitted : \n" +
                  "\tCSJobId = " + job.getJobId() + "\n" +
                  "\tStdOut = " + job.getStdOut() + "\n" +
                  "\tStdErr = " + job.getStdErr(), 2);
      String jobUser = csPluginMgr.getUserInfo(job.getCSName());
      Debug.debug("Setting job user :"+jobUser+":", 3);
      job.setUser(jobUser);
      if(!dbPluginMgr.updateJobDefinition(
              job.getJobDefId(),
              new String []{jobUser, job.getJobId(), job.getName(),
              job.getStdOut(), job.getStdErr()})){
        logFile.addMessage("DB update(" + job.getJobDefId() + ", " +
                           job.getJobId() + ", " + job.getName() + ", " +
                           job.getStdOut() + ", " + job.getStdErr() +
                           ") failed", job);
      }
      statusTable.setValueAt(job.getJobId(), job.getTableRow(),
          JobMgr.FIELD_JOBID);
      statusTable.setValueAt(job.getUser(), job.getTableRow(),
          JobMgr.FIELD_USER);
      statusTable.updateSelection();
      job.setNeedToBeRefreshed(true);
      job.setInternalStatus(ComputingSystem.STATUS_WAIT);
    }
    else{
      statusTable.setValueAt("Not submitted!", job.getTableRow(),
          JobMgr.FIELD_JOBID);
      job.setInternalStatus(ComputingSystem.STATUS_FAILED);
      job.setNeedToBeRefreshed(false);
      if(jobMgr.updateDBStatus(job, DBPluginMgr.FAILED)){
        job.setDBStatus(DBPluginMgr.FAILED);
      }
      else{
        logFile.addMessage("DB update status(" + job.getJobDefId() + ", " +
            DBPluginMgr.getStatusName(job.getDBStatus()) +
                           ") failed", job);
      }
      jobMgr.updateDBCell(job);
      //jobControl.updateJobsByStatus();
    }
    //jobControl.updateJobsByStatus();
    for(Iterator it = GridPilot.getClassMgr().getJobMgrs().iterator(); it.hasNext();){
      ((JobMgr) it.next()).updateJobsByStatus();
    }
    submittingJobs.remove(job);
    if(!timer.isRunning()){
      timer.restart();
    }
    pbSubmission.setValue(pbSubmission.getValue() + 1);
    if(pbSubmission.getPercentComplete()==1.0){
      statusBar.removeProgressBar(pbSubmission);
      isProgressBarSet = false;
      pbSubmission.setMaximum(0);
      pbSubmission.setValue(0);
      //statusBar.setLabel("Submission done.");
      GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar.setLabel("Submission done.");
    }
    // remove iconSubmitting
    statusTable.setValueAt(null, job.getTableRow(), JobMgr.FIELD_CONTROL);
  }

  public boolean isSubmitting(){
    //return timer.isRunning();
    return !(submittingJobs.isEmpty() && toSubmitJobs.isEmpty());
  }

  /**
   * Stops the submission. <br>
   * Empties toSubmitJobs, and set these jobs to Failed.
   */
  private void cancelSubmission(){
    timer.stop();
    Enumeration e = toSubmitJobs.elements();
    JobInfo job = null;
    JobMgr jobMgr = null;
    while(e.hasMoreElements()){
      job = (JobInfo) e.nextElement();
      statusTable.setValueAt("Not submitted (cancelled)!", job.getTableRow(), JobMgr.FIELD_JOBID);
      statusTable.setValueAt(job.getName(), job.getTableRow(), JobMgr.FIELD_JOBNAME);
      job.setInternalStatus(ComputingSystem.STATUS_FAILED);
      job.setNeedToBeRefreshed(false);
      jobMgr = GridPilot.getClassMgr().getJobMgr(job.getDBName());
      if(jobMgr.updateDBStatus(job, DBPluginMgr.FAILED)){
        job.setDBStatus(DBPluginMgr.FAILED);
      }
      else{
        logFile.addMessage("DB update status(" + job.getJobDefId() + ", " +
            DBPluginMgr.getStatusName(job.getDBStatus()) + ") failed", job);
      }
    }
    jobMgr.updateDBCells(toSubmitJobs);
    toSubmitJobs.removeAllElements();
    statusBar.removeProgressBar(pbSubmission);
    pbSubmission.setMaximum(0);
    pbSubmission.setValue(0);
  }
}