package gridpilot;

import javax.swing.*;

import java.awt.event.*;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Random;
import java.util.Vector;

import gridpilot.Database.DBRecord;


/**
 * Controls the job submission. <p>
 * When submitting some jobs (giving logical file id or jobs already created),
 * these jobs are put in a queue (<code>toSubmitJobs</code>). Each <code>timeBetweenSubmission</code>,
 * a Timer checks if there is some jobs in this queue (this timer is stopped when the
 * queue is empty, and re-started when new jobs arrive). <br>
 * If there is any, the first job is removed from <code>toSubmitJobs</code>, put in
 * <code>submittingJobs</code>
 */
public class SubmissionControl{
  private Vector submittedJobs;
  private StatusBar statusBar;
  private JProgressBar pbSubmission;
  private boolean isProgressBarSet=false;
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
  private int maxSimultaneousSubmission = 5;
  /** Delay between the begin of two submission threads */
  private int timeBetweenSubmission = 1000;
  private Random rand = new Random();

  public SubmissionControl(){
    submittedJobs = GridPilot.getClassMgr().getSubmittedJobs();
    statusTable = GridPilot.getClassMgr().getStatusTable();
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
    String resourcesPath = configFile.getValue("GridPilot", "resources");
    if(resourcesPath != null && !resourcesPath.endsWith("/"))
      resourcesPath += "/";
    try{
      iconSubmitting = new ImageIcon(resourcesPath + "submitting.png");
    }
    catch(Exception e){
      Debug.debug("Could not find image "+ resourcesPath + "submitting.png", 3);
      iconSubmitting = new ImageIcon();
    }
  }

  /**
   * Reloads some values from configuration file. <p>
   * Theses values are : <ul>
   * <li>{@link #defaultUserName}
   * <li>{@link #maxSimultaneousSubmission}
   * <li>{@link #timeBetweenSubmission} </ul><p>
   *
   */
  public void loadValues(){
    String tmp = configFile.getValue("GridPilot", "maximum simultaneous submission");
    if(tmp != null){
      try{
        maxSimultaneousSubmission = Integer.parseInt(tmp);
      }catch(NumberFormatException nfe){
        logFile.addMessage("Value of \"maximum simultaneoud submission\" "+
                                    "is not an integer in configuration file", nfe);
      }
    }
    else
      logFile.addMessage(configFile.getMissingMessage("GridPilot", "maximum simultaneous submission") + "\n" +
                              "Default value = " + maxSimultaneousSubmission);
    tmp = configFile.getValue("GridPilot", "time between submissions");
    if(tmp != null){
      try{
        timeBetweenSubmission = Integer.parseInt(tmp);
      }
      catch(NumberFormatException nfe){
        logFile.addMessage("Value of \"time between submission\" is not"+
                                    " an integer in configuration file", nfe);
      }
    }
    else{
      logFile.addMessage(configFile.getMissingMessage("GridPilot", "time between submissions") + "\n" +
                              "Default value = " + timeBetweenSubmission);
    }
    Debug.debug("Setting time between submissions "+timeBetweenSubmission, 3);
    timer.setInitialDelay(0);
    timer.setDelay(timeBetweenSubmission);
  }

  /**
   * Creates the jobs for the specified job definitions and submits them on the specified
   * computing system. <p>
   * Each job is reserved, the job is created, a row is added to statusTable,
   * and {@link #submit(JobInfo)} is called. <p>
   */
  public void submitJobDefinitions(/*vector of JobDefinitions*/Vector selectedJobs,
      String csName, DBPluginMgr dbPluginMgr){
    GridPilot.getClassMgr().getGlobalFrame().showMonitoringPanel();
    synchronized(submittedJobs){
      Vector newJobs = new Vector();
      // This label is not shown because this function is not called in a Thread.
      // It seems to be quite dangereous to call this function in a thread, because
      // if one does it, you can "load job from db" during reservation (when jobs
      // are not yet put in toSubmitJobs).
      String resourcesPath =  GridPilot.getClassMgr().getConfigFile().getValue("GridPilot", "resources");
      if(resourcesPath==null){
        GridPilot.getClassMgr().getLogFile().addMessage(
            GridPilot.getClassMgr().getConfigFile().getMissingMessage("GridPilot", "resources"));
        resourcesPath = ".";
      }
      else{
        if(!resourcesPath.endsWith("/")){
          resourcesPath = resourcesPath + "/";
        }
      }
      statusBar.setLabel("Reserving. Please wait...");
      statusBar.animateProgressBar();
      String jobDefIdentifier = dbPluginMgr.getIdentifierField("jobDefinition");
      for(int i=0; i<selectedJobs.size(); ++i){
        DBRecord jobDef = ((DBRecord) selectedJobs.get(i));
        int jobDefID = Integer.parseInt(
            jobDef.getValue(jobDefIdentifier).toString());
        String jobUser = csPluginMgr.getUserInfo(csName);
        Debug.debug("User: "+jobUser, 3);
        if(jobUser!=null && dbPluginMgr.reserveJobDefinition(jobDefID, jobUser, csName)){
          // checks if this partition has not been monitored (and is Submitted,
          // otherwise reservation doesn't work)
          JobInfo job = null;
          boolean isjobMonitored = false;
          for(Iterator it = submittedJobs.iterator(); it.hasNext();){
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
            Debug.debug("Setting job user :"+jobUser+":", 3);
            job.setUser(jobUser);
            job.setCSName(csName);
            //job.setOutputs(prefix + "stdout", withStdErr ? prefix + "stderr" : null);
            // Some validation scripts assume stderr to be present...
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
      statusBar.stopAnimation();
      if(!newJobs.isEmpty()){
        statusBar.setLabel("Monitoring. Please wait...");
        statusBar.animateProgressBar();
        // new rows in table
        statusTable.createRows(submittedJobs.size());
        //jobControl.initChanges();
        for(Iterator it = GridPilot.getClassMgr().getDatasetMgrs().iterator(); it.hasNext();){
          ((DatasetMgr) it.next()).initChanges();
        }
        // jobControl.updateJobsByStatus();
        statusBar.setLabel("Monitoring done.");
        statusBar.stopAnimation();
        submit(newJobs);
      }
    }
  }

  /**
   * Submits the specified jobs on the given computing system. <p>
   */
  public void submitJobs(Vector jobs, String csName){
    synchronized(submittedJobs){
      Vector newJobs = new Vector();
      for(int i=0; i< jobs.size(); ++i){
        JobInfo job = (JobInfo) jobs.get(i);
        DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
        String jobUser = csPluginMgr.getUserInfo(csName);
        if(jobUser != null && dbPluginMgr.reserveJobDefinition(job.getJobDefId(), jobUser, csName)){
          job.setCSName(csName);
          newJobs.add(job);
          job.setDBStatus(DBPluginMgr.SUBMITTED);
          job.setLocalStatus(ComputingSystem.STATUS_WAIT);
          job.setUser(jobUser);
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
        submit(newJobs);
      }
    }
  }

  /**
   * Submits the given jobs, on the computing system given by job.getComputingSystem. <p>
   * Theses job are put in {@link #toSubmitJobs}. <p>
   */
  private void submit(Vector jobs){
    String isRand = configFile.getValue("GridPilot", "randomized submission");
    Debug.debug("isRand = " + isRand, 2);
    if(isRand != null && isRand.equalsIgnoreCase("yes")){
      jobs = shuffle(jobs);
    }
    pbSubmission.setMaximum(pbSubmission.getMaximum() + jobs.size());
    pbSubmission.addMouseListener(new MouseAdapter(){
      public void mouseClicked(MouseEvent e){
        cancelSubmission();
      }
    });
    statusBar.setLabel("Adding to submission queue. Please wait...");
    statusBar.animateProgressBar();
    pbSubmission.setToolTipText("Click here to cancel submission");
    if(!isProgressBarSet){
      statusBar.setProgressBar(pbSubmission);
      isProgressBarSet = true;
    }
    DatasetMgr.updateDBCells(jobs, statusTable);
    DatasetMgr.updateJobCells(jobs, statusTable);
    toSubmitJobs.addAll(jobs);
    if(!timer.isRunning()){
      timer.restart();
    }
    statusBar.setLabel("Adding done.");
    statusBar.stopAnimation();
  }

  /**
   * Returns a Vector which contains all jobs from <code>v</code>, but in a
   * random order. <p>
   */
  private Vector shuffle(Vector v){
    Vector w = new Vector();
    while(v.size() > 0)
      w.add(v.remove(rand.nextInt(v.size())));
    return w;
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
    for(int i=0; i < jobs.size() ; ++i){

      JobInfo job = (JobInfo) jobs.get(i);
      ShellMgr shell = null;
      try{
        shell = GridPilot.getClassMgr().getCSPluginMgr().getShellMgr(job);
      }
      catch(Exception e){
        Debug.debug("ERROR getting shell manager: "+e.getMessage(), 1);
      }

      if(job.getDBStatus() != DBPluginMgr.FAILED &&
          job.getDBStatus() != DBPluginMgr.UNEXPECTED){
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
    while (jobs.size() > 0){
      JobInfo job = (JobInfo) jobs.remove(0);
      //jobControl.updateDBStatus(job, DBPluginMgr.SUBMITTED);
      DatasetMgr datasetMgr = GridPilot.getClassMgr().getDatasetMgr(job.getDBName(),
          GridPilot.getClassMgr().getDBPluginMgr(job.getDBName()).getJobDefDatasetID(
              job.getJobDefId()));
      datasetMgr.updateDBStatus(job, DBPluginMgr.SUBMITTED);
      if(job.getDBStatus() != DBPluginMgr.SUBMITTED){
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
    DatasetMgr.updateJobCells(submitables, statusTable);
    submit(submitables);
  }

 /**
  * Checks if there are not too many active Threads (for submission), and there are
  * waiting jobs. If there are any, creates new submission threads
  */
  private synchronized void trigSubmission(){
    if(submittingJobs.size()<maxSimultaneousSubmission && !toSubmitJobs.isEmpty()){
      // transferts job from toSubmitJobs to submittingJobs
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
        DatasetMgr.FIELD_CS);
    //statusTable.setValueAt(job.getName(), job.getTableRow(),
    //                       DatasetMgr);
    //Debug.debug("jobName : " + job.getName(), 3);
    statusTable.setValueAt(iconSubmitting, job.getTableRow(),
        DatasetMgr.FIELD_CONTROL);
    if(csPluginMgr.submit(job)){
      Debug.debug("Job " + job.getName() + " submitted : \n" +
                  "\tCSJobId = " + job.getJobId() + "\n" +
                  "\tStdOut = " + job.getStdOut() + "\n" +
                  "\tStdErr = " + job.getStdErr(), 2);
      if(!dbPluginMgr.updateJobDefinition(
              job.getJobDefId(),
              new String []{job.getJobId(), job.getName(),
              job.getStdOut(), job.getStdErr()})){
        logFile.addMessage("DB update(" + job.getJobDefId() + ", " +
                           job.getJobId() + ", " + job.getName() + ", " +
                           job.getStdOut() + ", " + job.getStdErr() +
                           ") failed", job);
      }
      statusTable.setValueAt(job.getJobId(), job.getTableRow(),
          DatasetMgr.FIELD_JOBID);
      Debug.debug("Setting job user "+job.getUser(), 3);
      statusTable.setValueAt(job.getUser(), job.getTableRow(),
          DatasetMgr.FIELD_USER);
      statusTable.updateSelection();
      job.setNeedToBeRefreshed(true);
      job.setLocalStatus(ComputingSystem.STATUS_WAIT);
    }
    else{
      statusTable.setValueAt("Not submitted !", job.getTableRow(),
          DatasetMgr.FIELD_JOBID);
      job.setLocalStatus(ComputingSystem.STATUS_FAILED);
      job.setNeedToBeRefreshed(false);
      DatasetMgr datasetMgr = GridPilot.getClassMgr().getDatasetMgr(job.getDBName(),
          GridPilot.getClassMgr().getDBPluginMgr(job.getDBName()).getJobDefDatasetID(
              job.getJobDefId()));
      if(/*dbPluginMgr.updateJobDefStatus(job.getJobDefId(),
         Integer.toString(DBPluginMgr.FAILED)*/
          datasetMgr.updateDBStatus(job, DBPluginMgr.FAILED)){
        job.setDBStatus(DBPluginMgr.FAILED);
      }
      else{
        logFile.addMessage("DB update status(" + job.getJobDefId() + ", " +
            DBPluginMgr.getStatusName(job.getDBStatus()) +
                           ") failed", job);
      }
      DatasetMgr.updateDBCell(job, statusTable);
      //jobControl.updateJobsByStatus();
    }
    // remove iconSubmitting
    statusTable.setValueAt(null, job.getTableRow(), DatasetMgr.FIELD_CONTROL);
    //jobControl.updateJobsByStatus();
    for(Iterator it = GridPilot.getClassMgr().getDatasetMgrs().iterator(); it.hasNext();){
      ((DatasetMgr) it.next()).updateJobsByStatus();
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
      statusBar.setLabel("Submission done.");
    }
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
    while(e.hasMoreElements()){
      JobInfo job = (JobInfo) e.nextElement();
      statusTable.setValueAt("Not submitted (Cancelled)!", job.getTableRow(), DatasetMgr.FIELD_JOBID);
      statusTable.setValueAt(job.getName(), job.getTableRow(), DatasetMgr.FIELD_JOBNAME);
      job.setLocalStatus(ComputingSystem.STATUS_FAILED);
      job.setNeedToBeRefreshed(false);
      //DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
      DatasetMgr datasetMgr = GridPilot.getClassMgr().getDatasetMgr(job.getDBName(),
          GridPilot.getClassMgr().getDBPluginMgr(job.getDBName()).getJobDefDatasetID(
              job.getJobDefId()));
      if(datasetMgr.updateDBStatus(job, DBPluginMgr.FAILED)
          /*dbPluginMgr.updateJobDefStatus(job.getJobDefId(),
         Integer.toString(DBPluginMgr.FAILED))*/){
        job.setDBStatus(DBPluginMgr.FAILED);
      }
      else{
        logFile.addMessage("DB update status(" + job.getJobDefId() + ", " +
            DBPluginMgr.getStatusName(job.getDBStatus()) + ") failed", job);
      }
    }
    DatasetMgr.updateDBCells(toSubmitJobs, statusTable);
    toSubmitJobs.removeAllElements();
    statusBar.removeProgressBar(pbSubmission);
    pbSubmission.setMaximum(0);
    pbSubmission.setValue(0);
  }
}