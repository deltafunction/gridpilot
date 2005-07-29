package gridpilot;

import javax.swing.*;

import java.awt.event.*;
import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.Random;


/**
 * Controls the job submission. <p>
 * When JobControl asks for submitting some jobs (giving logical file Id or jobs already created),
 * these jobs are put in a queue (<code>toSubmitJobs</code>). Each <code>timeBetweenSubmission</code>,
 * a Timer checks if there is some jobs in this queue (this timer is stopped when the
 * queue is empty, and re-started when new jobs arrive). <br>
 * If there is any, the first job is removed from <code>toSubmitJobs</code>, put in
 * <code>submittingJobs</code>
 *
 * <p><a href="SubmissionControl.java.html">see sources</a>
 */
public class SubmissionControl {

  /** @see JobControl#submittedJobs */
  private JobVector submittedJobs;
  private AMIMgt amiMgt;
  private StatusBar statusBar;
  private JProgressBar pbSubmission;
  private boolean isProgressBarSet=false;
  private ConfigFile configFile;
  private LogFile logFile;

  private JobControl jobControl;

  private Table statusTable;

  private PluginMgr pluginMgr;

  private Timer timer;

  /**
   * name used for reservation
   */
  private String userName;

  private ImageIcon iconSubmitting;

  /** All jobs for which the submission is not made yet */
  private JobVector toSubmitJobs = new JobVector();

  /** All jobs for which the submission is in progress */
  private JobVector submittingJobs = new JobVector();

 /** Maximum number of simulaneous threads for submission. <br>
   * It is not the maximum number of running jobs on the Computing System */
  private int maxSimultaneousSubmission = 5;

  /** Delay between the begin of two submission threads */
  private int timeBetweenSubmission = 1000;


  private Random rand = new Random();

  public SubmissionControl(PluginMgr _pluginMgr, JobVector _submittedJobs, Table _statusTable) {
    submittedJobs = _submittedJobs;
//    cs = _cs;
//    csNames = _csNames;
    statusTable = _statusTable;

    pluginMgr = _pluginMgr;


    amiMgt = AtCom.getClassMgr().getAMIMgt();
    statusBar = AtCom.getClassMgr().getStatusBar();
    configFile = AtCom.getClassMgr().getConfigFile();
    logFile = AtCom.getClassMgr().getLogFile();

    timer = new Timer(0, new ActionListener(){
      public void actionPerformed(ActionEvent e){
        trigSubmission();
      }
    });

    loadValues();

    pbSubmission = new JProgressBar(0,0);

    String resourcesPath = configFile.getValue("AtCom", "resources");
    if(resourcesPath != null && !resourcesPath.endsWith("/"))
      resourcesPath += "/";
    ImageIcon iconSubmitting;
    URL imgURL=null;
    try{
      imgURL = AtCom.class.getResource(resourcesPath + "submitting.gif");
      iconSubmitting = new ImageIcon(resourcesPath + "submitting.gif");
    }catch(Exception e){
      Debug.debug("Could not find image "+ resourcesPath + "submitting.gif", 3);
      iconSubmitting = new ImageIcon();
    }
  }

  /**
   * Reloads some values from configuration file. <p>
   * Theses values are : <ul>
   * <li>{@link #userName}
   * <li>{@link #maxSimultaneousSubmission}
   * <li>{@link #timeBetweenSubmission} </ul><p>
   *
   * Called by {@link JobControl#reloadValues()}
   *
   */
  
  public void loadValues(){
    userName = configFile.getValue("AtCom", "username");

    String tmp = configFile.getValue("AtCom", "maximum simultaneous submission");
    if(tmp != null){
      try{
        maxSimultaneousSubmission = Integer.parseInt(tmp);
      }catch(NumberFormatException nfe){
        logFile.addMessage("Value of \"maximum simultaneoud submission\" "+
                                    "is not an integer in configuration file", nfe);
      }
    }
    else
      logFile.addMessage(configFile.getMissingMessage("AtCom", "maximum simultaneous submission") + "\n" +
                              "Default value = " + maxSimultaneousSubmission);


    tmp = configFile.getValue("AtCom", "time between submissions");
    if(tmp != null){
      try{
        timeBetweenSubmission = Integer.parseInt(tmp);
      }catch(NumberFormatException nfe){
        logFile.addMessage("Value of \"time between submission\" is not"+
                                    " an integer in configuration file", nfe);
      }
    }
    else
      logFile.addMessage(configFile.getMissingMessage("AtCom", "time between submissions") + "\n" +
                              "Default value = " + timeBetweenSubmission);

    timer.setDelay(timeBetweenSubmission);
  }

  /**
   * Creates the jobs for the specified logical files (partitions), and submits them on the specified
   * computing system. <p>
   * Each logical file is reserved, the job is created, a row is added to statusTable,
   * and {@link #submit(JobInfo)} is called. <p>
   *
   * Called by {@link JobControl#submitPartitions(int[], int)}
   */
  public void submitPartitions(int [] selectedPartitions, int computingSystem){
    if(jobControl == null)
      jobControl = AtCom.getClassMgr().getJobControl();
    synchronized(submittedJobs){
      JobVector newJobs = new JobVector();
      statusBar.setLabel("reserving logical files ...");
      // This label is not shown because this function is not called in a Thread.
      // It seems to be quite dangereous to call this function in a thread, because
      // if one does it, you can "load job from ami" during reservation (when jobs
      // are not yet put in toSubmitJobs).

      String resourcesPath =  atcom.AtCom.getClassMgr().getConfigFile().getValue("AtCom", "resources");
      if(resourcesPath == null){
        atcom.AtCom.getClassMgr().getLogFile().addMessage(
            atcom.AtCom.getClassMgr().getConfigFile().getMissingMessage("AtCom", "resources"));
        resourcesPath = ".";
      }
      else{
        if (!resourcesPath.endsWith("/"))
          resourcesPath = resourcesPath + "/";
      }
      Splash splash = new Splash(resourcesPath, "wait.gif");
      splash.show("Submitting. Please wait...");
      for(int i=0; i< selectedPartitions.length; ++i){
        String csName = pluginMgr.getCSName(computingSystem);
        if(userName != null && amiMgt.reservePart(selectedPartitions[i], userName, csName)){
          // checks if this partition has not been monitored (and is Submitted,
          // otherwise reservation don't work)
          JobInfo job = submittedJobs.getJobWithPartId(selectedPartitions[i]);

          if(job == null){ // this job doesn't exist yet
            job = new JobInfo(selectedPartitions[i], computingSystem);
            job.setTableRow(submittedJobs.size());
            submittedJobs.add(job);
            job.setAMIStatus(AMIMgt.SUBMITTED);
            job.setName(amiMgt.getPartLFN(job.getPartId()));
            if(AtCom.showAllJobs){
              String jobUser = amiMgt.getPartJobUser(job.getPartId());
              if(jobUser==null){
                Debug.debug("Job user null, getting from DB, "+jobUser, 3);
                jobUser = amiMgt.getPartReserved(job.getPartId());
              }
              Debug.debug("Setting job user :"+jobUser+":", 3);
              job.setUser(jobUser);
            }
          }
          else{ // this job exists, it gets computingSystem, keeps its row number,
            // and is not added to submittedJobs
            job.setComputingSystem(computingSystem);
            job.setAMIStatus(AMIMgt.SUBMITTED);
          }
          newJobs.add(job);

//          ++jobControl.jobsByStatus[JobControl.waitIndex];

          Debug.debug("logical file " + job.getPartId() + "("+job.getName()+") reserved", 2);
        }
        else{
          if(userName == null)
            logFile.addMessage(configFile.getMissingMessage("AtCom", "username"));
          logFile.addMessage("cannot reserve logical file "+selectedPartitions[i]);

          Debug.debug("logical file " + selectedPartitions[i] + " cannot be reserved", 3);
        }
      }
      splash.stopSplash();
      statusBar.removeLabel();
      if(!newJobs.isEmpty()){
        // new rows in table
        statusTable.createRows(submittedJobs.size());
        jobControl.initChanges();
//        jobControl.updateJobsByStatus();
        submit(newJobs);
      }
    }
  }

  /**
   * Submits the specified jobs on the given computing system. <p>
   *
   * Each logical file (partition) is reserved, and {@link #submit(JobInfo)} is called. <p>
   *
   * Called by {@link JobControl#submitJobs(int[],int)}
   */
  public void submitJobs(JobVector jobs, int computingSystem){
    if(jobControl == null)
      jobControl = AtCom.getClassMgr().getJobControl();
    synchronized(submittedJobs){
      JobVector newJobs = new JobVector();


      for(int i=0; i< jobs.size(); ++i){
        String csName = pluginMgr.getCSName(computingSystem);
        JobInfo job = jobs.get(i);
        if(userName != null && amiMgt.reservePart(job.getPartId(), userName, csName)){
          job.setComputingSystem(computingSystem);
          newJobs.add(job);
          job.setAMIStatus(AMIMgt.SUBMITTED);
          job.setAtComStatus(ComputingSystem.STATUS_WAIT);
          job.setUser(userName);
          job.setJobId(null);
          job.setHost(null);
          job.setJobStatus(null);

          Debug.debug("logical file " + job.getPartId() + "("+job.getName()+") reserved", 2);
        }
        else{
          if(userName == null)
            logFile.addMessage(configFile.getMissingMessage("AtCom", "username"));
          logFile.addMessage("cannot reserve logical file "+job.getPartId());
        }
      }
      statusBar.removeLabel();
      if(!newJobs.isEmpty()){
//        jobControl.updateJobsByStatus();
        submit(newJobs);
      }
    }
  }

  /**
   * Submits the given jobs, on the computing system given by job.getComputingSystem. <p>
   * Theses job are put in {@link #toSubmitJobs}. <p>
   *
   * Called by : <ul>
   * <li>{@link #submitPartitions(int[],int)}
   * <li>{@link #submitJobs(JobVector,int)} </ul>
   *
   */
  private void submit(JobVector jobs){
    String isRand = configFile.getValue("AtCom", "randomized submission");
    Debug.debug("isRand = " + isRand, 2);
    if(isRand != null && isRand.equalsIgnoreCase("yes"))
      jobs = shuffle(jobs);


    if(jobControl == null)
      jobControl = AtCom.getClassMgr().getJobControl();

    pbSubmission.setMaximum(pbSubmission.getMaximum() + jobs.size());
    pbSubmission.addMouseListener(new MouseAdapter(){
      public void mouseClicked(MouseEvent e){
        cancelSubmission();
      }
    });
    pbSubmission.setToolTipText("Click here to cancel submission");

    if(!isProgressBarSet){
      statusBar.setProgressBar(pbSubmission);
      isProgressBarSet = true;
    }

    jobControl.updateAMICells(jobs);
    jobControl.updateJobCells(jobs);


    toSubmitJobs.addAll(jobs);
//    if(timer != null && !timer.isRunning())
    if(!timer.isRunning())
      timer.restart();
  }

  /**
   * Returns a JobVector which contains all jobs from <code>v</code>, but in a
   * random order. <p>
   *
   * Called by {@link #submit(JobVector)}
   */
  private JobVector shuffle(JobVector v){
    JobVector w = new JobVector();
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
   *
   * Called by {@link JobControl#resubmit(int[])}
   */
  public void resubmit(JobVector jobs){

    boolean askSave = false;
    boolean deleteFiles = false;
    for(int i=0; i < jobs.size() ; ++i){

      JobInfo job = jobs.get(i);

      ShellMgr shell = pluginMgr.getShellMgr(job);

      if (job.getAMIStatus() != AMIMgt.FAILED &&
          job.getAMIStatus() != AMIMgt.UNEXPECTED) {
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
      catch(IOException e){
        Debug.debug("ERROR checking for stdout: "+e.getMessage(), 2);
        logFile.addMessage("ERROR checking for stdout: "+e.getMessage());
        //throw e;
      }
      try{
        stdErrExists = job.getStdErr() != null &&
        shell.existsFile(job.getStdErr());
      }
      catch(IOException e){
        Debug.debug("ERROR checking for stdout: "+e.getMessage(), 2);
        logFile.addMessage("ERROR checking for stdout: "+e.getMessage());
        //throw e;
      }

      if (!askSave) {
        if (deleteFiles) {
          if (stdOutExists)
            shell.deleteFile(job.getStdOut());
          if (stdErrExists)
            shell.deleteFile(job.getStdErr());
        }
      }
      else {
        if (stdOutExists || stdErrExists) {
          Object[] options = {"Yes", "No", "No for all", "Cancel"};

          int choice = JOptionPane.showOptionDialog(JOptionPane.getRootFrame(),
              "Do you want to save job \n" +
              "outputs before resubmit it", "Save outputs for " + job.getName() + " ?",
              JOptionPane.YES_NO_CANCEL_OPTION,
              JOptionPane.QUESTION_MESSAGE, null,
              options, options[0]);

          switch (choice) {
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
              if (stdOutExists) {
                if (f.showDialog(JOptionPane.getRootFrame(), "Save stdout") ==
                    JFileChooser.APPROVE_OPTION) {

                  java.io.File stdOut = f.getSelectedFile();
                  if (stdOut != null) {
                    try {
                      pluginMgr.getLocalShellMgr().writeFile(stdOut.getPath(),
                          shell.readFile(job.getStdOut()), false);
//                  File oldStdOut = new File(job.getStdOut());
//                  oldStdOut.renameTo(stdOut);
                    }
                    catch (java.io.IOException ioe) {
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

              if (stdErrExists) {
                if (f.showDialog(JOptionPane.getRootFrame(), "Save stderr") ==
                    JFileChooser.APPROVE_OPTION) {
                  java.io.File stdErr = f.getSelectedFile();
                  if (stdErr != null) {
                    try {
                      pluginMgr.getLocalShellMgr().writeFile(stdErr.getPath(),
                          shell.readFile(job.getStdErr()), false);
//                  File oldStdErr = new File(job.getStdErr());
//                  oldStdErr.renameTo(stdErr);
                    }
                    catch (java.io.IOException ioe) {
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

    if (jobControl == null)
      jobControl = AtCom.getClassMgr().getJobControl();

    JobVector submitables = new JobVector();

    while (jobs.size() > 0) {
      JobInfo job = jobs.remove(0);

      jobControl.updateAMIStatus(job, AMIMgt.SUBMITTED);

      if (job.getAMIStatus() != AMIMgt.SUBMITTED) { // updateAMIStatus didn't work
        logFile.addMessage(
            "This job cannot be set Submitted -> this job cannot be resubmited",
            job);
      }
      else {
        job.setNeedToBeRefreshed(false);
        job.setJobId(null);
        job.setHost(null);
        job.setJobStatus(null);
        submitables.add(job);
      }
    }

    jobControl.updateJobCells(submitables);

    submit(submitables);
  }

 /**
  * Checks if there are not too many active Threads (for submission), and there are
  * waiting jobs. If there are any, creates new submission threads
  *
  * Called by timer
  */

  private synchronized void trigSubmission() {

    if(submittingJobs.size() < maxSimultaneousSubmission && !toSubmitJobs.isEmpty()){
      // transferts job from toSubmitJobs to submittingJobs
      final JobInfo job = toSubmitJobs.remove(0);

      submittingJobs.add(job);

      new Thread(){
        public void run() {
          submit(job);
        }
      }.start();
    }
    else
      timer.stop();
  }


  /**
   * Submits the specified job. <p>
   * Creates these job outputs, and calls the plugin submission method (via PluginMgr). <br>
   * This method is started in a thread. <p>
   *
   * Called by {@link #trigSubmission()}
   */
  private void submit(final JobInfo job) {

    job.setName(amiMgt.getPartLFN(job.getPartId()));

    statusTable.setValueAt(pluginMgr.getCSName(job), job.getTableRow(),
                           JobControl.FIELD_CS);
/*    statusTable.setValueAt(job.getName(), job.getTableRow(),
                           JobControl.FIELD_JOBNAME);
    Debug.debug("jobName : " + job.getName(), 3);*/
    statusTable.setValueAt(iconSubmitting, job.getTableRow(),
                           JobControl.FIELD_CONTROL);

    if (createOutputs(job) && pluginMgr.submit(job)) {

      Debug.debug("Job " + job.getName() + " submitted : \n" +
                  "\tCSJobId = " + job.getJobId() + "\n" +
                  "\tStdOut = " + job.getStdOut() + "\n" +
                  "\tStdErr = " + job.getStdErr(), 2);

      if (!amiMgt.updatePartJob(job.getPartId(), job.getJobId(), job.getName(),
                                job.getStdOut(), job.getStdErr()))
        logFile.addMessage("AMI updatePartJob(" + job.getPartId() + ", " +
                           job.getJobId() + ", " + job.getName() + ", " +
                           job.getStdOut() + ", " + job.getStdErr() +
                           ") failed", job);

      statusTable.setValueAt(job.getJobId(), job.getTableRow(),
                             JobControl.FIELD_JOBID);
      if(AtCom.showAllJobs){
        Debug.debug("Setting job user "+job.getUser(), 3);
        statusTable.setValueAt(job.getUser(), job.getTableRow(),
           JobControl.FIELD_USER);
        statusTable.updateSelection();
      }
      job.setNeedToBeRefreshed(true);
      job.setAtComStatus(ComputingSystem.STATUS_WAIT);
    }
    else {
      statusTable.setValueAt("Not submitted !", job.getTableRow(),
                             JobControl.FIELD_JOBID);

      job.setAtComStatus(ComputingSystem.STATUS_FAILED);


      job.setNeedToBeRefreshed(false);

      if (amiMgt.updatePartStatus(job.getPartId(), AMIMgt.FAILED))
        job.setAMIStatus(AMIMgt.FAILED);
      else
        logFile.addMessage("AMI updatePartStatus(" + job.getPartId() + ", " +
                           AMIMgt.getStatusName(job.getAMIStatus()) +
                           ") failed", job);

      jobControl.updateAMICell(job);
      jobControl.updateJobsByStatus();

    }
	// remove iconSubmitting
    statusTable.setValueAt(null, job.getTableRow(), JobControl.FIELD_CONTROL);
    jobControl.updateJobsByStatus();

    submittingJobs.remove(job);

    if (!timer.isRunning())
      timer.restart();

    pbSubmission.setValue(pbSubmission.getValue() + 1);

    if (pbSubmission.getPercentComplete() == 1.0) {
      statusBar.removeProgressBar(pbSubmission);
      isProgressBarSet = false;
      pbSubmission.setMaximum(0);
      pbSubmission.setValue(0);
    }
  }

  /**
   * Create fields stdOut and (possibly) stdErr in this job. <br>
   * These names are &lt;working path&gt;/&lt;logicalFile (partition) LFN&gt;.&lt;rand&gt/stdout and /stderr <br>
   * If stderrDest is not defined in AMI, stdErr = null. <p>
   *
   * Called by {@link #submit(JobInfo)}
   */
  private boolean createOutputs(JobInfo job) {

    ShellMgr shell = pluginMgr.getShellMgr(job);

    String workingPath = configFile.getValue(pluginMgr.getCSName(job),
                                             "working path");

    if (workingPath == null)
      workingPath = pluginMgr.getCSName(job);



    String finalStdErr = AtCom.getClassMgr().getAMIMgt().getPartStderrDest(job.getPartId());
    //boolean withStdErr = finalStdErr != null && finalStdErr.trim().length() > 0;

    String prefix = null;
    
    try{
      prefix = shell.createTempDir(job.getName()+".", workingPath);
    }
    catch(IOException e){
      Debug.debug("ERROR checking for stdout: "+e.getMessage(), 2);
      logFile.addMessage("ERROR checking for stdout: "+e.getMessage());
      //throw e;
    }
    
    if(prefix == null){ // if dir cannot be created
      logFile.addMessage("Temp dir cannot be created with prefix " + job.getName()+
                         ". in the directory " + workingPath, job);
      return false;
    }

    if(!prefix.endsWith("/"))
      prefix += "/";

    //job.setOutputs(prefix + "stdout", withStdErr ? prefix + "stderr" : null);
    // Some validation scripts assume stderr to be present...
    job.setOutputs(prefix + "stdout", prefix + "stderr");

    return true;

  }


  public boolean isSubmitting() {
//    return timer.isRunning();
    return !(submittingJobs.isEmpty() && toSubmitJobs.isEmpty());
  }

  /**
   * Stops the submission. <br>
   * Empies toSubmitJobs, and set theses jobs to Failed. <p>
   *
   * Called when the user click on the submission progress bar
   */
  private void cancelSubmission(){
    timer.stop();
    Enumeration e = toSubmitJobs.elements();
    while(e.hasMoreElements()){
      JobInfo job = (JobInfo) e.nextElement();
      statusTable.setValueAt("Not submitted (Cancelled)!", job.getTableRow(), JobControl.FIELD_JOBID);
      statusTable.setValueAt(job.getName(), job.getTableRow(), JobControl.FIELD_JOBNAME);

      job.setAtComStatus(ComputingSystem.STATUS_FAILED);

      job.setNeedToBeRefreshed(false);
      if(amiMgt.updatePartStatus(job.getPartId(), AMIMgt.FAILED))
	      job.setAMIStatus(AMIMgt.FAILED);
      else
        logFile.addMessage("AMI updatePartStatus(" + job.getPartId() + ", " +
                           AMIMgt.getStatusName(job.getAMIStatus()) + ") failed", job);

    }
    jobControl.updateAMICells(toSubmitJobs);

    toSubmitJobs.removeAllElements();

    statusBar.removeProgressBar(pbSubmission);
    pbSubmission.setMaximum(0);
    pbSubmission.setValue(0);
  }
}