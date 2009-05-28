package gridpilot;

import javax.swing.*;

import java.awt.event.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import gridfactory.common.ConfigFile;
import gridfactory.common.DBRecord;
import gridfactory.common.Debug;
import gridfactory.common.Shell;
import gridfactory.common.StatusBar;

/**
 * Controls the job submission. <p>
 * Submitted jobs are put in a queue (<code>toSubmitJobs</code>). Each
 * <code>timeBetweenSubmissions</code>, a Timer checks if there is some jobs in
 * this queue (this timer is stopped when the queue is empty, and restarted when
 * new jobs arrive). <br>
 * If there are any, the first job is removed from <code>toSubmitJobs</code> and
 * put in <code>submittingJobs</code>
 */
public class SubmissionControl{
  private Vector submittedJobs;
  private StatusBar statusBar;
  private StatusBar monitorStatusBar;
  private JProgressBar pbSubmission;
  private boolean isProgressBarSet = false;
  private ConfigFile configFile;
  private MyLogFile logFile;
  private MyJTable statusTable;
  private CSPluginMgr csPluginMgr;
  private Timer timer;
  private ImageIcon iconSubmitting;
  private ImageIcon iconWaiting;
  /** All jobs for which the submission is not done yet. */
  private Vector toSubmitJobs = new Vector();
  /** All jobs for which the submission is in progress. */
  private Vector submittingJobs = new Vector();
 /** Maximum number of simulaneous threads for submission. */
  private int maxSimultaneousSubmissions = 5;
  /** Total maximum total number of simultaneously running jobs. */
  private int totalMaxRunning = 10;
  /** Maximum total number of simultaneously running jobs per CS. */
  private int [] maxRunningPerCS;
  /** Delay between the begin of two submission threads. */
  private int timeBetweenSubmissions = 5000;
  private String isRand = null;
  private String [] csNames;

  public SubmissionControl() throws Exception{
    submittedJobs = GridPilot.getClassMgr().getSubmittedJobs();
    statusTable = GridPilot.getClassMgr().getJobStatusTable();
    csPluginMgr = GridPilot.getClassMgr().getCSPluginMgr();
    
    statusBar = GridPilot.getClassMgr().getStatusBar();
    monitorStatusBar = GridPilot.getClassMgr().getGlobalFrame().getMonitoringPanel().getStatusBar();
    configFile = GridPilot.getClassMgr().getConfigFile();
    logFile = GridPilot.getClassMgr().getLogFile();

    timer = new Timer(0, new ActionListener(){
      public void actionPerformed(ActionEvent e){
        trigSubmission();
      }
    });
    
    loadValues();
    createProgressBar();
  }
  
  private void createProgressBar() throws InterruptedException, InvocationTargetException{
    SwingUtilities.invokeAndWait(new Runnable() {
      public void run(){
        try{
          pbSubmission = new JProgressBar(0,0);
        }
        catch(Exception e){
          e.printStackTrace();
        }
      }
    });
  }
  
  /**
   * Reloads some values from configuration file. <p>
   * These values are : <ul>
   * <li>{@link #defaultUserName}
   * <li>{@link #maxSimultaneousSubmissions}
   * <li>{@link #maxRunning}
   * <li>{@link #timeBetweenSubmissions} </ul><p>
   *
   */
  public void loadValues(){
    String tmp = configFile.getValue("Computing systems", "Max simultaneous submissions");
    if(tmp!=null){
      try{
        maxSimultaneousSubmissions = Integer.parseInt(tmp);
      }
      catch(NumberFormatException nfe){
        logFile.addMessage("Value of \"max simultaneous submission\" "+
                                    "is not an integer in configuration file", nfe);
      }
    }
    else{
      logFile.addMessage(configFile.getMissingMessage("Computing systems", "max simultaneous submissions") + "\n" +
                              "Default value = " + maxSimultaneousSubmissions);
    }
    totalMaxRunning = 0;
    csNames = GridPilot.getClassMgr().getCSPluginMgr().getEnabledCSNames();
    maxRunningPerCS = new int[csNames.length];
    for(int i=0; i<csNames.length; ++i){
      maxRunningPerCS[i] = MyUtil.getMaxSimultaneousRunningJobs(csNames[i]);
      totalMaxRunning += maxRunningPerCS[i];
    }
    
    tmp = configFile.getValue("Computing systems", "Time between submissions");
    if(tmp!=null){
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
    String resourcesPath = configFile.getValue(GridPilot.TOP_CONFIG_SECTION, "resources");
    if(resourcesPath!=null && !resourcesPath.endsWith("/"))
      resourcesPath += "/";
    URL imgURL=null;
    try{
      imgURL = GridPilot.class.getResource(GridPilot.RESOURCES_PATH + "submitting.png");
      iconSubmitting = new ImageIcon(imgURL);
    }
    catch(Exception e){
      logFile.addMessage("Could not find image "+ resourcesPath + "submitting.png");
      iconSubmitting = new ImageIcon();
    }
    try{
      imgURL = GridPilot.class.getResource(GridPilot.RESOURCES_PATH + "waiting.png");
      iconWaiting = new ImageIcon(imgURL);
    }
    catch(Exception e){
      logFile.addMessage("Could not find image "+ resourcesPath + "submitting.png");
      iconWaiting = new ImageIcon();
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
    GridPilot.getClassMgr().getGlobalFrame().showMonitoringPanel(MonitoringPanel.TAB_INDEX_JOBS);
    synchronized(submittedJobs){
      Vector newJobs = new Vector();
      // This label is not shown because this function is not called in a Thread.
      // It seems to be quite dangereous to call this function in a thread, because
      // if one does it, you can "load job from db" during reservation (when jobs
      // are not yet put in toSubmitJobs).
      statusBar.setLabel("Reserving. Please wait...");
      //statusBar.animateProgressBar();
      String jobDefIdentifier = MyUtil.getIdentifierField(dbPluginMgr.getDBName(), "jobDefinition");
      for(int i=0; i<selectedJobs.size(); ++i){
        DBRecord jobDef = ((DBRecord) selectedJobs.get(i));
        String jobDefID = jobDef.getValue(jobDefIdentifier).toString();
        String userInfo = null;
        try{
          jobDef.getValue("userInfo").toString();
        }
        catch(Exception e){
        }
        if(userInfo==null){
          userInfo = "";
        }
        if(dbPluginMgr.reserveJobDefinition(jobDefID, userInfo, csName)){
          // checks if this partition has not been monitored (and is Submitted,
          // otherwise reservation doesn't work)
          MyJobInfo job = null;
          boolean isjobMonitored = false;
          for(Iterator it=submittedJobs.iterator(); it.hasNext();){
            job = ((MyJobInfo) it.next());
            if(job.getIdentifier()==jobDefID){
              isjobMonitored = true;
              break;
            };
          }
          if(isjobMonitored==false){ // this job doesn't exist yet
            job = new MyJobInfo(
                jobDefID,
                dbPluginMgr.getJobDefName(jobDefID),
                csName,
                dbPluginMgr.getDBName()
                );
            job.setTableRow(submittedJobs.size());
            submittedJobs.add(job);
            //job.setDBStatus(DBPluginMgr.SUBMITTED);
            job.setDBStatus(DBPluginMgr.DEFINED);
            job.setName(dbPluginMgr.getJobDefinition(jobDefID).getValue("name").toString());
            job.setCSName(csName);
          }
          else{
            // this job exists, get its computingSystem, keeps its row number,
            // and do not add to submittedJobs
            job.setCSName(csName);
            //job.setDBStatus(DBPluginMgr.SUBMITTED);
            job.setDBStatus(DBPluginMgr.DEFINED);
          }
          newJobs.add(job);
          Debug.debug("job definition " + job.getIdentifier() + "("+job.getName()+") reserved", 2);
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
        MyJobInfo job = (MyJobInfo) jobs.get(i);
        DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
        if(dbPluginMgr.reserveJobDefinition(job.getIdentifier(), "", csName)){
          job.setCSName(csName);
          newJobs.add(job);
          //job.setDBStatus(DBPluginMgr.SUBMITTED);
          //job.setDBStatus(DBPluginMgr.DEFINED);
          job.setCSStatus(MyJobInfo.CS_STATUS_WAIT);
          job.setJobId(null);
          job.setHost(null);
          job.setStatusReady();
          Debug.debug("identifier " + job.getIdentifier() + "("+job.getName()+") reserved", 2);
        }
        else{
          logFile.addMessage("cannot reserve job "+job.getIdentifier());
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
      jobs = MyUtil.shuffle(jobs);
    }
    MyJobInfo job;
    for(Iterator it=jobs.iterator(); it.hasNext();){
      job = (MyJobInfo) it.next();
      statusTable.setValueAt(iconWaiting, job.getTableRow(),
          JobMgr.FIELD_CONTROL);
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
    job = (MyJobInfo) jobs.get(0);
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
  public void resubmit(Vector<MyJobInfo> jobs){
    boolean askSave = false;
    boolean deleteFiles = false;
    monitorStatusBar.setLabel("Cleaning up jobs...");
    for(int i=0; i<jobs.size() ; ++i){
      MyJobInfo job = jobs.get(i);
      Shell shell = null;
      try{
        shell = GridPilot.getClassMgr().getShell(job);
      }
      catch(Exception e){
        Debug.debug("WARNING: no shell manager: "+e.getMessage(), 1);
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
        stdOutExists = job.getOutTmp()!=null &&
           shell.existsFile(job.getOutTmp());
      }
      catch(Exception e){
        Debug.debug("ERROR checking for stdout: "+e.getMessage(), 2);
        //logFile.addMessage("ERROR checking for stdout: "+e.getMessage());
        //throw e;
      }
      try{
        stdErrExists = job.getErrTmp() != null &&
           shell.existsFile(job.getErrTmp());
      }
      catch(Exception e){
        Debug.debug("ERROR checking for stderr: "+e.getMessage(), 2);
        //logFile.addMessage("ERROR checking for stdout: "+e.getMessage());
        //throw e;
      }
      if(!askSave){
        if(deleteFiles){
          if(stdOutExists){
            shell.deleteFile(job.getOutTmp());
          }
          if(stdErrExists){
            shell.deleteFile(job.getErrTmp());
          }
        }
      }
      else{
        askSaveOutputs(job, stdOutExists, stdErrExists, askSave, deleteFiles, shell);
      }
    }
    cleanupAndQueue(jobs);
  }
  
  private void cleanupAndQueue(Vector<MyJobInfo> jobs){
    MyJobInfo job = null;
    JobMgr jobMgr = null;
    HashMap<String, Vector<MyJobInfo>> submitables = new HashMap<String, Vector<MyJobInfo>>();
    monitorStatusBar.setLabel("Updating job status...");
    while(jobs.size()>0){
      job = (MyJobInfo) jobs.remove(0);
      jobMgr = GridPilot.getClassMgr().getJobMgr(job.getDBName());
      if(!submitables.keySet().contains(job.getDBName())){
        submitables.put(job.getDBName(), new Vector<MyJobInfo>());
      }
      // first clean up old job
      jobMgr.getDBPluginMgr().cleanRunInfo(job.getIdentifier());
      GridPilot.getClassMgr().getCSPluginMgr().cleanup(job);
      // then set the status to Submitted
      // (this will not cause a cleanup, as setting the status to Defined would)
      jobMgr.updateDBStatus(job, DBPluginMgr.ABORTED);
      if(job.getDBStatus()!=DBPluginMgr.ABORTED){
        // updateDBStatus didn't work
        logFile.addMessage(
            "This job cannot be set Submitted -> this job cannot be resubmited",
            job);
      }
      else{
        job.setNeedsUpdate(false);
        job.setJobId(null);
        job.setHost(null);
        job.setStatusReady();
        submitables.get(job.getDBName()).add(job);
      }
    }
    // if all went well we can now submit
    //monitorStatusBar.setLabel("Submitting job(s)...");
    String dbName;
    for(Iterator<String> it=submitables.keySet().iterator(); it.hasNext();){
      dbName = it.next();
      jobMgr.updateJobCells(submitables.get(dbName));
      Debug.debug("Queueing "+submitables.get(dbName), 2);
      queue(submitables.get(dbName));
    }
    if(!timer.isRunning()){
      timer.restart();
    }
  }

  private void askSaveOutputs(MyJobInfo job, boolean stdOutExists, boolean stdErrExists,
     boolean askSave, boolean deleteFiles, Shell shell) {
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
             if(!shell.deleteFile(job.getOutTmp()))
               logFile.addMessage("Cannot delete stdout", job);
           if(stdErrExists)
             if(!shell.deleteFile(job.getErrTmp()))
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
                   shell.readFile(job.getOutTmp()), false);
               }
               catch(java.io.IOException ioe){
                 logFile.addMessage("Cannot rename " + job.getOutTmp() +
                                    " in " + stdOut.getPath(), ioe);
               }
             }
           }
           else
           if(deleteFiles) //remove StdOut
             shell.deleteFile(job.getOutTmp());
         }
         // stderr
         if(stdErrExists){
           if(f.showDialog(JOptionPane.getRootFrame(), "Save stderr")==
               JFileChooser.APPROVE_OPTION){
             java.io.File stdErr = f.getSelectedFile();
             if(stdErr != null){
               try{
                 shell.writeFile(stdErr.getPath(),
                    shell.readFile(job.getErrTmp()), false);
               }
               catch(java.io.IOException ioe){
                 logFile.addMessage("Cannot rename " + job.getErrTmp() +
                                    " in " + stdErr.getPath(), ioe);
               }
             }
           }
           else
           if(deleteFiles) //remove StdErr
             shell.deleteFile(job.getErrTmp());
         }
         break;

       default:
         return;
     }
   }
  }

/**
  * Checks if there are not too many active submission threads and waiting jobs.
  * If there are any left, create new submission threads.
  */
  private synchronized void trigSubmission(){
    if(toSubmitJobs.isEmpty()){
      timer.stop();
      return;
    }
    final MyJobInfo job = (MyJobInfo) toSubmitJobs.get(0);
    if(checkRunning(job)){
      job.setDBStatus(DBPluginMgr.SUBMITTED);
      // transfer job from toSubmitJobs to submittingJobs
      toSubmitJobs.remove(job);
      submittingJobs.add(job);
      new Thread(){
        public void run(){
          submit(job);
        }
      }.start();
    }
    if(toSubmitJobs.isEmpty()){
      timer.stop();
    }
  }
  
  /**
   * Check if this job can be submitted or we should wait.
   * @param job
   * @return
   */
  private boolean checkRunning(MyJobInfo job){
    int runningJobs = 0;
    JobMgr mgr = null;
    MyJobInfo tmpJob;
    int [] jobsByStatus = null;
    int [] rJobsByCS = new int[csNames.length];
    int [] rjc = new int[csNames.length];
    int jobCsIndex = -1;
    for(int i=0; i<csNames.length; ++i){
      rJobsByCS[i] = 0;
    }
    for(Iterator it = GridPilot.getClassMgr().getJobMgrs().iterator(); it.hasNext();){
      mgr = ((JobMgr) it.next());
      mgr.updateJobsByStatus();
      jobsByStatus = mgr.getJobsByStatus();
      rjc = mgr.getSubmittedJobsByCS();
      for(int i=0; i<csNames.length; ++i){
        rJobsByCS[i] += rjc[i];
        //Debug.debug("Upping job count for CS "+csNames[i]+" with "+rjc[i], 3);
        if(csNames[i].equalsIgnoreCase(job.getCSName())){
          jobCsIndex = i;
        }
      }
      runningJobs += (jobsByStatus[0]+jobsByStatus[1]);
    }
    for(Iterator it = submittingJobs.iterator(); it.hasNext();){
      tmpJob = (MyJobInfo) it.next();
      for(int i=0; i<csNames.length; ++i){
        if(csNames[i].equalsIgnoreCase(tmpJob.getCSName())){
          Debug.debug("Upping job count for CS "+csNames[i], 3);
          ++rJobsByCS[i];
        }
      }
    }
    boolean ret = submittingJobs.size()<maxSimultaneousSubmissions &&
      submittingJobs.size()+runningJobs<totalMaxRunning &&
      rJobsByCS[jobCsIndex]<maxRunningPerCS[jobCsIndex];
    if(!ret){
      Debug.debug("Found running jobs: "+MyUtil.arrayToString(csNames)+" --> "+MyUtil.arrayToString(rJobsByCS)+
          " --> "+MyUtil.arrayToString(maxRunningPerCS), 3);
    }
    return ret;
  }

  /**
   * Submits the specified job. <p>
   * Calls the plugin submission method (via PluginMgr). <br>
   * This method is started in a thread. <p>
   */
  private void submit(final MyJobInfo job){
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    job.setName(dbPluginMgr.getJobDefName(job.getIdentifier()));
    statusTable.setValueAt(job.getCSName(), job.getTableRow(),
        JobMgr.FIELD_CS);
    //statusTable.setValueAt(job.getName(), job.getTableRow(),
    //                       JobMgr);
    Debug.debug("Submitting : " + job.getName()+" : "+statusTable.getRowCount()+
        " : "+job.getTableRow()+" : "+iconSubmitting, 3);
    statusTable.setValueAt(iconSubmitting, job.getTableRow(),
        JobMgr.FIELD_CONTROL);
    JobMgr jobMgr = GridPilot.getClassMgr().getJobMgr(job.getDBName());
    Vector updV = new Vector();
    updV.add(job);
    jobMgr.updateDBCells(updV);
    if(csPluginMgr.preProcess(job) && csPluginMgr.submit(job)){
      Debug.debug("Job " + job.getName() + " submitted : \n" +
                  "\tCSJobId = " + job.getJobId() + "\n" +
                  "\tStdOut = " + job.getOutTmp() + "\n" +
                  "\tStdErr = " + job.getErrTmp(), 2);
      String jobUser = csPluginMgr.getUserInfo(job.getCSName());
      Debug.debug("Setting job user :"+jobUser+":", 3);
      job.setUserInfo(jobUser);
      if(!dbPluginMgr.updateJobDefinition(
              job.getIdentifier(),
              new String []{jobUser, job.getJobId(), job.getName(),
              job.getOutTmp(), job.getErrTmp()})){
        logFile.addMessage("DB update(" + job.getIdentifier() + ", " +
                           job.getJobId() + ", " + job.getName() + ", " +
                           job.getOutTmp() + ", " + job.getErrTmp() +
                           ") failed", job);
      }
      statusTable.setValueAt(job.getJobId(), job.getTableRow(),
          JobMgr.FIELD_JOBID);
      statusTable.setValueAt(job.getUserInfo(), job.getTableRow(),
          JobMgr.FIELD_USER);
      statusTable.updateSelection();
      job.setNeedsUpdate(true);
      job.setCSStatus(MyJobInfo.CS_STATUS_WAIT);
    }
    else{
      statusTable.setValueAt("Not submitted!", job.getTableRow(),
          JobMgr.FIELD_JOBID);
      job.setCSStatus(MyJobInfo.CS_STATUS_FAILED);
      job.setNeedsUpdate(false);
      if(jobMgr.updateDBStatus(job, DBPluginMgr.FAILED)){
        job.setDBStatus(DBPluginMgr.FAILED);
      }
      else{
        logFile.addMessage("DB update status(" + job.getIdentifier() + ", " +
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
    statusBar.incrementProgressBarValue(pbSubmission, 1);
    statusBar.cleanupProgressBar(pbSubmission);
    if(statusBar.cleanupProgressBar(pbSubmission)==1){
      isProgressBarSet = false;
      monitorStatusBar.setLabel("Submission done.");
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
    MyJobInfo job = null;
    JobMgr jobMgr = null;
    while(e.hasMoreElements()){
      job = (MyJobInfo) e.nextElement();
      statusTable.setValueAt("Not submitted (cancelled)!", job.getTableRow(), JobMgr.FIELD_JOBID);
      statusTable.setValueAt(job.getName(), job.getTableRow(), JobMgr.FIELD_JOBNAME);
      statusTable.setValueAt(null, job.getTableRow(), JobMgr.FIELD_CONTROL);
      job.setCSStatus(MyJobInfo.CS_STATUS_FAILED);
      job.setNeedsUpdate(false);
      jobMgr = GridPilot.getClassMgr().getJobMgr(job.getDBName());
      if(jobMgr.updateDBStatus(job, DBPluginMgr.FAILED)){
        job.setDBStatus(DBPluginMgr.FAILED);
      }
      else{
        logFile.addMessage("DB update status(" + job.getIdentifier() + ", " +
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