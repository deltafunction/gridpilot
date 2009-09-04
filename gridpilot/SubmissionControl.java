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
 * Each <code>timeBetweenSubmissions</code>, a Timer checks if there is some jobs in
 * the queue (the timer is stopped when the queue is empty, and restarted when
 * new jobs arrive).
 */
public class SubmissionControl{
  private StatusBar monitorStatusBar;
  private JProgressBar pbSubmission;
  private boolean isProgressBarSet = false;
  private ConfigFile configFile;
  private MyLogFile logFile;
  private MyJTable statusTable;
  private CSPluginMgr csPluginMgr;
  private Timer preprocessTimer;
  private Timer submitTimer;
  private ImageIcon iconSubmitting;
  private ImageIcon iconProcessing;
  private ImageIcon iconWaiting;
  /** All jobs in the system. */
  private Vector<MyJobInfo> monitoredJobs;
  /** All jobs for which the submission is not done yet. */
  private Vector<MyJobInfo> toPreprocessJobs = new Vector<MyJobInfo>();
  /** All jobs for which the submission is in progress. */
  private Vector<MyJobInfo> preprocessingJobs = new Vector<MyJobInfo>();
  /** All jobs for which the submission is in progress. */
  private Vector<MyJobInfo> submittingJobs = new Vector<MyJobInfo>();
 /** Maximum number of simultaneous threads for submission. */
  private int maxSimultaneousSubmissions = 5;
  /** Total maximum total number of simultaneously running jobs. */
  private int totalMaxRunning = 10;
  /** Maximum total number of simultaneously running jobs for each CS. */
  private int [] maxRunningOnEachCS;
  /** Maximum total number of simultaneously running jobs per CS. */
  private HashMap<String, Integer> maxRunningPerHostOnEachCS;
  /** Maximum total number of simultaneously preprocessing jobs per CS. */
  private HashMap<String, Integer> maxPreprocessingPerHostOnEachCS;
  /** Total maximum total number of simultaneously running jobs. */
  private int totalMaxPreprocessing;
  /** Maximum total number of simultaneously running jobs per CS. */
  private int [] maxPreprocessingPerCS;
  /** Delay between the begin of two submission threads. */
  private int timeBetweenSubmissions = 10000;
  /** Number of times to try and find a host for a job. */
  private int PREPROCESS_RETRIES = 3;
  private HashMap<MyJobInfo, Integer> preprocessRetryJobs;
  private String isRand = null;
  private String [] csNames;
  /** Number of milliseconds to wait for each preprocessing thread. */
  private int PREPROCESS_TIMEOUT = 240000;
  /** Number of milliseconds to wait for each submit thread. */
  private int SUBMIT_TIMEOUT = 240000;
  private static final int CANNOT_PREPROCESS_OR_RUN = 0;
  private static final int CAN_PREPROCESS = 1;
  private static final int CAN_RUN = 2;

  public SubmissionControl() throws Exception{
    monitoredJobs = GridPilot.getClassMgr().getMonitoredJobs();
    statusTable = GridPilot.getClassMgr().getJobStatusTable();
    csPluginMgr = GridPilot.getClassMgr().getCSPluginMgr();
    
    monitorStatusBar = GridPilot.getClassMgr().getGlobalFrame().getMonitoringPanel().getStatusBar();
    configFile = GridPilot.getClassMgr().getConfigFile();
    logFile = GridPilot.getClassMgr().getLogFile();
    
    preprocessRetryJobs = new HashMap<MyJobInfo, Integer>();

    preprocessTimer = new Timer(0, new ActionListener(){
      public void actionPerformed(ActionEvent e){
        (new Thread(){
          public void run(){
            trigPreprocess();
          }
        }).start();
      }
    });
    
    submitTimer = new Timer(0, new ActionListener(){
      public void actionPerformed(ActionEvent e){
        (new Thread(){
          public void run(){
            trigSubmit();
          }
        }).start();
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
    maxRunningPerHostOnEachCS = new HashMap<String, Integer>();
    for(int i=0; i<csNames.length; ++i){
      try{
        tmp = configFile.getValue(csNames[i], "Max running jobs per host");
        maxRunningPerHostOnEachCS.put(csNames[i], Integer.parseInt(tmp));
      }
      catch(Exception e){
        maxRunningPerHostOnEachCS.put(csNames[i], 1);
      }
    }
    Debug.debug("maxRunningPerHostOnEachCS: "+maxRunningPerHostOnEachCS, 2);
    maxPreprocessingPerHostOnEachCS = new HashMap<String, Integer>();
    for(int i=0; i<csNames.length; ++i){
      try{
        tmp = configFile.getValue(csNames[i], "Max preprocessing jobs per host");
        maxPreprocessingPerHostOnEachCS.put(csNames[i], Integer.parseInt(tmp));
      }
      catch(Exception e){
        maxPreprocessingPerHostOnEachCS.put(csNames[i], 1);
      }
    }
    Debug.debug("maxPreprocessingPerHostOnEachCS: "+maxPreprocessingPerHostOnEachCS, 2);
    maxRunningOnEachCS = new int[csNames.length];
    for(int i=0; i<csNames.length; ++i){
      maxRunningOnEachCS[i] = MyUtil.getTotalMaxSimultaneousRunningJobs(csNames[i]);
      if(!MyUtil.checkCSEnabled(csNames[i])){
        continue;
      }
      totalMaxRunning += maxRunningOnEachCS[i];
    }
    maxPreprocessingPerCS = new int[csNames.length];
    totalMaxPreprocessing = 0;
    for(int i=0; i<csNames.length; ++i){
      maxPreprocessingPerCS[i] = MyUtil.getTotalMaxSimultaneousPreprocessingJobs(csNames[i]);
      totalMaxPreprocessing += maxPreprocessingPerCS[i];
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
    preprocessTimer.setInitialDelay(0);
    preprocessTimer.setDelay(timeBetweenSubmissions);
    submitTimer.setInitialDelay(0);
    submitTimer.setDelay(timeBetweenSubmissions);
    URL imgURL=null;
    try{
      imgURL = GridPilot.class.getResource(GridPilot.ICONS_PATH + "submitting.png");
      iconSubmitting = new ImageIcon(imgURL);
    }
    catch(Exception e){
      logFile.addMessage("Could not find image "+ GridPilot.ICONS_PATH + "submitting.png");
      iconSubmitting = new ImageIcon();
    }
    try{
      imgURL = GridPilot.class.getResource(GridPilot.ICONS_PATH + "waiting.png");
      iconWaiting = new ImageIcon(imgURL);
    }
    catch(Exception e){
      logFile.addMessage("Could not find image "+ GridPilot.ICONS_PATH + "waiting.png");
      iconWaiting = new ImageIcon();
    }
    try{
      imgURL = GridPilot.class.getResource(GridPilot.ICONS_PATH + "processing.png");
      iconProcessing = new ImageIcon(imgURL);
    }
    catch(Exception e){
      logFile.addMessage("Could not find image "+ GridPilot.ICONS_PATH + "processing.png");
      iconProcessing = new ImageIcon();
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
  public void submitJobDefinitions(/*vector of DBRecords*/Vector <DBRecord>selectedJobs,
      String csName, DBPluginMgr dbPluginMgr){
    GridPilot.getClassMgr().getGlobalFrame().showMonitoringPanel(MonitoringPanel.TAB_INDEX_JOBS);
    synchronized(monitoredJobs){
      Vector<MyJobInfo> newJobs = new Vector<MyJobInfo>();
      // This label is not shown because this function is not called in a Thread.
      // It seems to be quite dangerous to call this function in a thread, because
      // if one does it, you can "load job from db" during reservation (when jobs
      // are not yet put in toSubmitJobs).
      monitorStatusBar.setLabel("Reserving. Please wait...");
      //monitorStatusBar.animateProgressBar();
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
          for(Iterator<MyJobInfo> it=monitoredJobs.iterator(); it.hasNext();){
            job = it.next();
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
            job.setTableRow(monitoredJobs.size());
            monitoredJobs.add(job);
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
      monitorStatusBar.setLabel("Reserving done.");
      //monitorStatusBar.removeLabel();
      //monitorStatusBar.stopAnimation();
      if(!newJobs.isEmpty()){
        monitorStatusBar.setLabel("Monitoring. Please wait...");
        //monitorStatusBar.animateProgressBar();
        // new rows in table
        statusTable.createRows(monitoredJobs.size());
        //jobControl.initChanges();
        for(Iterator<JobMgr> it = GridPilot.getClassMgr().getJobMgrs().iterator(); it.hasNext();){
          it.next().initChanges();
        }
        // jobControl.updateJobsByStatus();
        monitorStatusBar.setLabel("Monitoring done.");
        //monitorStatusBar.stopAnimation();
        queue(newJobs);
      }
    }
  }

  /**
   * Submits the specified jobs on the given computing system. <p>
   */
  public void submitJobs(Vector<MyJobInfo> jobs, String csName){
    synchronized(monitoredJobs){
      Vector<MyJobInfo> newJobs = new Vector<MyJobInfo>();
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
      if(!newJobs.isEmpty()){
        // jobControl.updateJobsByStatus();
        queue(newJobs);
      }
    }
  }

  /**
   * Submits the given jobs, on the computing system given by job.getComputingSystem. <p>
   * These jobs are put in toSubmitJobs.
   */
  private void queue(Vector<MyJobInfo> jobs){
    if(jobs==null || jobs.size()==0){
      return;
    }
    if(isRand!=null && isRand.equalsIgnoreCase("yes")){
      jobs = MyUtil.shuffle(jobs);
    }
    MyJobInfo job;
    /*for(Iterator<MyJobInfo> it=jobs.iterator(); it.hasNext();){
      job = it.next();
      statusTable.setValueAt(iconWaiting, job.getTableRow(), JobMgr.FIELD_CONTROL);
    }*/
    pbSubmission.setMaximum(pbSubmission.getMaximum() + jobs.size());
    pbSubmission.addMouseListener(new MouseAdapter(){
      public void mouseClicked(MouseEvent e){
        cancelSubmission();
      }
    });
    monitorStatusBar.setLabel("Adding to submission queue. Please wait...");
    //monitorStatusBar.animateProgressBar();
    pbSubmission.setToolTipText("Click here to cancel submission");
    if(!isProgressBarSet){
      monitorStatusBar.setProgressBar(pbSubmission);
      isProgressBarSet = true;
    }
    job = jobs.get(0);
    JobMgr jobMgr = GridPilot.getClassMgr().getJobMgr(job.getDBName());
    jobMgr.updateDBCells(jobs);
    jobMgr.updateJobCells(jobs);
    toPreprocessJobs.addAll(jobs);
    if(!preprocessTimer.isRunning()){
      preprocessTimer.restart();
    }
    monitorStatusBar.setLabel("Adding done.");
    //monitorStatusBar.stopAnimation();
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
      }
      try{
        stdErrExists = job.getErrTmp() != null &&
           shell.existsFile(job.getErrTmp());
      }
      catch(Exception e){
        Debug.debug("ERROR checking for stderr: "+e.getMessage(), 2);
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
        statusTable.setValueAt(job.getHost()==null?"":job.getHost(), job.getTableRow(), JobMgr.FIELD_HOST);
        submitables.get(job.getDBName()).add(job);
      }
    }
    // if all went well we can now submit
    monitorStatusBar.setLabel("Queueing job(s)...");
    String dbName;
    for(Iterator<String> it=submitables.keySet().iterator(); it.hasNext();){
      dbName = it.next();
      jobMgr.updateJobCells(submitables.get(dbName));
      Debug.debug("Queueing "+submitables.get(dbName), 2);
      queue(submitables.get(dbName));
    }
    monitorStatusBar.setLabel("");
    if(!preprocessTimer.isRunning()){
      preprocessTimer.restart();
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
  * Checks if there are not too many active preprocessing threads and waiting jobs.
  * If there are any slots left, create new preprocessing thread.
  */
  private /*synchronized*/ void trigPreprocess(){
    if(toPreprocessJobs.isEmpty()){
      Debug.debug("No jobs in queue", 2);
      preprocessTimer.stop();
      return;
    }
    final MyJobInfo job = toPreprocessJobs.get(0);
    int runOk = -1;
    try{
      runOk = checkRunning(job);
    }
    catch(Exception e){
      e.printStackTrace();
    }
    if(runOk!=CAN_PREPROCESS){
      toPreprocessJobs.remove(job);
      toPreprocessJobs.add(job);
      return;
    }
    Debug.debug("Preprocess timer kicking on "+job.getName(), 3);
    final int runOk1 = runOk;
    // prepare job (download input files to worker node if possible)
    MyResThread t = new MyResThread(){
      public void run(){
        try{
          tryPreprocess(job, runOk1);
          Vector<MyJobInfo> updV = new Vector<MyJobInfo>();
          updV.add(job);
          JobMgr jobMgr = GridPilot.getClassMgr().getJobMgr(job.getDBName());
          jobMgr.updateDBCells(updV);
          for(Iterator<JobMgr> it = GridPilot.getClassMgr().getJobMgrs().iterator(); it.hasNext();){
            it.next().updateJobsByStatus();
          }
        }
        catch(Exception e){
          e.printStackTrace();
          setException(e);
        }
      }
    };
    t.start();
    if(!MyUtil.waitForThread(t, "Preprocess job "+job.getName(),
        PREPROCESS_TIMEOUT, "trigPreprocess", logFile) || t.getException()!=null){
      Debug.debug("Preprocessing failed. "+t.getException(), 2);
      failJob(job);
    }
  }
  
  /**
   * Checks if there are not too many active submission threads and waiting jobs.
   * If there are any slots left, create new submission thread.
   */
   private /*synchronized*/ void trigSubmit(){
     if(preprocessingJobs.isEmpty()){
       Debug.debug("No jobs in queue", 2);
       submitTimer.stop();
       return;
     }
     final MyJobInfo job = preprocessingJobs.get(0);
     int runOk = -1;
     try{
       runOk = checkRunning(job);
     }
     catch(Exception e){
       e.printStackTrace();
     }
     if(runOk!=CAN_RUN){
       preprocessingJobs.remove(job);
       preprocessingJobs.add(job);
       return;
     }
     Debug.debug("Submit timer kicking on "+job.getName(), 3);
     final int runOk1 = runOk;
     // prepare job (download input files to worker node if possible)
     MyResThread t = new MyResThread(){
       public void run(){
         try{
           trySubmit(job, runOk1);
           Vector<MyJobInfo> updV = new Vector<MyJobInfo>();
           updV.add(job);
           JobMgr jobMgr = GridPilot.getClassMgr().getJobMgr(job.getDBName());
           jobMgr.updateDBCells(updV);
           for(Iterator<JobMgr> it = GridPilot.getClassMgr().getJobMgrs().iterator(); it.hasNext();){
             it.next().updateJobsByStatus();
           }
         }
         catch(Exception e){
           setException(e);
         }
       }
     };
     t.start();
     if(!MyUtil.waitForThread(t, "Submit job "+job.getName(),
         SUBMIT_TIMEOUT, "trigSubmit", logFile) || t.getException()!=null){
       failJob(job);
     }
   }
   
   public Vector<MyJobInfo> getSubmittingJobs(){
     return submittingJobs;
   }
   
  /**
   * Check if this job can be preprocessed, run or we should wait.
   * If the job can be preprocessed, the best host is chosen among the already
   * booted ones and set with JobInfo.setHost().
   * @param job
   * @return 0 if nothing should be done,
   *         1 if the job can be prepared but not submitted,
   *         2 if the job can be submitted
   */
  private int checkRunning(MyJobInfo job){

    if(submittingJobs.size()>=maxSimultaneousSubmissions){
      Debug.debug("Cannot preprocess or run: "+
          submittingJobs.size()+">="+maxSimultaneousSubmissions, 3);
      return CANNOT_PREPROCESS_OR_RUN;
    }
    
    if(!checkDependenceOnOtherJobs(job)){
      Debug.debug("Cannot preprocess or run. Dependencies of job not met.", 3);
      return CANNOT_PREPROCESS_OR_RUN;
    }
    
    int runningJobs = 0;
    int runningJobsOnThisHost = 0;
    int preprocessedJobs = 0;
    JobMgr mgr = null;
    MyJobInfo tmpJob;
    int [] jobsByStatus = null;
    int [] rJobsByCS = new int[csNames.length];
    int [] pJobsByCS = new int[csNames.length];
    int [] submittedJobsByCS = new int[csNames.length];
    int [] preprocessedJobsByCS = new int[csNames.length];
    int jobCsIndex = -1;
    for(int i=0; i<csNames.length; ++i){
      pJobsByCS[i] = 0;
      rJobsByCS[i] = 0;
    }
    for(Iterator<JobMgr> it=GridPilot.getClassMgr().getJobMgrs().iterator(); it.hasNext();){
      
      mgr = it.next();
      mgr.updateJobsByStatus();
      
      preprocessedJobsByCS = mgr.getPreprocessedJobsByCS();
      for(int i=0; i<csNames.length; ++i){
        pJobsByCS[i] += preprocessedJobsByCS[i];
        Debug.debug("Upping preprocessed job count for CS "+csNames[i]+" with "+preprocessedJobsByCS[i], 3);
        if(csNames[i].equalsIgnoreCase(job.getCSName())){
          jobCsIndex = i;
        }
      }
      
      submittedJobsByCS = mgr.getSubmittedJobsByCS();
      for(int i=0; i<csNames.length; ++i){
        rJobsByCS[i] += submittedJobsByCS[i];
        Debug.debug("Upping submitted job count for CS "+csNames[i]+" with "+submittedJobsByCS[i], 3);
        if(csNames[i].equalsIgnoreCase(job.getCSName())){
          jobCsIndex = i;
        }
      }

      jobsByStatus = mgr.getJobsByStatus();
      runningJobs += (jobsByStatus[DBPluginMgr.STAT_STATUS_RUN]);
      // Jobs that have been preprocessed, but no yet submitted by the CS
      preprocessedJobs += (jobsByStatus[DBPluginMgr.STAT_STATUS_WAIT]);
    }
    
    for(Iterator<MyJobInfo> it=preprocessingJobs.iterator(); it.hasNext();){
      tmpJob = it.next();
      for(int i=0; i<csNames.length; ++i){
        if(csNames[i].equalsIgnoreCase(tmpJob.getCSName())){
          Debug.debug("Upping preprocessing job count for CS "+csNames[i], 3);
          ++pJobsByCS[i];
        }
      }
    }
    
    for(Iterator<MyJobInfo> it=submittingJobs.iterator(); it.hasNext();){
      tmpJob = it.next();
      for(int i=0; i<csNames.length; ++i){
        if(csNames[i].equalsIgnoreCase(tmpJob.getCSName())){
          if(job.getDBStatus()==DBPluginMgr.PREPARED &&
              tmpJob.getHost()!=null && tmpJob.getHost().equals(job.getHost())){
            Debug.debug("Race condition - backing out for now.", 2);
            return CANNOT_PREPROCESS_OR_RUN;
          }
          Debug.debug("Upping submitting job count for CS "+csNames[i], 3);
          ++rJobsByCS[i];
        }
      }
    }
    
    if(job.getHost()!=null &&
       maxRunningPerHostOnEachCS.get(job.getHost())!=null &&
       job.getDBStatus()==DBPluginMgr.PREPARED){
      for(Iterator<MyJobInfo> it=monitoredJobs.iterator(); it.hasNext();){
        tmpJob = it.next();
        // TODO: get this to work
        if((tmpJob.getStatus()==MyJobInfo.STATUS_RUNNING ||
            tmpJob.getDBStatus()==DBPluginMgr.SUBMITTED) &&
            tmpJob.getHost()!=null && tmpJob.getHost().equals(job.getHost())){
          Debug.debug("This host, "+job.getHost()+" is already running job "+tmpJob.getName(), 2);
          ++runningJobsOnThisHost;
        }
      }
      if(runningJobsOnThisHost>=maxRunningPerHostOnEachCS.get(job.getHost())){
        Debug.debug("Cannot run job "+job.getName()+" on host "+job.getHost(), 1);
        return CANNOT_PREPROCESS_OR_RUN;
      }
    }
    
    int ret = -1;
    if(jobCsIndex>=0 && pJobsByCS[jobCsIndex]>0 &&
        job.getDBStatus()==DBPluginMgr.PREPARED && 
        runningJobs<totalMaxRunning &&
        submittingJobs.size()+runningJobs<totalMaxRunning &&
        rJobsByCS[jobCsIndex]<maxRunningOnEachCS[jobCsIndex]){
      ret = CAN_RUN;
    }
    else if(job.getDBStatus()==DBPluginMgr.DEFINED &&
        preprocessingJobs.size()<totalMaxPreprocessing){
      // If the CS in question is one that has "max running jobs per host" set and
      // there's a running host with free slot(s), tag the job to use it already now.
      tagJobHost(job);
      ret = CAN_PREPROCESS;
    }
    
    Debug.debug("Found running jobs: "+
        runningJobsOnThisHost+"<"+maxRunningPerHostOnEachCS.get(job.getHost())+":"+
        submittingJobs.size()+"<"+maxSimultaneousSubmissions+":"+
        runningJobs+"<"+totalMaxRunning+":"+
        preprocessingJobs.size()+"<"+totalMaxPreprocessing+":"+
        job.getName()+":"+
        job.getHost()+":"+
        DBPluginMgr.getStatusName(job.getDBStatus())+":"+
        MyUtil.arrayToString(csNames)+
        " :: "+MyUtil.arrayToString(pJobsByCS)+
        " --> "+MyUtil.arrayToString(maxPreprocessingPerCS)+
        " :: "+MyUtil.arrayToString(rJobsByCS)+
        " --> "+MyUtil.arrayToString(maxRunningOnEachCS), 3);

    Debug.debug("Returning "+ret, 3);
    return ret;
  }

  private void tagJobHost(MyJobInfo job) {
    if(job.getHost()!=null && !job.getHost().equals("")){
      return;
    }
    HashMap<String, Integer> hostsWithPreprocessingJobs = new HashMap<String, Integer>();
    HashMap<String, Integer> hostsWithRunningJobs = new HashMap<String, Integer>();
    HashMap<String, Integer> hostsWithDoneJobs = new HashMap<String, Integer>();
    MyJobInfo tmpJob;
    int pJobs = 0;
    int rJobs = 0;
    Integer val;
    for(Iterator<MyJobInfo> it=monitoredJobs.iterator(); it.hasNext();){
      tmpJob = it.next();
      if(tmpJob.getCSName()!=null && tmpJob.getCSName().equals(job.getCSName()) &&
          tmpJob.getHost()!=null && !tmpJob.getHost().equals("")){
        if(tmpJob.getDBStatus()==DBPluginMgr.PREPARED ||
            submittingJobs.contains(tmpJob) || preprocessingJobs.contains(tmpJob)){
          if(!hostsWithPreprocessingJobs.containsKey(tmpJob.getHost())){
            hostsWithPreprocessingJobs.put(tmpJob.getHost(), 0);
          }
          val = hostsWithPreprocessingJobs.get(tmpJob.getHost());
          hostsWithPreprocessingJobs.put(tmpJob.getHost(), val+1);
        }
        if(tmpJob.getDBStatus()==DBPluginMgr.SUBMITTED){
          if(!hostsWithRunningJobs.containsKey(tmpJob.getHost())){
            hostsWithRunningJobs.put(tmpJob.getHost(), 0);
          }
          val = hostsWithRunningJobs.get(tmpJob.getHost());
          hostsWithRunningJobs.put(tmpJob.getHost(), val+1);
        }
        else if(tmpJob.getDBStatus()==DBPluginMgr.VALIDATED){
          if(!hostsWithRunningJobs.containsKey(tmpJob.getHost())){
            hostsWithDoneJobs.put(tmpJob.getHost(), 0);
          }
          val = hostsWithDoneJobs.get(tmpJob.getHost());
          hostsWithDoneJobs.put(tmpJob.getHost(), val+1);
        }
      }
    }
    Debug.debug("Found hosts with preprocessing jobs: "+hostsWithPreprocessingJobs, 3);
    Debug.debug("Found hosts with running jobs: "+hostsWithRunningJobs, 3);
    Debug.debug("Found hosts with done jobs: "+hostsWithDoneJobs, 3);
    HashMap<String, Integer> hostsWithJobs = new HashMap<String, Integer>();
    hostsWithJobs.putAll(hostsWithPreprocessingJobs);
    hostsWithJobs.putAll(hostsWithRunningJobs);
    hostsWithJobs.putAll(hostsWithDoneJobs);
    String host;
    int hostPJobs;
    int hostRJobs;
    for(Iterator<String> it=hostsWithJobs.keySet().iterator(); it.hasNext();){
      host = it.next();
      hostPJobs = hostsWithPreprocessingJobs.containsKey(host)?hostsWithPreprocessingJobs.get(host):0;
      hostRJobs = hostsWithRunningJobs.containsKey(host)?hostsWithRunningJobs.get(host):0;
      Debug.debug(host+"-->"+hostPJobs+"<"+maxPreprocessingPerHostOnEachCS.get(job.getCSName()), 3);
      Debug.debug(host+"-->"+hostRJobs+"<"+maxRunningPerHostOnEachCS.get(job.getCSName()), 3);
      if(/*hostPJobs<maxPreprocessingPerHostOnEachCS.get(job.getCSName())&&*/ 
          hostRJobs<maxRunningPerHostOnEachCS.get(job.getCSName())){
        if((hostRJobs<=rJobs || hostPJobs<=pJobs) &&
            hostRJobs+hostPJobs<=pJobs+rJobs){
          pJobs = hostRJobs;
          rJobs = hostPJobs;
          job.setHost(host);
        }
      }
    }
    if(job.getHost()!=null && !job.getHost().equals("")){
      statusTable.setValueAt(job.getHost()==null?"":job.getHost(), job.getTableRow(), JobMgr.FIELD_HOST);
    }
    Debug.debug("Set host of job "+job.getName()+" to "+job.getHost(), 1);
  }

  /**
   * Check if a job depends on the finishing of other jobs.
   * @param job
   * @return
   */
  private boolean checkDependenceOnOtherJobs(MyJobInfo job) {
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String [] depJobs;
    if(job.getDepJobs()!=null){
      depJobs = job.getDepJobs();
    }
    else{
      DBRecord jobDef = dbPluginMgr.getJobDefinition(job.getIdentifier());
      String depJobsStr = (String) jobDef.getValue("depJobs");
      if(depJobsStr!=null && !depJobsStr.trim().equals("")){
        depJobs = MyUtil.split(depJobsStr);
      }
      else{
        depJobs = new String [] {};
      }
    }
    String status;
    for(int i=0; i<depJobs.length; ++i){
      status = dbPluginMgr.getJobDefStatus(depJobs[i]);
      if(status==null || DBPluginMgr.getStatusId(status)!=DBPluginMgr.VALIDATED){
        Debug.debug("Job "+depJobs[i]+" has not finished. Cannot start "+job.getIdentifier(), 2);
        return false;
      }
    }
    return true;
  }

  /**
   * Preprocesses the specified job. <p>
   * Calls the plugin submission method (via PluginMgr). <br>
   * This method is started in a thread.
   */
  private void tryPreprocess(final MyJobInfo job, int runOk){
    
    int dbStatus = job.getDBStatus();
    
    if(job.getName()==null || job.getName().equals("")){
      DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
      job.setName(dbPluginMgr.getJobDefName(job.getIdentifier()));
      statusTable.setValueAt(job.getCSName(), job.getTableRow(), JobMgr.FIELD_CS);
      //statusTable.setValueAt(job.getName(), job.getTableRow(), JobMgr);
    }
    Debug.debug("Trying to preprocess : " + job.getName()+" : "+
        job.getIdentifier()+" : "+DBPluginMgr.getStatusName(dbStatus)+" : "+runOk+" : "+
        statusTable.getRowCount()+" : "+job.getTableRow(), 2);
    
    boolean ok = false;
    
    if(toPreprocessJobs.contains(job) && !preprocessingJobs.contains(job) &&
        (dbStatus==DBPluginMgr.DEFINED || dbStatus==DBPluginMgr.ABORTED) && runOk==CAN_PREPROCESS){
      // transfer job from toPreprocessJobs to preprocessingJobs
      Debug.debug("Will preprocess "+job.getName(), 2);
      toPreprocessJobs.remove(job);
      preprocessingJobs.add(job);
      statusTable.setValueAt(iconProcessing, job.getTableRow(), JobMgr.FIELD_CONTROL);
      ok = csPluginMgr.preProcess(job);
      if(ok){
        dbStatus = DBPluginMgr.PREPARED;
        if(!submitTimer.isRunning()){
          submitTimer.restart();
        }
        preprocessRetryJobs.remove(job);
        preprocessingDone(job, dbStatus);
      }
      else if(checkPreprocessTimeout(job)){
        dbStatus = DBPluginMgr.DEFINED;
        Debug.debug("Job "+job.getName()+" cannot be preprocessed right now, leaving in queue.", 1);
        preprocessingJobs.remove(job);
        toPreprocessJobs.add(job);
        statusTable.setValueAt(null, job.getTableRow(), JobMgr.FIELD_CONTROL);
        JobMgr jobMgr = GridPilot.getClassMgr().getJobMgr(job.getDBName());
        jobMgr.updateDBCell(job);
        jobMgr.updateJobsByStatus();
        if(!preprocessTimer.isRunning()){
          preprocessTimer.restart();
        }
      }
      else{
        logFile.addMessage("Job "+job.getName()+" cannot be preprocessed." +
        		" Cannot find or provision worker node.");
        dbStatus = DBPluginMgr.FAILED;
        preprocessingDone(job, dbStatus);
        failJob(job);
      }
    }
    
    job.setDBStatus(dbStatus);

  }

  private void preprocessingDone(MyJobInfo job, int dbStatus) {
    Debug.debug("Preprocessing of "+job.getIdentifier()+" done.", 3);
    incrementProgressBar(dbStatus);
    statusTable.setValueAt(job.getHost()==null?"":job.getHost(), job.getTableRow(), JobMgr.FIELD_HOST);
    statusTable.setValueAt(iconWaiting, job.getTableRow(), JobMgr.FIELD_CONTROL);
  }

  private void incrementProgressBar(int dbStatus) {
    if(dbStatus==DBPluginMgr.FAILED ||
        dbStatus==DBPluginMgr.UNDECIDED || dbStatus==DBPluginMgr.UNEXPECTED){
      monitorStatusBar.incrementProgressBarValue(pbSubmission, 1);
      if(monitorStatusBar.cleanupProgressBar(pbSubmission)==1){
        isProgressBarSet = false;
        monitorStatusBar.setLabel("Submission done.");
      }
    }
  }

  /**
   * Return true if this job can be preprocessed, false otherwise.
   * @param job
   * @return
   */
  private boolean checkPreprocessTimeout(MyJobInfo job) {
    Integer retr = preprocessRetryJobs.get(job);
    if(retr==null || retr==-1){
      preprocessRetryJobs.put(job, 0);
    }
    else{
      retr = preprocessRetryJobs.get(job);
    }
    retr = preprocessRetryJobs.get(job);
    ++retr;
    Debug.debug("Checking if we can still preprocess this job, "+
        job.getName()+":"+retr+">"+PREPROCESS_RETRIES, 2);
    if(retr>PREPROCESS_RETRIES){
      preprocessRetryJobs.remove(job);
      return false;
    }
    preprocessRetryJobs.put(job, retr);
    return true;
  }

  /**
   * Submits the specified job. <p>
   * Calls the plugin submission method (via PluginMgr). <br>
   * This method is started in a thread. <p>
   */
  private void trySubmit(final MyJobInfo job, int runOk){
    
    int dbStatus = job.getDBStatus();
    
    //statusTable.setValueAt(job.getName(), job.getTableRow(), JobMgr);
    Debug.debug("Trying to submit : " + job.getName()+" : "+
        job.getIdentifier()+" : "+DBPluginMgr.getStatusName(dbStatus)+" : "+runOk+" : "+
        statusTable.getRowCount()+" : "+job.getTableRow(), 2);
    
    boolean ok = false;
    int submitRes = -1;
    
    if(preprocessingJobs.contains(job) && !submittingJobs.contains(job) &&
        dbStatus==DBPluginMgr.PREPARED && runOk==CAN_RUN){
      Debug.debug("Will run "+job.getName(), 2);
      // transfer job from preprocessingJobs to submittingJobs
      preprocessingJobs.remove(job);
      submittingJobs.add(job);
      ok = true;
      statusTable.setValueAt(iconSubmitting, job.getTableRow(), JobMgr.FIELD_CONTROL);
      submitRes = csPluginMgr.run(job);
    }

    if(ok && submitRes==MyComputingSystem.RUN_WAIT){
      Debug.debug("Waiting on CS for "+job.getIdentifier(), 2);
      job.setCSStatus(MyJobInfo.CS_STATUS_WAIT);
      statusTable.setValueAt(iconWaiting, job.getTableRow(), JobMgr.FIELD_CONTROL);
      dbStatus = DBPluginMgr.PREPARED;
      job.setDBStatus(dbStatus);
      preprocessingJobs.add(job);
      JobMgr jobMgr = GridPilot.getClassMgr().getJobMgr(job.getDBName());
      jobMgr.updateDBCell(job);
      jobMgr.updateJobsByStatus();
      statusTable.setValueAt(iconWaiting, job.getTableRow(), JobMgr.FIELD_CONTROL);
    }
    if(ok && submitRes==MyComputingSystem.RUN_OK){
      dbStatus = DBPluginMgr.SUBMITTED;
      job.setDBStatus(dbStatus);
      Debug.debug("Job " + job.getName() + " submitted : \n" +
                  "\tCSJobId = " + job.getJobId() + "\n" +
                  "\tStdOut = " + job.getOutTmp() + "\n" +
                  "\tStdErr = " + job.getErrTmp(), 2);
      String jobUser = csPluginMgr.getUserInfo(job.getCSName());
      Debug.debug("Setting job user :"+jobUser+":", 3);
      job.setUserInfo(jobUser);
      DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
      if(!dbPluginMgr.updateJobDefinition(
              job.getIdentifier(),
              new String []{jobUser, job.getJobId(), job.getName(),
              job.getOutTmp(), job.getErrTmp()}) ||
         !dbPluginMgr.updateJobDefinition(
              job.getIdentifier(),
              new String []{"status"},
              new String []{DBPluginMgr.getStatusName(DBPluginMgr.SUBMITTED)})){
        logFile.addMessage("DB update(" + job.getIdentifier() + ", " +
                           job.getJobId() + ", " + job.getName() + ", " +
                           job.getOutTmp() + ", " + job.getErrTmp() +
                           ") failed", job);
      }
      statusTable.setValueAt(job.getJobId(), job.getTableRow(), JobMgr.FIELD_JOBID);
      statusTable.setValueAt(job.getUserInfo(), job.getTableRow(), JobMgr.FIELD_USER);
      statusTable.updateSelection();
      statusTable.setValueAt(null, job.getTableRow(), JobMgr.FIELD_CONTROL);
      job.setNeedsUpdate(true);
      job.setCSStatus(MyJobInfo.CS_STATUS_WAIT);
    }
    else if(submitRes==MyComputingSystem.RUN_FAILED){
      failJob(job);
    }
    
    submittingJobs.remove(job);
    incrementProgressBar(dbStatus);

  }

  private void failJob(MyJobInfo job) {
    toPreprocessJobs.remove(job);
    preprocessingJobs.remove(job);
    submittingJobs.remove(job);
    preprocessRetryJobs.remove(job);
    Debug.debug("Setting job "+job.getName()+" to Failed.", 2);
    statusTable.setValueAt("Not submitted!", job.getTableRow(), JobMgr.FIELD_JOBID);
    // remove iconSubmitting
    statusTable.setValueAt(null, job.getTableRow(), JobMgr.FIELD_CONTROL);
    JobMgr jobMgr = GridPilot.getClassMgr().getJobMgr(job.getDBName());
    job.setStatusFailed();
    if(jobMgr.updateDBStatus(job, DBPluginMgr.FAILED)){
      job.setDBStatus(DBPluginMgr.FAILED);
    }
    else{
      logFile.addMessage("DB update status(" + job.getIdentifier() + ", " +
          DBPluginMgr.getStatusName(job.getDBStatus()) + ") failed", job);
    }
  }

  public boolean isSubmitting(){
    //return timer.isRunning();
    return !submittingJobs.isEmpty() || !toPreprocessJobs.isEmpty();
  }

  /**
   * Stops the submission. <br>
   * Empties toSubmitJobs, and set these jobs to Failed.
   */
  private void cancelSubmission(){
    submitTimer.stop();
    Vector<MyJobInfo> toCancelJobs = new Vector<MyJobInfo>();
    toCancelJobs.addAll(toPreprocessJobs);
    toCancelJobs.addAll(preprocessingJobs);
    if(toCancelJobs.isEmpty()){
      return;
    }
    Enumeration<MyJobInfo> e = toCancelJobs.elements();
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
      jobMgr.updateDBCells(toCancelJobs);
    }
    toPreprocessJobs.removeAllElements();
    preprocessingJobs.removeAllElements();
    monitorStatusBar.removeProgressBar(pbSubmission);
    pbSubmission.setMaximum(0);
    pbSubmission.setValue(0);
  }
}