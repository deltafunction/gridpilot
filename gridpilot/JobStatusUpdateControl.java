package gridpilot;

import gridfactory.common.ConfigFile;
import gridfactory.common.Debug;

import java.net.URL;
import java.util.Iterator;
import java.util.Vector;
import java.util.Enumeration;
import javax.swing.*;
import java.util.HashMap;

import java.awt.event.*;

/**
 * This class manages the jobs status update. <p>
 * When JobMgr requests for job updates, these jobs are appended in the queue
 * <code>toCheckJobs</code> (if they need to be refreshed, and they are not already
 * in this queue).
 * A timer (<code>timerChecking</code>, running if <code>toCheckJob</code> is not empty),
 * calls <code>trigCheck()</code> every <code>timeBetweenCheking</code> ms.
 * This method checks first if a new checking is allowed to start (not too many current checking),
 * and looks then at the first jobs of <code>toCheckJobs</code> ; if this job plug-in
 * can update more than one job at the same time, the maximum nubmer of job for this system
 * is taken in <code>toCheckJobs</code>.
 *
 * <p><a href="JobStatusUpdateControl.java.html">see sources</a>
 */
public class JobStatusUpdateControl{
  /** The timer which triggers the checking */
  private Timer timerChecking;

  private ConfigFile configFile;
  private MyLogFile logFile;

  /** Maximun number of simultaneous thread for checking */
  private static int maxSimultaneousChecking = 3;
  /** Delay of <code>timerChecking</code> */
  private static int timeBetweenCheking = 1000;

  /** For each plug-in, maximum number of job for one update (0 = INF)*/
  private HashMap maxJobsByUpdate;

  private MyJTable statusTable;

  /**
   * Contains all jobs which should be updated. <p>
   * All these jobs should be "needed to be refreshed" (otherwise, they are not
   * put in this job vector, and a job cannot become "not needed to be refreshed"
   * when it is in this job vector) and should belong to submittedJobs. <br>
   * Except if the method {@link #reset()} is called, each job in toCheckJobs is
   * going to be put in {@link #checkingJobs}
   */
  private Vector toCheckJobs = new Vector();

  /**
   * Contains all jobs for which update is processing. <p>
   * Each job in this job vector : <ul>
   * <li> corresponds to one and only one thread in {@link #checkingThread}
   *      (but a thread in checkingThread could correspond to several jobs in checkingJobs),
   * <li> should be "needed to be refreshed" (see {@link #toCheckJobs})and has
   *      belonged to {@link #toCheckJobs}, but doesn't belong to it anymore,
   */
  private Vector checkingJobs = new Vector(); //

  /**
   * Thread vector. <p>
   * @see #checkingJobs
   */
  public Vector checkingThread = new Vector();

  private ImageIcon iconChecking;

  private Vector jobMgrs;

  public JobStatusUpdateControl() throws Exception{
    statusTable = GridPilot.getClassMgr().getJobStatusTable();
    configFile = GridPilot.getClassMgr().getConfigFile();
    logFile = GridPilot.getClassMgr().getLogFile();

    timerChecking = new Timer(0, new ActionListener(){
      public void actionPerformed(ActionEvent ae){
        //Debug.debug(checkingThread.size()+":"+maxSimultaneousChecking +":"+
            //!toCheckJobs.isEmpty(), 3);
        if(checkingThread.size()<maxSimultaneousChecking && !toCheckJobs.isEmpty()){
          Thread t = new Thread(){
            public void run(){
              Debug.debug("Checking...", 3);
              trigCheck();
              checkingThread.remove(this);
          }};
          checkingThread.add(t);
          t.start();
        }
      }
    });

    maxJobsByUpdate = new HashMap();

    loadValues();

    URL imgURL=null;
    try{
      imgURL = GridPilot.class.getResource(GridPilot.RESOURCES_PATH + "checking.png");
      iconChecking = new ImageIcon(imgURL);
    }
    catch(Exception e){
      logFile.addMessage("Could not find image "+ GridPilot.RESOURCES_PATH + "checking.png");
      iconChecking = new ImageIcon();
    }
    Debug.debug("iconChecking: "+imgURL, 3);
  }

  /**
   * Reads values in config files. <p>
   * Theses values are :  <ul>
   * <li>maxSimultaneousChecking
   * <li>timeBetweenCheking
   * <li>delayBeforeValidation
   * <li>for each plug-in : maxJobsByUpdate
   * </ul> <p>
   */
  public void loadValues(){

    /**
     * Load maxSimultaneousChecking
     */
    String tmp = configFile.getValue(GridPilot.TOP_CONFIG_SECTION, "max simultaneous checking");
    if(tmp!=null){
      try{
        maxSimultaneousChecking = Integer.parseInt(tmp);
      }
      catch(NumberFormatException nfe){
        logFile.addMessage("Value of \"max simultaneous checking\" "+
                           "is not an integer in configuration file", nfe);
      }
    }
    else
      logFile.addMessage(configFile.getMissingMessage(GridPilot.TOP_CONFIG_SECTION, "max simultaneous checking") + "\n" +
                         "Default value = " + maxSimultaneousChecking);
    /**
     * Load timeBetweenCheking
     */
    tmp = configFile.getValue(GridPilot.TOP_CONFIG_SECTION, "time between checks");
    if(tmp!=null){
      try{
        timeBetweenCheking = Integer.parseInt(tmp);
      }
      catch(NumberFormatException nfe){
        logFile.addMessage("Value of \"time between checks\" "+
                           "is not an integer in configuration file", nfe);
      }
    }
    else
      logFile.addMessage(configFile.getMissingMessage(GridPilot.TOP_CONFIG_SECTION, "time between checks") + "\n" +
                         "Default value = " + timeBetweenCheking);

    timerChecking.setDelay(timeBetweenCheking);

    /**
     * Load of maxJobsByUpdate
     */
    String enabled = "no";
    for(int i=0; i<GridPilot.CS_NAMES.length; ++i){
      try{
        enabled = GridPilot.getClassMgr().getConfigFile().getValue(GridPilot.CS_NAMES[i], "Enabled");
      }
      catch(Exception e){
        continue;
      }
      if(enabled==null || !enabled.equalsIgnoreCase("yes") &&
          !enabled.equalsIgnoreCase("true")){
        continue;
      }
      tmp = configFile.getValue(GridPilot.CS_NAMES[i], "max jobs by update");
      if(tmp!=null){
        try{
          maxJobsByUpdate.put(GridPilot.CS_NAMES[i], new Integer(Integer.parseInt(tmp)));
        }
        catch(NumberFormatException nfe){
          logFile.addMessage("Value of \"max jobs by update\" in section " + GridPilot.CS_NAMES[i] +
                             " is not an integer in configuration file", nfe);
          maxJobsByUpdate.put(GridPilot.CS_NAMES[i], new Integer(1));
        }
      }
      else{
        maxJobsByUpdate.put(GridPilot.CS_NAMES[i], new Integer(1));
        logFile.addMessage(configFile.getMissingMessage(GridPilot.CS_NAMES[i], "max jobs by update") + "\n" +
                           "Default value = " + maxJobsByUpdate.get(GridPilot.CS_NAMES[i]));
      }
    }
  }

  public void exit(){
  }

  /**
   * Updates the status for all selected jobs. <p>
   * Add each job from <code>jobs</code> in <code>toCheckJobs</code> if it needs
   * to be refreshed, and is not in <code>toCheckJobs</code> or <code>checkingJobs</code>. <p>
   * If timer <code>timerChecking</code> was not running, restarts it. <p>
   *
   */
  public void updateStatus(int [] _rows){
    Debug.debug("updateStatus", 1);
    
    // get job vector
    int [] rows = _rows;
    Vector jobs = null;
    if(rows==null || rows.length==0){
      // if nothing is selected, we refresh all jobs
      jobs = GridPilot.getClassMgr().getSubmittedJobs();
    }
    else{
      //rows = statusTable.getSelectedRows();
      jobs = JobMgr.getJobsAtRows(rows);
    }
   
    // fill toCheckJobs with running jobs
    synchronized(toCheckJobs){
      Enumeration e = jobs.elements();
      while(e.hasMoreElements()){
        MyJobInfo job = (MyJobInfo) e.nextElement();
        Debug.debug("Checking job: "+job.getName()+" "+
            job.getNeedsUpdate() +" "+ !toCheckJobs.contains(job) +" "+
            !checkingJobs.contains(job), 3);
        if(job.getNeedsUpdate() && !toCheckJobs.contains(job) &&
            !checkingJobs.contains(job)){
          Debug.debug("Adding job to toCheckJobs", 3);
          toCheckJobs.add(job);
        }
      }
      Debug.debug("Finished adding job to toCheckJobs", 3);
    }
    if(!timerChecking.isRunning()){
      Debug.debug("WARNING: timer not running, restarting...", 3);
      timerChecking.restart();
    }
  }

  /**
   * Called by <code>timerChecking</code> time outs. <p>
   * This method is invocated in a thread. <br>
   * Takes the maximum number of jobs in <code>toCheckJobs</code>, saves their status
   * and calls the computing system plugin with these jobs. <br>
   * If a status has changed, an action is performed: <ul>
   * <li>STATUS_ERRROR : nothing
   * <li>STATUS_WAIT : nothing
   * <li>STATUS_DONE : call jobValidation.validate
   * <li>STATUS_RUN : update status and host in status table
   * <li>STATUS_FAILED : call jobMgr.jobFailure
   * </ul>
   *
   */
  private void trigCheck() {

    Debug.debug("trigCheck", 1);

    Vector jobs = new Vector();
    synchronized(toCheckJobs){
      if(toCheckJobs.isEmpty()){
        return;
      }
      String csName = ((MyJobInfo) toCheckJobs.get(0)).getCSName();

      int currentJob = 0;
      while((jobs.size()<((Integer) maxJobsByUpdate.get(csName)).intValue() ||
          ((Integer) maxJobsByUpdate.get(csName)).intValue()==0 )
            && currentJob<toCheckJobs.size()){
        Debug.debug("Adding job to toCheckJobs "+currentJob, 3);
        if(((MyJobInfo) toCheckJobs.get(
            currentJob)).getCSName().toString().equalsIgnoreCase(csName)){
          jobs.add(toCheckJobs.remove(currentJob));
        }
        else{
          ++currentJob;
        }
      }
    }
    checkingJobs.addAll(jobs);

    int [] previousStatus = new int [jobs.size()];
    for(int i=0; i<jobs.size(); ++i){
      Debug.debug("Setting value for job "+i, 3);
      statusTable.setValueAt(iconChecking, ((MyJobInfo) jobs.get(i)).getTableRow(), JobMgr.FIELD_CONTROL);
      previousStatus[i] = ((MyJobInfo) jobs.get(i)).getStatus();
    }
    
    Debug.debug("Updating status of "+jobs.size()+" jobs", 3);

    GridPilot.getClassMgr().getCSPluginMgr().updateStatus(jobs);

    Debug.debug("Removing "+jobs.size()+" jobs from checkingJobs", 3);
    
    checkingJobs.removeAll(jobs);

    for(int i=0; i<jobs.size(); ++i){
      Debug.debug("Setting value of job #"+i, 3);
      statusTable.setValueAt(null, ((MyJobInfo) jobs.get(i)).getTableRow(), JobMgr.FIELD_CONTROL);
      statusTable.setValueAt(((MyJobInfo) jobs.get(i)).getCSStatus(),
          ((MyJobInfo) jobs.get(i)).getTableRow(), JobMgr.FIELD_STATUS);
    }
  
    for(int i=0; i<jobs.size(); ++i){
      MyJobInfo job = (MyJobInfo) jobs.get(i);
      Debug.debug("Setting computing system status of job #"+i+"; "+
          job.getStatus()+"<->"+previousStatus[i], 3);
      if(job.getStatus()!=previousStatus[i]){       
        switch(job.getStatus()){
        case MyJobInfo.STATUS_READY :
          break;
        case MyJobInfo.STATUS_DONE:
          job.setNeedsUpdate(false);
          GridPilot.getClassMgr().getJobValidation().validate(job);
          break;
        // This is only used by ForkComputingSystem from GridFactory (i.e. by VMForkComputingSystem)
        case MyJobInfo.STATUS_EXECUTED:
          job.setNeedsUpdate(false);
          GridPilot.getClassMgr().getJobValidation().validate(job);
          break;
        case MyJobInfo.STATUS_RUNNING:
          statusTable.setValueAt(job.getHost(), job.getTableRow(), JobMgr.FIELD_HOST);
          break;
        case MyJobInfo.STATUS_ERROR:
          // Without the line below: leave as refreshable, in case the error is intermittent.
          // With the line below: avoid checking over and over again.
          //                      To recheck, clear and add again to monitoring panel.
          //job.setNeedsUpdate(false);
          if(job.getDBStatus()==DBPluginMgr.UNDECIDED || job.getDBStatus()==DBPluginMgr.UNEXPECTED){
            job.setNeedsUpdate(false);
          }
          break;
        case MyJobInfo.STATUS_FAILED:
          job.setNeedsUpdate(false);
          jobMgrs = GridPilot.getClassMgr().getJobMgrs();
          JobMgr mgr = null;
          for(Iterator it = jobMgrs.iterator(); it.hasNext();){
            mgr = ((JobMgr) it.next());
            if(mgr.dbName.equals(job.getDBName())){
              mgr.jobFailure(job);
              break;
            }
          }
          break;
        }
      }
    }
    
    Debug.debug("Updating "+jobs.size()+" jobs by status", 3);
    jobMgrs = GridPilot.getClassMgr().getJobMgrs();
    for(Iterator it = jobMgrs.iterator(); it.hasNext();){
      ((JobMgr) it.next()).updateJobsByStatus();
      // fix
      break;
    }
    
    Debug.debug("Finished trigCheck", 3);

    if(!timerChecking.isRunning()){
      timerChecking.restart();
    }
  }

  /**
   * Stops checking. <p>
   * Removes all jobs from queue, and waits for the end of the pending threads. <p>
   * Called by : <ul>
   */
  public void reset(){
    toCheckJobs.removeAllElements();

    for(int i=0; i<checkingThread.size(); ++i){
      Thread t = (Thread) checkingThread.get(i);
      Debug.debug("wait for thread " + i + "...", 2);
      try{
        t.join();
      }
      catch(InterruptedException ie){
        ie.printStackTrace();
      }
      Debug.debug("joined", 2);
    }
    // When selecting "Stop checking",
    // if a large number of threads were running, one could not refresh anymore.
    checkingThread.removeAllElements();
  }
}