package gridpilot;

import java.net.URL;
import java.util.Iterator;
import java.util.Vector;
import java.util.Enumeration;
import javax.swing.*;
import java.util.HashMap;

import java.awt.event.*;

/**
 * This class manages the jobs status update. <p>
 * When DatasetMgr requests for job updates, these jobs are appended in the queue
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
  private LogFile logFile;

  /** Maximun number of simultaneous thread for checking */
  private static int maxSimultaneousChecking = 3;
  /** Delay of <code>timerChecking</code> */
  private static int timeBetweenCheking = 1000;

  /** For each plug-in, maximum number of job for one update (0 = INF)*/
  private HashMap maxJobsByUpdate;

  private Table statusTable;

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

  private Vector datasetMgrs;

  public JobStatusUpdateControl(){
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
      imgURL = GridPilot.class.getResource(GridPilot.resourcesPath + "checking.png");
      iconChecking = new ImageIcon(imgURL);
    }
    catch(Exception e){
      Debug.debug("Could not find image "+ GridPilot.resourcesPath + "checking.png", 3);
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
    String tmp = configFile.getValue("GridPilot", "maximum simultaneous checking");
    if(tmp!=null){
      try{
        maxSimultaneousChecking = Integer.parseInt(tmp);
      }
      catch(NumberFormatException nfe){
        logFile.addMessage("Value of \"maximum simultaneous checking\" "+
                           "is not an integer in configuration file", nfe);
      }
    }
    else
      logFile.addMessage(configFile.getMissingMessage("GridPilot", "maximum simultaneous checking") + "\n" +
                         "Default value = " + maxSimultaneousChecking);
    /**
     * Load timeBetweenCheking
     */
    tmp = configFile.getValue("GridPilot", "time between checks");
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
      logFile.addMessage(configFile.getMissingMessage("GridPilot", "time between checks") + "\n" +
                         "Default value = " + timeBetweenCheking);

    timerChecking.setDelay(timeBetweenCheking);

    /**
     * Load of maxJobsByUpdate
     */
    for(int i=0; i<GridPilot.csNames.length; ++i){
      tmp = configFile.getValue(GridPilot.csNames[i], "max jobs by update");
      if(tmp!=null){
        try{
          maxJobsByUpdate.put(GridPilot.csNames[i], new Integer(Integer.parseInt(tmp)));
        }
        catch(NumberFormatException nfe){
          logFile.addMessage("Value of \"max jobs by update\" in section " + GridPilot.csNames[i] +
                             " is not an integer in configuration file", nfe);
          maxJobsByUpdate.put(GridPilot.csNames[i], new Integer(1));
        }
      }
      else{
        maxJobsByUpdate.put(GridPilot.csNames[i], new Integer(1));
        logFile.addMessage(configFile.getMissingMessage(GridPilot.csNames[i], "max jobs by update") + "\n" +
                           "Default value = " + maxJobsByUpdate.get(GridPilot.csNames[i]));
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
      rows = statusTable.getSelectedRows();
      jobs = DatasetMgr.getJobsAtRows(rows);
    }
   
    // fill toCheckJobs with running jobs
    synchronized(toCheckJobs){
      Enumeration e = jobs.elements();
      while(e.hasMoreElements()){
        JobInfo job = (JobInfo) e.nextElement();
        Debug.debug("Checking job: "+job.getName()+" "+
            job.needToBeRefreshed() +" "+ !toCheckJobs.contains(job) +" "+
            !checkingJobs.contains(job), 3);
        if(job.needToBeRefreshed() && !toCheckJobs.contains(job) &&
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
   * Takes the maximum number of jobs in <code>toCheckJobs</code>, saves their internal status
   * and calls the computing system plugin with these jobs. <br>
   * If a status has changed, an action is performed: <ul>
   * <li>STATUS_ERRROR : nothing
   * <li>STATUS_WAIT : nothing
   * <li>STATUS_DONE : call jobValidation.validate
   * <li>STATUS_RUN : update status and host in status table
   * <li>STATUS_FAILED : call datasetMgr.jobFailure
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
      String csName = ((JobInfo) toCheckJobs.get(0)).getCSName();

      int currentJob = 0;
      while((jobs.size()<((Integer) maxJobsByUpdate.get(csName)).intValue() ||
          ((Integer) maxJobsByUpdate.get(csName)).intValue()==0 )
            && currentJob<toCheckJobs.size()){
        Debug.debug("Adding job to toCheckJobs "+currentJob, 3);
        if(((JobInfo) toCheckJobs.get(
            currentJob)).getCSName().toString().equalsIgnoreCase(csName)){
          jobs.add(toCheckJobs.remove(currentJob));
        }
        else{
          ++currentJob;
        }
      }
    }
    checkingJobs.addAll(jobs);

    int [] previousInternalStatus = new int [jobs.size()];
    for(int i=0; i<jobs.size(); ++i){
      Debug.debug("Setting value for job "+i, 3);
      statusTable.setValueAt(iconChecking, ((JobInfo) jobs.get(i)).getTableRow(), DatasetMgr.FIELD_CONTROL);
      previousInternalStatus[i] = ((JobInfo) jobs.get(i)).getInternalStatus();
    }
    
    Debug.debug("Updating status of "+jobs.size()+" jobs", 3);

    GridPilot.getClassMgr().getCSPluginMgr().updateStatus(jobs);

    Debug.debug("Removing "+jobs.size()+" jobs from checkingJobs", 3);
    
    checkingJobs.removeAll(jobs);

    for(int i=0; i<jobs.size(); ++i){
      Debug.debug("Setting value of job #"+i, 3);
      statusTable.setValueAt(null, ((JobInfo) jobs.get(i)).getTableRow(), DatasetMgr.FIELD_CONTROL);
      statusTable.setValueAt(((JobInfo) jobs.get(i)).getJobStatus(),
          ((JobInfo) jobs.get(i)).getTableRow(), DatasetMgr.FIELD_STATUS);
    }

    Debug.debug("Updating "+jobs.size()+" jobs by status", 3);
    datasetMgrs = GridPilot.getClassMgr().getDatasetMgrs();
    for(Iterator it = datasetMgrs.iterator(); it.hasNext();){
      ((DatasetMgr) it.next()).updateJobsByStatus();
    }
  
    for(int i=0; i<jobs.size(); ++i){
      JobInfo job = (JobInfo) jobs.get(i);
      Debug.debug("Setting computing system status of job #"+i+"; "+
          job.getInternalStatus()+"<->"+previousInternalStatus[i], 3);
      if(job.getInternalStatus()!=previousInternalStatus[i]){       
        switch(job.getInternalStatus()){
        case ComputingSystem.STATUS_WAIT :
          break;
        case ComputingSystem.STATUS_DONE:
          job.setNeedToBeRefreshed(false);
          GridPilot.getClassMgr().getJobValidation().validate(job);
          break;
        case ComputingSystem.STATUS_RUNNING:
          statusTable.setValueAt(job.getHost(), job.getTableRow(), DatasetMgr.FIELD_HOST);
          break;
        case ComputingSystem.STATUS_ERROR:
          // Without the line below: leave as refreshable, in case the error is intermittent.
          // With the line below: avoid checking over and over again.
          //                      To recheck, clear and add again to monitoring panel.
          //job.setNeedToBeRefreshed(false);
          if(job.getDBStatus()==DBPluginMgr.UNDECIDED || job.getDBStatus()==DBPluginMgr.UNEXPECTED){
            job.setNeedToBeRefreshed(false);
          }
          break;
        case ComputingSystem.STATUS_FAILED:
          job.setNeedToBeRefreshed(false);
          datasetMgrs = GridPilot.getClassMgr().getDatasetMgrs();
          for(Iterator it = datasetMgrs.iterator(); it.hasNext();){
            ((DatasetMgr) it.next()).jobFailure(job);
          }
          break;
        }
      }
    }
    
    Debug.debug("Finished trigCheck", 3);

    if(!timerChecking.isRunning())
      timerChecking.restart();
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