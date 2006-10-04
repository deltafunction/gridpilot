package gridpilot;

import java.net.URL;

import java.util.Vector;
import java.util.Enumeration;
import java.util.HashMap;

import javax.swing.*;

import java.awt.event.*;

/**
 * This class manages the transfers status update. <p>
 * When TransferMonitoringPanel requests for transfer updates,
 * these transfers are appended in the queue
 * <code>toCheckTransfers</code> (if they need to be refreshed, and they are not already
 * in this queue).
 * A timer (<code>timerChecking</code>, running if <code>toCheckTransfer</code> is not empty),
 * calls <code>trigCheck()</code> every <code>timeBetweenCheking</code> ms.
 * This method checks first if a new checking is allowed to start (not too many current checking),
 * and looks then at the first transfers of <code>toCheckTransfers</code> ; if this transfer plug-in
 * can update more than one transfer at the same time, the maximum nubmer of transfer for this system
 * is taken in <code>toCheckTransfers</code>.
 *
 * <p><a href="TransferStatusUpdateControl.java.html">see sources</a>
 */
public class TransferStatusUpdateControl{
  /** The timer which triggers the checking */
  private Timer timerChecking;

  private ConfigFile configFile;
  private LogFile logFile;

  /** Maximun number of simultaneous thread for checking */
  private static int maxSimultaneousChecking = 3;
  /** Delay of <code>timerChecking</code> */
  private static int timeBetweenCheking = 1000;

  /** For each plug-in, maximum number of transfer for one update (0 = INF)*/
  private HashMap maxTransfersByUpdate;

  private Table statusTable;

  /** counters of jobs ordered by local status
   */
  private int[] transfersByStatus = new int[statusNames.length];
 
  /**
   * Returns status names for statistics panel.
   */
  public static String [] statusNames = new String [] {"Wait", "Run", "Done"};

  /**
   * DB status names for statistics panel. <p>
   */
  public static String [] ftStatusNames = new String [] {
      "Queued",
      "Running",
      "Done",
      "Cancelled",
      "Failed"};
  
  /*public static String getStatusName(int status){
  switch(status){
    case FileTransfer.STATUS_WAIT : return "Queued";
    case FileTransfer.STATUS_RUNNING : return "Running";
    case FileTransfer.STATUS_DONE : return "Done";
    case FileTransfer.STATUS_FAILED : return "Failed";
    case FileTransfer.STATUS_ERROR : return "Error";
    default : return "status not found";
   }
  }*/

  /** counters of jobs ordered by transfer status
   */
  private int[] transfersByFTStatus = new int[ftStatusNames.length];

  /**
   * Contains all transfers which should be updated. <p>
   * All these transfers should be "needed to be refreshed" (otherwise, they are not
   * put in this transfer vector, and a transfer cannot become "not needed to be refreshed"
   * when it is in this transfer vector) and should belong to submittedTransfers. <br>
   * Except if the method {@link #reset()} is called, each transfer in toCheckTransfers is
   * going to be put in {@link #checkingTransfers}
   */
  private Vector toCheckTransfers = new Vector();

  /**
   * Contains all transfers for which update is processing. <p>
   * Each transfer in this transfer vector : <ul>
   * <li> corresponds to one and only one thread in {@link #checkingThread}
   *      (but a thread in checkingThread could correspond to several transfers in checkingTransfers),
   * <li> should be "needed to be refreshed" (see {@link #toCheckTransfers})and has
   *      belonged to {@link #toCheckTransfers}, but doesn't belong to it anymore,
   */
  private Vector checkingTransfers = new Vector(); //

  /**
   * Thread vector. <p>
   * @see #checkingTransfers
   */
  private Vector checkingThread = new Vector();
  private ImageIcon iconChecking;

  public TransferStatusUpdateControl(){
    statusTable = GridPilot.getClassMgr().getTransferStatusTable();
    configFile = GridPilot.getClassMgr().getConfigFile();
    logFile = GridPilot.getClassMgr().getLogFile();

    timerChecking = new Timer(0, new ActionListener(){
      public void actionPerformed(ActionEvent ae){
        //Debug.debug(checkingThread.size()+":"+maxSimultaneousChecking +":"+
            //!toCheckTransfers.isEmpty(), 3);
        if(checkingThread.size()<maxSimultaneousChecking && !toCheckTransfers.isEmpty()){
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

    maxTransfersByUpdate = new HashMap();

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
   * <li>for each plug-in : maxTransfersByUpdate
   * </ul> <p>
   */
  public void loadValues(){

    /**
     * Load of maxSimultaneousChecking
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
     * Load of timeBetweenCheking
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
     * Load of maxTransfersByUpdate
     */
    for(int i=0; i<GridPilot.ftNames.length; ++i){
      tmp = configFile.getValue(GridPilot.ftNames[i], "max transfers by update");
      if(tmp!=null){
        try{
          maxTransfersByUpdate.put(GridPilot.ftNames[i], new Integer(Integer.parseInt(tmp)));
        }
        catch(NumberFormatException nfe){
          logFile.addMessage("Value of \"max transfers by update\" in section " + GridPilot.ftNames[i] +
                             " is not an integer in configuration file", nfe);
          maxTransfersByUpdate.put(GridPilot.ftNames[i], new Integer(1));
        }
      }
      else{
        maxTransfersByUpdate.put(GridPilot.ftNames[i], new Integer(1));
        logFile.addMessage(configFile.getMissingMessage(GridPilot.ftNames[i], "max transfers by update") + "\n" +
                           "Default value = " + maxTransfersByUpdate.get(GridPilot.ftNames[i]));
      }
    }
  }

  public void exit(){
  }

  /**
   * Updates the status for all selected transfers. <p>
   * Add each transfer from <code>transfers</code> in <code>toCheckTransfers</code> if it needs
   * to be refreshed, and is not in <code>toCheckTransfers</code> or <code>checkingTransfers</code>. <p>
   * If timer <code>timerChecking</code> was not running, restarts it. <p>
   *
   */
  public void updateStatus(int [] _rows){
    Debug.debug("updateStatus", 1);
    
    // get transfer vector
    int [] rows = _rows;
    Vector transfers = null;
    if(rows==null || rows.length==0){
      // if nothing is selected, we refresh all transfers
      transfers = GridPilot.getClassMgr().getSubmittedTransfers();
    }
    else{
      rows = statusTable.getSelectedRows();
      transfers = TransferMonitoringPanel.getTransfersAtRows(rows);
    }
   
    // fill toCheckTransfers with running transfers
    synchronized(toCheckTransfers){
      Enumeration e = transfers.elements();
      while(e.hasMoreElements()){
        TransferInfo transfer = (TransferInfo) e.nextElement();
        Debug.debug("Adding transfer to toCheckTransfers: "+transfer.getTransferID()+" "+
            transfer.needToBeRefreshed() +" "+ !toCheckTransfers.contains(transfer) +" "+
            !checkingTransfers.contains(transfer), 3);
        if(transfer.needToBeRefreshed() && !toCheckTransfers.contains(transfer) &&
            !checkingTransfers.contains(transfer)){
          Debug.debug("Adding transfer to toCheckTransfers", 3);
          toCheckTransfers.add(transfer);
        }
      }
      Debug.debug("Finished adding transfer to toCheckTransfers", 3);
    }
    if(!timerChecking.isRunning()){
      Debug.debug("WARNING: timer not running, restarting...", 3);
      timerChecking.restart();
    }
  }

  /**
   * Called by <code>timerChecking</code> time outs. <p>
   * This method is invocated in a thread. <br>
   * Takes the maximum number of transfers in <code>toCheckTransfers</code>, saves their internal status
   * and calls the computing system plugin with these transfers. <br>
   * If a status has changed, an action is performed: <ul>
   * <li>STATUS_ERRROR : update status in status table
   * <li>STATUS_WAIT : nothing
   * <li>STATUS_DONE : update status in status table
   * <li>STATUS_RUN : update status and host in status table
   * <li>STATUS_FAILED : call TransferControl.transferFailure
   * </ul>
   *
   */
  private void trigCheck(){

    Debug.debug("trigCheck", 1);

    Vector transfers = new Vector();
    synchronized(toCheckTransfers){
      if(toCheckTransfers.isEmpty()){
        return;
      }
      String ftName = ((TransferInfo) toCheckTransfers.get(0)).getFTName();

      int currentTransfer = 0;
      while((transfers.size()<((Integer) maxTransfersByUpdate.get(ftName)).intValue() ||
          ((Integer) maxTransfersByUpdate.get(ftName)).intValue()==0)
            && currentTransfer<toCheckTransfers.size()){
        Debug.debug("Adding transfer to toCheckTransfers "+currentTransfer, 3);
        if(((TransferInfo) toCheckTransfers.get(
            currentTransfer)).getFTName().toString().equalsIgnoreCase(ftName)){
          transfers.add(toCheckTransfers.remove(currentTransfer));
        }
        else{
          ++currentTransfer;
        }
      }
    }
    checkingTransfers.addAll(transfers);

    int [] previousInternalStatus = new int [transfers.size()];
    for(int i=0; i<transfers.size(); ++i){
      statusTable.setValueAt(iconChecking, ((TransferInfo) transfers.get(i)).getTableRow(), TransferInfo.FIELD_CONTROL);
      previousInternalStatus[i] = ((TransferInfo) transfers.get(i)).getInternalStatus();
      Debug.debug("Setting previousInternalStatus for transfer "+i+" : "+
          previousInternalStatus[i], 3);
    }
    
    Debug.debug("Updating status of "+transfers.size()+" transfers", 3);

    // checks and updates with TransferInfo.setStatus() and
    // TransferInfo.setInternalStatus()
    TransferControl.updateStatus(transfers);

    Debug.debug("Removing "+transfers.size()+" transfers from checkingTransfers", 3);
    
    checkingTransfers.removeAll(transfers);

    for(int i=0; i<transfers.size(); ++i){
      Debug.debug("Setting value of transfer #"+i, 3);
      statusTable.setValueAt(null, ((TransferInfo) transfers.get(i)).getTableRow(),
          TransferInfo.FIELD_CONTROL);
      statusTable.setValueAt(((TransferInfo) transfers.get(i)).getStatus(),
          ((TransferInfo) transfers.get(i)).getTableRow(), TransferInfo.FIELD_STATUS);
      statusTable.setValueAt(((TransferInfo) transfers.get(i)).getTransferred(),
          ((TransferInfo) transfers.get(i)).getTableRow(), TransferInfo.FIELD_TRANSFERRED);
    }

    // Update the statistics information
    Debug.debug("Updating "+transfers.size()+" transfers by status", 3);
    updateTransfersByStatus();
    
    // Take actions depending on the change of status
    for(int i=0; i<transfers.size(); ++i){
      TransferInfo transfer = (TransferInfo) transfers.get(i);
      Debug.debug("Setting file transfer system status of transfer #"+i+"; "+
          transfer.getInternalStatus()+"<->"+previousInternalStatus[i], 3);
      if(transfer.getInternalStatus()!=previousInternalStatus[i]){       
        switch(transfer.getInternalStatus()){
        case FileTransfer.STATUS_WAIT :
          break;
        case FileTransfer.STATUS_DONE:
          transfer.setNeedToBeRefreshed(false);
          TransferControl.transferDone(transfer);
          break;
        case FileTransfer.STATUS_RUNNING:
          statusTable.setValueAt(transfer.getSource().getURL(),
              transfer.getTableRow(), TransferInfo.FIELD_SOURCE);
          statusTable.setValueAt(transfer.getDestination().getURL(),
              transfer.getTableRow(), TransferInfo.FIELD_DESTINATION);
          break;
        case FileTransfer.STATUS_ERROR:
          // Without the line below: leave as refreshable, in case the error is intermittent.
          // With the line below: avoid checking over and over again.
          //                      To recheck, clear and add again to monitoring panel.
          //transfer.setNeedToBeRefreshed(false);
          
          // If a job goes from running to error, the transfer has failed.
          if(previousInternalStatus[i]==FileTransfer.STATUS_RUNNING){
            transfer.setInternalStatus(FileTransfer.STATUS_FAILED);
            transfer.setNeedToBeRefreshed(true);
          }
          break;
        case FileTransfer.STATUS_FAILED:
          transfer.setNeedToBeRefreshed(false);
          TransferControl.transferFailure(transfer);
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
   * Removes all transfers from queue, and waits for the end of the pending threads. <p>
   * Called by : <ul>
   */
  public void reset(){
    toCheckTransfers.removeAllElements();

    for(int i =0; i<checkingThread.size(); ++i){
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
    // When selecting "Stop update",
    // if a large number of threads were running, one could not refresh anymore.
    checkingThread.removeAllElements();
  }
  
  /**
   * Updates the statistics panel
   */
  protected void updateTransfersByStatus(){
    // jobsByDBStatus
    for(int i=0; i<transfersByFTStatus.length;++i){
      transfersByFTStatus[i] = 0;
    }
    // transfersByStatus
    for(int i=0; i<transfersByStatus.length;++i){
      transfersByStatus[i] = 0;
    }
    int waitIndex = 0;
    int runIndex = 1;
    int doneIndex = 2;

    Vector submittedTransfers = GridPilot.getClassMgr().getSubmittedTransfers();

    for(int i=0; i<submittedTransfers.size(); ++i){
      
      ++transfersByFTStatus[((TransferInfo) submittedTransfers.get(i)).getInternalStatus()-1];
      
      switch(((TransferInfo) submittedTransfers.get(i)).getInternalStatus()){
      
        case FileTransfer.STATUS_WAIT:
          ++transfersByStatus[waitIndex];
          break;

        case FileTransfer.STATUS_RUNNING:
          ++transfersByStatus[runIndex];
          break;

        case FileTransfer.STATUS_DONE:
          ++transfersByStatus[doneIndex];
          break;

        case FileTransfer.STATUS_FAILED:
          ++transfersByStatus[doneIndex];
          break;
      }
    }
    
    GridPilot.getClassMgr().getTransferStatisticsPanel().update();
  }
  
  public int [] getTransfersByStatus(){
    return transfersByStatus;
  }

  public int [] getTransfersByFTStatus(){
    return transfersByFTStatus;
  }


}