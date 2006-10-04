package gridpilot;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Vector;

import javax.swing.ImageIcon;
import javax.swing.JProgressBar;
import javax.swing.Timer;

import org.globus.util.GlobusURL;

/**
 * Controls the file downloads. <p>
 * When starting a new download (giving logical transfer ids), these transfers are
 * put in a queue (<code>toSubmitTransfers</code>). Each
 * <code>timeBetweenTransfers</code>, a Timer checks if there is some transfers
 * in this queue (this timer is stopped when the queue is empty, and restarted when
 * new transfers arrive). <br>
 * If there are any, the first transfer is removed from <code>toSubmitTransfers</code> and
 * put in <code>submittingTransfers</code>.
 */
public class TransferControl{
  
  private Vector submittedTransfers;
  private StatusBar statusBar;
  private JProgressBar pbSubmission;
  private boolean isProgressBarSet = false;
  private ConfigFile configFile;
  private LogFile logFile;
  private Table statusTable;
  private Timer timer;
  private ImageIcon iconSubmitting;
  /** All transfers for which the submission is not made yet */
  private Vector toSubmitTransfers = new Vector();
  /** All transfers for which the submission is in progress */
  private Vector submittingTransfers = new Vector();
 /** Maximum number of simulaneous threads for submission. <br>
   * It is not the maximum number of running transfers on the storage system */
  private int maxSimultaneousTransfers = 5;
  /** Delay between the begin of two submission threads */
  private int timeBetweenTransfers = 1000;
  private String isRand = null;

  private static int TRANSFER_SUBMIT_TIMEOUT = 60*1000;
  private static int TRANSFER_CANCEL_TIMEOUT = 60*1000;
  Object [][] tableValues = new String[0][GridPilot.transferStatusFields.length+1];;
  
  public TransferControl(){
    submittedTransfers = GridPilot.getClassMgr().getSubmittedTransfers();
    statusTable = GridPilot.getClassMgr().getTransferStatusTable();    
    configFile = GridPilot.getClassMgr().getConfigFile();
    logFile = GridPilot.getClassMgr().getLogFile();

    timer = new Timer(0, new ActionListener(){
      public void actionPerformed(ActionEvent e){
        trigSubmission();
        // TODO: update status and progress bars
      }
    });
    loadValues();
    pbSubmission = new JProgressBar(0,0);
  }
  
  /**
   * Checks if there are not too many active Threads (for submission), and there are
   * waiting jobs. If there are any, creates new submission threads
   */
  private synchronized void trigSubmission(){
    // If there are no requests, stop the timer
    if(toSubmitTransfers.isEmpty()){
      timer.stop();
      return;
    }
    synchronized(toSubmitTransfers){
      synchronized(submittingTransfers){
        // use status bar on monitoring frame
        statusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;

        GlobusURL firstSrc = ((TransferInfo) toSubmitTransfers.get(0)).getSource();
        GlobusURL firstDest = ((TransferInfo) toSubmitTransfers.get(0)).getDestination();
        
        Debug.debug("First transfer: "+((TransferInfo) toSubmitTransfers.get(0)), 3);
        
        String [] fts = GridPilot.ftNames;
        String pluginName = null;
        Vector transferVector = null;
        GlobusURL [] theseSources = null;
        GlobusURL [] theseDestinations = null;
        TransferInfo [] theseTransfers = null;
        GlobusURL [] checkSources = null;
        GlobusURL [] checkDestinations = null;
        
        try{
          // Select first plugin that supports the protocol of the next transfer
          for(int i=0; i<fts.length; ++i){
            Debug.debug("Checking plugin "+fts[i], 3);
            checkSources = new GlobusURL [] {firstSrc};
            checkDestinations = new GlobusURL [] {firstDest};
            if(GridPilot.getClassMgr().getFTPlugin(
                fts[i]).checkURLs(checkSources, checkDestinations)){
              pluginName = fts[i];
              break;
            };      
          }
          if(pluginName==null){
            throw new IOException("ERROR: protocol not supported or" +
                " plugin initialization " +
                "failed. "+firstSrc+"->"+firstDest);
          }
          Debug.debug("plugin selected: "+pluginName, 3);
          
          // Transfer jobs from toSubmitJobs to submittingJobs.
          // First construct uniform batch.
          transferVector = new Vector();
          while(submittingTransfers.size()<maxSimultaneousTransfers &&
              !toSubmitTransfers.isEmpty()){
            
            // break if next transfer is not uniform with the previous ones (in this batch)
            transferVector.add(((TransferInfo) toSubmitTransfers.get(0)));
            theseSources = new GlobusURL[transferVector.size()];
            theseDestinations = new GlobusURL[transferVector.size()];
            theseTransfers = new TransferInfo[transferVector.size()];
            TransferInfo tmpTransfer = null;
            for(int i=0; i<theseDestinations.length; ++i){
              tmpTransfer = (TransferInfo) transferVector.get(i);
              theseSources[i] = tmpTransfer.getSource();
              theseDestinations[i] = tmpTransfer.getDestination();
              theseTransfers[i] = tmpTransfer;
            }
            if(!GridPilot.getClassMgr().getFTPlugin(
                firstSrc.getProtocol()).checkURLs(theseSources, theseDestinations)){
              break;
            }
            
            TransferInfo transfer = (TransferInfo) toSubmitTransfers.remove(0);
            //statusBar.setLabel("Queueing "+transfer.getSource().getURL()+"->"+
            //    transfer.getDestination().getURL());
            submittingTransfers.add(transfer);
            Debug.debug("Removed "+transfer.getSource().getURL(), 3);
            Debug.debug("with id "+transfer.getTransferID(), 3);
            Debug.debug("Now toSubmitTransfers has "+toSubmitTransfers.size()+" elements", 3);
            Debug.debug("Now submittingTransfers has "+submittingTransfers.size()+" elements", 3);
          }
        }
        catch(Exception e){
          statusBar.setLabel("ERROR: queueing transfer(s) failed");
          logFile.addMessage("ERROR: queueing transfer(s) failed:\n"+
              Util.arrayToString(transferVector.toArray()), e);
          return;
        }
        
        final TransferInfo [] finalTransfers = theseTransfers;
        final String finalPluginName = pluginName;
        
        new Thread(){
          public void run(){
            // use status bar on monitoring frame
            statusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;
            try{
              submit(finalPluginName, finalTransfers);
            }
            catch(Exception e){
              statusBar.setLabel("ERROR: starting transfer(s) failed. "+
                  e.getMessage());
              logFile.addMessage("ERROR: starting transfer(s) failed.", e);
              e.printStackTrace();
            }
          }
        }.start();
      }
    }
    
  }
  
  /**
   * Reloads some values from configuration file. <p>
   */
  public void loadValues(){
    String tmp = configFile.getValue("GridPilot", "maximum simultaneous transfers");
    if(tmp != null){
      try{
        maxSimultaneousTransfers = Integer.parseInt(tmp);
      }
      catch(NumberFormatException nfe){
        logFile.addMessage("Value of \"maximum simultaneoud transfers\" "+
                                    "is not an integer in configuration file", nfe);
      }
    }
    else
      logFile.addMessage(configFile.getMissingMessage("GridPilot", "maximum simultaneous transfers") + "\n" +
                              "Default value = " + maxSimultaneousTransfers);
    tmp = configFile.getValue("GridPilot", "time between transfers");
    if(tmp != null){
      try{
        timeBetweenTransfers = Integer.parseInt(tmp);
      }
      catch(NumberFormatException nfe){
        logFile.addMessage("Value of \"time between transfers\" is not"+
                                    " an integer in configuration file", nfe);
      }
    }
    else{
      logFile.addMessage(configFile.getMissingMessage("GridPilot", "time between transfers") + "\n" +
                              "Default value = " + timeBetweenTransfers);
    }
    Debug.debug("Setting time between transfers "+timeBetweenTransfers, 3);
    timer.setInitialDelay(0);
    timer.setDelay(timeBetweenTransfers);
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
    isRand = configFile.getValue("GridPilot", "randomized transfers");
    Debug.debug("isRand = " + isRand, 2);
  }

  /**
   * This is used by the plugins for callcback, e.g.
   * by the SRM plugin for handling e.g. gsiftp TURLs
   * @param   srcUrls    Array of source URLs.
   * @param   destUrls   Array of destination URLs.
   */
  public static String [] startCopyFiles(GlobusURL [] srcUrls, GlobusURL [] destUrls)
     throws Exception {

    Debug.debug("Starting to copy files "+Util.arrayToString(srcUrls)+" -> "+
        Util.arrayToString(destUrls), 2);
    
    String ftPluginName = null;
    boolean protocolOK = false;
    String [] fts = GridPilot.ftNames;
    String [] ids = null;
    
    // Select first plugin that supports the protocol of the these transfers
    for(int i=0; i<fts.length; ++i){
      Debug.debug("Checking plugin "+fts[i], 3);
      if(GridPilot.getClassMgr().getFTPlugin(
          fts[i]).checkURLs(srcUrls, destUrls)){
        ftPluginName = fts[i];
        Debug.debug("Selected plugin "+fts[i], 3);
        break;
      };      
    }
    if(!protocolOK || ftPluginName==null){
      throw new IOException("ERROR: protocol not supported or" +
          " plugin initialization " +
          "failed. "+Util.arrayToString(srcUrls)+"->"+Util.arrayToString(destUrls));
    }
    
    // Start the transfers
    ids = GridPilot.getClassMgr().getFTPlugin(
        ftPluginName).startCopyFiles(srcUrls, destUrls);
    return ids;
  }
  
  /**
   * Adds transfers to toSubmitTransfers.
   * @param   transfers     Vector of TransferInfo's.
   */
  public void queue(final Vector _transfers) throws Exception {

    MyThread t = new MyThread(){
      Vector transfers;
      public void run(){
        // use status bar on monitoring frame
        statusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;
        if(isRand!=null && isRand.equalsIgnoreCase("yes")){
          transfers = Util.shuffle(_transfers);
        }
        else{
          transfers = _transfers;
        }
        pbSubmission.setMaximum(pbSubmission.getMaximum() + transfers.size());
        pbSubmission.addMouseListener(new MouseAdapter(){
          public void mouseClicked(MouseEvent e){
            cancelQueueing();
          }
        });
        Debug.debug("queueing "+transfers.size()+" transfers", 2);
        statusBar.setLabel("Adding to submission queue. Please wait...");
        statusBar.animateProgressBar();
        pbSubmission.setToolTipText("Click here to cancel queueing");
        if(!isProgressBarSet){
          statusBar.setProgressBar(pbSubmission);
          isProgressBarSet = true;
        }
        toSubmitTransfers.addAll(transfers);
        if(!timer.isRunning()){
          timer.restart();
        }
        statusBar.setLabel("Queueing done.");
        statusBar.stopAnimation();
      }
    };
    t.start();

    if(!Util.waitForThread(t, "", TRANSFER_SUBMIT_TIMEOUT, "transfer")){
      statusBar.setLabel("WARNING: queueing transfers timed out.");
      statusBar.stopAnimation();
    }
  }
  
  /**
   * Starts transfers using a given plugin
   * @param   ftPlugin    Name ofthe ft plugin.
   * @param   transfers    URL of the SRM server.
   */
  public void submit(String ftPlugin, TransferInfo [] transfers)
     throws Exception {

    if(isRand!=null && isRand.equalsIgnoreCase("yes")){
      transfers = (TransferInfo []) Util.shuffle(transfers);
    }

    // use status bar on monitoring frame
    statusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;
    GlobusURL [] sources = new GlobusURL [transfers.length];
    GlobusURL [] destinations = new GlobusURL [transfers.length];
    String [] ids = null;
    
    Object [][] appendTablevalues = new String [transfers.length][GridPilot.transferStatusFields.length+1];
    Object [][] newTablevalues = new String [tableValues.length+appendTablevalues.length][GridPilot.transferStatusFields.length+1];
    int startRow = statusTable.getRowCount();
    for(int i=0; i<transfers.length; ++i){
      sources[i] = transfers[i].getSource();
      destinations[i] = transfers[i].getDestination();
      
      transfers[i].setTableRow(submittedTransfers.size());
      transfers[i].setInternalStatus(FileTransfer.STATUS_WAIT);
      submittedTransfers.add(transfers[i]);

      // add to status table
      statusTable.createRows(submittedTransfers.size());
      statusTable.setValueAt(transfers[i].getSource().getURL(),
          transfers[i].getTableRow(), TransferInfo.FIELD_SOURCE);
      statusTable.setValueAt(transfers[i].getDestination().getURL(),
          transfers[i].getTableRow(), TransferInfo.FIELD_DESTINATION);
      statusTable.setValueAt(iconSubmitting, transfers[i].getTableRow(),
          TransferInfo.FIELD_CONTROL);
      
      for(int j=1; j<GridPilot.transferStatusFields.length; ++j){
        appendTablevalues[i][j] = statusTable.getValueAt(startRow+i, j);
      }
    }
    // TODO: check why this does not fix the problem with sorting...
    System.arraycopy(tableValues, 0, newTablevalues, 0, tableValues.length);
    System.arraycopy(appendTablevalues, 0, newTablevalues, tableValues.length, appendTablevalues.length);
    tableValues = newTablevalues;
    statusTable.setTable(tableValues, GridPilot.transferStatusFields);

    try{
      ids = GridPilot.getClassMgr().getFTPlugin(ftPlugin).startCopyFiles(
          sources, destinations);
      
      if(ids.length!=transfers.length){
        throw new IOException("ERROR: returned number of transfer ids don't agree " +
            "with number of submitted tranfers: "+ids.length+"!="+transfers.length+
            ". Don't know what to do; cancelling all.");
      }

      String userInfo = GridPilot.getClassMgr().getFTPlugin(
          ftPlugin).getUserInfo();
      for(int i=0; i<transfers.length; ++i){
        
        transfers[i].setNeedToBeRefreshed(true);
        transfers[i].setTransferID(ids[i]);
        
        statusTable.setValueAt(transfers[i].getTransferID(), transfers[i].getTableRow(),
            TransferInfo.FIELD_TRANSFER_ID);
        statusTable.setValueAt(userInfo, transfers[i].getTableRow(),
            TransferInfo.FIELD_USER);
        Debug.debug("Transfer submitted", 2);
      }
      statusTable.updateSelection();
    }
    catch(Exception e){
      // Cancel all transfers
      this.cancelQueueing();
      for(int i=0; i<sources.length; ++i){
        try{
          statusTable.setValueAt("NOT started!", transfers[i].getTableRow(),
              TransferInfo.FIELD_TRANSFER_ID);
          transfers[i].setInternalStatus(FileTransfer.STATUS_ERROR);
          transfers[i].setNeedToBeRefreshed(false);
          GridPilot.getClassMgr().getFTPlugin(
              ftPlugin).cancel(ids[i]);
        }
        catch(Exception ee){
        }
      }
      throw e;
    }
    finally{
      for(int i=0; i<transfers.length; ++i){
        submittingTransfers.remove(transfers[i]);
        // remove iconSubmitting
        statusTable.setValueAt(null, transfers[i].getTableRow(), TransferInfo.FIELD_CONTROL);
      }
      
      if(!timer.isRunning()){
        timer.restart();
      }
      pbSubmission.setValue(pbSubmission.getValue() + transfers.length);
      if(pbSubmission.getPercentComplete()==1.0){
        statusBar.removeProgressBar(pbSubmission);
        isProgressBarSet = false;
        pbSubmission.setMaximum(0);
        pbSubmission.setValue(0);
        statusBar.setLabel("Queueing transfers done.");
      }
    }
  };

  public static String getStatus(String fileTransferID) throws Exception {
    return findFTPlugin(fileTransferID).getStatus(fileTransferID);
  }

  public static int getPercentComplete(String fileTransferID) throws Exception {
    return findFTPlugin(fileTransferID).getPercentComplete(fileTransferID);
  }

  public static long getBytesTransferred(String fileTransferID) throws Exception {
    return findFTPlugin(fileTransferID).getBytesTransferred(fileTransferID);
  }

  /**
   * Stops the submission. <br>
   * Empties toSubmitJobs, and set these jobs to Failed.
   */
  private void cancelQueueing(){
    // use status bar on monitoring frame
    statusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;
    timer.stop();
    Enumeration e = toSubmitTransfers.elements();
    while(e.hasMoreElements()){
      TransferInfo transfer = (TransferInfo) e.nextElement();
      statusTable.setValueAt("Transfer not queued (cancelled)!",
          transfer.getTableRow(), TransferInfo.FIELD_TRANSFER_ID);
      statusTable.setValueAt(transfer.getSource(), transfer.getTableRow(),
          TransferInfo.FIELD_SOURCE);
      statusTable.setValueAt(transfer.getDestination(), transfer.getTableRow(),
          TransferInfo.FIELD_DESTINATION);
      transfer.setNeedToBeRefreshed(false);
    }
    toSubmitTransfers.removeAllElements();
    statusBar.removeProgressBar(pbSubmission);
    pbSubmission.setMaximum(0);
    pbSubmission.setValue(0);
  }

  public void cancel(final Vector transfers){
    MyThread t = new MyThread(){
      String res = null;
      TransferInfo transfer = null;
      public void run(){
        // use status bar on monitoring frame
        statusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;
        statusBar.animateProgressBar();
        if(!isProgressBarSet){
          statusBar.setProgressBar(pbSubmission);
          isProgressBarSet = true;
        }
        try{
          for(int i=0; i<transfers.size(); ++i){
            try{             
              transfer = (TransferInfo) transfers.get(i);
              Debug.debug("Cancelling transfer "+transfer.getTransferID(), 2);
              FileTransfer ft = findFTPlugin(transfer.getTransferID());
              ft.cancel(transfer.getTransferID());
              String status = "Cancelled";
              statusTable.setValueAt(status, transfer.getTableRow(),
                  TransferInfo.FIELD_STATUS);
              transfer.setStatus(status);
              transfer.setInternalStatus(FileTransfer.STATUS_ERROR);
              transfer.setNeedToBeRefreshed(false);
            }
            catch(Exception ee){
            }
          }        
        }
        catch(Throwable t){
          GridPilot.getClassMgr().getLogFile().addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " +
                             " for download", t);
          res = null;
        }
        statusBar.stopAnimation();
      }
      public String getStringRes(){
        return res;
      }
    };

    t.start();

    if(!Util.waitForThread(t, "", TRANSFER_CANCEL_TIMEOUT, "transfer")){
      statusBar.stopAnimation();
      statusBar.setLabel("WARNING: cancel transfers timed out.");
    }
  }
  
  /**
   * Select first plugin that supports the protocol of the this transfer.
   * @param   transferID    Transfer ID. Format:
   *                        "protocol-(get|put|copy):...:srcTURL destTURL [SURL]".
   */
  public static FileTransfer findFTPlugin(String transferID)
     throws Exception{
    
    Debug.debug("Finding plugin for transfer "+transferID, 2);
    String ftPluginName = null;
    String [] checkArr = Util.split(transferID, ":");
    // Only if the ID is of the second type will checkArr[0] contain a -.
    // It the ID is of the first type checkArr[0] will be a protocol.
    if(checkArr[0].indexOf("-")>0){
      String [] arr = Util.split(checkArr[0], "-");
      String type = arr[1];
      String [] turls = null;
      // For put or get requests, the work can have been
      // delegated from one plugin to another (srm -> gsiftp).
      // We assume the delegation was done by a first match and
      // repeat the first matching to find the plugin.
      // gsiftp-get:dadf:asdad:adf:file://afdad gsiftp://adadf srm://dsaadf
      if(type.equalsIgnoreCase("get") || type.equalsIgnoreCase("put")){
        int startIndex = transferID.indexOf("://");
        int tmpindex = 0;
        String tmpID = transferID;
        String [] fts = GridPilot.ftNames;
        while(tmpindex<startIndex){
          tmpID = transferID.substring(tmpindex);
          tmpindex += tmpID.indexOf(":") + 1;
        }
        turls = Util.split(tmpID);
        for(int i=0; i<fts.length; ++i){
          if(GridPilot.getClassMgr().getFTPlugin(
              fts[i]).checkURLs(
                  new GlobusURL [] {new GlobusURL(turls[0])},
                  new GlobusURL [] {new GlobusURL(turls[1])})){
            ftPluginName = fts[i];
            break;
          };      
        }
      }
      else{
        // For copy requests, we just take the plugin name directly form the ID header.
        ftPluginName = arr[0];
      }
      Debug.debug("Found plugin "+ftPluginName, 2);
    }
    else{
      throw new IOException("ERROR: malformed ID "+transferID);
    }
    if(ftPluginName==null){
      throw new IOException("ERROR: could not get file transfer plugin for "+transferID);
    }

    return GridPilot.getClassMgr().getFTPlugin(ftPluginName);
    
  }
  
  public boolean isSubmitting(){
    //return timer.isRunning();
    return !(submittingTransfers.isEmpty() && toSubmitTransfers.isEmpty());
  }
  
  /**
   * Checks the status of the transfers and updates the TransferInfo objects. <p>
   */
  public static void updateStatus(Vector transfers){
    String status = null;
    String transferred = null;
    int transInt = -1;
    TransferInfo transfer = null;
    String id = null;
    int internalStatus = -1;
    for(Iterator it=transfers.iterator(); it.hasNext();){
      transfer = (TransferInfo) it.next();
      id = null;
      try{
        id = transfer.getTransferID();
      }
      catch(Exception e){
        Debug.debug("skipping "+transfer, 3);
      }
      if(id==null){
        continue;
      }
      try{
        status = getStatus(id);
        Debug.debug("Got status: "+status, 2);
        transfer.setStatus(status);
        try{
          transInt = getPercentComplete(id);
          transferred = Integer.toString(transInt);
          Debug.debug("Got transferred: "+transferred, 2);
          transfer.setTransferred(transferred+"%");
        }
        catch(Exception e){
        }
        if(transferred==null || transInt<0){
          transferred = Long.toString(getBytesTransferred(id)/1000);
          Debug.debug("Got transferred: "+transferred, 2);
          transfer.setTransferred(transferred+" kB");
        }
        
        internalStatus = findFTPlugin(id).getInternalStatus(status);
        transfer.setInternalStatus(internalStatus);
      }
      catch(Exception e){
        Debug.debug("WARNING: could not get status of "+id+
            ". skipping. "+e.getMessage(), 3);
        continue;
      }
    }
  }
  
  /**
   *  Take some action on transfer failure, like asking to delete partly copied
   *  target file.
   */
  public static void transferFailure(TransferInfo transfer){
    // TODO
    GridPilot.getClassMgr().getGlobalFrame(
        ).monitoringPanel.statusBar.setLabel("Transfer "+transfer.getTransferID()+" failed");
  }

  /**
   * Take some action on successful transfer completion,
   * like registering the new location.
   */
  public static void transferDone(TransferInfo transfer){
    if(transfer.getDBPluginMgr()!=null && transfer.getDestination()!=null){
      String message = "Registering new location "+transfer.getDestination();
      Debug.debug(message, 2);
      GridPilot.getClassMgr().getGlobalFrame(
         ).monitoringPanel.statusBar.setLabel(message);
      transfer.getDBPluginMgr().registerFileLocation(
          transfer.getTransferID(), transfer.getDestination().getURL());
      GridPilot.getClassMgr().getGlobalFrame(
         ).monitoringPanel.statusBar.setLabel("Registering done");
    }
  }

}
