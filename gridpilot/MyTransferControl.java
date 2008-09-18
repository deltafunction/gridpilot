package gridpilot;

import gridfactory.common.ConfigFile;
import gridfactory.common.DBVectorTableModel;
import gridfactory.common.Debug;
import gridfactory.common.FileTransfer;
import gridfactory.common.LocalStaticShell;
import gridfactory.common.ResThread;
import gridfactory.common.Shell;
import gridfactory.common.TransferControl;
import gridfactory.common.Util;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Vector;

import javax.swing.ImageIcon;
import javax.swing.JProgressBar;
import javax.swing.Timer;

import jonelo.jacksum.JacksumAPI;
import jonelo.jacksum.algorithm.AbstractChecksum;

import org.globus.ftp.exception.FTPException;
import org.globus.util.GlobusURL;
import org.safehaus.uuid.UUIDGenerator;

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
public class MyTransferControl extends TransferControl {
  
  private StatusBar statusBar;
  private JProgressBar pbSubmission;
  private boolean isProgressBarSet = false;
  private ConfigFile configFile;
  private Timer timer;
  private ImageIcon iconSubmitting;
  /** All transfers for which the submission is not made yet */
  private Vector toSubmitTransfers = new Vector();
  /** All transfers for which the submission is in progress */
  private Vector submittingTransfers = new Vector();
  /** All running transfers */
  protected Vector runningTransfers = new Vector();
 /** Maximum number of simulaneous threads for submission. <br>
   * It is not the maximum number of running transfers on the storage system */
  private int maxSimultaneousTransfers = 5;
  /** Delay between the begin of two submission threads */
  private int timeBetweenTransfers = 1000;
  private String isRand = null;

  private static int TRANSFER_SUBMIT_TIMEOUT = 60*1000;
  private static int TRANSFER_CANCEL_TIMEOUT = 60*1000;
  private Object [][] tableValues = new Object[0][GridPilot.transferStatusFields.length];
  
  public MyTransferControl() throws Exception{
    super(GridPilot.getClassMgr().getLogFile(), GridPilot.getClassMgr().getTransferStatusTable());
    configFile = GridPilot.getClassMgr().getConfigFile();

    timer = new Timer(0, new ActionListener(){
      public void actionPerformed(ActionEvent e){
        trigSubmission();
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
    synchronized(runningTransfers){
      synchronized(toSubmitTransfers){
        synchronized(submittingTransfers){
          // use status bar on monitoring frame
          statusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;

          GlobusURL firstSrc = ((MyTransferInfo) toSubmitTransfers.get(0)).getSource();
          GlobusURL firstDest = ((MyTransferInfo) toSubmitTransfers.get(0)).getDestination();
          
          Debug.debug("First transfer: "+((MyTransferInfo) toSubmitTransfers.get(0)), 3);
          
          String [] fts = GridPilot.ftNames;
          String pluginName = null;
          Vector transferVector = null;
          GlobusURL [] theseSources = null;
          GlobusURL [] theseDestinations = null;
          MyTransferInfo [] theseTransfers = null;
          GlobusURL [] checkSources = null;
          GlobusURL [] checkDestinations = null;
          
          boolean brokenOut = false;
          try{
            // Select first plugin that supports the protocol of the next transfer
            for(int i=0; i<fts.length; ++i){
              Debug.debug("Checking plugin "+fts[i], 3);
              checkSources = new GlobusURL [] {firstSrc};
              checkDestinations = new GlobusURL [] {firstDest};
              try{
                if(GridPilot.getClassMgr().getFTPlugin(
                    fts[i]).checkURLs(checkSources, checkDestinations)){
                  pluginName = fts[i];
                  break;
                };      
              }
              catch(Exception e){
                e.printStackTrace();
              }
            }
            if(pluginName==null){
              throw new IOException("ERROR: protocol not supported or plugin initialization " +
                  "failed.\n"+firstSrc+"\n->\n\n"+firstDest);
            }
            Debug.debug("plugin selected: "+pluginName, 3);
            
            // Transfer jobs from toSubmitTransfers to submittingTransfers.
            // First construct uniform batch.
            transferVector = new Vector();
            while(runningTransfers.size()+submittingTransfers.size()<maxSimultaneousTransfers &&
                !toSubmitTransfers.isEmpty()){
              
              // break if next transfer is not uniform with the previous ones (in this batch)
              transferVector.add(((MyTransferInfo) toSubmitTransfers.get(0)));
              theseSources = new GlobusURL[transferVector.size()];
              theseDestinations = new GlobusURL[transferVector.size()];
              theseTransfers = new MyTransferInfo[transferVector.size()];
              MyTransferInfo tmpTransfer = null;
              for(int i=0; i<theseDestinations.length; ++i){
                tmpTransfer = (MyTransferInfo) transferVector.get(i);
                theseSources[i] = tmpTransfer.getSource();
                theseDestinations[i] = tmpTransfer.getDestination();
                theseTransfers[i] = tmpTransfer;
              }
              try{
                if(!GridPilot.getClassMgr().getFTPlugin(pluginName).checkURLs(
                    theseSources, theseDestinations)){
                  brokenOut = true;
                  Debug.debug("Breaking because of non-uniform URLs", 2);
                  break;
                }
              }
              catch(Exception e){
                brokenOut = true;
                Debug.debug("Problem checking URLs", 2);
                logFile.addMessage("WARNING: there was a problem queueing all " +
                      "transfers.", e);
                e.printStackTrace();
                toSubmitTransfers.removeAllElements();
                break;
              }
              
              MyTransferInfo transfer = (MyTransferInfo) toSubmitTransfers.remove(0);
              //statusBar.setLabel("Queueing "+transfer.getSourceURL()+"->"+
              //    transfer.getDestination().getURL());
              submittingTransfers.add(transfer);
              Debug.debug("Removed "+transfer.getSourceURL(), 3);
              Debug.debug("with id "+transfer.getTransferID(), 3);
              Debug.debug("Now toSubmitTransfers has "+toSubmitTransfers.size()+" elements", 3);
              Debug.debug("Now submittingTransfers has "+submittingTransfers.size()+" elements", 3);
            }
          }
          catch(Exception e){
            toSubmitTransfers.removeAllElements();
            statusBar.setLabel("ERROR: queueing transfer(s) failed.");
            logFile.addMessage("ERROR: queueing transfer(s) failed:\n"+
                ((transferVector==null||transferVector.toArray()==null)?"":MyUtil.arrayToString(transferVector.toArray())), e);
            return;
          }
          
          // Remove the wrongly added last transfer from theseTransfers before submitting
          if(brokenOut && theseTransfers.length>1){
            MyTransferInfo [] tmpTransfers = new MyTransferInfo [theseTransfers.length-1];
            for(int i=0; i<theseTransfers.length-1; ++i){
              tmpTransfers[i] = theseTransfers[i];
            }
            theseTransfers = tmpTransfers;
          }
          final MyTransferInfo [] finalTransfers = theseTransfers;
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
  }
  
  /**
   * Reloads some values from configuration file. <p>
   */
  public void loadValues(){
    String tmp = configFile.getValue("File transfer systems", "maximum simultaneous running");
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
      logFile.addMessage(configFile.getMissingMessage("File transfer systems", "maximum simultaneous running") + "\n" +
                              "Default value = " + maxSimultaneousTransfers);
    tmp = configFile.getValue("File transfer systems", "time between transfers");
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
      logFile.addMessage(configFile.getMissingMessage("File transfer systems", "time between transfers") + "\n" +
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
    isRand = configFile.getValue("File transfer systems", "randomized transfers");
    Debug.debug("isRand = " + isRand, 2);
  }

  /**
   * This is used by the plugins for callcback, e.g.
   * by the SRM plugin for handling e.g. gsiftp TURLs
   * @param   srcUrls    Array of source URLs.
   * @param   destUrls   Array of destination URLs.
   */
  public String [] startCopyFiles(GlobusURL [] srcUrls, GlobusURL [] destUrls)
     throws Exception {

    for(int i=0; i<srcUrls.length; ++i){
      Debug.debug("Starting to copy files "+srcUrls[i]+" -> "+
          destUrls[i], 2);
    }
    
    String ftPluginName = null;
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
    if(ftPluginName==null){
      throw new IOException("ERROR: protocol not supported for " +
          MyUtil.arrayToString(srcUrls)+"->"+MyUtil.arrayToString(destUrls));
    }
    
    // TODO: caching: check if srcUrls have already been downloaded and
    // if they have changed
    
    // Start the transfers
    ids = GridPilot.getClassMgr().getFTPlugin(
        ftPluginName).startCopyFiles(srcUrls, destUrls);
    return ids;
  }
  
  /**
   * Adds transfers to toSubmitTransfers.
   * @param   transfers     Vector of MyTransferInfo's.
   */
  public void queue(final Vector _transfers) throws Exception {

    ResThread t = new ResThread(){
      Vector transfers;
      public void run(){
        // use status bar on monitoring frame
        statusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;
        if(isRand!=null && isRand.equalsIgnoreCase("yes")){
          transfers = MyUtil.shuffle(_transfers);
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

    if(!MyUtil.waitForThread(t, "", TRANSFER_SUBMIT_TIMEOUT, "transfer")){
      statusBar.setLabel("WARNING: queueing transfers timed out.");
      statusBar.stopAnimation();
    }
  }
  
  public void clearTableRows(int [] clearRows){
    Object [][] newTablevalues = new Object [tableValues.length-clearRows.length][GridPilot.transferStatusFields.length];
    MyTransferInfo transfer = null;
    MyTransferInfo [] toClearTransfers = new MyTransferInfo [clearRows.length];
    int rowNr = 0;
    int clearNr = 0;
    for(int i=0; i<tableValues.length; ++i){
      boolean match = false;
      for(int j=0; j<clearRows.length; ++j){
        if(clearRows[j]==i){
          match = true;
          break;
        }
      }
      transfer = TransferMonitoringPanel.getTransferAtRow(i);
      if(!match){
        for(int k=0; k<GridPilot.transferStatusFields.length; ++k){
          newTablevalues[rowNr][k] = tableValues[i][k];
        }
        transfer.setTableRow(rowNr);
        ++rowNr;
      }
      else{
        toClearTransfers[clearNr] = transfer;
        ++clearNr;
      }
    }

    for(int i=toClearTransfers.length-1; i>-1; --i){
      GridPilot.getClassMgr().getSubmittedTransfers().remove(toClearTransfers[i]);
      statusTable.removeRow(toClearTransfers[i].getTableRow());
    }

    tableValues = newTablevalues;
    try {
      ((DBVectorTableModel) ((MyJTable) statusTable).getModel()).setTable(tableValues, GridPilot.transferStatusFields);
    }
    catch(Exception e){
      e.printStackTrace();
      logFile.addMessage("WARNING: Could not clear rows "+MyUtil.arrayToString(clearRows), e);
    }
  }
  
  /**
   * Starts transfers using a given plugin
   * @param   ftPlugin    Name ofthe ft plugin.
   * @param   transfers    URL of the SRM server.
   */
  public void submit(String ftPlugin, MyTransferInfo [] transfers)
     throws Exception {

    if(isRand!=null && isRand.equalsIgnoreCase("yes")){
      transfers = (MyTransferInfo []) MyUtil.shuffle(transfers);
    }

    // use status bar on monitoring frame
    statusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;
    GlobusURL [] sources = new GlobusURL [transfers.length];
    GlobusURL [] destinations = new GlobusURL [transfers.length];
    String [] ids = null;
    
    Object [][] appendTablevalues = new Object [transfers.length][GridPilot.transferStatusFields.length];
    Object [][] newTablevalues = new Object [tableValues.length+appendTablevalues.length][GridPilot.transferStatusFields.length];
    int startRow = statusTable.getRowCount();
    boolean resubmit = false;

    for(int i=0; i<transfers.length; ++i){
      
      sources[i] = transfers[i].getSource();
      destinations[i] = transfers[i].getDestination();
            
      // This does not catch transfers that have not gotten an id
      /*Vector submittedTransfers = GridPilot.getClassMgr().getSubmittedTransfers();
      for(int j=0; j<submittedTransfers.size(); ++j){
        try{
          String id = ((MyTransferInfo) submittedTransfers.get(j)).getTransferID();
          if(transfers[i].getTransferID().equals(id)){
            resubmit = true;
            break;
          }
        }
        catch(Exception e){
        }
      }*/
      
      resubmit = (transfers[i].getInternalStatus()>-1);
      
      if(!resubmit){
        transfers[i].setTableRow(GridPilot.getClassMgr().getSubmittedTransfers().size());
        GridPilot.getClassMgr().getSubmittedTransfers().add(transfers[i]);
        // add to status table
        statusTable.createRows(GridPilot.getClassMgr().getSubmittedTransfers().size());
        
        for(int j=1; j<GridPilot.transferStatusFields.length; ++j){
          appendTablevalues[i][j] = statusTable.getValueAt(startRow+i, j);
        }
      }

      transfers[i].setInternalStatus(FileTransfer.STATUS_WAIT);

      statusTable.setValueAt(transfers[i].getSourceURL(),
          transfers[i].getTableRow(), MyTransferInfo.FIELD_SOURCE);
      statusTable.setValueAt(transfers[i].getDestination().getURL(),
          transfers[i].getTableRow(), MyTransferInfo.FIELD_DESTINATION);
      statusTable.setValueAt(iconSubmitting, transfers[i].getTableRow(),
          MyTransferInfo.FIELD_CONTROL);
      
    }
    
    if(!resubmit){
      // this fixes the problem with sorting
      System.arraycopy(tableValues, 0, newTablevalues, 0, tableValues.length);
      System.arraycopy(appendTablevalues, 0, newTablevalues, tableValues.length, appendTablevalues.length);
      tableValues = newTablevalues;
      Debug.debug("Setting table", 3);
      ((DBVectorTableModel) ((MyJTable) statusTable).getModel()).setTable(tableValues, GridPilot.transferStatusFields);
    }

    // find shortest list of source URLs
    int sourceListLen = -1;
    for(int i=0; i<transfers.length; ++i){
      if(sourceListLen==-1 || transfers[i].getSources().length<sourceListLen){
        sourceListLen = transfers[i].getSources().length;
      }
    }
    Debug.debug("Trying "+sourceListLen+" sources", 3);

    try{
      // try source URLs one by one
      for(int i=0; i<sourceListLen; ++i){
        Debug.debug("trial --->"+i, 3);

        try{          
          ids = GridPilot.getClassMgr().getFTPlugin(ftPlugin).startCopyFiles(
              sources, destinations);          
          if(ids.length!=transfers.length){
            throw new IOException("ERROR: returned number of transfer ids don't agree " +
                "with number of submitted tranfers: "+ids.length+"!="+transfers.length+
                ". Don't know what to do; cancelling all.");
          }
        }
        catch(Exception ee){
          // try on csc11.005145.PythiaZmumu.recon.AOD.v11004205
          ee.printStackTrace();
          // Make sure we don't try this source again... In fact we make sure
          // none of the sources of this batch are tried again. Perhaps a bit overkill...
          // If the reason is that a server is down it's fine, but if the reason is that
          // a single file is simply missing on this server, we could have gotten all the others.
          if(i<sourceListLen-1){
            for(int j=0; j<transfers.length; ++j){
              transfers[j].removeSource(transfers[j].getSource());
              MyUtil.setClosestSource(transfers[j]);
              Debug.debug("Retrying transfer, sources left: " +
                  transfers[j].getSources().length+
                  ". Closest source is now: "+
                  transfers[j].getSourceURL(), 2);
              logFile.addMessage("WARNING: retrying transfer, closest source is now: "+
                  transfers[j].getSourceURL(), ee);
            }
            for(int j=0; j<transfers.length; ++j){
              sources[j] = transfers[j].getSource();
              destinations[j] = transfers[j].getDestination();
            }
          }
          continue;
        }
        break;
      }
      
      if(ids==null){
        throw new IOException("Starting transfer failed for all transfers in this batch.");
      }

      String userInfo = GridPilot.getClassMgr().getFTPlugin(
          ftPlugin).getUserInfo();
      
      for(int i=0; i<transfers.length; ++i){
        
        transfers[i].setNeedsUpdate(true);
        transfers[i].setTransferID(ids[i]);
        
        statusTable.setValueAt(transfers[i].getTransferID(), transfers[i].getTableRow(),
            MyTransferInfo.FIELD_TRANSFER_ID);
        statusTable.setValueAt(userInfo, transfers[i].getTableRow(),
            MyTransferInfo.FIELD_USER);
        pbSubmission.setValue(pbSubmission.getValue() + 1);
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
              MyTransferInfo.FIELD_TRANSFER_ID);
          statusTable.setValueAt(transfers[i].getSourceURL(), transfers[i].getTableRow(),
              MyTransferInfo.FIELD_SOURCE);
          statusTable.setValueAt(transfers[i].getDestination().getURL(), transfers[i].getTableRow(),
              MyTransferInfo.FIELD_DESTINATION);
          transfers[i].setInternalStatus(FileTransfer.STATUS_ERROR);
          transfers[i].setNeedsUpdate(false);
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
        runningTransfers.add(transfers[i]);
        // remove iconSubmitting
        statusTable.setValueAt(null, transfers[i].getTableRow(), MyTransferInfo.FIELD_CONTROL);
      }
      
      if(!timer.isRunning()){
        timer.restart();
      }
      if(pbSubmission.getPercentComplete()==1.0){
        statusBar.removeProgressBar(pbSubmission);
        isProgressBarSet = false;
        pbSubmission.setMaximum(0);
        pbSubmission.setValue(0);
        //statusBar.setLabel("Queueing all transfers done.");
        GridPilot.getClassMgr().getStatusBar().setLabel("");
      }
    }
  };

  public String getStatus(String fileTransferID) throws Exception {
    String status = findFTPlugin(fileTransferID).getStatus(fileTransferID);
    // This means there's a TURL returned by the SRM plugin
    if(status!=null && status.matches("^\\w+://.*")){
      return "Ready";
    }
    else{
      return status;
    }
  }

  public String getFullStatus(String fileTransferID) throws Exception {
    return findFTPlugin(fileTransferID).getFullStatus(fileTransferID);
  }

  public int getPercentComplete(String fileTransferID) throws Exception {
    return findFTPlugin(fileTransferID).getPercentComplete(fileTransferID);
  }

  public long getBytesTransferred(String fileTransferID) throws Exception {
    return findFTPlugin(fileTransferID).getBytesTransferred(fileTransferID);
  }
  
  /**
   * Delete a list of files; local and/or remote. Local are handled
   * by LocalStaticShellMgr, remote by the available plugins.
   * @param toDeleteFiles URL strings spefifying the file locations.
   */
  public void deleteFiles(String [] toDeleteFiles){
    // get the list of remote files, ordered by protocol
    HashMap remoteFiles = new HashMap();
    String protocol = null;
    for(int i=0; i<toDeleteFiles.length; ++i){
      if(toDeleteFiles[i].matches("^\\w+:.*") &&
          !toDeleteFiles[i].toLowerCase().matches("\\w:.*") &&
          !toDeleteFiles[i].toLowerCase().startsWith("file:")){
        protocol = toDeleteFiles[i].replaceFirst("^(\\w+):.*", "$1");
        if(remoteFiles.get(protocol)==null){
          remoteFiles.put(protocol, new HashSet());
        }
        ((HashSet) remoteFiles.get(protocol)).add(toDeleteFiles[i]);
      }
    }
    // Delete local files
    for(int i=0; i<toDeleteFiles.length; ++i){
      if(!toDeleteFiles[i].matches("^\\w+:.*") ||
          toDeleteFiles[i].matches("\\w:.*") ||
          toDeleteFiles[i].toLowerCase().startsWith("file:")){
        try{
          LocalStaticShell.deleteFile(toDeleteFiles[i]);
        }
        catch(Exception e){
          logFile.addMessage("WARNING: Could not delete file "+
              toDeleteFiles[i]+". Please do so by hand.");
        }
      }
    }
    // Delete remote files
    GlobusURL [] remoteUrls = null;
    HashSet urlSet = null;
    int j = 0;
    for(Iterator it=remoteFiles.keySet().iterator(); it.hasNext();){
      urlSet = ((HashSet) remoteFiles.get(it.next()));
      remoteUrls = new GlobusURL [urlSet.size()];
      j = 0;
      for(Iterator itt=urlSet.iterator(); itt.hasNext(); ++j){
        try{
          remoteUrls[j] = new GlobusURL((String) itt.next());
        }
        catch(MalformedURLException e){
          e.printStackTrace();
        }
      }
      try{
        Debug.debug("Deleting "+MyUtil.arrayToString(remoteUrls), 2);
        deleteFiles(remoteUrls);
      }
      catch(Exception e){
        e.printStackTrace();
        logFile.addMessage("WARNING: Could not delete files "+
            MyUtil.arrayToString(remoteUrls)+". Please do so by hand.");
      }
    }
  }
  
  /**
   * Delete a list of files, using the available plugins.
   * @param urls array of URLs
   * @throws Exception
   */
  public void deleteFiles(GlobusURL [] urls) throws Exception {
    String ftPluginName = null;
    String [] fts = GridPilot.ftNames;
    
    // Construct dummy array of file URLs
    GlobusURL [] srcUrls = new GlobusURL[urls.length];
    for(int i=0; i<urls.length; ++i){
      srcUrls[i] = new GlobusURL("file:////dum");
    }
    // Select first plugin that supports the protocol of the these transfers
    for(int i=0; i<fts.length; ++i){
      Debug.debug("Checking plugin "+fts[i], 3);
      if(GridPilot.getClassMgr().getFTPlugin(fts[i]).checkURLs(srcUrls, urls)){
        ftPluginName = fts[i];
        Debug.debug("Selected plugin "+fts[i], 3);
        break;
      };      
    }
    if(ftPluginName==null){
      throw new IOException("ERROR: protocol not supported or plugin initialization failed.\n"+
          MyUtil.arrayToString(srcUrls)+"\n->\n"+MyUtil.arrayToString(urls));
    }
    
    // delete the files
    GridPilot.getClassMgr().getFTPlugin(
        ftPluginName).deleteFiles(urls);
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
      MyTransferInfo transfer = (MyTransferInfo) e.nextElement();
      statusTable.setValueAt("Transfer not queued (cancelled)!",
          transfer.getTableRow(), MyTransferInfo.FIELD_TRANSFER_ID);
      statusTable.setValueAt(transfer.getSourceURL(), transfer.getTableRow(),
          MyTransferInfo.FIELD_SOURCE);
      statusTable.setValueAt(transfer.getDestination().getURL(), transfer.getTableRow(),
          MyTransferInfo.FIELD_DESTINATION);
      transfer.setNeedsUpdate(false);
    }
    toSubmitTransfers.removeAllElements();
    statusBar.removeProgressBar(pbSubmission);
    pbSubmission.setMaximum(0);
    pbSubmission.setValue(0);
  }

  /**
   * Just cancels the transfer with the corresponding plugin
   */
  public void cancel(final String fileTransferID) throws Exception{
    findFTPlugin(fileTransferID).cancel(fileTransferID);
  }
  
  /**
   * Cancels a Vector of transfers.
   * @param transfers Vector of MyTransferInfo's
   */
  public void cancel(final Vector transfers){
    ResThread t = new ResThread(){
      MyTransferInfo transfer = null;
      public void run(){
        // use status bar on monitoring frame
        StatusBar statusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;
        statusBar.animateProgressBar();
        statusBar.setLabel("Cancelling...");
        Vector submittedTransfers = GridPilot.getClassMgr().getSubmittedTransfers();
        try{
          for(int i=0; i<transfers.size(); ++i){
            try{             
              transfer = (MyTransferInfo) transfers.get(i);
              GridPilot.getClassMgr().getTransferControl().runningTransfers.remove(transfer);
              // skip if not running
              boolean isRunning = false;
              for(int j=0; j<submittedTransfers.size(); ++j){
                try{
                  String id = ((MyTransferInfo) submittedTransfers.get(j)).getTransferID();
                  if(transfer.getTransferID().equals(id)){
                    isRunning = true;
                    break;
                  }
                }
                catch(Exception e){
                }
              }
              if(!isRunning){
                continue;
              }
              Debug.debug("Cancelling transfer "+transfer.getTransferID(), 2);
              FileTransfer ft = findFTPlugin(transfer.getTransferID());
              ft.cancel(transfer.getTransferID());
              String status = "Cancelled";
              GridPilot.getClassMgr().getTransferStatusTable().setValueAt(
                  status, transfer.getTableRow(), MyTransferInfo.FIELD_STATUS);
              transfer.setStatus(status);
              transfer.setInternalStatus(FileTransfer.STATUS_ERROR);
              transfer.setNeedsUpdate(false);
            }
            catch(Exception ee){
              Debug.debug("WARNING: Could not cancel transfer "+transfer.getTransferID(), 2);
              ee.printStackTrace();
            }
          }        
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " +
                             " for download", t);
        }
        statusBar.stopAnimation();
        statusBar.setLabel("Cancelling done.");
      }
    };

    t.start();

    if(!MyUtil.waitForThread(t, "", TRANSFER_CANCEL_TIMEOUT, "transfer")){
      StatusBar statusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;
      statusBar.stopAnimation();
      statusBar.setLabel("WARNING: cancel transfers timed out.");
    }
  }
  
  public void resubmit(final Vector transfers){
    ResThread t = new ResThread(){
      MyTransferInfo transfer = null;
      public void run(){
        // use status bar on monitoring frame
        StatusBar statusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;
        statusBar.animateProgressBar();
        statusBar.setLabel("Cancelling...");
        Vector submittedTransfers = GridPilot.getClassMgr().getSubmittedTransfers();
        try{
          GlobusURL [] srcUrls = new GlobusURL [transfers.size()];
          GlobusURL [] destUrls = new GlobusURL [transfers.size()];
          for(int i=0; i<transfers.size(); ++i){
            try{             
              transfer = (MyTransferInfo) transfers.get(i);
              // abort if a job is running
              boolean isRunning = false;
              for(int j=0; j<submittedTransfers.size(); ++j){
                try{
                  String id = ((MyTransferInfo) submittedTransfers.get(j)).getTransferID();
                  if(transfer.getTransferID().equals(id)){
                    if(isRunning(transfer)){
                      isRunning = true;
                      break;
                    }
                  }
                }
                catch(Exception e){
                }
              }
              if(isRunning){
                throw new IOException("Cannot requeue running transfers.");
              }
              Debug.debug("Requeueing transfer "+transfer.getTransferID(), 2);
              String status = "Wait";
              GridPilot.getClassMgr().getTransferStatusTable().setValueAt(
                  status, transfer.getTableRow(), MyTransferInfo.FIELD_STATUS);
              transfer.setStatus(status);
              transfer.setInternalStatus(FileTransfer.STATUS_WAIT);
              transfer.setNeedsUpdate(true);
              srcUrls[i] = transfer.getSource();
              destUrls[i] = transfer.getDestination();
            }
            catch(Exception ee){
              Debug.debug("WARNING: Could not cancel transfer "+transfer.getTransferID(), 2);
              ee.printStackTrace();
            }
          }
          queue(transfers);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " +
                             " for download", t);
        }
        statusBar.stopAnimation();
        statusBar.setLabel("Resubmitting done.");
      }
    };

    t.start();

    if(!MyUtil.waitForThread(t, "", TRANSFER_CANCEL_TIMEOUT, "transfer")){
      StatusBar statusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;
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
    String [] checkArr = MyUtil.split(transferID, "::");
    if(checkArr[0].indexOf("-")>0){
      String [] arr = MyUtil.split(checkArr[0], "-");
      ftPluginName = arr[0];
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
  
  /**
   * Find the plugin that's actually carrying out the work. E.g. for srm this will
   * typically be gsiftp.
   * @param   transferID    Transfer ID. Format:
   *                        "protocol-(get|put|copy):...:srcTURL destTURL [SURL]".
   */
  /*public static FileTransfer findRealTransferPlugin(String transferID)
     throws Exception{
    
    Debug.debug("Finding plugin for transfer "+transferID, 2);
    String ftPluginName = null;
    String [] checkArr = Util.split(transferID, "::");
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
        Debug.debug("type: "+type, 3);
        int tmpindex = 0;
        String tmpID = transferID;
        String [] fts = GridPilot.ftNames;
        while(tmpID.indexOf("::")>0){
          tmpindex += tmpID.indexOf("::") + 2;
          tmpID = transferID.substring(tmpindex);
        }
        turls = Util.split(tmpID);
        for(int i=0; i<fts.length; ++i){
          if(GridPilot.getClassMgr().getFTPlugin(
              fts[i]).checkURLs(
                  new GlobusURL [] {new GlobusURL(turls[0].replaceAll("'", ""))},
                  new GlobusURL [] {new GlobusURL(turls[1].replaceAll("'", ""))})){
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
  }*/
  
  public boolean isSubmitting(){
    //return timer.isRunning();
    Debug.debug("submittingTransfers: "+submittingTransfers.size(), 3);
    Debug.debug("toSubmitTransfers: "+toSubmitTransfers.size(), 3);
    return !(submittingTransfers.isEmpty() && toSubmitTransfers.isEmpty());
  }
  
  /**
   * Checks the status of the transfers and updates the MyTransferInfo objects. <p>
   */
  public void updateStatus(Vector transfers){
    String status = null;
    String transferred = null;
    int percentComplete = -1;
    MyTransferInfo transfer = null;
    String id = null;
    int internalStatus = -1;
    for(Iterator it=transfers.iterator(); it.hasNext();){
      transfer = (MyTransferInfo) it.next();
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
          percentComplete = getPercentComplete(id);
          transferred = Integer.toString(percentComplete);
          Debug.debug("Got transferred: "+transferred, 2);
          transfer.setTransferred(transferred+"%");
        }
        catch(Exception e){
        }
        if(transferred==null || percentComplete<0 || percentComplete>100){
          Debug.debug("Could not understand transferred = "+transferred, 3);
          transferred = Long.toString(getBytesTransferred(id)/1000);
          Debug.debug("Got transferred: "+transferred, 2);
          transfer.setTransferred(transferred+" kB");
        }
        
        internalStatus = getInternalStatus(id, status);
        transfer.setInternalStatus(internalStatus);
      }
      catch(Exception e){
        Debug.debug("WARNING: could not get status of "+id+
            ". skipping. "+e.getMessage(), 3);
        e.printStackTrace();
        continue;
      }
    }
  }
  
  /**
   *  Find the GridPilot-specific internal status from the corresponding plugin.
   */
  public int getInternalStatus(String id, String status) throws Exception{
    return findFTPlugin(id).getInternalStatus(id, status);
  }
  
  /**
   *  Take some action on transfer failure, like asking to delete partly copied
   *  target file or retry...
   */
  public static void transferFailure(MyTransferInfo transfer){
    // TODO: retry on transfer failure
    GridPilot.getClassMgr().getGlobalFrame(
        ).monitoringPanel.statusBar.setLabel("Transfer "+transfer.getTransferID()+" failed");
  }

  /**
   * Call plugins to clean up all running transfers. We keep no persistent table of
   * transfers. So, all get/put transfers will die when we exit and running
   * transfers should be cancelled before exiting.
   */
  public void exit(){
    Vector submittedTransfers = GridPilot.getClassMgr().getSubmittedTransfers();
    for(int i=0; i<submittedTransfers.size(); ++i){
      try{
        String id = ((MyTransferInfo) submittedTransfers.get(i)).getTransferID();
        Debug.debug("Cancelling "+id, 3);
        findFTPlugin(id).cancel(id);
      }
      catch(Exception e){
        continue;
      }
    }
  }

    /**
   * Take some action on successful transfer completion,
   * like registering the new location.
   */
  public void transferDone(MyTransferInfo transfer){
    GridPilot.getClassMgr().getTransferControl().runningTransfers.remove(transfer);
    // Do plugin-specific finalization. E.g. for SRM, set the status to Done.
    String id = null;
    try{
      id = transfer.getTransferID();
      findFTPlugin(transfer.getTransferID()).finalize(transfer.getTransferID());
    }
    catch(Exception e){
      logFile.addMessage("WARNING: finalize could not " +
          "be done for transfer "+id, e);
    }
    // If transfer.getDBPluginMgr() is not null, it is the convention that this
    // DBPluginMgr is used for registering the file.
    if(transfer.getDBPluginMgr()!=null && transfer.getDestination()!=null){
      String destination = transfer.getDestination().getURL();
      String lfn = null;
      try{
        lfn= transfer.getLFN();
      }
      catch(Exception e){
      }
      if(lfn==null || lfn.equals("")){
        logFile.addMessage("WARNING: LFN not found. "+
            "This file will be named after the physical file and may not keep its name in the file catalog.");
        int lastSlash = destination.lastIndexOf("/");
        lfn = destination;
        if(lastSlash>-1){
          lfn = destination.substring(lastSlash + 1);
        }
      }
      String guid = null;
      try{
        guid= transfer.getGUID();
      }
      catch(Exception e){
      }
      if(guid==null || guid.equals("")){
        logFile.addMessage("WARNING: GUID not found. "+
            "This file, "+lfn+", will not keep its GUID - a new one will be generated.");
        String uuid = UUIDGenerator.getInstance().generateTimeBasedUUID().toString();
        String message = "Registering UUID "+uuid.toString()+" and LFN "+lfn+
           " for new location "+transfer.getDestination();
        GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar.setLabel(message);
        Debug.debug(message, 2);
        guid = uuid;
      }
      String datasetID = null;
      try{
        datasetID= transfer.getDatasetID();
      }
      catch(Exception e){
      }
      if(datasetID==null || datasetID.equals("")){
        logFile.addMessage("WARNING: no dataset found. "+
            "This file, "+lfn+", will NOT be registered in dataset catalog, only in file catalog.");
      }
      String datasetName = null;
      try{
        datasetName= transfer.getDatasetName();
      }
      catch(Exception e){
      }
      if(datasetName==null || datasetName.equals("")){
        logFile.addMessage("WARNING: dataset name not found. "+
            "This file, "+lfn+", may not keep its dataset association.");
      }
      String fileBytes = transfer.getBytes();
      String checksum = transfer.getChecksum();
      // lookup size the hard way
      if(fileBytes==null || fileBytes.equals("") || fileBytes.equals("-1")){
        // TODO generalize beyound gsiftp and https
        try{
          if(destination.startsWith("https:") || destination.startsWith("gsiftp:")){
            GlobusURL globusUrl = new GlobusURL(destination);
            fileBytes = Long.toString(GridPilot.getClassMgr().getFTPlugin(
                globusUrl.getProtocol()).getFileBytes(globusUrl));
          }
          else if(!MyUtil.urlIsRemote(destination)){
            File fil = new File(MyUtil.clearTildeLocally(MyUtil.clearFile(destination)));
            fileBytes = Long.toString(fil.length());
          }
        }
        catch(Exception e){
        }
      }
      // If there's no checksum, calculate the md5sum
      if((checksum==null || checksum.equals("") || checksum.equals("-1")) &&
          !MyUtil.urlIsRemote(destination)){
        try{
          AbstractChecksum cs = JacksumAPI.getChecksumInstance("md5");
          cs.update(LocalStaticShell.readFile(
              MyUtil.clearTildeLocally(MyUtil.clearFile(destination))).getBytes());
          checksum = "md5:"+cs.getFormattedValue();
        }
        catch(Exception e){
        }
      }
      // Now register the file
      try{
        transfer.getDBPluginMgr().registerFileLocation(
            datasetID, datasetName, guid, lfn, destination, fileBytes, checksum, false);
      }
      catch(Exception e){
        logFile.addMessage(
            "ERROR: could not register "+destination+" for file "+
            lfn+" in dataset "+datasetName, e);
        GridPilot.getClassMgr().getGlobalFrame(
        ).monitoringPanel.statusBar.setLabel("ERROR: could not register "+destination);
      }
      GridPilot.getClassMgr().getGlobalFrame(
         ).monitoringPanel.statusBar.setLabel("Registration done");
    }
    else{
      GridPilot.getClassMgr().getGlobalFrame(
      ).monitoringPanel.statusBar.setLabel("Copying done");
    }
  }
  
  public static boolean isRunning(MyTransferInfo transfer){
    int internalStatus = transfer.getInternalStatus();
    if(internalStatus==FileTransfer.STATUS_WAIT ||
        internalStatus==FileTransfer.STATUS_RUNNING){
      return true;
    }
    else{
      return false;
    }
  }

  /**
   * Download file from URL. Quick method - bypassing transfer control and monitoring.
   * @param url
   * @param fileName
   * @param destination    can be either a directory or a file
   * @throws IOException
   */
  public void download(final String url, File destination) throws IOException{
    try{
      
      if(url==null || url.endsWith("/")|| destination==null){
        throw new IOException("ERROR: source or destination not given. "+
            url+":"+destination);
      }      
      
      File downloadDir = null;
      String destFileName = null;
      String srcUrlDir = null;
      String srcFileName = null;
      
      int lastSlash = url.lastIndexOf("/");
      srcUrlDir = url.substring(0, lastSlash + 1);
      srcFileName = url.substring(lastSlash + 1);
     
      if(destination.isDirectory()){
        downloadDir = destination;
        destFileName = url.substring(lastSlash + 1);
      }
      else{
        downloadDir = destination.getParentFile();
        destFileName = destination.getName();
      }
      
      GridPilot.getClassMgr().getStatusBar().setLabel("Downloading "+srcFileName+" from "+srcUrlDir+
          " to "+downloadDir);

      Debug.debug("Downloading file from "+srcUrlDir, 3);
      // local directory
      if(!MyUtil.urlIsRemote(srcUrlDir)/*srcUrlDir.startsWith("file:")*/){
        String fsPath = MyUtil.clearTildeLocally(MyUtil.clearFile(url));
        Debug.debug("Downloading file to "+downloadDir.getAbsolutePath(), 3);        
        if(fsPath==null || downloadDir==null){
          throw new IOException("ERROR: source or destination directory not given. "+
              fsPath+":"+downloadDir);
        }        
        localCopy(new File(fsPath),
            (new File(downloadDir, destFileName)).getAbsolutePath());
        GridPilot.getClassMgr().getStatusBar().setLabel(fsPath+destFileName+" copied");
      }
      // remote gsiftp or https/webdav directory
      else if(srcUrlDir.startsWith("gsiftp://") || srcUrlDir.startsWith("https://") ||
          srcUrlDir.startsWith("sss://")){
        Debug.debug("Downloading to "+downloadDir.getAbsolutePath(), 3);        
        Debug.debug("Downloading "+destFileName+" from "+srcUrlDir, 3);
        GlobusURL globusUrl = new GlobusURL(url);
        FileTransfer fileTransfer =
           GridPilot.getClassMgr().getFTPlugin(url.replaceFirst("^(\\w+):/.*", "$1"));
        try {
          fileTransfer.getFile(globusUrl, destination/*dName*/);
        }
        catch (Exception e) {
          e.printStackTrace();
          throw new IOException(e.getCause());
        }
        GridPilot.getClassMgr().getStatusBar().setLabel(url+" downloaded");
      }
      else if(srcUrlDir.startsWith("http://")){
        Debug.debug("Downloading file to "+downloadDir.getAbsolutePath(), 3);        
        String dirPath = downloadDir.getAbsolutePath();
        if(!dirPath.endsWith("/") && !destFileName.startsWith("/")){
          dirPath = dirPath+"/";
        }
        Debug.debug("Downloading from "+url+" to "+dirPath+destFileName, 2);
        final String fName = destFileName;
        final String dName = dirPath;
        InputStream is = (new URL(url)).openStream();
        DataInputStream dis = new DataInputStream(new BufferedInputStream(is));
        FileOutputStream os = new FileOutputStream(new File(dName+fName));
        // read data in chunks of 1 kB
        byte [] b = new byte[1024];
        int len = -1;
        while(true){
          len = is.read(b, 0, b.length);
          if(len<0){
            break;
          }
          os.write(b, 0, len);
        }
        dis.close();
        is.close();
        os.close();
        GridPilot.getClassMgr().getStatusBar().setLabel(url+" downloaded");
      }
      else{
        throw(new IOException("Unknown protocol for "+url));
      }
    }
    catch(IOException e){
      Debug.debug("Could not get URL "+url+". "+e.getMessage(), 1);
      e.printStackTrace();
      throw e;
    }   
  }

  /**
   * Upload file to URL.
   * @param uploadUrl the destination; can be either a directory or a file name
   * @param file
   * @throws IOException
   * @throws FTPException
   */
  public void upload(File file, final String uploadUrl) throws Exception, FTPException{
    String uploadUrlDir = null;
    String uploadFileName = null;
    
    if(uploadUrl.endsWith("/")){
      uploadUrlDir = uploadUrl;
      uploadFileName = file.getName();
    }
    else if(uploadUrl.endsWith("\\")){
      uploadUrlDir = uploadUrl;
      uploadFileName = file.getName();
    }
    if(uploadUrlDir==null){
      int lastSlash1 = uploadUrl.lastIndexOf("/");
      int lastSlash2 = uploadUrl.lastIndexOf("\\");
      int lastSlash = lastSlash2>lastSlash1 ? lastSlash2 : lastSlash1;
      if(lastSlash>-1){
        uploadFileName = uploadUrl.substring(lastSlash + 1);
        uploadUrlDir = uploadUrl.substring(0, lastSlash + 1);
      }
    }
    if(uploadUrlDir==null){
      throw new IOException("Could not get upload directory from "+uploadUrl);
    }
    
    Debug.debug("Uploading file "+file+" to directory "+uploadUrlDir, 3);
    // local directory
    if(!MyUtil.urlIsRemote(uploadUrlDir)/*uploadUrlDir.startsWith("file:")*/){
      String fsPath = MyUtil.clearTildeLocally(MyUtil.clearFile(uploadUrl));
      Debug.debug("Local directory path: "+fsPath, 3);        
      if(fsPath==null || uploadFileName==null || file==null){
        throw(new IOException("ERROR: fsPath==null || uploadFileName==null || file==null"));
      }
      localCopy(file, fsPath);
    }
    // remote directory
    else if(uploadUrlDir.startsWith("gsiftp://") || uploadUrlDir.startsWith("https") ||
       uploadUrlDir.startsWith("sss")){
      if(!uploadUrlDir.endsWith("/")){
        throw(new IOException("Upload location must be a directory. "+uploadUrl));
      }
      GlobusURL globusUrl = new GlobusURL(uploadUrlDir+uploadFileName);
      FileTransfer fileTransfer = (FileTransfer) GridPilot.getClassMgr(
          ).getFTPlugin(uploadUrlDir.replaceFirst("^(\\w+):/.*", "$1"));
      fileTransfer.putFile(file, globusUrl);
    }
    else{
      throw(new IOException("Unknown protocol for "+uploadUrl));
    }
  }
  
  /**
   * Copy file to a file or directory fsPath.
   */
  private static void localCopy(File file, String fsPath) throws IOException{
    String fileName = file.getName();
    if(LocalStaticShell.isDirectory(fsPath)){
      fsPath = (new File(MyUtil.clearTildeLocally(MyUtil.clearFile(fsPath)), fileName)).getAbsolutePath();
    }
    if(!LocalStaticShell.copyFile(file.getAbsolutePath(), fsPath)){
      throw new IOException(file.getAbsolutePath()+
          " could not be copied to "+fsPath+fileName);
    }
    // if we don't get an exception, the file got written...
    Debug.debug("File "+file.getAbsolutePath()+" written to " +
        fsPath+fileName, 2);
  }
  
  /**
   * Method for copying input files from the local hard disk (file://..) or a remote location
   * to the run directory of the job, using the ShellMgr of the job.
   */
  public boolean copyInputFile(String src, String dest, Shell shellMgr,  boolean overWrite, String error){
    
    // First check if the file has already been copied
    if(!overWrite){
      if(shellMgr!=null){
        if(shellMgr.existsFile(Util.clearFile(dest))){
          Long destSize = null;
          try{
            destSize = shellMgr.getSize(Util.clearFile(dest));
          }
          catch(IOException e){
            e.printStackTrace();
          }
          if(destSize!=null && src.length()==destSize){    
            Debug.debug("file " + src + " already there. NOT uploading.", 2);
            return true;
          }
        }
      }
      else{
        File destFile = new File(Util.clearTildeLocally(Util.clearFile(dest)));
        if(destFile.exists()){
          if(destFile.length()==src.length()){
            Debug.debug("file " + src + " already there. NOT uploading.", 2);
            return true;
          }        
        }        
      }
    }

    if(error==null){
      error = new String();
    }
    
    // If the shell is not local, first get the source to a local temp file and change dest
    // temporarily
    // TODO: consider using caching
    String tempFileName = dest;
    if(!shellMgr.isLocal()){
      File tempFile;
      try{
        tempFile=File.createTempFile("GridPilot-", "");
        tempFileName = "file:"+tempFile.getAbsolutePath();
        tempFile.delete();
        //  hack to have the file deleted on exit
        GridPilot.tmpConfFile.put(tempFileName, new File(tempFileName));
      }
      catch(IOException e){
        e.printStackTrace();
      }
    }
    
    // Local src
    if(/*Linux local file*/(src.matches("^file:~[^:]*") || src.matches("^file:/[^:]*") || src.startsWith("/") || src.startsWith("~")) ||
        /*Windows local file*/(src.matches("\\w:.*") || src.matches("^file:/*\\w:.*")) && shellMgr.isLocal()){
      try{
        if(!LocalStaticShell.existsFile(MyUtil.clearFile(src))){
          error = "File " + src + " doesn't exist";
          logFile.addMessage(error);
          return false;
        }
      }
      catch(Throwable e){
        error = "ERROR checking for "+src+": "+e.getMessage();
        Debug.debug(error, 2);
        logFile.addMessage(error);
        return false;
      }
      // by convention, if the destination starts with file:, we get the file to the local disk
      if(tempFileName.startsWith("file:") && !shellMgr.isLocal()){
        Debug.debug("getting " + src + " -> " + tempFileName, 2);
        try{
          String realDest = MyUtil.clearTildeLocally(MyUtil.clearFile(tempFileName));
          File realParentDir = (new File(realDest)).getParentFile();
          Debug.debug("real locations: " + src + " -> " + realDest, 2);
          // Check if parent dir exists, create if not
          if(!realParentDir.exists()){
            try{
              if(!realParentDir.mkdir()){
                throw new Exception("Error creating directory "+realParentDir.getAbsolutePath());
              }
            }
            catch(Exception e){
              error = "ERROR: could not create directory for job outputs.";
              logFile.addMessage(error, e);
              return false;
            }
          }
          if(!LocalStaticShell.copyFile(MyUtil.clearFile(src), realDest)){
            error = "Cannot get \n\t" +
            src + "\n to \n\t" + dest;
            logFile.addMessage(error);
            return false;
          }
        }
        catch(Throwable e){
          error = "ERROR getting "+src+": "+e.getMessage();
          Debug.debug(error, 2);
          logFile.addMessage(error);
          return false;
        }
      }
      // if we have a fully qualified name (and for Windows a local shell), we just copy it with the shellMgr
      else{
        Debug.debug("copying " + src + " to " + tempFileName, 2);
        try{
          if(!shellMgr.copyFile(MyUtil.clearFile(src), tempFileName)){
            error = "Cannot copy \n\t" + src +
            "\n to \n\t" + tempFileName;
            logFile.addMessage(error);
            return false;
          }
        }
        catch(Throwable e){
          error = "ERROR copying "+src+" -> "+tempFileName+": "+e.getMessage();
          Debug.debug(error, 2);
          logFile.addMessage(error);
          return false;
        }
      }
    }
    // Remote src
    else if(MyUtil.urlIsRemote(src)){
      try{
        download(
            src,
            new File(MyUtil.clearTildeLocally(MyUtil.clearFile(tempFileName))));
      }
      catch(Exception e){
        error = "ERROR copying "+src+" -> "+tempFileName+": "+e.getMessage();
        Debug.debug(error, 2);
        logFile.addMessage(error);
        return false;
      }
    }
    // relative paths or getting files (via ssh) from a Windows server is not supported
    else{
      error = "ERROR copying : unqualified paths or putting files on a " +
          "Windows server is not supported. "+src;
      logFile.addMessage(error);
      return false;
    }
    
    // if a temp file was written, copy it to the real remote destination via ssh
    if(!tempFileName.equals(dest)){
      try{
        Debug.debug("Uploading tmp file "+tempFileName+" -> "+dest, 2);
        shellMgr.upload(MyUtil.clearFile(tempFileName), dest);
      }
      catch(Exception e){
        error = "ERROR copying "+tempFileName+" -> "+dest+": "+e.getMessage();
        Debug.debug(error, 2);
        logFile.addMessage(error);
        return false;
      }
    }
    
    return true;
  }

  /**
   * Method for copying output files from the run directory of the job to a local or
   * remote destination, using the ShellMgr of the job.
   * 
   * NOTICE: too messy - needs a complete rewrite
   * 
   *  @param src URL of the source file
   *  @param dest URL of the destination file
   *  @param shellMgr ShellMgr object to be used to copy the file
   *         from its source to a local cache. If it is null, it
   *         is assumed that the source is local
   *  @param error string object that will be set to the error string in case an error occurs
   */
  public boolean copyOutputFile(String src, String dest, Shell shellMgr, String error){
    
    if(error==null){
      error = new String();
    }

    // If the shell is not local, first get the source to a local temp file and change src
    String tempFileName = null;
    if(!shellMgr.isLocal()){
      File tempFile;
      try{
        tempFile=File.createTempFile("GridPilot-", "");
        tempFileName = tempFile.getAbsolutePath();
        tempFile.delete();
        shellMgr.download(src, tempFileName);
        src = tempFileName;
        //  hack to have the file deleted on exit
        GridPilot.tmpConfFile.put(tempFileName, new File(tempFileName));
      }
      catch(IOException e){
        e.printStackTrace();
      }
    }
    
    // Local destination
    if(/*Linux local file*/(dest.matches("^file:~[^:]*") || dest.matches("^file:/[^:]*") || dest.startsWith("/")) ||
        /*Windows local file*/(dest.matches("\\w:.*") || dest.matches("^file:/*\\w:.*")) && shellMgr.isLocal()){
      try{
        if(!shellMgr.existsFile(src)){
          error = "File " + src + " doesn't exist";
          logFile.addMessage(error);
          return false;
        }
      }
      catch(Throwable e){
        error = "ERROR checking for "+src+": "+e.getMessage();
        Debug.debug(error, 2);
        logFile.addMessage(error);
        return false;
      }
      // by convention, if the destination starts with file:, we get the file to the local disk
      if(dest.startsWith("file:") && !shellMgr.isLocal()){
        Debug.debug("putting " + src + " -> " + dest, 2);
        try{
          String realDest = MyUtil.clearTildeLocally(MyUtil.clearFile(dest));
          File realParentDir = (new File(realDest)).getParentFile();
          Debug.debug("real locations: " + src + " -> " + realDest, 2);
          // Check if parent dir exists, create if not
          if(!realParentDir.exists()){
            try{
              if(!realParentDir.mkdir()){
                throw new Exception("Error creating directory "+realParentDir.getAbsolutePath());
              }
            }
            catch(Exception e){
              error = "ERROR: could not create directory for job outputs.";
              logFile.addMessage(error, e);
              return false;
            }
          }
          if(!shellMgr.download(src, realDest)){
            error = "Cannot get \n\t" +
            src + "\n to \n\t" + dest;
            logFile.addMessage(error);
            return false;
          }
        }
        catch(Throwable e){
          error = "ERROR getting "+src+": "+e.getMessage();
          Debug.debug(error, 2);
          logFile.addMessage(error);
          return false;
        }
      }
      // if we have a fully qualified name (and for Windows a local shell), we just copy it with the shellMgr
      else{
        Debug.debug("copying " + src + " to " + dest, 2);
        try{
          if(!shellMgr.copyFile(src, dest)){
            error = "Cannot copy \n\t" + src +
            "\n to \n\t" + dest;
            logFile.addMessage(error);
            return false;
          }
        }
        catch(Throwable e){
          error = "ERROR copying "+src+" -> "+dest+": "+e.getMessage();
          Debug.debug(error, 2);
          logFile.addMessage(error);
          return false;
        }
      }
    }
    // Remote destination
    else if(MyUtil.urlIsRemote(dest)){
      try{
        upload(
            new File(MyUtil.clearTildeLocally(MyUtil.clearFile(src))),
            dest);
      }
      catch(Exception e){
        error = "ERROR copying "+src+" -> "+dest+": "+e.getMessage();
        Debug.debug(error, 2);
        logFile.addMessage(error);
        return false;
      }
      try{
        // clean up the tmp file (is also done on exit, just in case)
        LocalStaticShell.deleteFile(tempFileName);
      }
      catch(Exception e){
      }
    }
    // relative paths or copying files (via ssh) to a Windows server is not supported
    else{
      error = "ERROR copying : unqualified paths or putting files on a " +
          "Windows server is not supported. "+dest;
      logFile.addMessage(error);
      return false;
    }
    return true;
  }
  
}