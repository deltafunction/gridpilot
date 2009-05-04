package gridpilot;

import gridfactory.common.ConfigFile;
import gridfactory.common.DBVectorTableModel;
import gridfactory.common.Debug;
import gridfactory.common.FileTransfer;
import gridfactory.common.LocalStaticShell;
import gridfactory.common.ResThread;
import gridfactory.common.StatusBar;
import gridfactory.common.TransferControl;
import gridfactory.common.TransferInfo;
import gridfactory.common.Util;
import gridfactory.common.https.HTTPSFileTransfer;

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
import javax.swing.SwingUtilities;
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
  private HashMap<String, String> serverPluginMap;
  private String isRand = null;

  private static int TRANSFER_SUBMIT_TIMEOUT = 60*1000;
  private static int TRANSFER_CANCEL_TIMEOUT = 60*1000;
  private Object [][] tableValues = new Object[0][GridPilot.TRANSFER_STATUS_FIELDS.length];
  
  final private static String SRM2_PLUGIN_NAME = "srm2";
  
  public MyTransferControl() throws Exception{
    // We set getFileTransfer() to return the HTTPS FileTransfer.
    super(GridPilot.getClassMgr().getFTPlugin("https"),
        GridPilot.getClassMgr().getLogFile(),
        GridPilot.getClassMgr().getTransferStatusTable(),
        /*GridPilot.getClassMgr().getStatusBar()*/null);
    configFile = GridPilot.getClassMgr().getConfigFile();
    serverPluginMap = new HashMap<String, String>();
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

          GlobusURL firstSrc = ((TransferInfo) toSubmitTransfers.get(0)).getSource();
          GlobusURL firstDest = ((TransferInfo) toSubmitTransfers.get(0)).getDestination();
          
          Debug.debug("First transfer: "+((TransferInfo) toSubmitTransfers.get(0)), 3);
          
          String pluginName = null;
          TransferInfo [] theseTransfers = null;
          
          try{
            pluginName = findPlugin(firstSrc, firstDest);
            // Select first plugin that supports the protocol of the next transfer
            if(pluginName==null){
              throw new IOException("ERROR: protocol not supported or plugin initialization " +
                  "failed.\n"+firstSrc+"\n->\n\n"+firstDest);
            }
            Debug.debug("plugin selected: "+pluginName, 3);
            
            theseTransfers = prepareTransfers(pluginName);
            
          }
          catch(Exception e){
            toSubmitTransfers.removeAllElements();
            setMonitorStatus("ERROR: queueing transfer(s) failed.");
            logFile.addMessage("ERROR: queueing transfer(s) failed:\n"+
                ((toSubmitTransfers==null||toSubmitTransfers.toArray()==null)?"":
                  MyUtil.arrayToString(toSubmitTransfers.toArray())), e);
            return;
          }
          
          final TransferInfo [] finalTransfers = theseTransfers;
          final String finalPluginName = pluginName;
          
          new Thread(){
            public void run(){
              try{
                submit(finalPluginName, finalTransfers);
              }
              catch(Exception e){
                setMonitorStatus("ERROR: starting transfer(s) failed. "+
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
  
  private String findPlugin(GlobusURL firstSrc, GlobusURL firstDest) {
    String [] fts = GridPilot.FT_NAMES;
    String pluginName = null;
    GlobusURL [] checkSources = null;
    GlobusURL [] checkDestinations = null;

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
    return pluginName;
  }
  
  private TransferInfo[] prepareTransfers(String pluginName){

    Vector transferVector = null;
    GlobusURL [] theseSources = null;
    GlobusURL [] theseDestinations = null;
    TransferInfo [] theseTransfers = null;
    
    boolean brokenOut = false;
    // Transfer jobs from toSubmitTransfers to submittingTransfers.
    // First construct uniform batch.
    transferVector = new Vector();
    while(runningTransfers.size()+submittingTransfers.size()<maxSimultaneousTransfers &&
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
      
      TransferInfo transfer = (TransferInfo) toSubmitTransfers.remove(0);
      submittingTransfers.add(transfer);
      Debug.debug("Removed "+transfer.getSourceURL(), 3);
      Debug.debug("with id "+transfer.getTransferID(), 3);
      Debug.debug("Now toSubmitTransfers has "+toSubmitTransfers.size()+" elements", 3);
      Debug.debug("Now submittingTransfers has "+submittingTransfers.size()+" elements", 3);
    }
    
    // Remove the wrongly added last transfer from theseTransfers before submitting
    if(brokenOut && theseTransfers.length>1){
      TransferInfo [] tmpTransfers = new TransferInfo[theseTransfers.length-1];
      for(int i=0; i<theseTransfers.length-1; ++i){
        tmpTransfers[i] = theseTransfers[i];
      }
      theseTransfers = tmpTransfers;
    }
    
    return theseTransfers;
    
  }

  private void setMonitorStatus(final String text){
    ResThread t = (new ResThread(){
      public void run(){
        // use status bar on monitoring frame
        StatusBar myStatusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;
        try{
          myStatusBar.setLabel(text);
        }
        catch(Exception e){
          e.printStackTrace();
        }
       }
    });     
    SwingUtilities.invokeLater(t);    
  }
  
  private void setStatus(final String text){
    ResThread t = (new ResThread(){
      public void run(){
        try{
          GridPilot.getClassMgr().getStatusBar().setLabel(text);
        }
        catch(Exception e){
          e.printStackTrace();
        }
       }
    });     
    SwingUtilities.invokeLater(t);    
  }
  
  private void animateMonitorStatus() {
    ResThread t = (new ResThread(){
      public void run(){
        // use status bar on monitoring frame
        StatusBar myStatusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;
        try{
          myStatusBar.animateProgressBar();
        }
        catch(Exception e){
          e.printStackTrace();
        }
       }
    });     
    SwingUtilities.invokeLater(t);    
  }
  
  private void stopMonitorAnimation() {
    ResThread t = (new ResThread(){
      public void run(){
        // use status bar on monitoring frame
        StatusBar myStatusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;
        try{
          myStatusBar.stopAnimation();
        }
        catch(Exception e){
          e.printStackTrace();
        }
       }
    });     
    SwingUtilities.invokeLater(t);    
  }
  
  private void removeMonitorProgressBar() {
    setMonitorProgressBar(0, 0);
    try{
      GridPilot.getClassMgr().getStatusBar().removeProgressBar(pbSubmission);
    }
    catch(Exception e){
    }
    ResThread t = (new ResThread(){
      public void run(){
        // use status bar on monitoring frame
        StatusBar myStatusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;
        try{
          myStatusBar.stopAnimation();
          isProgressBarSet = false;
        }
        catch(Exception e){
          e.printStackTrace();
        }
       }
    });     
    SwingUtilities.invokeLater(t);    
  }

  private void setMonitorProgressBar(final int i, final int j) {
    ResThread t = (new ResThread(){
      public void run(){
        // use status bar on monitoring frame
        try{
          if(!isProgressBarSet){
            GridPilot.getClassMgr().getStatusBar().setProgressBar(pbSubmission);
            isProgressBarSet = true;
          }
          pbSubmission.setMaximum(i);
          pbSubmission.setValue(j);
        }
        catch(Exception e){
          e.printStackTrace();
        }
       }
    });     
    SwingUtilities.invokeLater(t);    
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
    String resourcesPath = configFile.getValue(GridPilot.TOP_CONFIG_SECTION, "resources");
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
   * @return a list of identifiers of the form "https-(get|put|copy)::'srcUrl' 'destUrl'"
   */
  public String [] startCopyFiles(GlobusURL [] srcUrls, GlobusURL [] destUrls)
     throws Exception {

    for(int i=0; i<srcUrls.length; ++i){
      Debug.debug("Starting to copy files "+srcUrls[i]+" -> "+
          destUrls[i], 2);
    }
    
    String ftPluginName = null;
    String [] fts = GridPilot.FT_NAMES;
    String [] ids = null;
    
    // Select first plugin that supports the protocol of the these transfers
    for(int i=0; i<fts.length; ++i){
      Debug.debug("Checking plugin "+fts[i], 3);
      if(GridPilot.getClassMgr().getFTPlugin(fts[i]).checkURLs(srcUrls, destUrls)){
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
    Exception ee = null;
    try{
      ids = GridPilot.getClassMgr().getFTPlugin(ftPluginName).startCopyFiles(srcUrls, destUrls);
    }
    catch(Exception e){
      ee = e;
    }
    
    if(ids==null){
      // If the above failed, it could be because we're trying to do srm-1 on an srm-2 server.
      // Give it a try with srm-2.
      // This is a hack to deal with the fact that the protocol srm can mean both version 1 or version 2
      // (and the fact that the two protocols are not compatible).
      if(ftPluginName.equalsIgnoreCase("srm") && MyUtil.arrayContains(GridPilot.FT_NAMES, SRM2_PLUGIN_NAME)){
        ids = GridPilot.getClassMgr().getFTPlugin(SRM2_PLUGIN_NAME).startCopyFiles(srcUrls, destUrls);
        // If successful, have findFTPlugin remember to pick the right class.
        if(ids!=null){
          for(int i=0; i<srcUrls.length; ++i){
            if(srcUrls[i].getProtocol().equalsIgnoreCase("srm")){
              Debug.debug("Remembering host "+srcUrls[i].getHost(), 2);
              serverPluginMap.put(srcUrls[i].getHost(), SRM2_PLUGIN_NAME);
            }
            if(destUrls[i].getProtocol().equalsIgnoreCase("srm")){
              serverPluginMap.put(destUrls[i].getHost(), SRM2_PLUGIN_NAME);
              Debug.debug("Remembering host "+destUrls[i].getHost(), 2);
            }
          }
        }
      }
      else if(ee!=null){
        throw ee;
      }
    }
    
    return ids;
  }
  
  /**
   * Adds transfers to toSubmitTransfers.
   * @param   transfers     Vector of TransferInfo's.
   */
  public void queue(final Vector _transfers) throws Exception {

    ResThread t = new ResThread(){
      Vector transfers;
      public void run(){
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
        setMonitorStatus("Adding to submission queue. Please wait...");
        animateMonitorStatus();
        pbSubmission.setToolTipText("Click here to cancel queueing");
        setMonitorProgressBar(0, 0);
        toSubmitTransfers.addAll(transfers);
        for(Iterator it=transfers.iterator(); it.hasNext();){
          toSubmitTransfers.add((TransferInfo) it.next());
        }
        if(!timer.isRunning()){
          timer.restart();
        }
        setMonitorStatus("Queueing done.");
        stopMonitorAnimation();
      }
    };
    t.start();

    if(!MyUtil.myWaitForThread(t, "", TRANSFER_SUBMIT_TIMEOUT, "transfer")){
      setMonitorStatus("WARNING: queueing transfers timed out.");
      stopMonitorAnimation();
    }
  }
  
  public void clearTableRows(int [] clearRows){
    Object [][] newTablevalues = new Object [tableValues.length-clearRows.length][GridPilot.TRANSFER_STATUS_FIELDS.length];
    TransferInfo transfer = null;
    TransferInfo [] toClearTransfers = new TransferInfo[clearRows.length];
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
        for(int k=0; k<GridPilot.TRANSFER_STATUS_FIELDS.length; ++k){
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
      ((DBVectorTableModel) ((MyJTable) statusTable).getModel()).setTable(tableValues, GridPilot.TRANSFER_STATUS_FIELDS);
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
  private void submit(String ftPlugin, TransferInfo [] transfers)
     throws Exception {

    if(isRand!=null && isRand.equalsIgnoreCase("yes")){
      transfers = (TransferInfo []) MyUtil.shuffle(transfers);
    }

    GlobusURL [] sources = new GlobusURL [transfers.length];
    GlobusURL [] destinations = new GlobusURL [transfers.length];
    String [] ids = null;
    
    Object [][] appendTablevalues = new Object [transfers.length][GridPilot.TRANSFER_STATUS_FIELDS.length];
    Object [][] newTablevalues = new Object [tableValues.length+appendTablevalues.length][GridPilot.TRANSFER_STATUS_FIELDS.length];
    int startRow = statusTable.getRowCount();
    boolean resubmit = false;

    for(int i=0; i<transfers.length; ++i){
      
      sources[i] = transfers[i].getSource();
      destinations[i] = transfers[i].getDestination();
            
      resubmit = (transfers[i].getInternalStatus()>-1);
      
      if(!resubmit){
        transfers[i].setTableRow(GridPilot.getClassMgr().getSubmittedTransfers().size());
        GridPilot.getClassMgr().getSubmittedTransfers().add(transfers[i]);
        // add to status table
        statusTable.createRows(GridPilot.getClassMgr().getSubmittedTransfers().size());
        
        for(int j=1; j<GridPilot.TRANSFER_STATUS_FIELDS.length; ++j){
          appendTablevalues[i][j] = statusTable.getValueAt(startRow+i, j);
        }
      }

      transfers[i].setInternalStatus(FileTransfer.STATUS_WAIT);

      statusTable.setValueAt(transfers[i].getSourceURL(),
          transfers[i].getTableRow(), MyTransferStatusUpdateControl.FIELD_SOURCE);
      statusTable.setValueAt(transfers[i].getDestination().getURL(),
          transfers[i].getTableRow(), MyTransferStatusUpdateControl.FIELD_DESTINATION);
      statusTable.setValueAt(iconSubmitting, transfers[i].getTableRow(),
          MyTransferStatusUpdateControl.FIELD_CONTROL);
      
    }
    
    if(!resubmit){
      // this fixes the problem with sorting
      System.arraycopy(tableValues, 0, newTablevalues, 0, tableValues.length);
      System.arraycopy(appendTablevalues, 0, newTablevalues, tableValues.length, appendTablevalues.length);
      tableValues = newTablevalues;
      Debug.debug("Setting table", 3);
      ((DBVectorTableModel) ((MyJTable) statusTable).getModel()).setTable(tableValues, GridPilot.TRANSFER_STATUS_FIELDS);
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
      Exception re = null;
      // try source URLs one by one
      for(int i=0; i<sourceListLen; ++i){
        Debug.debug("trial --->"+i, 3);

        try{
          re = null;
          try{
            ids = GridPilot.getClassMgr().getFTPlugin(ftPlugin).startCopyFiles(
                sources, destinations);
          }
          catch(Exception e){
            re = e;
          }
          if(ids==null){
            // If the above failed, it could be because we're trying to do srm-1 on an srm-2 server.
            // Give it a try with srm-2.
            // This is a hack to deal with the fact that the protocol srm can mean both version 1 or version 2
            // (and the fact that the two protocols are not compatible).
            if(ftPlugin.equalsIgnoreCase("srm") && MyUtil.arrayContains(GridPilot.FT_NAMES, SRM2_PLUGIN_NAME)){
              ids = GridPilot.getClassMgr().getFTPlugin(SRM2_PLUGIN_NAME).startCopyFiles(
                  sources, destinations);
              // If successful, have findFTPlugin remember to pick the right class.
              if(ids!=null){
                for(int j=0; j<sources.length; ++j){
                  if(sources[j].getProtocol().equalsIgnoreCase("srm")){
                    Debug.debug("Remembering host "+sources[j].getHost(), 2);
                    serverPluginMap.put(sources[j].getHost(), SRM2_PLUGIN_NAME);
                  }
                  if(destinations[j].getProtocol().equalsIgnoreCase("srm")){
                    Debug.debug("Remembering host "+destinations[j].getHost(), 2);
                    serverPluginMap.put(destinations[j].getHost(), SRM2_PLUGIN_NAME);
                  }
                }
              }
            }
            else if(re!=null){
              throw re;
            }
          }
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
            MyTransferStatusUpdateControl.FIELD_TRANSFER_ID);
        statusTable.setValueAt(userInfo, transfers[i].getTableRow(),
            MyTransferStatusUpdateControl.FIELD_USER);
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
              MyTransferStatusUpdateControl.FIELD_TRANSFER_ID);
          statusTable.setValueAt(transfers[i].getSourceURL(), transfers[i].getTableRow(),
              MyTransferStatusUpdateControl.FIELD_SOURCE);
          statusTable.setValueAt(transfers[i].getDestination().getURL(), transfers[i].getTableRow(),
              MyTransferStatusUpdateControl.FIELD_DESTINATION);
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
        statusTable.setValueAt(null, transfers[i].getTableRow(), MyTransferStatusUpdateControl.FIELD_CONTROL);
      }
      
      if(!timer.isRunning()){
        timer.restart();
      }
      if(pbSubmission.getPercentComplete()==1.0){
        removeMonitorProgressBar();
        setStatus("");
      }
    }
  };

  public String getStatus(String fileTransferID) throws Exception {
    FileTransfer ft = findFTPlugin(fileTransferID);
    String status = ft.getStatus(fileTransferID);
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
    String [] fts = GridPilot.FT_NAMES;
    
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
    timer.stop();
    Enumeration e = toSubmitTransfers.elements();
    while(e.hasMoreElements()){
      TransferInfo transfer = (TransferInfo) e.nextElement();
      statusTable.setValueAt("Transfer not queued (cancelled)!",
          transfer.getTableRow(), MyTransferStatusUpdateControl.FIELD_TRANSFER_ID);
      statusTable.setValueAt(transfer.getSourceURL(), transfer.getTableRow(),
          MyTransferStatusUpdateControl.FIELD_SOURCE);
      statusTable.setValueAt(transfer.getDestination().getURL(), transfer.getTableRow(),
          MyTransferStatusUpdateControl.FIELD_DESTINATION);
      transfer.setNeedsUpdate(false);
    }
    toSubmitTransfers.removeAllElements();
    removeMonitorProgressBar();
  }

  /**
   * Just cancels the transfer with the corresponding plugin
   */
  public void cancel(final String fileTransferID) throws Exception{
    findFTPlugin(fileTransferID).cancel(fileTransferID);
  }
  
  /**
   * Cancels a Vector of transfers.
   * @param transfers Vector of TransferInfo's
   */
  public void cancel(final Vector transfers){
    ResThread t = new ResThread(){
      TransferInfo transfer = null;
      public void run(){
        animateMonitorStatus();
        setMonitorStatus("Cancelling...");
        Vector submittedTransfers = GridPilot.getClassMgr().getSubmittedTransfers();
        try{
          for(int i=0; i<transfers.size(); ++i){
            try{             
              transfer = (TransferInfo) transfers.get(i);
              GridPilot.getClassMgr().getTransferControl().runningTransfers.remove(transfer);
              // skip if not running
              boolean isRunning = false;
              for(int j=0; j<submittedTransfers.size(); ++j){
                try{
                  String id = ((TransferInfo) submittedTransfers.get(j)).getTransferID();
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
                  status, transfer.getTableRow(), MyTransferStatusUpdateControl.FIELD_STATUS);
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
        stopMonitorAnimation();
        setMonitorStatus("Cancelling done.");
      }
    };

    t.start();

    if(!MyUtil.myWaitForThread(t, "", TRANSFER_CANCEL_TIMEOUT, "transfer")){
      stopMonitorAnimation();
      setMonitorStatus("WARNING: cancel transfers timed out.");
    }
  }
  
  public void resubmit(final Vector transfers){
    ResThread t = new ResThread(){
      TransferInfo transfer = null;
      public void run(){
        animateMonitorStatus();
        setMonitorStatus("Cancelling...");
        Vector submittedTransfers = GridPilot.getClassMgr().getSubmittedTransfers();
        try{
          GlobusURL [] srcUrls = new GlobusURL [transfers.size()];
          GlobusURL [] destUrls = new GlobusURL [transfers.size()];
          for(int i=0; i<transfers.size(); ++i){
            try{             
              transfer = (TransferInfo) transfers.get(i);
              // abort if a job is running
              boolean isRunning = false;
              for(int j=0; j<submittedTransfers.size(); ++j){
                try{
                  String id = ((TransferInfo) submittedTransfers.get(j)).getTransferID();
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
                  status, transfer.getTableRow(), MyTransferStatusUpdateControl.FIELD_STATUS);
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
        stopMonitorAnimation();
        setMonitorStatus("Resubmitting done.");
      }
    };

    t.start();

    if(!MyUtil.myWaitForThread(t, "", TRANSFER_CANCEL_TIMEOUT, "transfer")){
      stopMonitorAnimation();
      setMonitorStatus("WARNING: cancel transfers timed out.");
    }
  }
  
  /**
   * Select first plugin that supports the protocol of the this transfer.
   * @param   transferID    Transfer ID. Formats:
   * 
   *                        protocol-(get|put|copy)::srcTURL destTURL [SURL]
   *                        srm-{get|put|copy}::srm request id::transfer index::'srcTurl' 'destTurl' 'srmSurl'
   */
  private FileTransfer findFTPlugin(String transferID)
     throws Exception{
    String ftPluginName = null;
    String [] checkArr = MyUtil.split(transferID, "::");
    String [] arr = MyUtil.splitUrls(checkArr[checkArr.length-1]);
    Debug.debug("Finding status of transfer "+MyUtil.arrayToString(arr, "-->"), 3);
    GlobusURL srmUrl = new GlobusURL(arr[arr.length-1]);
    Debug.debug("Finding plugin for transfer "+transferID, 2);
    Debug.debug("Checking host "+srmUrl.getHost(), 2);
    if(serverPluginMap.containsKey(srmUrl.getHost())){
      return GridPilot.getClassMgr().getFTPlugin(serverPluginMap.get(srmUrl.getHost()));
    }
    ftPluginName = Util.split(checkArr[0], "-")[0];
    return GridPilot.getClassMgr().getFTPlugin(ftPluginName);
  }
  
  public boolean isSubmitting(){
    //return timer.isRunning();
    Debug.debug("submittingTransfers: "+submittingTransfers.size(), 3);
    Debug.debug("toSubmitTransfers: "+toSubmitTransfers.size(), 3);
    return !(submittingTransfers.isEmpty() && toSubmitTransfers.isEmpty());
  }
  
  /**
   * Checks the status of the transfers and updates the TransferInfo objects. <p>
   */
  public void updateStatus(Vector transfers){
    String status = null;
    String transferred = null;
    int percentComplete = -1;
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
          percentComplete = getPercentComplete(id);
          transferred = Integer.toString(percentComplete);
          Debug.debug("Got transferred: "+transferred, 2);
          transfer.setTransferred(transferred+"%");
        }
        catch(Exception e){
          e.printStackTrace();
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
  public void transferFailure(TransferInfo transfer){
    // TODO: retry on transfer failure
    setMonitorStatus("Transfer "+transfer.getTransferID()+" failed");
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
        String id = ((TransferInfo) submittedTransfers.get(i)).getTransferID();
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
  public void transferDone(TransferInfo transfer){
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
    if(((TransferInfo) transfer).getDBName()!=null && transfer.getDestination()!=null){
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
        setMonitorStatus(message);
        Debug.debug(message, 2);
        guid = uuid;
      }
      String datasetID = null;
      try{
        datasetID = ((TransferInfo) transfer).getDatasetID();
      }
      catch(Exception e){
      }
      if(datasetID==null || datasetID.equals("")){
        logFile.addMessage("WARNING: no dataset found. "+
            "This file, "+lfn+", will NOT be registered in dataset catalog, only in file catalog.");
      }
      String datasetName = null;
      try{
        datasetName = ((TransferInfo) transfer).getDatasetName();
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
        GridPilot.getClassMgr().getDBPluginMgr(((TransferInfo) transfer).getDBName()).registerFileLocation(
            datasetID, datasetName, guid, lfn, destination, fileBytes, checksum, false);
      }
      catch(Exception e){
        logFile.addMessage(
            "ERROR: could not register "+destination+" for file "+
            lfn+" in dataset "+datasetName, e);
        setMonitorStatus("ERROR: could not register "+destination);
      }
      setMonitorStatus("Registration done");
    }
    else{
      setMonitorStatus("Copying done");
    }
  }
  
  public static boolean isRunning(TransferInfo transfer){
    int internalStatus = transfer.getInternalStatus();
    if(internalStatus==FileTransfer.STATUS_WAIT ||
        internalStatus==FileTransfer.STATUS_RUNNING){
      return true;
    }
    else{
      return false;
    }
  }
  
  public void httpsDownload(String url, File destination) throws Exception{
    MySSL ssl = GridPilot.getClassMgr().getSSL();
    super.setFileTransfer(new HTTPSFileTransfer(ssl.getGridSubject(), ssl, logFile));
    super.download(url, destination);
  }
  
  /**
   * Download file from URL. Quick method - bypassing transfer control and monitoring.
   * @param url
   * @param fileName
   * @param destination    can be either a directory or a file
   * @throws Exception 
   */
  public void download(final String url, File destination) throws Exception{
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
    
    setStatus("Downloading "+srcFileName+" from "+srcUrlDir+" to "+downloadDir);

    Debug.debug("Downloading "+srcFileName+" from "+srcUrlDir, 3);
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
      //setStatus(fsPath+destFileName+" copied");
    }
    // remote gsiftp or https/webdav directory
    else if(srcUrlDir.startsWith("gsiftp://") || srcUrlDir.startsWith("https://") ||
        srcUrlDir.startsWith("sss://")){
      Debug.debug("Downloading "+destFileName+" from "+srcUrlDir+" to "+downloadDir.getAbsolutePath(), 3);
      GlobusURL globusUrl = new GlobusURL(url);
      FileTransfer fileTransfer =
        GridPilot.getClassMgr().getFTPlugin(url.replaceFirst("^(\\w+):/.*", "$1"));
     fileTransfer.getFile(globusUrl, destination/*dName*/);
     Debug.debug("Download done - "+destFileName, 3);        
     //setStatus(url+" downloaded");
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
      //setStatus(url+" downloaded");
    }
    else{
      throw(new IOException("Unknown protocol for "+url));
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

}
