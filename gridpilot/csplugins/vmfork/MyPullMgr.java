package gridpilot.csplugins.vmfork;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;

import gridfactory.common.Debug;
import gridfactory.common.FileTransfer;
import gridfactory.common.JobInfo;
import gridfactory.common.LocalStaticShell;
import gridfactory.common.PullMgr;
import gridfactory.common.TransferInfo;

import gridpilot.GridPilot;
import gridpilot.MyUtil;

public class MyPullMgr extends PullMgr {

  /** How many seconds to wait for a download to start before giving up. */
  protected static long WAIT_DOWNLOAD_START_SECONDS = 60L;

  public MyPullMgr() {
    logFile = GridPilot.getClassMgr().getLogFile();
    String localRteDir = GridPilot.RUNTIME_DIR;
    logFile = GridPilot.getClassMgr().getLogFile();
    String [] rteCatalogUrls = GridPilot.getClassMgr().getConfigFile().getValues(GridPilot.TOP_CONFIG_SECTION, "runtime catalog URLs");
    transferControl = GridPilot.getClassMgr().getTransferControl();
    transferStatusUpdateControl = GridPilot.getClassMgr().getTransferStatusUpdateControl();
    rteMgr = GridPilot.getClassMgr().getRTEMgr(localRteDir, rteCatalogUrls);
    jobCacheDir = GridPilot.getClassMgr().getConfigFile().getValue("VMFork", "File cache directory");
    if(jobCacheDir==null){
      logFile.addMessage("WARNING: 'File cache directory not' set in configuration file. " +
          "You will not be able to run jobs requiring input files from file servers.");
    }
    myMaxRunningSeconds = -1;
  }
  
  public void deleteInputs(JobInfo job){
    String subDir = MyUtil.getjobDirName(job);
    File jobDlDir = new File(MyUtil.clearTildeLocally(MyUtil.clearFile(jobCacheDir)), subDir);
    LocalStaticShell.deleteDir(jobDlDir.getAbsolutePath());
  }
  
  /**
   * Check if input files of a job have finished downloading.
   * @param job the job in question
   * @return true if all input files have been downloaded, false otherwise
   * @throws IOException 
   */
  // TODO: eliminate code duplication downloadInputsDone() <-> runJob()
  public boolean downloadInputsDone(JobInfo job) throws IOException{
    Vector<TransferInfo> transfers = jobTransfers.get(job);
    boolean ok = true;
    String transferStatus = null;
    int ts;
    int oldStatus;
    for(Iterator<TransferInfo> itt=transfers.iterator(); itt.hasNext();){
      TransferInfo transfer = itt.next();
      // If a transfer doesn't have an ID yet, give it some time before giving up
      if(transfer==null || transfer.getTransferID()==null){
        try{
          transferStatusUpdateControl.updateStatus(null);
          Thread.sleep(10000L);
        }
        catch(InterruptedException e){
          e.printStackTrace();
        }
      }
      if(transfer==null || transfer.getTransferID()==null){
        logFile.addInfo("WARNING: transfer has no ID.");
        ok = false;
        break;
      }
      oldStatus = transfer.getInternalStatus();
      try{
        transferStatus = transferControl.getStatus(transfer.getTransferID());
        ts = transferControl.getInternalStatus(transfer.getTransferID(), transferStatus);
      }
      catch(Exception e2){
        Debug.debug("Could not get status of tranfer "+transfer.getTransferID(), 3);
        e2.printStackTrace();
        ok = false;
        ts = FileTransfer.STATUS_ERROR;
      }
      if(ts==FileTransfer.STATUS_RUNNING || ts==FileTransfer.STATUS_WAIT){
        Debug.debug("Transfer is still running: "+transfer.getTransferID(), 2);
        ok = false;
        break;
      }
      else if(ts==FileTransfer.STATUS_ERROR){
        ok = false;
        if(oldStatus==FileTransfer.STATUS_ERROR){
          throw new IOException("Transfer did not start in "+
              WAIT_DOWNLOAD_START_SECONDS+" seconds.");
        }
        try{
          Thread.sleep(WAIT_DOWNLOAD_START_SECONDS*1000L);
        }
        catch(InterruptedException e){
          e.printStackTrace();
        }
      }
    }
    return ok;
  }

}
