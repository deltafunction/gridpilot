package gridpilot.csplugins.fork;

import gridfactory.common.Debug;
import gridfactory.common.LogFile;
import gridfactory.common.Shell;
import gridpilot.GridPilot;
import gridpilot.MyTransferControl;
import gridpilot.MyTransferInfo;
import gridpilot.MyTransferStatusUpdateControl;
import gridpilot.MyUtil;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;

import javax.naming.TimeLimitExceededException;

import org.globus.util.GlobusURL;

/**
 * This class supports looking up a KnowARC runtime environment (RTE)
 * in a catalog in RDF format, downloading and installing the software,
 * using a UNIX shell.
 * 
 * Supported RTE formats: tar.gz, img.gz (GridPilot extension).
 * 
 */
public class RteInstaller {
  
  private String url;
  private String remoteDir;
  private String cacheDir;
  private String rteName;
  private Shell shellMgr;
  private LogFile logFile;
  
  // Wait max 240 seconds for all output files to be downloaded
  private static int MAX_DOWNLOAD_WAIT = 240000;


  /**
   * If given shell manager '_shellMgr' is local, the remote directory
   * '_remoteDir' is irrelevant/ignored. 
   * 
   * @param _url the URL of the catalog
   * @param _remoteDir full path to the directory on the worker node in which to keep runtime environments
   * @param _localDir cache directory on the host where GridWorker is running
   * @param _shellMgr ShellMgr object to be used for the installation
   */
  RteInstaller(String _url, String _remoteDir, String _localDir, String _rteName, Shell _shellMgr){
    url = _url;
    remoteDir = _remoteDir;
    cacheDir = _localDir;
    rteName = _rteName;
    shellMgr = _shellMgr;
    logFile = GridPilot.getClassMgr().getLogFile();
  }
  
  /**
   * Download and install the RTE.
   * @throws Exception
   */
  public void install() throws Exception{
    
    if(shellMgr==null){
      throw new IOException("No shell available; cannot install");
    }
    
    String tarBallName = url.replaceFirst("^.*/([^/]+)$", "$1");
    String unpackName = tarBallName.replaceFirst("\\.tar\\.gz$", "");
    unpackName = unpackName.replaceFirst("\\.tgz$", "");
    File localCacheDir = new File(cacheDir);
    String remoteCacheDir = remoteDir+"/softwareCache";
    
    // Check if cache directory exists locally
    if(!localCacheDir.exists()){
      logFile.addInfo("Software cache directory "+
          localCacheDir.getAbsolutePath()+" does not exist, creating.");
      localCacheDir.mkdirs();
    }
    // We need a remote directory as well
    if(!shellMgr.existsFile(remoteCacheDir)){
      logFile.addInfo("Software cache directory "+
          remoteCacheDir+" does not exist, creating.");
      shellMgr.mkdirs(remoteCacheDir);
    }
    
    File dlDir = new File(localCacheDir, unpackName);
    // Check if the unpack directory is already there. Should not happen...
    if(dlDir.exists()){
      throw new IOException("There is already a file or directory called "+
          dlDir.getAbsolutePath()+". Please remove it.");
    }
    // Make the directory where we will unpack the tarball.
    dlDir.mkdir();
    
    // Download the tarball to the local disk - should use caching already
    localDownload(url, tarBallName, localCacheDir);
    File fullDlFile = (new File(localCacheDir, tarBallName));
    if(shellMgr.isLocal()){
      // If this RTE has already been installed, return
      if(shellMgr.existsFile((new File(localCacheDir, ".install_ok_"+rteName)).getAbsolutePath())){
        return;
      }
      // Unpack the tarball
      File fullGunzipName = (new File(dlDir, unpackName));
      Debug.debug("Gunzipping "+fullDlFile, 2);
      MyUtil.gunzip(fullDlFile, fullGunzipName);
      Debug.debug("Untarring "+fullDlFile, 2);
      MyUtil.unTar(fullGunzipName, dlDir);
      // Rename "data" to "pkg"
      (new File(dlDir, "data")).renameTo(new File(dlDir, "pkg"));
    }
    else{
      // For remote shells, copy over the downloaded tarball and then unpack it
      // If this RTE has already been installed, return
      if(shellMgr.existsFile(remoteCacheDir+"/.install_ok_"+rteName)){
        return;
      }
      Debug.debug("Copying over downloaded file via ssh; "+fullDlFile.getAbsolutePath(), 2);
      GridPilot.getClassMgr().getTransferControl().copyOutputFile(
          fullDlFile.getAbsolutePath(), remoteCacheDir+"/"+fullDlFile.getName(), shellMgr, null);
      // Then unpack the tarball and rename "data" to "pkg"
      StringBuffer stdout = new StringBuffer();
      StringBuffer stderr = new StringBuffer();
      if(shellMgr.exec("cd "+remoteCacheDir+"; tar -xzf "+
          remoteCacheDir+"/"+fullDlFile.getName()+"; mv data pkg", stdout, stderr)!=0 ||
          stderr!=null && stderr.length()!=0){
        throw new IOException("could not unpack tarball; "+stdout.toString()+":"+stderr.toString());
      }
    }
    
    // NOTICE: we do NOT delete the downloaded tarball, in order to avoid
    // downloading it again; i.e. to take advantage of the caching mechanism
    // of TransferControl.
    
    // cd to "pkg" and Execute the install script.
    // Do this only if we have a shellMgr.
    StringBuffer stdout = new StringBuffer();
    StringBuffer stderr = new StringBuffer();
    // TODO: support Windows bat scripts.
    if(shellMgr.exec("cd "+remoteCacheDir+
        "/pkg; /bin/bash ../control/install", stdout, stderr)!=0 ||
        stderr!=null && stderr.length()!=0){
      throw new IOException("could not run install script; "+stdout.toString()+":"+stderr.toString());
    }
    
    // Replace %BASEDIR% with the pkg dir in then runtime script.
    // TODO: support Windows: unpack locally (in java) and
    // then copy over the runtime script if needed
    if(shellMgr.exec("cd "+remoteCacheDir+
        "/control; sed -i 's|%BASEDIR%|"+remoteCacheDir+
        "/pkg|g runtime'", stdout, stderr)!=0 || stderr!=null && stderr.length()!=0){
      throw new IOException("could not run install script; "+stdout.toString()+":"+stderr.toString());
    }
    
    // Create the subdirs (if any) of rteName.
    String subDir = rteName.replaceFirst("^(.*)/[^/]+$", "$1");
    if(!subDir.equals(rteName)){
      if(!shellMgr.mkdirs(remoteDir+"/"+subDir)){
        throw new IOException("could not create directory "+subDir+"; "+stdout.toString()+":"+stderr.toString());
      }
    }
    
    // Move the runtime script to rteName
    if(!shellMgr.moveFile(remoteCacheDir+"/pkg", remoteDir+"/"+rteName)){
      throw new IOException("could not move runtime script to final destination; "+stdout.toString()+":"+stderr.toString());
    }
    
    // Tag this as successfully installed
    if(shellMgr.isLocal()){
      shellMgr.writeFile((new File(localCacheDir, ".install_ok_"+rteName)).getAbsolutePath(),
          "", false);
    }
    else{
      shellMgr.writeFile(remoteCacheDir+"/.install_ok_"+rteName, "", false);
    }
    
  }
  
  /**
   * Download file from remote location to local directory.
   * @param url URL of the file to download
   * @param fileName name of local file
   * @param downloadDir name of local directory
   * @throws Exception
   */
  private void localDownload(String url, String fileName, File downloadDir) throws Exception{
    // Construct the download transfer vector (one transfer).
    Vector transferVector = new Vector();
    MyTransferInfo transfer = new MyTransferInfo(
              new GlobusURL(url),
              new GlobusURL("file:///"+(new File(downloadDir.getAbsolutePath(),
                  fileName)).getAbsolutePath()));
    transferVector.add(transfer);
    // Carry out the transfers.
    GridPilot.getClassMgr().getTransferControl().queue(transferVector);
    // Wait for transfers to finish
    boolean transfersDone = false;
    int sleepT = 3000;
    int waitT = 0;
    MyTransferStatusUpdateControl statusUpdateControl =
      GridPilot.getClassMgr().getTransferStatusUpdateControl();
    while(!transfersDone && waitT*sleepT<MAX_DOWNLOAD_WAIT){
      transfersDone = true;
      statusUpdateControl.updateStatus(null);
      for(Iterator itt=transferVector.iterator(); itt.hasNext();){
        transfer = (MyTransferInfo) itt.next();
        if(MyTransferControl.isRunning(transfer)){
          transfersDone = false;
          break;
        }
      }
      if(transfersDone){
        break;
      }
      Debug.debug("Waiting for transfer(s)...", 2);
      Thread.sleep(sleepT);
      ++ waitT;
    }
    if(!transfersDone){
      GridPilot.getClassMgr().getTransferControl().cancel(transferVector);
      throw new TimeLimitExceededException("Download took too long, aborting.");
    }
  }
  
}
