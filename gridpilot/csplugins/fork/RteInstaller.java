package gridpilot.csplugins.fork;

import gridpilot.Debug;
import gridpilot.GridPilot;
import gridpilot.LogFile;
import gridpilot.ShellMgr;
import gridpilot.TransferControl;
import gridpilot.TransferInfo;
import gridpilot.TransferStatusUpdateControl;
import gridpilot.Util;

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
  private String dir;
  private String rteName;
  private ShellMgr shellMgr;
  private LogFile logFile;
  
  // Wait max 240 seconds for all output files to be downloaded
  private static int MAX_DOWNLOAD_WAIT = 240000;


  RteInstaller(String _url, String _dir, String _rteName, ShellMgr _shellMgr){
    url = _url;
    dir = _dir;
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
    File cacheDir = new File(dir, "softwareCache");
    
    // Check if cache directory exists locally
    if(!cacheDir.exists()){
      logFile.addInfo("Software cache directory "+
          cacheDir.getAbsolutePath()+" does not exist, creating.");
      cacheDir.mkdirs();
    }
    // We need it remotely as well
    if(!shellMgr.existsFile(cacheDir.getAbsolutePath())){
      logFile.addInfo("Software cache directory "+
          cacheDir.getAbsolutePath()+" does not exist, creating.");
      shellMgr.mkdirs(cacheDir.getAbsolutePath());
    }
    
    File unpackDir = new File(cacheDir, unpackName);
    // Check if the unpack directory is already there. Should not happen...
    if(unpackDir.exists()){
      throw new IOException("There is already a file or directory called "+
          unpackDir.getAbsolutePath()+". Please remove it.");
    }
    // Make the directory where we will unpack the tarball.
    unpackDir.mkdir();
    
    // Download the tarball to the local disk
    localDownload(url, tarBallName, cacheDir);
    File fullDlFile = (new File(cacheDir, tarBallName));
    if(shellMgr.isLocal()){
      // Unpack the tarball
      File fullGunzipName = (new File(unpackDir, unpackName));
      Debug.debug("Gunzipping "+fullDlFile, 2);
      Util.gunzip(fullDlFile, fullGunzipName);
      Debug.debug("Untarring "+fullDlFile, 2);
      Util.unTar(fullGunzipName, unpackDir);
      // Rename "data" to "pkg"
      (new File(unpackDir, "data")).renameTo(new File(unpackDir, "pkg"));
    }
    else{
      // For remote shells, copy over the downloaded tarball and then unpack it
      Debug.debug("Copying over downloaded file via ssh; "+fullDlFile.getAbsolutePath(), 2);
      TransferControl.copyOutputFile(fullDlFile.getAbsolutePath(), fullDlFile.getAbsolutePath(), shellMgr, null,
          logFile);
      // Then unpack the tarball and rename "data" to "pkg"
      StringBuffer stdout = new StringBuffer();
      StringBuffer stderr = new StringBuffer();
      if(shellMgr.exec("cd "+unpackDir.getAbsolutePath()+"; tar -xzf "+
          fullDlFile.getAbsolutePath()+"; mv data pkg", stdout, stderr)!=0 ||
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
    if(shellMgr.exec("cd "+unpackDir.getAbsolutePath()+
        "/pkg; /bin/sh ../control/install", stdout, stderr)!=0 ||
        stderr!=null && stderr.length()!=0){
      throw new IOException("could not run install script; "+stdout.toString()+":"+stderr.toString());
    }
    
    // Replace %BASEDIR% with the pkg dir in then runtime script.
    // TODO: support Windows: unpack locally (in java) and
    // then copy over the runtime script if needed
    if(shellMgr.exec("cd "+unpackDir.getAbsolutePath()+
        "/control; sed -i 's|%BASEDIR%|"+unpackDir.getAbsolutePath()+
        "/pkg|g runtime'", stdout, stderr)!=0 || stderr!=null && stderr.length()!=0){
      throw new IOException("could not run install script; "+stdout.toString()+":"+stderr.toString());
    }
    
    // Create the subdirs (if any) of rteName.
    String subDir = rteName.replaceFirst("^(.*)/[^/]+$", "$1");
    if(!subDir.equals(rteName)){
      if(!shellMgr.mkdirs(dir+"/"+subDir)){
        throw new IOException("could not create directory "+subDir+"; "+stdout.toString()+":"+stderr.toString());
      }
    }
    
    // Move the runtime script to rteName
    if(!shellMgr.moveFile(unpackDir.getAbsolutePath()+"/pkg", dir+"/"+rteName)){
      throw new IOException("could not move runtime script to final destination; "+stdout.toString()+":"+stderr.toString());
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
    TransferInfo transfer = new TransferInfo(
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
    TransferStatusUpdateControl statusUpdateControl =
      GridPilot.getClassMgr().getTransferStatusUpdateControl();
    while(!transfersDone && waitT*sleepT<MAX_DOWNLOAD_WAIT){
      transfersDone = true;
      statusUpdateControl.updateStatus(null);
      for(Iterator itt=transferVector.iterator(); itt.hasNext();){
        transfer = (TransferInfo) itt.next();
        if(TransferControl.isRunning(transfer)){
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
      TransferControl.cancel(transferVector);
      throw new TimeLimitExceededException("Download took too long, aborting.");
    }
  }
  
}
