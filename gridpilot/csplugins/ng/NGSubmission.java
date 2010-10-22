package gridpilot.csplugins.ng;

import java.awt.GridLayout;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import javax.swing.JCheckBox;
import javax.swing.JPanel;

import org.globus.gsi.GlobusCredential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.util.GlobusURL;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.nordugrid.gridftp.ARCGridFTPJob;
import org.nordugrid.gridftp.ARCGridFTPJobException;
import org.nordugrid.is.ARCDiscoveryException;
import org.nordugrid.matcher.Matcher;
import org.nordugrid.matcher.SimpleMatcher;
import org.nordugrid.model.ARCResource;

import gridfactory.common.ConfirmBox;
import gridfactory.common.Debug;
import gridfactory.common.JobInfo;
import gridpilot.DBPluginMgr;
import gridpilot.MyJobInfo;
import gridpilot.GridPilot;
import gridpilot.MyLogFile;
import gridpilot.MyUtil;

/**
 * Submission class for the NorduGrid plugin.
 * <p><a href="NGSubmission.java.html">see sources</a>
 */

public class NGSubmission{
 
  private MyLogFile logFile;
  private String csName;
  private String [] clusters = null;
  private int clusterIndex = 0;
  private ARCResource [] resources = null;
  private NGScriptGenerator scriptGenerator;
  private String xrslFileName;
  private List files;
  private List fileNames;
  private String xrsl;
  private String submissionHost;
  private String queue;
  private static String[] LAST_SELECTED_CLUSTERS;
  private static ARCResource[] LAST_SELECTED_RESOURCES;
  private static boolean REMEMBER_CLUSTERS = false;
  private static int MAX_SUBMIT_RETRIES = 3;

  public NGSubmission(String _csName, String [] _clusters){
    Debug.debug("Loading class NGSubmission", 3);
    logFile = GridPilot.getClassMgr().getLogFile();
    csName = _csName;
    clusters = _clusters;
  }

  public NGSubmission(String _csName, ARCResource [] _resources){
    Debug.debug("Loading class NGSubmission", 3);
    logFile = GridPilot.getClassMgr().getLogFile();
    csName = _csName;
    resources = _resources;
    rankResources(resources);
  }

  public boolean submit(JobInfo job, String scriptName, String _xrslFileName) throws ARCDiscoveryException,
  MalformedURLException, IOException{

    xrslFileName = _xrslFileName;
    scriptGenerator =  new NGScriptGenerator(csName);
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName());
    
    String finalStdErr = dbPluginMgr.getStdOutFinalDest(job.getIdentifier());
    String finalStdOut = dbPluginMgr.getStdErrFinalDest(job.getIdentifier());
    // The default is now to create both stderr if either
    // final destination is specified for stderr or final destination is speficied for
    // neither stdout nor stderr
    boolean withStdErr = finalStdErr!=null && finalStdErr.trim().length()>0 ||
       (finalStdErr==null || finalStdErr.equals("")) &&
       (finalStdOut==null || finalStdOut.equals(""));
    Debug.debug("stdout/err: "+finalStdOut+":"+finalStdErr, 3);

    files = scriptGenerator.createXRSL((MyJobInfo) job, scriptName, xrslFileName, !withStdErr);
    // Now the short file names
    fileNames = new Vector();
    String file;
    for(Iterator it=files.iterator(); it.hasNext();){
      file = (new File((String) it.next())).getName();
      fileNames.add(file);
    }
    
    if(files==null){
      logFile.addMessage("Cannot create scripts for job " + job.getName() +
                              " on " + csName);
      return false;
    }
    String ngJobId = null;
    try{
      ngJobId = doSubmit();
    }
    catch(Exception e){
      e.printStackTrace();
      logFile.addMessage("Cannot submit job " + job.getName() +
          " to " + csName, e);
    }
    if(ngJobId==null){
      ((MyJobInfo) job).setCSStatus(NGComputingSystem.NG_STATUS_ERROR);
      return false;
    }
    else{
      job.setJobId(ngJobId);
      return true;
    }
  }

  private String doSubmit() throws ARCDiscoveryException,
  MalformedURLException, ARCGridFTPJobException, IOException, GeneralSecurityException, GSSException{
    
    queue = null;
    xrsl = "";
    BufferedReader in = null;
    try{
      Debug.debug("Reading file "+xrslFileName, 3);
      in = new BufferedReader(
        new InputStreamReader((new URL("file:"+xrslFileName)).openStream()));
    }
    catch(IOException ioe){
      logFile.addMessage("IOException during submission of " + xrslFileName + ":\n" +
                         "\tException\t: " + ioe.getMessage(), ioe);
      throw ioe;
    }
    try{
      String line;
      int lineNumber = 0;
      while((line=in.readLine())!=null){
        ++lineNumber;
        line = line.replaceAll("\\r", "");
        line = line.replaceAll("\\n", "");
        xrsl += line;
      }
      in.close();
    }
    catch(IOException ioe){
      logFile.addMessage("IOException during submission of " + xrslFileName + ":\n" +
                         "\tException\t: " + ioe.getMessage(), ioe);
      throw ioe;
    }
    submissionHost = null;
    if(resources==null){
      String [] selectedClusters;
      if(REMEMBER_CLUSTERS){
        selectedClusters = LAST_SELECTED_CLUSTERS;
      }
      else{
        if(clusters.length==1){
          selectedClusters = clusters;
        }
        else{
          selectedClusters = selectClusters(clusters);
        }
        LAST_SELECTED_CLUSTERS = selectedClusters;
      }
      // No information system.
      // Extremely simple brokering, but without the information system
      // and only a list of hosts we cannot do much else.
      submissionHost = selectedClusters[clusterIndex];
      if(clusterIndex>clusters.length-1){
        clusterIndex = 0;
      }
      else{
        ++clusterIndex;
      }
      // add gsiftp:// if not there
      if(submissionHost.startsWith("ldap://")){
        try{
          submissionHost =(new GlobusURL(submissionHost)).getHost();
        }
        catch(MalformedURLException e){
          throw new MalformedURLException("ERROR: host could not be parsed from "+submissionHost);
        }
      }
      if(!submissionHost.startsWith("gsiftp://")){
        submissionHost = "gsiftp://"+submissionHost+":2811/jobs";
      }
      // TODO: don't know if this will work - well it works with >0.5, NOT with 0.4.5
      queue = "";
    }
    else{
      findQueueWithInfoSys();
    }
    Debug.debug("Submitting to "+ submissionHost, 2);

    /*String testXrsl = "&(executable=/bin/echo)(jobName=\"Jarclib test submission \")" +
    "(action=request)(arguments=\"/bin/echo\" \"Test\")"+
    "(join=yes)(stdout=out.txt)(outputfiles=(\"test\" \"\"))(queue=\"" +
     "short" + "\")";*/
    
    if(queue==null){
      throw new ARCDiscoveryException("No queues found, cannot submit. Submission will not " +
              "work on 0.4.x servers.");
      //xrsl = xrsl.replaceFirst("\\(\\*(?i)queue=\"_submitqueue_\"\\*\\)",
      //    "");
    }
    else{
      xrsl = xrsl.replaceFirst("\\(\\*(?i)queue=\"_submitqueue_\"\\*\\)",
          "(queue=\""+queue+"\")");
    }
    xrsl = xrsl.replaceFirst("\\(\\*(?i)action=\"request\"\\*\\)",
        "(action=\"request\")");
    // Since cpuTime has been used to find queue, we no longer need it.
    // Moreover, it is minutes with 0.4.5 and seconds with 0.6 servers.
    xrsl = xrsl.replaceFirst("\\((?i)cputime=.*\\)\\(\\*endCpu\\*\\)", "");
    
    Debug.debug("XRSL: "+xrsl, 3);
    Debug.debug("Submitting with input files: "+MyUtil.arrayToString(files.toArray()), 2);
    Debug.debug("Submitting with input file names: "+MyUtil.arrayToString(fileNames.toArray()), 2);

    return arcSubmit();
  }
  
  private void findQueueWithInfoSys() throws ARCDiscoveryException, IOException, GeneralSecurityException {
    // Use information system
    Matcher matcher = new SimpleMatcher();
    
    ARCResource resource = null;
    ARCResource selectedResources [];
    if(REMEMBER_CLUSTERS){
      selectedResources = LAST_SELECTED_RESOURCES;
    }
    else{
      if(resources.length==1){
        selectedResources = resources;
      }
      else{
        selectedResources = selectResources(resources);
      }
      LAST_SELECTED_RESOURCES = selectedResources;
    }

    for(int i=0; i<selectedResources.length; ++i){
      try{
        Debug.debug("Checking resource "+selectedResources[i].getClusterName()+
            "/"+selectedResources[i].getQueueName()+
            //". RTES: "+MyUtil.arrayToString(resources[i].getRuntimeenvironment().toArray())+
            ". Min CPU time: "+selectedResources[i].getMinCpuTime()+
            ". Max CPU time: "+selectedResources[i].getMaxCpuTime(), 2);
        // Very simple brokering.
        // If resource is suitable and has free slots, the job
        // is submitted, otherwise try next resource.
        // If no free slots are found, submit to the resource
        // with highest number of CPUs
        if(matcher.isResourceSuitable(xrsl, selectedResources[i]) &&
            selectedResources[i].getFreejobs()>0){
          submissionHost = selectedResources[i].getClusterName();
          queue = selectedResources[i].getQueueName();
          Debug.debug("Submitting to: "+submissionHost+"/"+queue, 2);
          break;
        }
        else{
          Debug.debug("Resources "+selectedResources[i].getClusterName()+"/"+selectedResources[i].getQueueName()+" does not match requirements. Free jobs: "+
              selectedResources[i].getFreejobs(), 2);
        }
      }
      catch(ARCGridFTPJobException ae){
        Debug.debug("ARCGridFTPJobException during submission of " + xrslFileName + ":\n" +
            "\tException\t: " + ae.getMessage(), 2);
        ae.printStackTrace();
      }
    }
    if(submissionHost==null){
      // no free slots found, take best bet
      for(int i=0; i<selectedResources.length; ++i){
        try{
          if(matcher.isResourceSuitable(xrsl, selectedResources[i]) &&
              (resource==null ||
                  selectedResources[i].getTotalQueueCPUs()>resource.getTotalQueueCPUs() ||
                  selectedResources[i].getFreejobs()>resource.getFreejobs())){
              resource = selectedResources[i];
              queue = resource.getQueueName();
              Debug.debug("Submitting to: "+submissionHost+"/"+queue, 2);          
              break;
           }
           else{
             logFile.addInfo("Resource rejected: \n"+
                 "Max jobs:"+selectedResources[i].getMaxjobs()+
                 "\nTotal CPUs:"+selectedResources[i].getTotalQueueCPUs());
           }
        }
        catch(ARCGridFTPJobException ae){
          ae.printStackTrace();
        }
      }
      if(resource!=null){
        submissionHost = resource.getClusterName();
      }
    }
    if(submissionHost==null){
      throw new ARCDiscoveryException("No suitable clusters found.");
    }
    // add gsiftp:// if not there
    if(submissionHost.startsWith("ldap://")){
      try{
        submissionHost =(new GlobusURL(submissionHost)).getHost();
      }
      catch(MalformedURLException e){
        throw new MalformedURLException("ERROR: host could not be parsed from "+submissionHost);
      }
    }
    if(!submissionHost.startsWith("gsiftp://")){
      submissionHost = "gsiftp://"+submissionHost+":2811/jobs";
    }
    Debug.debug("Submitting to: "+submissionHost+"/"+queue, 2);
  }

  private String[] selectClusters(String[] _clusters) {

    int choice = -1;
    
    JCheckBox [] cbsClusters = new JCheckBox[_clusters.length];
    for(int i=0; i<cbsClusters.length; ++i){
      cbsClusters[i] = new JCheckBox(_clusters[i], true);
    }
    final JCheckBox cbRemember = new JCheckBox("Remember selection", true);
    cbRemember.setSelected(false);
    ConfirmBox confirmBox = new ConfirmBox();
    final Object [] displayObjects = new Object[3];
    JPanel jp = new JPanel(new GridLayout(cbsClusters.length, 1));
    for(int i=0; i<cbsClusters.length; ++i){
      jp.add(cbsClusters[i]);
    }
    displayObjects[0] = "OK";
    displayObjects[1] = "Cancel";
    displayObjects[2] = cbRemember;
    try{
      choice = confirmBox.getConfirm("Select submission target(s)",
          jp, displayObjects);
    }
    catch(Exception e){
      e.printStackTrace();
      return null;
    }
    
    if(choice==displayObjects.length-3){
      if(cbRemember.isSelected()){
        REMEMBER_CLUSTERS = true;
      }
      Vector<String> retVec = new Vector<String>();
      for(int i=0; i<cbsClusters.length; ++i){
        if(cbsClusters[i].isSelected()){
          retVec.add(_clusters[i]);
        }
      }
      return retVec.toArray(new String[retVec.size()]);
    }
    return null;
  }

  private ARCResource[] selectResources(ARCResource[] _resources) {
    String[] resNames = new String[_resources.length];
    for(int i=0; i<resNames.length; ++i){
      resNames[i] = _resources[i].getClusterName()+"/"+_resources[i].getQueueName()+
         " ("+_resources[i].getClusterAlias()+")";
    }
    String [] newResNames = selectClusters(resNames);
    if(newResNames==null){
      return null;
    }
    Vector<ARCResource> retVec = new Vector<ARCResource>();
    for(int i=0; i<newResNames.length; ++i){
      for(int j=0; j<resNames.length; ++j){
        if(newResNames[i].equals(resNames[j])){
          retVec.add(_resources[j]);
          break;
        }
      }
    }
    return retVec.toArray(new ARCResource[retVec.size()]);
  }

  private String arcSubmit() throws IOException, GeneralSecurityException, GSSException {
    String ngJobId = null;
    int i = 0;
    ARCGridFTPJob gridJob = null;
    while(true){
      try{
        gridJob = new ARCGridFTPJob(submissionHost);
        GSSCredential credential = GridPilot.getClassMgr().getSSL().getGridCredential();
        GlobusCredential globusCred = null;
        if(credential instanceof GlobusGSSCredentialImpl){
          globusCred = ((GlobusGSSCredentialImpl)credential).getGlobusCredential();
        }
        gridJob.addProxy(globusCred);
        gridJob.connect();
        gridJob.submit(xrsl, files, fileNames);       
        ngJobId = gridJob.getGlobalId();
        Debug.debug("NG Job Id: " + ngJobId, 3);
        gridJob.disconnect();
        break;
      }
      catch(ARCGridFTPJobException ae){
        gridJob.disconnect();
        logFile.addMessage("ARCGridFTPJobException during submission of " + xrslFileName + ":\n" +
            "\tException\t: " + ae.getMessage(), ae);
        if(i>MAX_SUBMIT_RETRIES){
          Debug.debug("WARNING: could not submit job", 1);
          break;
        }
        else{
          Debug.debug("WARNING: problem submitting, retrying", 2);
          ++i;
          try{
            Thread.sleep(5000);
          }
          catch(Exception ee){
            break;
          }
          continue;
        }
      }
    }
    return ngJobId;
  }

  private ARCResource [] rankResources(ARCResource [] resources){
    ARCResource [] ret = null;
    for(int i=0; i<resources.length; ++i){
      // It seems that only getFreejobs and getTotalQueueCPUs can be trusted
      Debug.debug("Resource "+resources[i].getClusterName(), 2);
      Debug.debug("   getFreejobs: "+resources[i].getFreejobs(), 2);
      Debug.debug("   getMaxjobs: "+resources[i].getMaxjobs(), 2);
      Debug.debug("   getNodeCPU: "+resources[i].getNodeCPU(), 2);
      Debug.debug("   getNodememory: "+resources[i].getNodememory(), 2);
      Debug.debug("   getQueued: "+resources[i].getQueued(), 2);
      Debug.debug("   getQueueName: "+resources[i].getQueueName(), 2);
      Debug.debug("   getTotalClusterCPUs: "+resources[i].getTotalClusterCPUs(), 2);
      Debug.debug("   getTotalQueueCPUs: "+resources[i].getTotalQueueCPUs(), 2);
    }
    return ret;
  }
}