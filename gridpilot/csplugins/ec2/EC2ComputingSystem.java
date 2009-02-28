package gridpilot.csplugins.ec2;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import javax.swing.JOptionPane;

import org.globus.gsi.GlobusCredentialException;
import org.globus.util.GlobusURL;
import org.ietf.jgss.GSSException;

import com.jcraft.jsch.JSchException;
import com.xerox.amazonws.ec2.EC2Exception;
import com.xerox.amazonws.ec2.ImageDescription;
import com.xerox.amazonws.ec2.ReservationDescription;
import com.xerox.amazonws.ec2.ReservationDescription.Instance;

import gridfactory.common.ConfirmBox;
import gridfactory.common.DBRecord;
import gridfactory.common.DBResult;
import gridfactory.common.Debug;
import gridfactory.common.JobInfo;
import gridfactory.common.Shell;
import gridfactory.common.jobrun.RTECatalog;
import gridfactory.common.jobrun.RTEMgr;
import gridfactory.common.jobrun.RTECatalog.EBSSnapshotPackage;
import gridfactory.common.jobrun.RTECatalog.InstancePackage;
import gridfactory.common.jobrun.RTECatalog.MetaPackage;
import gridpilot.DBPluginMgr;
import gridpilot.MyComputingSystem;
import gridpilot.GridPilot;
import gridpilot.MyJobInfo;
import gridpilot.MySecureShell;
import gridpilot.MyUtil;
import gridpilot.RteRdfParser;
import gridpilot.csplugins.forkpool.ForkPoolComputingSystem;

public class EC2ComputingSystem extends ForkPoolComputingSystem implements MyComputingSystem {

  private EC2Mgr ec2mgr = null;
  private String fallbackAmiID = null;
  // max time to wait for booting a virtual machine when submitting a job
  private static long MAX_BOOT_WAIT = 5*60*1000;
  // the user to use for running jobs on the virtual machines
  private static String USER = "root";
  private int maxMachines = 0;
  private HashMap<String, ArrayList<DBRecord>> allEC2RTEs;

  public EC2ComputingSystem(String _csName) throws Exception {
    super(_csName);
    
    allEC2RTEs = null;
    
    basicOSRTES = new String [] {"Linux"/*, "Windows"*/
        /* Windows instances allow only connections via VRDP - and to connect a keypair must be associated. */};

    fallbackAmiID = GridPilot.getClassMgr().getConfigFile().getValue(csName,
       "Fallback ami id");
    boolean ec2Secure = true;
    String ec2SecureStr = GridPilot.getClassMgr().getConfigFile().getValue(csName,
       "Secure");
    if(ec2SecureStr!=null && !ec2SecureStr.equalsIgnoreCase("yes") && !ec2SecureStr.equalsIgnoreCase("true")){
      ec2Secure = false;
    }

    String ec2Server = GridPilot.getClassMgr().getConfigFile().getValue(csName,
      "Server address");
    if(ec2Server==null || ec2Server.equals("")){
      ec2Server = "ec2.amazonaws.com";
    }
    String ec2Path = GridPilot.getClassMgr().getConfigFile().getValue(csName,
       "Service path");
    if(ec2Path==null){
      ec2Path = "";
    }
    String ec2PortStr = GridPilot.getClassMgr().getConfigFile().getValue(csName,
       "Port number");
    int ec2Port = ec2Secure?443:80;
    if(ec2PortStr!=null){
      ec2Port = Integer.parseInt(ec2PortStr);
    }
   
    String accessKey = GridPilot.getClassMgr().getConfigFile().getValue(csName,
       "AWS access key id");
    String secretKey = GridPilot.getClassMgr().getConfigFile().getValue(csName,
       "AWS secret access key");
    String sshAccessSubnet = GridPilot.getClassMgr().getConfigFile().getValue(csName,
       "SSH access subnet");
    if(sshAccessSubnet==null || sshAccessSubnet.equals("")){
      // Default to global access
      sshAccessSubnet = "0.0.0.0/0";
    }
    String runDir = MyUtil.clearTildeLocally(MyUtil.clearFile(workingDir));
    Debug.debug("Using workingDir "+workingDir, 2);
    ec2mgr = new EC2Mgr(ec2Server, ec2Port, ec2Path, ec2Secure,
        accessKey, secretKey, sshAccessSubnet, getUserInfo(csName),
        runDir, transferControl);
 
    Debug.debug("Adding EC2 monitor", 2);
    EC2MonitoringPanel panel = new EC2MonitoringPanel(ec2mgr);
    // This causes the panel to be added to the monitoring window as a tab,
    // right after the transfer monitoring tab and before the log tab.
    GridPilot.extraMonitorTabs.add(panel);
        
    try{
      String mms = GridPilot.getClassMgr().getConfigFile().getValue(csName,
         "Maximum machines");
      maxMachines = Integer.parseInt(mms);
    }
    catch(Exception e){
      e.printStackTrace();
    }
    String jobsPerMachine = "1";
    try{
      jobsPerMachine = GridPilot.getClassMgr().getConfigFile().getValue(csName,
         "Jobs per machine");
    }
    catch(Exception e){
      e.printStackTrace();
    }
    // Fill hosts with nulls and assign values as jobs are submitted.
    hosts = new String[maxMachines];
    // Fill maxJobs with a constant number
    maxJobs = new String[maxMachines];
    Arrays.fill(maxJobs, jobsPerMachine);
    
    submittingHostJobs = new HashMap<String, HashSet<String>>();
    
    // Reuse running VMs
    discoverInstances();

  }
  
  public boolean preProcess(JobInfo job) throws Exception {
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName());
    String [] rtes = dbPluginMgr.getRuntimeEnvironments(job.getIdentifier());
    job.setRTEs(rtes);
    return super.preProcess(job);
  }

  /**
   * Override in order to postpone setting up shellMgrs till submission time (preProcess).
   */
  protected void setupRemoteShellMgrs(){
    remoteShellMgrs = new HashMap();
  }
  

  /**
   * Discover running virtual machines and ask to have them
   * included in the pool.
   */
  private void discoverInstances(){
    List reservations;
    try{
      reservations = ec2mgr.listReservations();
    }
    catch(Exception e1){
      e1.printStackTrace();
      return;
    }
    if(reservations==null){
      Debug.debug("No reservations found.", 2);
      return;
    }
    ReservationDescription res = null;
    Instance inst = null;
    ArrayList instances = new ArrayList();
    for(Iterator it=reservations.iterator(); it.hasNext();){
      res = (ReservationDescription) it.next();
      Debug.debug("checking reservation "+res.getReservationId(), 2);
      for(Iterator itt=res.getInstances().iterator(); itt.hasNext();){
        inst = (Instance) itt.next();
        if(inst.isShuttingDown() || inst.isTerminated()){
          continue;
        }
        Debug.debug("checking instance "+inst.getDnsName(), 2);
        // If we have no key for the host, we cannot use it.
        if(inst.getKeyName().equalsIgnoreCase(EC2Mgr.KEY_NAME)){
          instances.add(inst);
        }
      }
    }
    if(instances.isEmpty()){
      return;
    }
    String msg = "You have "+instances.size()+" running EC2 AMI instance(s).\n" +
       "Do you want to include it/them in the pool of compute hosts?";
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    if(GridPilot.splash!=null){
      GridPilot.splash.hide();
    }
    int choice = -1;
    try{
      choice = confirmBox.getConfirm("Confirm inclusion of hosts",
          msg, new Object[] {"Yes", "No"});
    }
    catch(Exception e){
      e.printStackTrace();
      return;
    }
    if(choice!=0){
      return;
    }
    try{
      ec2mgr.getKey();
    }
    catch(Exception e){
      e.printStackTrace();
      MyUtil.showError("Could not get SSH key.");
      return;
    }
    int i = 0;
    String hostName = null;
    for(Iterator it=instances.iterator(); it.hasNext();){
      hostName = ((Instance) it.next()).getDnsName();
      remoteShellMgrs.put(hostName, null);
      submittingHostJobs.put(hostName, new HashSet());
      if(i<maxMachines && hosts[i]==null){
        hosts[i] = hostName;
        ++i;
      }
    }
  }
  
  /**
   * Halt virtual machines with no running jobs -
   * ask for confirmation.
   */
  private void haltNonBusy(){
    List reservations;
    try{
      reservations = ec2mgr.listReservations();
    }
    catch(Exception e1){
      e1.printStackTrace();
      return;
    }
    if(reservations==null){
      Debug.debug("No reservations found.", 2);
      return;
    }
    ReservationDescription res = null;
    Instance inst = null;
    ArrayList passiveInstances = new ArrayList();
    ArrayList activeInstances = new ArrayList();
    for(Iterator it=reservations.iterator(); it.hasNext();){
      res = (ReservationDescription) it.next();
      Debug.debug("checking reservation"+res.getReservationId(), 2);
      for(Iterator itt=res.getInstances().iterator(); itt.hasNext();){
        inst = (Instance) itt.next();
        if(inst.isShuttingDown() || inst.isTerminated()){
          continue;
        }
        Debug.debug("checking instance "+inst.getDnsName(), 2);
        if(remoteShellMgrs.containsKey(inst.getDnsName()) &&
            remoteShellMgrs.get(inst.getDnsName())!=null &&
            ((MySecureShell) remoteShellMgrs.get(inst.getDnsName())).getJobsNumber()>0){
          activeInstances.add(inst.getInstanceId());
        }
        else{
          passiveInstances.add(inst.getInstanceId());
        }
      }
    }
    if(passiveInstances.isEmpty() && activeInstances.isEmpty()){
      return;
    }
    String msg = "You have running EC2 AMI instance(s).\n" +
    (passiveInstances.isEmpty()?"":"The following are not executing any GridPilot jobs:\n"+
    MyUtil.arrayToString(passiveInstances.toArray(), ",\n")+".\n") +
    (activeInstances.isEmpty()?"":"The following are executing GridPilot jobs:\n"+
    MyUtil.arrayToString(activeInstances.toArray(), ",\n")+".\n" )+
    "What do you want to do?";
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    int choice = -1;
    try{
      if(passiveInstances.isEmpty() || activeInstances.isEmpty()){
        choice = confirmBox.getConfirm("Confirm terminate instances",
            msg, new Object[] {"Do nothing", "Terminate all"});
      }
      else{
        choice = confirmBox.getConfirm("Confirm terminate instances",
            msg, new Object[] {"Do nothing", "Terminate all", "Terminate passive"});
      }
      if(choice==0){
        return;
      }
    }
    catch(Exception e){
      e.printStackTrace();
      return;
    }
    if(choice==1 || choice==2){
      int i = 0;
      String [] termArr = new String [passiveInstances.size()];
      for(Iterator it=passiveInstances.iterator(); it.hasNext();){
        termArr[i] = (String) it.next();
        ++i;
      }
      try{
        ec2mgr.terminateInstances(termArr);
      }
      catch(EC2Exception e){
        e.printStackTrace();
      }
    }
    if(choice==1){
      int i = 0;
      String [] termArr = new String [activeInstances.size()];
      for(Iterator it=activeInstances.iterator(); it.hasNext();){
        termArr[i] = (String) it.next();
        ++i;
      }
      try{
        ec2mgr.terminateInstances(termArr);
      }
      catch(EC2Exception e){
        e.printStackTrace();
      }
    }
  }

  public void exit() {
    try{
      super.exit();
    }
    catch(Exception e){
      e.printStackTrace();
    }
    haltNonBusy();
    ec2mgr.exit();
  }

  /**
   * Finds a Shell for a given host.
   * If the Shell is dead it is attempted to start a new one.
   * If no Shell exists for this host, a new one is created.
   * @param host a string identifying a host
   * @return a Shell
   * @throws JSchException 
   */
  protected Shell getShell(String host) throws JSchException{
    Shell mgr = null;
    /*
     * If there is no keyFile set, this is a VM reused from a previous GridPilot session.
     * If the secret key corresponding to the public key currently uploaded with EC2 is
     * available either on the hard disk or in the grid homedir, ec2mgr.getKey() will
     * set the keyFile.
     * If this fails, we cannot use this host and it is dropped from remoteShellMgrs and
     * hosts.
     */
    if(ec2mgr.getKeyFile()==null){
      try{
        ec2mgr.getKey();
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    if(ec2mgr.getKeyFile()==null){
      remoteShellMgrs.remove(host);
      for(int i=0; i<hosts.length; ++i){
        if(hosts[i].equals(host)){
          hosts[i] = null;
          break;
        }
      }
    }
    Debug.debug("host: "+host+"-->"+remoteShellMgrs.get(host)+"-->"+ec2mgr.getKeyFile(), 2);
    Debug.debug("remoteShellMgrs: "+MyUtil.arrayToString(remoteShellMgrs.keySet().toArray()), 2);
    if(host!=null && !host.equals("") &&
        !host.startsWith("localhost") && !host.equals("127.0.0.1")){
      if(!remoteShellMgrs.containsKey(host)){
        // This means the VM has just been booted
        Shell newShellMgr = new MySecureShell(host, USER, ec2mgr.getKeyFile(), "");
        remoteShellMgrs.put(host, newShellMgr);
        setupRuntimeEnvironmentsSSH(newShellMgr);
      }
      else if(remoteShellMgrs.get(host)==null){
        // This means the VM was running before starting
        // GridPilot and we need to reconnect. RTEs should already be setup.
        // -- well, we do it anyway, just in case
        Shell newShellMgr = new MySecureShell(host, USER, ec2mgr.getKeyFile(), "");
        Debug.debug("Added ShellMgr on already running host "+newShellMgr.getHostName(), 2);
        remoteShellMgrs.put(host, newShellMgr);
        setupRuntimeEnvironmentsSSH(newShellMgr);
      }
      MySecureShell sMgr = (MySecureShell) remoteShellMgrs.get(host);
      if(!sMgr.isConnected()){
        sMgr.reconnect();
      }
      mgr = sMgr;
    }
    else if(host!=null && !host.equals("") &&
        (host.startsWith("localhost") || host.equals("127.0.0.1"))){
      mgr = (Shell) remoteShellMgrs.get(host);
    }
    return mgr;
  }

  private boolean checkHostForJobs(int i) throws JSchException {
    String host = hosts[i];
    Shell mgr = null;
    int maxR = 1;
    int submitting = 0;
    maxR = 1;
    mgr = getShell(host);
    if(maxJobs!=null && maxJobs.length>i && maxJobs[i]!=null){
      maxR = Integer.parseInt(maxJobs[i]);
    }
    submitting = submittingHostJobs.get(host)!=null?((HashSet)submittingHostJobs.get(host)).size():0;
    if(mgr.getJobsNumber()+submitting<maxR){
      return true;
    }
    return false;
  }
  
  private String findAmiId(JobInfo job, String fallbackAmiId) {
    DBPluginMgr dbMgr;
    try{
      dbMgr = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName());    
    }
    catch(Exception e){
      logFile.addMessage("ERROR: could not load database", e);
      return null;
    }
    String nameField = MyUtil.getNameField(dbMgr.getDBName(), "runtimeEnvironment");
    if(allEC2RTEs==null){
      findAllEC2RTEs(dbMgr);
    }
    DBRecord rte;
    String rteName;
    String amiId;
    try{
      for(Iterator<DBRecord> it=allEC2RTEs.get(dbMgr.getDBName()).iterator(); it.hasNext();){
        amiId = null;
        rte = it.next();
        rteName = (String) rte.getValue(nameField);
        if(checkProvides(job, rte, rteName)){
          amiId = getAmiId(rteName);
          if(amiId!=null){
            return amiId;
          }
        }
      }
    }
    catch(Exception e){
      e.printStackTrace();
    }
    logFile.addInfo("No RTE found that provides "+MyUtil.arrayToString(job.getRTEs())+
        ". Falling back to "+fallbackAmiId);
    return fallbackAmiId;
  }
  
  private String getAmiId(String rteName) throws EC2Exception, IOException {
    List<ImageDescription> gpAMIs = ec2mgr.listAvailableAMIs(false, true);
    ImageDescription desc;
    for(Iterator<ImageDescription> it=gpAMIs.iterator(); it.hasNext();){
      desc = it.next();
      Debug.debug("Checking AMI "+getRteNameFromLocation(desc.getImageLocation())+"<->"+rteName, 3);
      if(getRteNameFromLocation(desc.getImageLocation()).equalsIgnoreCase(rteName)){
        return desc.getImageId();
      }
    }
    Debug.debug("No AMI matching "+rteName, 2);
    return null;
  }

  private boolean checkProvides(JobInfo job, DBRecord rte, String rteName) {
    String [] requestedRtes = job.getRTEs();
    if(requestedRtes==null || requestedRtes.length==0){
      Debug.debug("No RTEs requested, using "+rte, 1);
      return true;
    }
    Object providesStr = rte.getValue("provides");
    String [] provides;
    if(providesStr==null){
      provides = new String [] {rteName};
    }
    else{
      String [] rtes = MyUtil.split((String) providesStr);
      provides = new String [rtes.length+1];
      provides[0] = rteName;
      System.arraycopy(rtes, 0, provides, 1, rtes.length);
    }
    for(int i=0; i<requestedRtes.length; ++i){
      if(!MyUtil.arrayContains(provides, requestedRtes[i])){
        Debug.debug("Requested RTE "+requestedRtes[i]+" not provided by "+rteName, 3);
        return false;
      }
    }
    Debug.debug("All requested RTEs "+MyUtil.arrayToString(requestedRtes)+" provided by "+rteName, 3);
    return true;
  }

  /**
   * Get a list of all RTEs with computingSystem EC2.
   * These will all correspond to an AMI and each provide
   * some software RTEs.
   * @param dbMgr
   */
     
  private void findAllEC2RTEs(DBPluginMgr dbMgr) {
    DBResult rtes = dbMgr.getRuntimeEnvironments();
    DBRecord rte;
    ArrayList<DBRecord> ret = new ArrayList();
    String nameField = MyUtil.getNameField(dbMgr.getDBName(), "runtimeEnvironment");
    String rteName;
    for(int i=0; i<rtes.values.length; ++i){
      rte = rtes.getRow(i);
      rteName = (String) rte.getValue(nameField);
      // TODO: consider using RTEMgr.isVM() instead of relying on people starting their
      //       VM RTE names with VM/
      if(rteName.startsWith(RteRdfParser.VM_PREFIX) &&
          ((String) rte.getValue("computingSystem")).equalsIgnoreCase(csName)){
        ret.add(rte);
      }
    }
    allEC2RTEs.put(dbMgr.getDBName(), ret);
  }

  /**
   * Check if a list of dependencies are provided by the AMI of a given host.
   * @param host
   * @param deps
   * @return
   * @throws EC2Exception
   * @throws GlobusCredentialException
   * @throws IOException
   * @throws GeneralSecurityException
   * @throws GSSException
   */
  private boolean checkHostProvides(String host, String deps []) throws EC2Exception, GlobusCredentialException, IOException, GeneralSecurityException, GSSException {
    List reservationList = ec2mgr.listReservations();
    ArrayList<String> provides = new ArrayList();
    List instanceList = null;
    Instance instance = null;
    ReservationDescription reservation = null;
    String manifest = null;
    String rteName = null;
    for(Iterator it=reservationList.iterator(); it.hasNext();){
      reservation = (ReservationDescription) it.next();
      instanceList = ec2mgr.listInstances(reservation);
      for(Iterator itt=instanceList.iterator(); itt.hasNext();){
        instance = (Instance) itt.next();
        if(instance.getDnsName().equals(host)){
          manifest = ec2mgr.getImageDescription(instance.getImageId()).getImageLocation();
          rteName = getRteNameFromLocation(manifest);
          provides = getProvides(rteName);
          break;
        }
      }
    }
    for(int i=0; i<deps.length; ++i){
      if(!provides.contains(deps[i])){
        return false;
      }
    }
    return true;
  }
  
  private String getRteNameFromLocation(String manifest){
    String ret = manifest.replaceFirst("(?i)\\.xml$", "");
    ret = ret.replaceFirst("(?i)\\.manifest$", "");
    ret = ret.replaceFirst("(?i)^"+EC2Mgr.AMI_BUCKET, "");
    return ret;
  }
  
  /**
   * Get a list of RTE names provided by a given RTE, according
   * to one of the used databases (they are tried one by one).
   * These databases have been updated at startup with information
   * from the software catalog fragments expected to be present
   * in S3 (SSS).
   * @param rteName
   * @return list of provided RTEs
   */
  private ArrayList<String> getProvides(String rteName) {
    ArrayList<String> provides = new ArrayList<String>();
    provides.add(rteName);
    DBPluginMgr dbMgr = null;
    String rteProvidesStr = null;
    String [] rteProvides = null;
    for(int i=0; i<runtimeDBs.length; ++i){
      try{
        dbMgr = GridPilot.getClassMgr().getDBPluginMgr(runtimeDBs[i]);
      }
      catch(Exception e){
        Debug.debug("WARNING: Could not load runtime DB "+
            runtimeDBs[i]+". "+e.getMessage(), 1);
        continue;
      }
      try{
        rteProvidesStr = (String) getRuntimeEnvironment(dbMgr, rteName).getValue("provides");
        if(rteProvidesStr!=null && !rteProvidesStr.equals("")){
          rteProvides = MyUtil.split(rteProvidesStr);
          for(int j=0; j<rteProvides.length; ++j){
            provides.add(rteProvides[j]);
          }
          break;
        }
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    return provides;
  }
  
  // TODO: consider providing this method in Database
  private DBRecord getRuntimeEnvironment(DBPluginMgr dbMgr, String rteName) throws IOException{
    DBResult rtes = dbMgr.getRuntimeEnvironments();
    String nameField = MyUtil.getNameField(dbMgr.getDBName(), "runtimeEnvironment");
    for(int i=0; i<rtes.values.length; ++i){
      if(rteName.equalsIgnoreCase((String) rtes.getValue(i, nameField))){
        return rtes.getRow(i);
      }
    }
    throw new IOException("No runtimeEnvironment with name "+rteName);
  }

  private File [] getAllTmpCatalogFiles() throws Exception{
    List<ImageDescription> gpAMIs = ec2mgr.listAvailableAMIs(false, true);
    ArrayList<File> files = new ArrayList();
    for(Iterator<ImageDescription> it=gpAMIs.iterator(); it.hasNext();){
      files.add(getTmpCatalogFile(it.next().getImageId()));
    }
    File [] ret = files.toArray(new File[files.size()]);
    Debug.debug("Saved the following RTE catalogs: "+MyUtil.arrayToString(ret), 2);
    return ret;
  }
  
  private File getTmpCatalogFile(String imageId) throws NullPointerException, MalformedURLException, Exception{
    String manifest = ec2mgr.getImageDescription(imageId).getImageLocation();
    String rdfFile = manifest;
    rdfFile = rdfFile.replaceFirst("(?i)\\.xml$", "");
    rdfFile = rdfFile.replaceFirst("(?i)\\.manifest$", "");
    rdfFile = rdfFile+".rdf";
    Debug.debug("rdfFile --> "+rdfFile, 2);
    String rteName = getRteNameFromLocation(manifest);
    File tmpCatalogFile = downloadFromSSS(rdfFile);
    GridPilot.addTmpFile(tmpCatalogFile.getAbsolutePath(), tmpCatalogFile);
    return tmpCatalogFile;
  }
  
  private File downloadFromSSS(String path) throws NullPointerException, MalformedURLException, Exception{
    File tmpFile = File.createTempFile(MyUtil.getTmpFilePrefix(), ".rdf");
    tmpFile.delete();
    GridPilot.getClassMgr().getFTPlugin("sss").getFile(new GlobusURL("sss://"+path), tmpFile);
    // If an AMIs has a '+' in its manifest path name, the actual path of the manifest in S3 will
    // be the path name with the + replaced with a space...
    if(!tmpFile.exists()){
      GridPilot.getClassMgr().getFTPlugin("sss").getFile(new GlobusURL("sss://"+path.replaceAll("\\+", " ")), tmpFile);
    }
    return tmpFile;
  }
  
  public void setupRuntimeEnvironments(String thisCs) {
    File[] allTmpCatalogFiles;
    try{
      allTmpCatalogFiles = getAllTmpCatalogFiles();
    }
    catch(Exception e){
      logFile.addMessage("ERROR: unable to get RTE information from AWS.", e);
      e.printStackTrace();
      return;
    }
    String [] allTmpCatalogs = new String[allTmpCatalogFiles.length];
    for(int i=0; i<allTmpCatalogFiles.length; ++i){
      allTmpCatalogs[i] = allTmpCatalogFiles[i].getAbsolutePath();
    }
    for(int i=0; i<runtimeDBs.length; ++i){
      try{
        MyUtil.syncRTEsFromCatalogs(csName, allTmpCatalogs, runtimeDBs, toDeleteRTEs,
            mkLocalOSRTE, includeVMRTEs, basicOSRTES, true);
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    //MyUtil.syncRTEsFromCatalogs(csName, rteCatalogUrls, runtimeDBs, toDeleteRTEs,
    //   mkLocalOSRTE, includeVMRTEs, basicOSRTES);
  }

  /**
   * The brokering algorithm. As simple as possible: FIFO.
   * Slight extension as compared to ForkPoolComputingSystem:
   * start shell and get host if none is running on the slot.
   */
  protected synchronized String selectHost(JobInfo job){
    // First try to use an already used instance
    for(int i=0; i<hosts.length; ++i){
      try{
        if(hosts[i]==null){
          continue;
        }
        if(/* This means that the host has been added by discoverInstances, i.e. that we can use it */
            submittingHostJobs.containsKey(hosts[i]) &&
            /**/
            checkHostProvides(hosts[i], job.getRTEs()) && checkHostForJobs(i)){
          return hosts[i];
        }
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    // then try to boot an instance
    String amiID = null;
    EBSSnapshotPackage [] ebsSnapshots = null;
    for(int i=0; i<hosts.length; ++i){
      ebsSnapshots = null;
      try{
        if(hosts[i]!=null){
          continue;
        }
        amiID = findAmiId(job, fallbackAmiID);
        Debug.debug("Booting "+amiID, 2);
        ReservationDescription desc = ec2mgr.launchInstances(amiID, 1);
        Instance inst = ((Instance) desc.getInstances().get(0));
        // Wait for the machine to boot
        long startMillis = MyUtil.getDateInMilliSeconds(null);
        long nowMillis = MyUtil.getDateInMilliSeconds(null);
        List reservationList = null;
        List instanceList = null;
        Instance instance = null;
        while(!inst.isRunning()){
          nowMillis = MyUtil.getDateInMilliSeconds(null);
          if(nowMillis-startMillis>MAX_BOOT_WAIT){
            logFile.addMessage("ERROR: timeout waiting for image "+inst.getImageId()+
                 "to boot for job "+job.getIdentifier());
            return null;
          }
          reservationList = ec2mgr.listReservations();
          instanceList = null;
          instance = null;
          ReservationDescription reservation = null;
          Debug.debug("Finding reservations... ", 1);
          for(Iterator it=reservationList.iterator(); it.hasNext();){
            reservation = (ReservationDescription) it.next();
            instanceList = ec2mgr.listInstances(reservation);
            // "Reservation ID", "Owner", "Instance ID", "AMI", "State", "Public DNS", "Key"
            for(Iterator itt=instanceList.iterator(); itt.hasNext();){
              instance = (Instance) itt.next();
              if(reservation.getReservationId().equalsIgnoreCase(reservation.getReservationId())){
                inst = instance;
              }
            }
          }
          Debug.debug("Waiting for EC2 machine to boot... "+inst.getState()+":"+inst.getStateCode(), 1);
          if(inst.isRunning() || inst.getState().equalsIgnoreCase("running")){
            break;
          }
          Thread.sleep(5000);
        }
        // If the VM RTE has any dependencies on EBSSnapshots, create EBS volume and mount it
        ebsSnapshots = getEBSSnapshots(amiID);
        mountEBSVolumes(inst, ebsSnapshots);
        hosts[i] = inst.getDnsName();
        Debug.debug("Returning host "+hosts[i]+" "+inst.getState(), 1);
        submittingHostJobs.put(hosts[i], new HashSet());
        return hosts[i];
      }
      catch(Exception e){
        e.printStackTrace();
        return null;
      }
    }
    return null;
  }
  
  /**
   * Convert an integer to any base.
   * The base can be in the range 2-26.
   * Each cipher of the resulting number is represented by a letter A-Z -
   * i.e. 1 is represented by A, 2 by B, etc.
   * From http://www.devx.com/vb2themax/Tip/19082
   * @param number the integer in question - starting with 1
   * @param base the base of the number system in question
   * @return the number in the base in question
   * @throws Exception 
   */
  private static String dec2any(int number, int base) throws Exception{
    String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    // check base
    if(base<2 || base>26){
      throw new Exception("base must be between 2 and 26");
    }
    //get the list of valid digits
    String digits = alphabet.substring(0, base);
    // convert to the other base
    int digitValue;
    String ret = "";
    do{
      digitValue = number % base;
      number = number / base;
      ret = digits.substring(digitValue-1, digitValue) + ret;
    }
    while(number>0);
    return ret;
  }

  /**
   * Find snapshots of software volumes to attach to a given AMI
   * by querying the software catalog.<br><br>
   * A given AMI corresponds to an RTE in the catalog. This RTE may
   * have a dependency on one or several EBSSnapshotPackages. Such
   * EBSSnapshotPackages are returned.
   * @param amiId ID of an AMI
   * @return a list of snapshot identifiers
   * @throws Exception 
   */
  private EBSSnapshotPackage[] getEBSSnapshots(String amiId) throws Exception{
    String rteName = getRteNameFromLocation(ec2mgr.getImageDescription(amiId).getImageLocation());
    RTEMgr rteMgr = GridPilot.getClassMgr().getRTEMgr(GridPilot.runtimeDir, rteCatalogUrls);
    RTECatalog catalog = rteMgr.getRTECatalog();
    HashMap<String, Vector<String>> depsMap = rteMgr.getVmRteDepends(rteName, null);
    Vector<String> deps = depsMap.get(null);
    String dep;
    MetaPackage mp;
    Vector<EBSSnapshotPackage> sPacks = new Vector();
    InstancePackage ip;
    for(Iterator<String> it=deps.iterator(); it.hasNext();){
      dep = it.next();
      mp = catalog.getMetaPackage(dep);
      if(mp.instances==null){
        continue;
      }
      for(int i=0; i<mp.instances.length; ++i){
        ip = catalog.getInstancePackage(mp.instances[i]);
        if(ip.getClass().getCanonicalName().equals(RTECatalog.EBSSnapshotPackage.class.getCanonicalName())){
          sPacks.add((EBSSnapshotPackage) ip);
          // There should be only one EBSSnapshotPackage instance of a MetaPackage.
          continue;
        }
      }
    }
    return sPacks.toArray(new EBSSnapshotPackage[sPacks.size()]);
  }

  /**
   * Mount an attached EBS volume inside an EC2 instance.
   * @param inst
   * @param pack
   * @param device
   * @throws Exception 
   */
  private void mountEBSVolumes(Instance inst, EBSSnapshotPackage[] ebsSnapshots) throws Exception {
    if(ebsSnapshots!=null && ebsSnapshots.length>0){
      String device;
      Shell shell = getShell(inst.getDnsName());
      StringBuffer stdOut = new StringBuffer();
      StringBuffer stdErr = new StringBuffer();
      for(int j=0; j<ebsSnapshots.length; ++j){
        // We assume there are not other devices mounted above /dev/sdd
        device = "/dev/sd"+dec2any(j+4, 26);
        ec2mgr.attachVolumeFromSnapshot(inst, ebsSnapshots[j].snapshotId, device);
        shell.exec("mount "+device+" "+ebsSnapshots[j].mountpoint, stdOut, stdErr);
        logFile.addInfo("Mounted: "+device+" on "+ebsSnapshots[j].mountpoint+"\n-->"+stdOut+":"+stdErr);
      }
    }
  }

}
