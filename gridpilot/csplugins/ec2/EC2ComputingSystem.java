package gridpilot.csplugins.ec2;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Vector;

import org.globus.gsi.GlobusCredentialException;
import org.globus.util.GlobusURL;
import org.ietf.jgss.GSSException;

import com.jcraft.jsch.JSchException;
import com.xerox.amazonws.ec2.EC2Exception;
import com.xerox.amazonws.ec2.ImageDescription;
import com.xerox.amazonws.ec2.ReservationDescription;
import com.xerox.amazonws.ec2.ReservationDescription.Instance;

import gridfactory.common.ConfigFile;
import gridfactory.common.ConfirmBox;
import gridfactory.common.DBRecord;
import gridfactory.common.DBResult;
import gridfactory.common.Debug;
import gridfactory.common.JobInfo;
import gridfactory.common.LocalStaticShell;
import gridfactory.common.MyLinkedHashSet;
import gridfactory.common.Shell;
import gridfactory.common.jobrun.RTECatalog;
import gridfactory.common.jobrun.RTEInstaller;
import gridfactory.common.jobrun.RTEMgr;
import gridfactory.common.jobrun.RTECatalog.AMIPackage;
import gridfactory.common.jobrun.RTECatalog.EBSSnapshotPackage;
import gridfactory.common.jobrun.RTECatalog.InstancePackage;
import gridfactory.common.jobrun.RTECatalog.MetaPackage;
import gridpilot.DBPluginMgr;
import gridpilot.MyComputingSystem;
import gridpilot.GridPilot;
import gridpilot.MyJobInfo;
import gridpilot.MyResThread;
import gridpilot.MySecureShell;
import gridpilot.MyUtil;
import gridpilot.csplugins.forkpool.ForkPoolComputingSystem;

public class EC2ComputingSystem extends ForkPoolComputingSystem implements MyComputingSystem {

  private EC2Mgr ec2mgr = null;
  private String fallbackAmiID = null;
  private String fallbackAmiName = null;  
  // max time to wait for booting a virtual machine when submitting a job
  private static int MAX_BOOT_WAIT_MILLIS = 5*60*1000;
  // max time to wait for booting a virtual machine and installing required RTEs
  private static int MAX_BOOT_INSTALL_WAIT_SECONDS = 10*60;
  private int maxMachines = 0;
  private HashMap<String, ArrayList<DBRecord>> allEC2RTEs;
  private String [] defaultEc2Catalogs;
  private HashMap<String, String> locationNameMap = new HashMap<String, String>();
  private String[] allTmpCatalogs;
  private String accessKey;
  private String secretKey;
  private HashMap<JobInfo, String> jobAmis;
  private HashMap<String, String> loginUsers;
  private int ramMB = -1;
  private String instanceType = null;
  
  // These variables will be set in the shells run on EC2 instances
  private static String AWS_ACCESS_KEY_ID_VAR = "AWS_ACCESS_KEY_ID";
  private static String AWS_SECRET_ACCESS_KEY_VAR = "AWS_SECRET_ACCESS_KEY";

  public static String[] AMI_PATTERNS = null;

  public EC2ComputingSystem(String _csName) throws Exception {
    super(_csName);
    
    if(MAX_BOOT_INSTALL_WAIT_SECONDS>GridPilot.FIRST_JOB_SUBMITTED_WAIT_SECONDS){
      GridPilot.FIRST_JOB_SUBMITTED_WAIT_SECONDS = MAX_BOOT_INSTALL_WAIT_SECONDS;
    }
    
    ConfigFile configFile = GridPilot.getClassMgr().getConfigFile();

    ignoreBaseSystemAndVMRTEs = false;
    
    allEC2RTEs = new HashMap<String, ArrayList<DBRecord>>();
    
    jobAmis = new HashMap<JobInfo, String>();
    loginUsers = new HashMap<String, String>();
    
    basicOSRTES = new String [] {"Linux"/*, "Windows"*/
        /* Windows instances allow only connections via VRDP - and to connect a keypair must be associated. */};

    fallbackAmiID = configFile.getValue(csName, "Fallback ami id");
    defaultEc2Catalogs = new String [] {"sss://gridpilot/ec2_rtes.xml"};
    String [] testEc2Catalogs = configFile.getValues(csName, "Runtime catalog URLs");
    if(testEc2Catalogs!=null && testEc2Catalogs.length>0){
      defaultEc2Catalogs = testEc2Catalogs;
    }
    String[] testAmiPatterns = configFile.getValues(csName, "AMI patterns");
    if(testAmiPatterns!=null){
      AMI_PATTERNS = testAmiPatterns;
    }
    else{
      logFile.addInfo("WARNING: AMI pattern not set. All AMIs will be listed as RTEs." +
          " This is very time consuming and may not be what you want.");
    }
    boolean ec2Secure = true;
    String ec2SecureStr = configFile.getValue(csName, "Secure");
    if(ec2SecureStr!=null && !ec2SecureStr.equalsIgnoreCase("yes") && !ec2SecureStr.equalsIgnoreCase("true")){
      ec2Secure = false;
    }

    String ec2Server = configFile.getValue(csName, "Server address");
    if(ec2Server==null || ec2Server.equals("")){
      ec2Server = "ec2.amazonaws.com";
    }
    String ec2Path = configFile.getValue(csName, "Service path");
    if(ec2Path==null){
      ec2Path = "";
    }
    String ec2PortStr = configFile.getValue(csName, "Port number");
    int ec2Port = ec2Secure?443:80;
    if(ec2PortStr!=null){
      ec2Port = Integer.parseInt(ec2PortStr);
    }
   
    accessKey = configFile.getValue(csName, "AWS access key id");
    secretKey = configFile.getValue(csName, "AWS secret access key");
    // Set up submit shell variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
    // - these are for use by s3_copy - and any other tools that might need them.
    submitEnvironment = new String [] {AWS_ACCESS_KEY_ID_VAR+"="+accessKey,
                                       AWS_SECRET_ACCESS_KEY_VAR+"="+secretKey};
    String sshAccessSubnet = configFile.getValue(csName, "SSH access subnet");
    if(sshAccessSubnet==null || sshAccessSubnet.equals("")){
      // Default to global access
      sshAccessSubnet = "0.0.0.0/0";
    }
    workingDir = configFile.getValue(csName, "working directory");
    if(workingDir==null || workingDir.equals("")){
      workingDir = "~";
    }
    String runDir = MyUtil.clearTildeLocally(MyUtil.clearFile(workingDir));
    Debug.debug("Using workingDir "+workingDir, 2);
    String memory = configFile.getValue(csName, "Memory");
    if(memory!=null && !memory.trim().equals("")){
      ramMB = Integer.parseInt(memory);
    }
    instanceType = configFile.getValue(csName, "Instance type");
    ec2mgr = new EC2Mgr(ec2Server, ec2Port, ec2Path, ec2Secure,
        accessKey, secretKey, sshAccessSubnet, getUserInfo(csName),
        runDir, transferControl);
    
    createMonitor();

    try{
      String mms = configFile.getValue(csName, "Max machines");
      maxMachines = Integer.parseInt(mms);
    }
    catch(Exception e){
      e.printStackTrace();
    }
    String jobsPerMachine = "1";
    try{
      jobsPerMachine = configFile.getValue(csName, "Max running jobs per host");
    }
    catch(Exception e){
      e.printStackTrace();
    }
    // Fill hosts with nulls and assign values as jobs are submitted.
    hosts = new String[maxMachines];
    // Fill maxJobs with a constant number
    maxRunningJobs = new String[maxMachines];
    Arrays.fill(maxRunningJobs, jobsPerMachine);    
    // Reuse running VMs
    discoverInstances();

  }
  
  private void createMonitor() throws InterruptedException, InvocationTargetException {
    javax.swing.SwingUtilities.invokeAndWait(new Runnable() {
      public void run(){
        try{
          Debug.debug("Adding EC2 monitor", 2);
          EC2MonitoringPanel panel = new EC2MonitoringPanel(ec2mgr);
          // This causes the panel to be added to the monitoring window as a tab,
          // right after the transfer monitoring tab and before the log tab.
          GridPilot.EXTRA_MONITOR_TABS.add(panel);
        }
        catch(Exception e){
          e.printStackTrace();
          logFile.addMessage("WARNING: could not create VM monitoring panel.", e);
        }
      }
    });
  }

  public boolean preProcess(JobInfo job) throws Exception {
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName());
    String [] rtes = dbPluginMgr.getRuntimeEnvironments(job.getIdentifier());
    job.setRTEs(rtes);
    if(ramMB>0){
      job.setRamMB(ramMB);
    }
    if(!super.preProcess(job) /* This is what boots up a VM. */){
      // There may still be hosts booting or unbooted - just return false - SubmissionControl
      // will check and fail job if necessary.
      Debug.debug("super.preProcess() failed, returning false", 2);
      return false;
    }
    return true;
  }
  
  private RTEMgr getRteMgr(){
    return GridPilot.getClassMgr().getRTEMgr(GridPilot.RUNTIME_DIR, allTmpCatalogs);
  }
  
  /**
   * Override to avoid trying to set up RTEs provided by the VM.
   */
  protected boolean setupJobRTEs(MyJobInfo job, Shell shell){
    try{
      MyUtil.setupJobRTEs(job, shell, getRteMgr(),
          GridPilot.getClassMgr().getTransferStatusUpdateControl(),
          runtimeDirectory, runtimeDirectory, true);
      return true;
    }
    catch(Exception e){
      e.printStackTrace();
      return false;
    }
  }

  /**
   * Override in order to postpone setting up shellMgrs till submission time (preProcess).
   */
  protected void setupRemoteShellMgrs(){
    remoteShellMgrs = new HashMap<String, Shell>();
  }
  

  /**
   * Discover running virtual machines and ask to have them
   * included in the pool.
   */
  private void discoverInstances(){
    List<ReservationDescription> reservations;
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
    ArrayList<Instance> instances = new ArrayList<Instance>();
    for(Iterator<ReservationDescription> it=reservations.iterator(); it.hasNext();){
      res = it.next();
      Debug.debug("checking reservation "+res.getReservationId(), 2);
      for(Iterator<Instance> itt=res.getInstances().iterator(); itt.hasNext();){
        inst = itt.next();
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
    
    try{
      checkForRunninginstances(instances);
    }
    catch(Exception e){
      e.printStackTrace();
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
    for(Iterator<Instance> it=instances.iterator(); it.hasNext();){
      inst = it.next();
      hostName = inst.getDnsName();
      remoteShellMgrs.put(hostName, null);
      preprocessingHostJobs.put(hostName, new HashSet<JobInfo>());
      if(i<maxMachines && hosts[i]==null){
        hosts[i] = hostName;
        ++i;
      }
    }
  }
  
  private void checkForRunninginstances(final ArrayList<Instance> instances) throws Exception {
    MyResThread rt = new MyResThread() {
      public void run(){
        try{
          String msg = "<html>You have "+instances.size()+" running EC2 AMI instance(s).\n<br>" +
          "Do you want to include it/them in the pool of compute hosts?</html>";
          ConfirmBox confirmBox = new ConfirmBox();
          if(GridPilot.SPLASH!=null){
            GridPilot.SPLASH.hide();
          }
          int choice = -1;
          choice = confirmBox.getConfirm("Confirm inclusion of hosts",
             msg, new Object[] {"Yes", "No"});
          GridPilot.SPLASH.show();
          if(choice!=0){
            throw new IOException("NOT including EC2 instances.");
          }
        }
        catch(Exception e){
          setException(e);
        }
      }
    };
    javax.swing.SwingUtilities.invokeAndWait(rt);
    if(rt.getException()!=null){
      throw rt.getException();
    }
  }

  /**
   * Halt virtual machines with no running jobs -
   * ask for confirmation.
   */
  private void haltNonBusy(){
    List<ReservationDescription> reservations;
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
    ArrayList<String> passiveInstances = new ArrayList<String>();
    ArrayList<String> activeInstances = new ArrayList<String>();
    for(Iterator<ReservationDescription> it=reservations.iterator(); it.hasNext();){
      res = (ReservationDescription) it.next();
      Debug.debug("checking reservation"+res.getReservationId(), 2);
      for(Iterator<Instance> itt=res.getInstances().iterator(); itt.hasNext();){
        inst = itt.next();
        if(!inst.isRunning()){
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
    ConfirmBox confirmBox = new ConfirmBox();
    int choice = -1;
    try{
      if(passiveInstances.isEmpty() || activeInstances.isEmpty()){
        choice = confirmBox.getConfirm("Confirm terminate instances",
            msg, new Object[] {"Do nothing", "Terminate"});
      }
      else{
        choice = confirmBox.getConfirm("Confirm terminate instance(s)",
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
      for(Iterator<String> it=passiveInstances.iterator(); it.hasNext();){
        termArr[i] = it.next();
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
      for(Iterator<String> it=activeInstances.iterator(); it.hasNext();){
        termArr[i] = it.next();
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

  public Shell getShell(JobInfo job){
    Debug.debug("Getting shell for job "+job.getIdentifier()+" on host "+job.getHost(), 2);
    Shell shell = null;
    try{
      if(job.getHost()!=null && !job.getHost().trim().equals("") &&
         job.getUserInfo()!=null && !job.getUserInfo().trim().equals("")){
        loginUsers.put(job.getHost().trim(), job.getUserInfo().trim());
      }
      shell = getShell(job.getHost());
    }
    catch(Exception e){
      e.printStackTrace();
    }
    Debug.debug("Returning "+shell, 2);
    return shell;
  }
  
  /**
   * Finds a Shell for a given host.
   * If the Shell is dead it is attempted to start a new one.
   * If no Shell exists for this host, a new one is created.
   * @param host a string identifying a host
   * @return a Shell
   * @throws Exception 
   */
  protected Shell getShell(String host) throws Exception{
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
    Debug.debug("remoteShellMgrs: "+remoteShellMgrs, 2);
    if(host!=null && !host.equals("") && !host.startsWith("localhost") && !host.equals("127.0.0.1")){
      String user = "root";
      if(loginUsers.containsKey(host) && loginUsers.get(host)!=null && !loginUsers.get(host).trim().equals("")){
        user = loginUsers.get(host);
      }
      if(!remoteShellMgrs.containsKey(host) || remoteShellMgrs.get(host)==null){
        // This means the VM has been terminated
        if(!MyUtil.arrayContains(hosts, host)){
          Debug.debug("Host terminated", 1);
          return null;
        }
        // This means the VM has just been booted or has been terminated or GridPilot has just been started
        Shell newShellMgr = new MySecureShell(host, user, ec2mgr.getKeyFile(), "");
        remoteShellMgrs.put(host, newShellMgr);
        setupRuntimeEnvironmentsSSH(newShellMgr);
      }
      else if(remoteShellMgrs.get(host)==null){
        // This means the VM was running before starting
        // GridPilot and we need to reconnect. RTEs should already be setup.
        // -- well, we do it anyway, just in case
        Shell newShell = new MySecureShell(host, user, ec2mgr.getKeyFile(), "");
        Debug.debug("Added ShellMgr on already running host "+newShell.getHostName(), 2);
        remoteShellMgrs.put(host, newShell);
        setupRuntimeEnvironmentsSSH(newShell);
      }
      MySecureShell sMgr = (MySecureShell) remoteShellMgrs.get(host);
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
    int maxP = 1;
    int preprocessing = 0;
    if(maxPreprocessingJobs!=null && maxPreprocessingJobs.length>i && maxPreprocessingJobs[i]!=null){
      maxP = Integer.parseInt(maxPreprocessingJobs[i]);
    }
    Debug.debug("Checking host "+host+" for preprocessing jobs - max "+maxP, 2);
    preprocessing = preprocessingHostJobs.get(host)!=null?((HashSet<JobInfo>)preprocessingHostJobs.get(host)).size():0;
    Debug.debug("--> Preprocessing: "+preprocessing, 2);
    if(preprocessing>=maxP){
      Debug.debug("Not selecting host "+host+" : "+preprocessing+">="+maxP, 2);
      return false;
    }
    /*int maxR = 1;
    int running = 0;
    if(maxRunningJobs!=null && maxRunningJobs.length>i && maxRunningJobs[i]!=null){
      maxR = Integer.parseInt(maxRunningJobs[i]);
    }
    Shell thisShell = getShell(host);
    if(thisShell!=null){
      running = thisShell.getJobsNumber();
    }
    Debug.debug("Checking host "+host+" for running jobs - max "+maxR, 2);
    Debug.debug("--> running: "+running, 2);
    if(running>=maxR){
      Debug.debug("Not selecting host "+host+" : "+running+">="+maxR, 2);
      return false;
    }*/
    return true;
  }
  
  public void setCSUserInfo(MyJobInfo job) {
    findAmiId(job);
  }
  
  private String findAmiId(JobInfo job) {
    if(jobAmis.containsKey(job)){
      return jobAmis.get(job);
    }
    DBPluginMgr dbMgr;
    try{
      dbMgr = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName());    
    }
    catch(Exception e){
      logFile.addMessage("ERROR: could not load database", e);
      return null;
    }
    String nameField = MyUtil.getNameField(dbMgr.getDBName(), "runtimeEnvironment");
    if(!allEC2RTEs.containsKey(dbMgr.getDBName())){
      findAllEC2RTEs(dbMgr);
    }
    DBRecord rte;
    String rteName;
    String[] amiIdAndUser = null;
    try{
      for(Iterator<DBRecord> it=allEC2RTEs.get(dbMgr.getDBName()).iterator(); it.hasNext();){
        amiIdAndUser = null;
        rte = it.next();
        rteName = (String) rte.getValue(nameField);
        //Debug.debug("Checking provides of "+rteName, 3);
        if(checkProvides(job, rte, rteName)){
          try{
            amiIdAndUser = getAmiIdAndUser(rteName, ((MyJobInfo)job).getDBName());
          }
          catch(Exception ee){
            ee.printStackTrace();
          }
          if(amiIdAndUser!=null && amiIdAndUser[0]!=null){
            job.setOpSys(rteName);
            job.setOpSysRTE(rteName);
            job.setUserInfo(amiIdAndUser[1]);
            Debug.debug("AMI user set to "+job.getUserInfo(), 2);
            Debug.debug("OpSysRTE set to "+job.getOpSysRTE(), 2);
            // This is just to have initLines written to the RTE DB record
            getEBSSnapshots(job.getOpSysRTE(), ((MyJobInfo) job).getDBName());
            //
            jobAmis.put(job, amiIdAndUser[0]);
            return amiIdAndUser[0];
          }
        }
      }
    }
    catch(Exception e){
      e.printStackTrace();
    }
    if(fallbackAmiID==null || fallbackAmiID.equals("")){
      logFile.addMessage("No RTE found that provides "+MyUtil.arrayToString(job.getRTEs())+
          " and could not find location/name/manifest of fallback AMI "+fallbackAmiID+":"+fallbackAmiName);
      return null;
    }
    logFile.addInfo("No RTE found that provides all "+MyUtil.arrayToString(job.getRTEs())+
        ". Falling back to "+fallbackAmiID+":"+fallbackAmiName);
    job.setOpSys(fallbackAmiName);
    job.setOpSysRTE(fallbackAmiName);
    return fallbackAmiID;
  }
  
  private String[] getAmiIdAndUser(String rteName, String dbName) throws Exception {
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
    String nameField = MyUtil.getNameField(dbPluginMgr.getDBName(), "runtimeEnvironment");
    DBResult allRtes = dbPluginMgr.getRuntimeEnvironments();
    DBRecord rec;
    String manifest = null;
    String[] ret = new String[]{"", ""};
    for(Iterator<DBRecord> it=allRtes.iterator(); it.hasNext();){
      rec = it.next();
      if(rec.getValue(nameField).equals(rteName) && rec.getValue("computingSystem").equals(csName)){
        manifest = (String) rec.getValue("url");
        if(manifest!=null){
          break;
        }
      }
    }
    if(manifest==null){
      throw new Exception("No RTE matching "+rteName+" found.");
    }
    List<ImageDescription> gpAMIs = ec2mgr.listAvailableAMIs(false, AMI_PATTERNS);
    ImageDescription desc;
    AMIPackage pack;
    RTEMgr ec2RteMgr = getRteMgr();
    for(Iterator<ImageDescription> it=gpAMIs.iterator(); it.hasNext();){
      desc = it.next();
      //Debug.debug("Finding AMI "+manifest+"<->"+desc.getImageLocation(), 3);
      if(desc.getImageLocation().equalsIgnoreCase(manifest)){
        ret[0] = desc.getImageId();
        pack = ec2RteMgr.getRTECatalog().getAMIPackage(desc.getImageId());
        if(pack==null){
          Debug.debug("WARNING: could not find AMIPackage with ID "+desc.getImageId(), 1);
        }
        else{
          ret[1] = ec2RteMgr.getRTECatalog().getAMIPackage(desc.getImageId()).user;
        }
        return ret;
      }
    }
    throw new Exception("No RTE matching "+rteName+" found.");
  }

  /**
   * Check if a given RTE (with record 'rte' and name 'rteName') provides
   * all RTEs requested by a given job.
   * @param job
   * @param rte
   * @param rteName
   * @return
   */
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
        //Debug.debug("Requested RTE "+requestedRtes[i]+" not provided by "+rteName, 3);
        return false;
      }
    }
    Debug.debug("All requested RTEs "+MyUtil.arrayToString(requestedRtes)+" provided by "+rteName, 2);
    return true;
  }

  /**
   * Get a list of all RTEs with computingSystem EC2.
   * These will all correspond to an AMI or an EBS snapshot and each provide
   * some software RTE(s).
   * @param dbMgr
   */
     
  private void findAllEC2RTEs(DBPluginMgr dbMgr) {
    DBResult rtes = dbMgr.getRuntimeEnvironments();
    DBRecord rte;
    ArrayList<DBRecord> ret = new ArrayList<DBRecord>();
    for(int i=0; i<rtes.values.length; ++i){
      rte = rtes.get(i);
      if(csName.equalsIgnoreCase(((String) rte.getValue("computingSystem")))){
        ret.add(rte);
      }
    }
    allEC2RTEs.put(dbMgr.getDBName(), ret);
  }

  /**
   * Check if dependencies of a job are provided by the AMI of a given host.
   * @param host
   * @param job
   * @return
   * @throws Exception 
   */
  private boolean checkHostProvides(String host, JobInfo job) throws Exception {
    
    String amiId = findAmiId(job);
    Debug.debug("Job "+job.getName()+" needs AMI "+amiId, 2);
    
    List<ReservationDescription> reservationList = ec2mgr.listReservations();
    List<Instance> instanceList = null;
    Instance instance = null;
    ReservationDescription reservation = null;
    for(Iterator<ReservationDescription> it=reservationList.iterator(); it.hasNext();){
      reservation = it.next();
      instanceList = ec2mgr.listInstances(reservation);
      for(Iterator<Instance> itt=instanceList.iterator(); itt.hasNext();){
        instance = itt.next();
        if(instance.getDnsName().equals(host)){
          Debug.debug("Host "+host+" is running AMI "+instance.getImageId(), 2);
          if(instance.getImageId().equals(amiId)){
            Debug.debug("Host "+host+" provides RTEs requested by job. "+job.getName()+":"+job.getUserInfo(), 2);
            if(job.getUserInfo()!=null && !job.getUserInfo().trim().equals("")){
              loginUsers.put(host, job.getUserInfo());
            }
            return true;
          }
        }
      }
    }
    Debug.debug("Host "+host+" does not provide RTEs requested by job. "+job.getName(), 2);
    return false;
  }
  
  private String getRteNameFromLocation(String manifest, String dbName) throws Exception{
    if(locationNameMap.containsKey(manifest)){
      return locationNameMap.get(manifest);
    }
    String ret;
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
    String nameField = MyUtil.getNameField(dbPluginMgr.getDBName(), "runtimeEnvironment");
    DBResult allRtes = dbPluginMgr.getRuntimeEnvironments();
    DBRecord rec;
    for(Iterator<DBRecord> it=allRtes.iterator(); it.hasNext();){
      rec = it.next();
      ret = (String) rec.getValue(nameField);
      if(rec.getValue("url")!=null && manifest.equalsIgnoreCase((String) rec.getValue("url")) &&
          !manifest.startsWith(ret)){
        locationNameMap.put(manifest, ret);
        return ret;
      }
    }
    throw new Exception("No RTE found with URL "+manifest);
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
        return rtes.get(i);
      }
    }
    throw new IOException("No runtimeEnvironment with name "+rteName);
  }

  private File [] getAllTmpCatalogFiles() throws EC2Exception {
    ArrayList<File> files = new ArrayList<File>();
    // First the default EC2-specific XML files
    File tmpCatalogFile;
    for(int i=0; i<defaultEc2Catalogs.length; ++i){
      try{
        tmpCatalogFile = downloadFromSSS(defaultEc2Catalogs[i]);
        files.add(tmpCatalogFile);
      }
      catch(Exception e){
        logFile.addMessage("WARNING: EC2 catalog misconfigured", e);
      }
    }
    List<ImageDescription> gpAMIs = ec2mgr.listAvailableAMIs(false, AMI_PATTERNS);
    ImageDescription desc;
    for(Iterator<ImageDescription> it=gpAMIs.iterator(); it.hasNext();){
      desc = it.next();
      if(!desc.getImageType().equalsIgnoreCase("machine")){
        continue;
      }
      // Since we are anyway iterating through the long list of AMIs,
      // find the name/manifest of the fallback AMI
      if(desc.getImageId().equalsIgnoreCase(fallbackAmiID)){
        fallbackAmiName = manifestToRTEName(desc.getImageLocation());
      }
      if(AMI_PATTERNS!=null && !AMI_PATTERNS.equals("")){
        try{
          files.add(getTmpCatalogFile(desc.getImageId()));
          continue;
        }
        catch(Exception e){
          //e.printStackTrace();
          Debug.debug("No XML file for AMI "+desc.getImageLocation()+". Using defaults.", 3);
        }
      }
      // If no custom XML file was found, add standard entry to RTE table
      createAmiOsRte(manifestToRTEName(desc.getImageLocation()),
          desc.getImageLocation(),
          desc.getArchitecture()+" "+desc.getImageType()+" "+
          /* This is a hack until Typica provides a getPlatform() method. */
          (desc.getImageType().equalsIgnoreCase("machine")?
          (desc.getImageLocation().matches("(?i).*windows.*")?"Windows":"Linux"):"")
      );
    }
    File [] ret = files.toArray(new File[files.size()]);
    Debug.debug("Saved the following RTE catalogs: "+MyUtil.arrayToString(ret), 2);
    return ret;
  }
  
  private String manifestToRTEName(String manifest){
    String ret = manifest.replaceFirst("(?i)\\.xml$", "");
    ret = ret.replaceFirst("(?i)\\.manifest$", "");
    return ret;
  }
  
  private void createAmiOsRte(String name, String url, String provides){
    DBPluginMgr dbMgr;
    String nameField;
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
        nameField = MyUtil.getNameField(runtimeDBs[i], "runtimeEnvironment");
        dbMgr.createRuntimeEnv(
            new String [] {nameField, "url", "computingSystem", "provides"},
            /*default to Linux*/
            new String [] {name, url, csName, provides});
        // Find the ID of the newly created RTE and tag it for deletion
        String [] rteIds = dbMgr.getRuntimeEnvironmentIDs(name, csName);
        for(int j=0; j<rteIds.length; ++j){
          toDeleteRTEs.put(rteIds[j], dbMgr.getDBName());
        }
      }
      catch(Exception e){
        e.printStackTrace();
        logFile.addMessage("WARNING: could not create RTE for local OS "+LocalStaticShell.getOSName()+
            " on "+csName, e);
      }
    }
  }
  
  /**
   * Check if there is an XML file next to a given AMI manifest and download it
   * to a temporary file if there is.
   * @param imageId the manifest location
   * @return the temporary file
   * @throws NullPointerException
   * @throws MalformedURLException
   * @throws Exception
   */
  private File getTmpCatalogFile(String imageId) throws NullPointerException, MalformedURLException, Exception{
    String manifest = ec2mgr.getImageDescription(imageId).getImageLocation();
    String xmlFile = manifestToRTEName(manifest)+".xml";
    Debug.debug("XML file --> "+xmlFile, 2);
    File tmpCatalogFile = downloadFromSSS(xmlFile);
    GridPilot.addTmpFile(tmpCatalogFile.getAbsolutePath(), tmpCatalogFile);
    if(tmpCatalogFile==null || tmpCatalogFile.length()==0){
      throw new Exception("No XML file found for "+imageId);
    }
    return tmpCatalogFile;
  }
  
  /**
   * 'path' can be of the form sss://bucket/file or just bucket/file.
   * @param path
   * @return
   * @throws NullPointerException
   * @throws MalformedURLException
   * @throws Exception
   */
  private File downloadFromSSS(String path) throws NullPointerException, MalformedURLException, Exception{
    File tmpFile = File.createTempFile(MyUtil.getTmpFilePrefix(), ".xml");
    tmpFile.delete();
    if(path.toLowerCase().startsWith("sss://")){
      GridPilot.getClassMgr().getFTPlugin("sss").getFile(new GlobusURL(path), tmpFile);
    }
    else{
      GridPilot.getClassMgr().getFTPlugin("sss").getFile(new GlobusURL("sss://"+path), tmpFile);
    }
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
    allTmpCatalogs = new String[allTmpCatalogFiles.length];
    for(int i=0; i<allTmpCatalogFiles.length; ++i){
      allTmpCatalogs[i] = allTmpCatalogFiles[i].getAbsolutePath();
    }
    for(int i=0; i<runtimeDBs.length; ++i){
      try{
        MyUtil.syncRTEsFromCatalogs(csName, allTmpCatalogs, runtimeDBs, toDeleteRTEs,
            mkLocalOSRTE, includeVMRTEs, basicOSRTES, false);
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
        if(!hostIsRunning(hosts[i])){
          logFile.addMessage("WARNING: host "+hosts[i]+" is not running.");
          remoteShellMgrs.remove(hosts[i]);
          hosts[i] = null;
          continue;
        }
        Debug.debug("Checking if we can reuse host "+hosts[i]+
            " from "+MyUtil.arrayToString(preprocessingHostJobs.keySet().toArray()), 2);
        if(/* This means that the host has been added by discoverInstances, i.e. that we can use it */
            preprocessingHostJobs.containsKey(hosts[i])){
          if(checkHostProvides(hosts[i], job)){
            Debug.debug("Dependencies provided by host", 2);
            if(checkHostForJobs(i)){
              job.setHost(hosts[i]);
              saveJobHost(job);
              return hosts[i];
            }
          }
          else{
            logFile.addInfo("Host "+hosts[i]+" does not provide "+MyUtil.arrayToString(job.getRTEs()));
          }
        }
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    // then try to boot an instance
    Debug.debug("Nope, no host can be reused, trying to boot a fresh one.", 2);
    String bootedHost = bootInstance(job);
    if(bootedHost!=null){
      job.setHost(bootedHost);
      saveJobHost(job);
      return bootedHost;
    }
    // then see if we can queue the job on a host
    //return super.selectHost(job);
    return null;
  }
  
  private boolean hostIsRunning(String host) throws EC2Exception, GlobusCredentialException, IOException, GeneralSecurityException, GSSException {
    List<ReservationDescription> reservationList = ec2mgr.listReservations();
    List<Instance> instanceList = null;
    Instance instance = null;
    ReservationDescription reservation = null;
    reservationList = ec2mgr.listReservations();
    instanceList = null;
    instance = null;
    Debug.debug("Finding reservations. "+reservationList.size(), 1);
    for(Iterator<ReservationDescription> it=reservationList.iterator(); it.hasNext();){
      reservation = it.next();
      instanceList = ec2mgr.listInstances(reservation);
      // "Reservation ID", "Owner", "Instance ID", "AMI", "State", "Public DNS", "Key"
      for(Iterator<Instance> itt=instanceList.iterator(); itt.hasNext();){
        instance = itt.next();
        if(instance.getDnsName().equals(host)){
          return (instance.isRunning() || instance.getState().equalsIgnoreCase("running"));
        }
      }
    }    
    return false;
  }

  // Types: "t1.micro", "m1.small", "m1.large", "m1.xlarge",
  // "c1.medium", "c1.xlarge",
  // "m2.xlarge", "m2.2xlarge", "m2.4xlarge",
  // "cc1.4xlarge", "cg1.4xlarge"
  // memory is in megabytes
  public  String getInstanceType(int memory) throws Exception{
    if(instanceType!=null && !instanceType.trim().equals("")){
      return instanceType;
    }
    String type;
    int[] desc;
    for(int i=0; i<EC2Mgr.INSTANCE_TYPES.length; ++i){
      type = EC2Mgr.INSTANCE_TYPES[i];
      desc = ec2mgr.instanceTypes.get(type);
      // TODO: add check on disc and CPU requirements. These fields should then be added to JobInfo.
      if(memory<=0 || memory<desc[1]){
        instanceType = type;
        return instanceType;
      }
    }
    throw new Exception("No EC2 instance type honors job requirements. "+memory);
  }
  
  private String bootInstance(JobInfo job){
    String host = null;
    for(int i=0; i<hosts.length; ++i){
      host = doBootInstance(i, job);
      if(host!=null){
        return host;
      }
    }
    Debug.debug("No free slot to boot host.", 2);
    return null;
  }
  
  private String doBootInstance(int i, JobInfo job) {
    EBSSnapshotPackage [] ebsSnapshots = null;
    try{
      if(hosts[i]!=null){
        return null;
      }
      String amiID = findAmiId(job);
      Debug.debug("Booting "+amiID, 2);
      String type = getInstanceType(job.getRamMB());
      ReservationDescription desc = ec2mgr.launchInstances(amiID, 1, type);
      Instance inst = ((Instance) desc.getInstances().get(0));
      // Wait for the machine to boot
      long startMillis = MyUtil.getDateInMilliSeconds(null);
      long nowMillis = MyUtil.getDateInMilliSeconds(null);
      List<ReservationDescription> reservationList = null;
      List<Instance> instanceList = null;
      Instance instance = null;
      Thread.sleep(10000);
      while(!inst.isRunning()){
        nowMillis = MyUtil.getDateInMilliSeconds(null);
        if(nowMillis-startMillis>MAX_BOOT_WAIT_MILLIS){
          logFile.addMessage("ERROR: timeout waiting for image "+inst.getImageId()+
               "to boot for job "+job.getIdentifier());
          return null;
        }
        reservationList = ec2mgr.listReservations();
        instanceList = null;
        instance = null;
        ReservationDescription reservation = null;
        Debug.debug("Finding reservations. "+reservationList.size(), 1);
        for(Iterator<ReservationDescription> it=reservationList.iterator(); it.hasNext();){
          reservation = it.next();
          instanceList = ec2mgr.listInstances(reservation);
          // "Reservation ID", "Owner", "Instance ID", "AMI", "State", "Public DNS", "Key"
          for(Iterator<Instance> itt=instanceList.iterator(); itt.hasNext();){
            instance = itt.next();
            if(reservation.getReservationId().equalsIgnoreCase(desc.getReservationId())){
              inst = instance;
              break;
            }
          }
        }
        if(inst.isRunning() || inst.getState().equalsIgnoreCase("running")){
          break;
        }
        Debug.debug("Waiting for EC2 machine to boot... "+inst.getState()+":"+inst.getStateCode(), 1);
        Thread.sleep(10000);
      }
      // If the VM RTE has any dependencies on EBSSnapshots, create EBS volume, 
      // and add initLines to the runtimeEnvironment record and mount the volume.
      ebsSnapshots = getEBSSnapshots(job.getOpSysRTE(), ((MyJobInfo) job).getDBName());
      Debug.debug("The AMI "+amiID+" will have the following EBS volumes attached: "+MyUtil.arrayToString(ebsSnapshots), 2);
      mountEBSVolumes(inst, ebsSnapshots);
      String [] tarPackages = getTarPackageRTEs(job);
      Debug.debug("Installing "+(tarPackages==null?"":MyUtil.arrayToString(tarPackages)), 3);
      if(job.getUserInfo()!=null && !job.getUserInfo().trim().equals("")){
        loginUsers.put(inst.getDnsName(), job.getUserInfo());
      }
      hosts[i] = inst.getDnsName();
      installTarPackages(inst, job.getOpSysRTE(), tarPackages);
      Debug.debug("Returning host "+hosts[i]+" "+inst.getState(), 1);
      preprocessingHostJobs.put(hosts[i], new HashSet<JobInfo>());
      // Make sure we remember the user in future sessions. - NO, don't. Makes it
      // impossible to know which jobs belong to who (and "Load my active jobs" will not work).
      //saveJobUser(job);
      return hosts[i];
    }
    catch(Exception e){
      logFile.addMessage("Problem booting host.", e);
      return null;
    }
  }

  private void saveJobUser(JobInfo job) {
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName());
    String [] fields = new String[]{JobInfo.USER_INFO};
    String [] values = new String[]{job.getUserInfo()};
    dbPluginMgr.updateJobDefinition(job.getIdentifier(), fields, values);
  }

  private void saveJobHost(JobInfo job) {
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName());
    String [] fields = new String[]{JobInfo.HOST};
    String [] values = new String[]{job.getHost()};
    dbPluginMgr.updateJobDefinition(job.getIdentifier(), fields, values);
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
    String alphabet = "abcdefghijklmnopqrstuvwxyz";
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
   * Find names of the parent MetaPackages of TarPackages that should be installed inside the AMI.<br><br>
   * Notice that VMForkComputingSystem has a different convention and installs these on the host machine.
   * @param job
   * @return a list of MetaPackage (RTE) names
   * @throws Exception
   */
  private String[] getTarPackageRTEs(JobInfo job) throws Exception{
    String opsysRte = job.getOpSysRTE();
    RTEMgr ec2RteMgr = getRteMgr();
    RTECatalog catalog = ec2RteMgr.getRTECatalog();
    LinkedHashSet<String> deps = new LinkedHashSet<String>();
    LinkedHashSet<String> provides = new LinkedHashSet<String>();
    try{
      // getVmRteDepends() throws an exception if no instance package can be found -
      // which is the case if opsysRte is an AMI name with no AMIPackage defined in
      // the catalog. Just ignore it.
      MetaPackage opsysMp = catalog.getMetaPackage(opsysRte);
      HashMap<String, LinkedHashSet<String>> depsMap = ec2RteMgr.getVmRteDepends(opsysRte, null);
      deps.addAll(depsMap.get(null));
      Collections.addAll(provides, opsysMp.provides);
      if(opsysMp.virtualMachine!=null && opsysMp.virtualMachine.os!=null){
        provides.add(opsysMp.virtualMachine.os);
      }
    }
    catch(Exception e){
    }
    // This should no longer be necessary - it's taken care of by ForkPoolComputingSystem.preProcess().
    //Collections.addAll(deps, requiredRuntimeEnvs);
    try{
      if(job.getRTEs()!=null && job.getRTEs().length>0){
        LinkedHashSet<String> rtesVec = new LinkedHashSet<String>();
        Collections.addAll(rtesVec, job.getRTEs());
        HashMap<String, MyLinkedHashSet<String>> depsMap =
          ec2RteMgr.getRteDepends(job.getVirtualize(), rtesVec, job.getOpSys(), null, true);
        String vmOs = depsMap.keySet().iterator().next();
        MyLinkedHashSet<String> depsVec = depsMap.get(vmOs);
        // Just in case the OS is listed as a dependence
        depsVec.remove(job.getOpSys());
        depsVec.remove(vmOs);
        deps.addAll(depsVec);
      }
    }
    catch(Exception e){
      e.printStackTrace();
    }
    Debug.debug("Found TarPackage dependences "+deps+" on "+opsysRte+" providing "+provides, 2);
    return MyUtil.removeBaseSystemAndVM(deps.toArray(new String[deps.size()]), provides.toArray(new String[provides.size()]));
  }

  /**
   * Find snapshots of software volumes to attach to a given AMI
   * by querying the software catalog.
   * <br><br>
   * A given AMI corresponds to an RTE in the catalog. This RTE may
   * have a dependency on one or several EBSSnapshotPackages. Such
   * EBSSnapshotPackages are returned.
   * <br><br>
   * initLines are added to the relevant runtimeEnvironment record, causing
   * the ScriptGenerator to add the right "source ..." lines to the job script.
   * @param opsysRte name of the VM RTE
   * @param dbName name of the database to use
   * @return a list of snapshot identifiers
   * @throws Exception 
   */
  private EBSSnapshotPackage[] getEBSSnapshots(String opsysRte, String dbName) throws Exception{
    RTEMgr ec2RteMgr = getRteMgr();
    RTECatalog catalog = ec2RteMgr.getRTECatalog();
    HashMap<String, LinkedHashSet<String>> depsMap = new HashMap<String, LinkedHashSet<String>>();
    try{
      // getVmRteDepends() throws an exception if no instance package can be found -
      // which is the case if opsysRte is an AMI name with no AMIPackage defined in
      // the catalog. Just ignore it.
      depsMap = ec2RteMgr.getVmRteDepends(opsysRte, null);
    }
    catch(Exception e){
      e.printStackTrace();
      return new EBSSnapshotPackage[] {};
    }
    //Vector<String> deps = depsMap.get(opsysRte);
    LinkedHashSet<String> deps = depsMap.get(null);
    Debug.debug("Found possible EBSSnapshot dependencies "+MyUtil.arrayToString(deps.toArray()), 2);
    String dep = null;
    MetaPackage mp;
    Vector<EBSSnapshotPackage> sPacks = new Vector<EBSSnapshotPackage>();
    InstancePackage ip;
    String initLines = "";
    for(Iterator<String> it=deps.iterator(); it.hasNext();){
      initLines = "";
      dep = it.next();
      mp = catalog.getMetaPackage(dep);
      Debug.debug("Checking possible EBSSnapshot dependence "+dep+":"+mp, 2);
      if(mp==null || mp.instances==null){
        // If mp has no instances, it certainly has no EBSSnapshotPackage instance
        continue;
      }
      Debug.debug("Possible EBSSnapshot instances:"+MyUtil.arrayToString(mp.instances), 2);
      for(int i=0; i<mp.instances.length; ++i){
        ip = catalog.getInstancePackage(mp.instances[i]);
        if(ip.getClass().getCanonicalName().equals(RTECatalog.EBSSnapshotPackage.class.getCanonicalName())){
          Debug.debug("Snapshot ID --> "+((RTECatalog.EBSSnapshotPackage)ip).snapshotId, 2);
          initLines += "source "+((EBSSnapshotPackage) ip).mountpoint+"/control/runtime 1";
          sPacks.add((EBSSnapshotPackage) ip);
          // There should be only one EBSSnapshotPackage instance of a MetaPackage.
          break;
        }
      }
      if(initLines.length()>0 && dep!=null){
        try{
          addEbsInitLines(/*opsysRte*/dep, initLines, dbName);
        }
        catch(Exception e){
        }
      }
    }
    return sPacks.toArray(new EBSSnapshotPackage[sPacks.size()]);
  }

  private void addEbsInitLines(String rteName, String initLines, String dbName) {
    DBPluginMgr mgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
    // There should be only one RTE with a given name
    String rteId = mgr.getRuntimeEnvironmentIDs(rteName, csName)[0];
    mgr.updateRuntimeEnvironment(rteId,
        new String[] {"initLines"},
        new String[] {initLines});
  }

  /**
   * Mount an attached EBS volume inside an EC2 instance.
   * @param inst
   * @param pack
   * @param device
   * @throws Exception 
   */
  private void mountEBSVolumes(Instance inst, EBSSnapshotPackage[] ebsSnapshots) throws Exception {
    if(ebsSnapshots==null || ebsSnapshots.length==0){
      return;
    }
    String device;
    Shell shell = getShell(inst.getDnsName());
    StringBuffer stdOut = new StringBuffer();
    StringBuffer stdErr = new StringBuffer();
    for(int j=0; j<ebsSnapshots.length; ++j){
      // We assume there are not other devices mounted above /dev/sdd
      device = "/dev/sd"+dec2any(j+5, 26);
      Debug.debug("Attaching snapshot "+ebsSnapshots[j].snapshotId+" on device "+device, 1);
      ec2mgr.attachVolumeFromSnapshot(inst, ebsSnapshots[j].snapshotId, device);
      shell.exec("mkdir "+ebsSnapshots[j].mountpoint, stdOut, stdErr);
      Exception ee = null;
      int i;
      int ret;
      // Try 5 times with 2 seconds in between to mount volume, then give up.
      for(i=0; i<5; ++i){
        try{
          Thread.sleep(2000);
          ret = shell.exec("mount "+device+"1 "+ebsSnapshots[j].mountpoint, stdOut, stdErr);
          if(ret==0){
            break;
          }
          else{
            throw new Exception(stdErr.toString());
          }
        }
        catch(Exception eee){
          ee = eee;
          continue;
        }
      }
      if(i==9 && ee!=null){
        throw ee;
      }
      logFile.addInfo("Mounted "+device+" on "+ebsSnapshots[j].mountpoint);
    }
  }
  
  private void installTarPackages(Instance inst, String os, String[] rtes) throws Exception {
        
    RTEMgr ec2RteMgr = getRteMgr();
    RTECatalog catalog = ec2RteMgr.getRTECatalog();    
    Shell shell = getShell(inst.getDnsName());
    
    RTEInstaller rteInstaller;
    MetaPackage mp;
    InstancePackage ip = null;
    for(int i=0; i<rtes.length; ++i){
      try{
        // First find the InstancePackage
        mp = catalog.getMetaPackage(rtes[i]);
        if(mp==null){
          logFile.addInfo("WARNING: no MetaPackage found with name "+rtes[i]+". Skipping.");
        }
        ip = null;
        try{
          ip = ec2RteMgr.getInstancePackage(mp, os);
        }
        catch(Exception ee){
          ee.printStackTrace();
        }
        if(mp!=null && ip==null){
          // If no InstancePackage could be found on the give OS, the OS is probably
          // an AMI not registered in the catalog - in this case, we just try the first
          // InstancePackage found...
          ip = ec2RteMgr.getInstancePackage(mp, null);
          if(ip!=null){
            logFile.addInfo(rtes[i]+" has no TarPackage instance on "+os+". Defaulting to "+ip.url+
                " on "+ip.baseSystem);
          }
        }
        // If it's a TarPackage, install it
        if(ip==null || !ip.getClass().getCanonicalName().equals(RTECatalog.TarPackage.class.getCanonicalName())){
          logFile.addInfo(rtes[i]+" has no TarPackage instance on "+os+". Skipping.");
          continue;
        }
      }
      catch(Exception e){
        logFile.addMessage("WARNING: problem installing "+rtes[i], e);
        continue;
      }
      rteInstaller = new RTEInstaller(runtimeDirectory, runtimeDirectory,
          GridPilot.getClassMgr().getTransferStatusUpdateControl(), logFile);
      rteInstaller.install(rtes[i], ip, shell);
      logFile.addInfo(rtes[i]+" successfully installed.");
    }
  }

}
