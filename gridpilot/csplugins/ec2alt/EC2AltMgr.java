package gridpilot.csplugins.ec2alt;

import gridfactory.common.Debug;
import gridfactory.common.LocalStaticShell;
import gridpilot.GridPilot;
import gridpilot.MyLogFile;
import gridpilot.MyTransferControl;
import gridpilot.MyUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.ec2.AmazonEC2;
import com.amazonaws.ec2.AmazonEC2Client;
import com.amazonaws.ec2.AmazonEC2Config;
import com.amazonaws.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.ec2.model.CreateKeyPairRequest;
import com.amazonaws.ec2.model.CreateKeyPairResponse;
import com.amazonaws.ec2.model.CreateSecurityGroupRequest;
import com.amazonaws.ec2.model.DeleteKeyPairRequest;
import com.amazonaws.ec2.model.DescribeImagesRequest;
import com.amazonaws.ec2.model.DescribeImagesResponse;
import com.amazonaws.ec2.model.DescribeInstancesRequest;
import com.amazonaws.ec2.model.DescribeInstancesResponse;
import com.amazonaws.ec2.model.DescribeKeyPairsRequest;
import com.amazonaws.ec2.model.DescribeSecurityGroupsRequest;
import com.amazonaws.ec2.model.DescribeSecurityGroupsResponse;
import com.amazonaws.ec2.model.Image;
import com.amazonaws.ec2.model.InstanceState;
import com.amazonaws.ec2.model.KeyPair;
import com.amazonaws.ec2.model.Reservation;
import com.amazonaws.ec2.model.RunInstancesRequest;
import com.amazonaws.ec2.model.RunInstancesResponse;
import com.amazonaws.ec2.model.RunningInstance;
import com.amazonaws.ec2.model.TerminateInstancesRequest;
import com.xerox.amazonws.ec2.InstanceType;

/**
 * This class contains methods for managing virtual machines in the
 * Amazon Elastic Compute Cloud (EC2).
 * It relies on the Typica library http://code.google.com/p/typica/wiki/TypicaSampleCode)
 */
public class EC2AltMgr {
  
  private AmazonEC2 ec2 = null;
  private String subnet = null;
  private MyLogFile logFile = null;
  private String owner = null;
  private String runDir = null;
  private MyTransferControl transferControl;

  public final static String GROUP_NAME = "GridPilot";
  public final static String KEY_NAME = "GridPilot_EC2_TMP_KEY";
  
  private File keyFile = null;

  /**
   * Construct an EucalyptusMgr.
   * @param ec2ServiceURL the URL of the web service
   * @param accessKey AWS access key
   * @param secretKey AWS secret key
   * @param _subnet subnet from which to allow access
   * @param _owner owner label
   * @param _runDir run directory
   * @param _transferControl TransferControl object to get remote files
   */
  public EC2AltMgr(String ec2ServiceURL,
      String accessKey, String secretKey, String _subnet, String _owner,
      String _runDir, MyTransferControl _transferControl) {
    
    AmazonEC2Config config = new AmazonEC2Config();
    config.setSignatureVersion("0");
    config.setServiceURL(ec2ServiceURL);
    ec2 = new AmazonEC2Client(accessKey, secretKey, config);
    subnet = _subnet;
    owner = _owner;
    runDir = _runDir;
    transferControl = _transferControl;
    logFile = GridPilot.getClassMgr().getLogFile();
  }
  
  /**
   * Create the security group GridPilot iff it doesn't already exist.
   */
  private void createSecurityGroup(){
    String description = "Security group for use by GridPilot. SSH access from submitting machine.";
    // First check if the group "GridPilot" already exists.
    // If not, create it.
    try{
      List groupList = null;
      DescribeSecurityGroupsResponse resp;
      CreateSecurityGroupRequest creq = new CreateSecurityGroupRequest();
      List groups = new ArrayList();
      DescribeSecurityGroupsRequest dreq = new DescribeSecurityGroupsRequest();
      AuthorizeSecurityGroupIngressRequest areq = new AuthorizeSecurityGroupIngressRequest();
      try{
        groups.add(GROUP_NAME);
        dreq.setGroupName(groups);
        resp = ec2.describeSecurityGroups(dreq);
        groupList = resp.getDescribeSecurityGroupsResult().getSecurityGroup();
      }
      catch(Exception e){
      }
      if(groupList==null || groupList.isEmpty()){
        creq.setGroupName(GROUP_NAME);
        creq.setGroupDescription(description);
        ec2.createSecurityGroup(creq);
        resp = ec2.describeSecurityGroups(dreq);
        groupList = resp.getDescribeSecurityGroupsResult().getSecurityGroup();
        // Allow ssh access
        areq.setGroupName(GROUP_NAME);
        areq.setIpProtocol("tcp");
        areq.setFromPort(22);
        areq.setToPort(22);
        areq.setCidrIp(subnet);
        ec2.authorizeSecurityGroupIngress(areq);
      }
    }
    catch(Exception e){
      logFile.addMessage("ERROR: Could not add inbound ssh access to AWS nodes.", e);
      e.printStackTrace();
    }
  }
  
  /**
   * Create a keypair with name GridPilot_EC2_TMP_KEY
   * and save the privte key locally and in the grid homedir
   * is possible.
   * 
   * @throws Exception
   */
  private KeyPair createKeyPair() throws Exception, Exception{
    Debug.debug("Generating new keypair "+KEY_NAME, 2);
    // Generate keypair
    CreateKeyPairRequest kreq = new CreateKeyPairRequest(KEY_NAME);
    CreateKeyPairResponse kresp = ec2.createKeyPair(kreq);
    KeyPair keyInfo = kresp.getCreateKeyPairResult().getKeyPair();
    // Save secret key to file
    keyFile = new File(runDir, KEY_NAME+"-"+
        keyInfo.getKeyFingerprint().replaceAll(":", ""));
    Debug.debug("Writing private key to "+runDir, 1);
    LocalStaticShell.writeFile(keyFile.getAbsolutePath(), keyInfo.getKeyMaterial(), false);
    keyFile.setReadable(false, false);
    keyFile.setReadable(true, true);
    keyFile.setWritable(false, false);
    keyFile.setWritable(true, true);
    if(GridPilot.GRID_HOME_URL!=null && !GridPilot.GRID_HOME_URL.equals("") && MyUtil.urlIsRemote(GridPilot.GRID_HOME_URL)){
      String uploadUrl = GridPilot.GRID_HOME_URL + (GridPilot.GRID_HOME_URL.endsWith("/")?"":"/");
      Debug.debug("Uploading private key to "+uploadUrl, 1);
      try{
        transferControl.upload(keyFile, uploadUrl);
      }
      catch(Exception e){
        logFile.addMessage("WARNING: could not upload private key to grid home directory. " +
            "Submitted EC2 jobs can only be queried from this machine.", e);
      }
    }
    return keyInfo;
  }
  
  /**
   * If no instances are running, delete keypair.
   */
  public void exit(){
    List keypairs;
    try{
      keypairs = listKeyPairs();
    }
    catch (Exception e1) {
       e1.printStackTrace();
       return;
    }
    KeyPair keyInfo = null;
    KeyPair tmpInfo = null;
    for(Iterator it=keypairs.iterator(); it.hasNext();){
      tmpInfo = (KeyPair) it.next();
      if(tmpInfo.getKeyName().equals(KEY_NAME)){
        Debug.debug("Key "+KEY_NAME+" found", 2);
        keyInfo = tmpInfo;
        break;
      }
    }
    if(keyInfo==null){
      return;
    }
    keyFile = new File(runDir, KEY_NAME+"-"+keyInfo.getKeyFingerprint().replaceAll(":", ""));
    String downloadUrl = GridPilot.GRID_HOME_URL + (GridPilot.GRID_HOME_URL.endsWith("/")?"":"/")+
      keyFile.getName();
    List<Reservation> reservations;
    try{
      reservations = listReservations();
    }
    catch (Exception e1){
      e1.printStackTrace();
      return;
    }
    Reservation res;
    RunningInstance inst;
    if(reservations!=null){
      for(Iterator<Reservation> it=reservations.iterator(); it.hasNext();){
        res = it.next();
        for(Iterator<RunningInstance> itt=res.getRunningInstance().iterator(); itt.hasNext();){
          inst = itt.next();
          if(inst.getKeyName().equals(KEY_NAME)){
            // The key is used, don't do anything
            return;
          }
        }
      }
    }
    // The key is not used, so we can delete it
    Debug.debug("Deleting keypair "+KEY_NAME, 2);
    try{
      DeleteKeyPairRequest dreq = new DeleteKeyPairRequest(KEY_NAME);
      ec2.deleteKeyPair(dreq);
    }
    catch (Exception e1) {
      e1.printStackTrace();
    }
    try{
      LocalStaticShell.deleteFile(keyFile.getAbsolutePath());
    }
    catch(Exception e){
    }
    try{
      transferControl.deleteFiles(new String[] {downloadUrl});
    }
    catch(Exception e){
    }
  }
  
  /**
   * @return the file used to store the unencrypted secret key.
   * Does not trigger loading of a stored key. To achieve this, first
   * call getKey().
   */
  public File getKeyFile(){
    return keyFile;
  }

  /**
   * First find out if the keypair GridPilot_EC2_TMP_KEY exists and we have
   * the secret key.
   * 
   * If the keypair exists and we don't have the secret key, check if any instances
   * in the security group GridPilot are using it.
   * 
   * If so, throw an exception. If not, delete it.
   * 
   *  If the keypair is not found, make a new one, save the private key locally
   *  and if possible upload it key to the grid homedir.
   *  
   * @return a keypair with non-null getKeyMaterial()
   * @throws Exception
   */
  public KeyPair getKey() throws Exception{
    List keypairs = listKeyPairs();
    KeyPair keyInfo = null;
    KeyPair tmpInfo = null;
    for(Iterator it=keypairs.iterator(); it.hasNext();){
      tmpInfo = (KeyPair) it.next();
      if(tmpInfo.getKeyName().equals(KEY_NAME)){
        Debug.debug("Key "+KEY_NAME+" found", 2);
        keyInfo = tmpInfo;
        break;
      }
    }
    if(keyInfo!=null){
      // See if the secret key is there
      if(keyInfo.getKeyMaterial()!=null){
        Debug.debug("Using existing keypair "+KEY_NAME, 2);
        return keyInfo;
      }
      // See if the corresponding file is there locally
      keyFile = new File(runDir, KEY_NAME+"-"+keyInfo.getKeyFingerprint().replaceAll(":", ""));
      if(keyFile.exists()){
        Debug.debug("Loading existing keypair "+KEY_NAME, 2);
        return new KeyPair(KEY_NAME, keyInfo.getKeyFingerprint(),
            LocalStaticShell.readFile(keyFile.getAbsolutePath()));
      }
      // See if we can download the private key
      String downloadUrl = GridPilot.GRID_HOME_URL + (GridPilot.GRID_HOME_URL.endsWith("/")?"":"/")+
        keyFile.getName();
      Debug.debug("Downloading private key from "+downloadUrl, 1);
      try{
        transferControl.download(downloadUrl, keyFile);
        if(keyFile.exists()){
          Debug.debug("Loading downloaded keypair "+KEY_NAME, 2);
          return new KeyPair(KEY_NAME, keyInfo.getKeyFingerprint(),
              LocalStaticShell.readFile(keyFile.getAbsolutePath()));
        }
      }
      catch(Exception e){
        logFile.addMessage("WARNING: could not download private key from "+downloadUrl, e);
      }
      // OK, no secret key found, check if this key is used
      List reservations = listReservations();
      Reservation res;
      RunningInstance inst;
      InstanceState state;
      if(reservations!=null){
        for(Iterator<Reservation> it=reservations.iterator(); it.hasNext();){
          res = it.next();
          for(Iterator<RunningInstance> itt=res.getRunningInstance().iterator(); itt.hasNext();){
            inst = itt.next();
            state = inst.getInstanceState();
            Debug.debug("The instance "+inst.getImageId()+" : "+inst.getInstanceId()+" is in state "+state.getCode(), 2);
            if((inst.getInstanceState().getName().equalsIgnoreCase("running") ||
                inst.getInstanceState().getName().equalsIgnoreCase("pending")) &&
                inst.getKeyName().equals(KEY_NAME)){
              throw new Exception("The keypair "+KEY_NAME+" is in use by the instance "+
                  inst.getInstanceId()+". But the secret key is not available. " +
                      "Please terminate this instance and try again.");
            }
          }
        }
      }
      // The key is not used, so we can delete it
      Debug.debug("Deleting keypair "+KEY_NAME, 2);
      DeleteKeyPairRequest dreq = new DeleteKeyPairRequest(KEY_NAME);
      ec2.deleteKeyPair(dreq);
      try{
        LocalStaticShell.deleteFile(keyFile.getAbsolutePath());
      }
      catch(Exception e){
      }
      try{
        transferControl.deleteFiles(new String[] {downloadUrl});
      }
      catch(Exception e){
      }
    }
    // OK, no key found, generate a new one
    return createKeyPair();
  }
  
  /**
   * Launch a number of instances of the same AMI.
   * 
   * @param amiID ID of the AMI to use
   * @param instances number of instances to launch
   * @return a List of elements of type ReservationDescription
   * @throws Exception 
   */
  public Reservation launchInstances(String amiID, int instances) throws Exception{
    KeyPair keypair = getKey();
    createSecurityGroup();
    List groupList = new ArrayList();
    groupList.add(GROUP_NAME);
    RunInstancesRequest lc = new RunInstancesRequest();
    lc.setImageId(amiID);
    lc.setMinCount(instances);
    lc.setMaxCount(instances);
    lc.setSecurityGroup(groupList);
    lc.setInstanceType(InstanceType.DEFAULT.getTypeId() /*i386*/);
    lc.setKeyName(keypair.getKeyName());
    lc.setUserData(owner);
    RunInstancesResponse resp =  ec2.runInstances(lc);
    return resp.getRunInstancesResult().getReservation();
  }
  
  /**
   * Terminate running instances.
   * 
   * @param instanceIDs IDs of the instances to terminate.
   * @throws Exception 
   */
  public void terminateInstances(String [] instanceIDs) throws Exception{
    Debug.debug("Terminating instance(s) "+MyUtil.arrayToString(instanceIDs), 1);
    if(instanceIDs.length==0){
      return;
    }
    List iList = new ArrayList();
    for(int i=0; i<instanceIDs.length; ++i){
      iList.add(instanceIDs[i]);
    }
    TerminateInstancesRequest treq = new TerminateInstancesRequest(iList);
    ec2.terminateInstances(treq);
  }
  
  /**
   * List available AMIs.
   * 
   * @return a List of elements of type Image
   * @throws Exception
   */
  public List<Image> listAvailableAMIs() throws Exception{
    DescribeImagesRequest dreq = new DescribeImagesRequest();
    DescribeImagesResponse images = ec2.describeImages(dreq);
    if(images==null){
      return null;
    }
    List<Image> list = new ArrayList<Image>();
    Debug.debug("Finding available images", 3);
    Image img = null;
    for(Iterator<Image> it=images.getDescribeImagesResult().getImage().iterator(); it.hasNext();){
      img = it.next();
      if(img.getImageState().equals("available")){
        list.add(img);
      }
    }
    return list;
  }

  /**
   * List reservations.
   * 
   * @return a List of elements of type Reservation
   * @throws Exception
   */
  public List<Reservation> listReservations() throws Exception{
    GridPilot.getClassMgr().getSSL().activateSSL();
    Debug.debug("Finding reservations", 3);
    DescribeInstancesRequest dreq = new DescribeInstancesRequest();
    DescribeInstancesResponse instances = ec2.describeInstances(dreq);
    List<Reservation> reservations = instances.getDescribeInstancesResult().getReservation();
    return reservations;
  }
  
  /**
   * List instances.
   * 
   * @return a List of elements of type Instance
   * @throws Exception
   */
  public List<RunningInstance> listInstances(Reservation res) throws Exception{
    Debug.debug("Finding running instances", 3);
    List<RunningInstance> instances = new ArrayList<RunningInstance>();
    RunningInstance inst;
    for(Iterator<RunningInstance> it=res.getRunningInstance().iterator(); it.hasNext();){
      inst = it.next();
      // We only consider instances started with GridPilot
      if(inst.getKeyName().equals(KEY_NAME)){
        instances.add(inst);
      }
    }
    return instances;
  }
  
  /**
   * List keypairs.
   * 
   * @return a List of elements of type KeyPairInfo
   * @throws Exception
   */
  public List<KeyPair> listKeyPairs() throws Exception{
    List list = new ArrayList();
    DescribeKeyPairsRequest dreq = new DescribeKeyPairsRequest();
    List<KeyPair> keypairs = ec2.describeKeyPairs(dreq).getDescribeKeyPairsResult().getKeyPair();
    if(keypairs==null){
      return null;
    }
    Debug.debug("Finding keypairs", 3);
    KeyPair keyInfo = null;
    for(Iterator<KeyPair> it=keypairs.iterator(); it.hasNext();){
      keyInfo = it.next();
      Debug.debug(keyInfo.getKeyName()+"-->"+keyInfo.getKeyFingerprint(), 3);
      if(keyInfo!=null){
        list.add(keyInfo);
      }
    }
    return list;
  }

}
