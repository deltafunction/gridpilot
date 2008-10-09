package gridpilot.csplugins.ec2soap;

import gridfactory.common.Debug;
import gridfactory.common.LocalStaticShell;
import gridpilot.GridPilot;
import gridpilot.MyLogFile;
import gridpilot.MyTransferControl;
import gridpilot.MyUtil;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jets3t.service.utils.ServiceUtils;

import com.amazon.aes.webservices.client.ImageDescription;
import com.amazon.aes.webservices.client.Jec2;
import com.amazon.aes.webservices.client.KeyPairInfo;
import com.amazon.aes.webservices.client.ReservationDescription;
import com.amazon.aes.webservices.client.SecurityGroupDescription;
import com.amazon.aes.webservices.client.ReservationDescription.Instance;
import com.amazon.aes.webservices.client.SecurityGroupDescription.IpPermission;

import com.xerox.amazonws.ec2.InstanceType;

/**
 * This class contains methods for managing virtual machines in the
 * Amazon Elastic Compute Cloud (EC2).
 * It relies on the Typica library http://code.google.com/p/typica/wiki/TypicaSampleCode)
 */
public class EC2SoapMgr {
  
  private Jec2 ec2 = null;
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
   * @param accessKey X.509 certificate file
   * @param secretKey RSA secret key file
   * @param _subnet subnet from which to allow access
   * @param _owner owner label
   * @param _runDir run directory
   * @param _transferControl TransferControl object to get remote files
   * @throws Exception 
   * @throws MalformedURLException 
   */
  public EC2SoapMgr(String ec2ServiceURL,
      String cert, String key, String _subnet, String _owner,
      String _runDir, MyTransferControl _transferControl) throws MalformedURLException, Exception {
    ec2 = new Jec2(new URL(ec2ServiceURL), key, cert);
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
      try{
        groupList = ec2.describeSecurityGroups(new String [] {GROUP_NAME});
      }
      catch(Exception e){
      }
      if(groupList==null || groupList.isEmpty()){
        ec2.createSecurityGroup(GROUP_NAME, description);
        groupList = ec2.describeSecurityGroups(new String [] {GROUP_NAME});
        // Allow ssh access
        SecurityGroupDescription desc = new SecurityGroupDescription(GROUP_NAME, description, owner);
        IpPermission ipPerm = desc.new IpPermission("tcp", 22, 22);
        ipPerm.addIpRange(subnet);
        List<IpPermission> ipPerms = new ArrayList<IpPermission>();
        ipPerms.add(ipPerm);
        desc.perms = ipPerms;
        ec2.authorizeSecurityGroupIngress(desc);
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
  private KeyPairInfo createKeyPair() throws Exception{
    Debug.debug("Generating new keypair "+KEY_NAME, 2);
    // Generate keypair
    KeyPairInfo keyInfo = ec2.createKeyPair(KEY_NAME);
    // Save secret key to file
    keyFile = new File(runDir, KEY_NAME+"-"+
        keyInfo.keyFingerprint.replaceAll(":", ""));
    Debug.debug("Writing private key to "+runDir, 1);
    LocalStaticShell.writeFile(keyFile.getAbsolutePath(), keyInfo.keyMaterial, false);
    keyFile.setReadable(false, false);
    keyFile.setReadable(true, true);
    keyFile.setWritable(false, false);
    keyFile.setWritable(true, true);
    if(GridPilot.gridHomeURL!=null && !GridPilot.gridHomeURL.equals("") && MyUtil.urlIsRemote(GridPilot.gridHomeURL)){
      String uploadUrl = GridPilot.gridHomeURL + (GridPilot.gridHomeURL.endsWith("/")?"":"/");
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
    KeyPairInfo keyInfo = null;
    KeyPairInfo tmpInfo = null;
    for(Iterator it=keypairs.iterator(); it.hasNext();){
      tmpInfo = (KeyPairInfo) it.next();
      if(tmpInfo.keyName.equals(KEY_NAME)){
        Debug.debug("Key "+KEY_NAME+" found", 2);
        keyInfo = tmpInfo;
        break;
      }
    }
    if(keyInfo==null){
      return;
    }
    keyFile = new File(runDir, KEY_NAME+"-"+keyInfo.keyFingerprint.replaceAll(":", ""));
    String downloadUrl = GridPilot.gridHomeURL + (GridPilot.gridHomeURL.endsWith("/")?"":"/")+
      keyFile.getName();
    List reservations;
    try{
      reservations = listReservations();
    }
    catch (Exception e1){
      e1.printStackTrace();
      return;
    }
    ReservationDescription res = null;
    Instance inst = null;
    if(reservations!=null){
      for(Iterator it=reservations.iterator(); it.hasNext();){
        res = (ReservationDescription) it.next();
        for(Iterator itt=res.instances.iterator(); itt.hasNext();){
          inst = (Instance) itt.next();
          if(inst.keyName.equals(KEY_NAME)){
            // The key is used, don't do anything
            return;
          }
        }
      }
    }
    // The key is not used, so we can delete it
    Debug.debug("Deleting keypair "+KEY_NAME, 2);
    try{
      ec2.deleteKeyPair(KEY_NAME);
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
  public KeyPairInfo getKey() throws Exception{
    List keypairs = listKeyPairs();
    KeyPairInfo keyInfo = null;
    KeyPairInfo tmpInfo = null;
    for(Iterator it=keypairs.iterator(); it.hasNext();){
      tmpInfo = (KeyPairInfo) it.next();
      if(tmpInfo.keyName.equals(KEY_NAME)){
        Debug.debug("Key "+KEY_NAME+" found", 2);
        keyInfo = tmpInfo;
        break;
      }
    }
    if(keyInfo!=null){
      // See if the secret key is there
      if(keyInfo.keyMaterial!=null){
        Debug.debug("Using existing keypair "+KEY_NAME, 2);
        return keyInfo;
      }
      // See if the corresponding file is there locally
      keyFile = new File(runDir, KEY_NAME+"-"+keyInfo.keyFingerprint.replaceAll(":", ""));
      if(keyFile.exists()){
        Debug.debug("Loading existing keypair "+KEY_NAME, 2);
        return new KeyPairInfo(KEY_NAME, keyInfo.keyFingerprint,
            LocalStaticShell.readFile(keyFile.getAbsolutePath()));
      }
      // See if we can download the private key
      String downloadUrl = GridPilot.gridHomeURL + (GridPilot.gridHomeURL.endsWith("/")?"":"/")+
        keyFile.getName();
      Debug.debug("Downloading private key from "+downloadUrl, 1);
      try{
        transferControl.download(downloadUrl, keyFile);
        if(keyFile.exists()){
          Debug.debug("Loading downloaded keypair "+KEY_NAME, 2);
          return new KeyPairInfo(KEY_NAME, keyInfo.keyFingerprint,
              LocalStaticShell.readFile(keyFile.getAbsolutePath()));
        }
      }
      catch(Exception e){
        logFile.addMessage("WARNING: could not download private key from "+downloadUrl, e);
      }
      // OK, no secret key found, check if this key is used
      List reservations = listReservations();
      ReservationDescription res = null;
      Instance inst = null;
      if(reservations!=null){
        for(Iterator it=reservations.iterator(); it.hasNext();){
          res = (ReservationDescription) it.next();
          for(Iterator itt=res.instances.iterator(); itt.hasNext();){
            inst = (Instance) itt.next();
            if((inst.isPending() || inst.isRunning()) && inst.keyName.equals(KEY_NAME)){
              throw new Exception("The keypair "+KEY_NAME+" is in use by the instance "+
                  inst.instanceId+". But the secret key is not available. " +
                      "Please terminate this instance and try again.");
            }
          }
        }
      }
      // The key is not used, so we can delete it
      Debug.debug("Deleting keypair "+KEY_NAME, 2);
      ec2.deleteKeyPair(KEY_NAME);
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
  public ReservationDescription launchInstances(String amiID, int instances) throws Exception{
    KeyPairInfo keypair = getKey();
    createSecurityGroup();
    List groupList = new ArrayList();
    groupList.add(GROUP_NAME);
    ReservationDescription desc =  ec2.runInstances(amiID, instances, instances, groupList, "",
        keypair.keyName, ServiceUtils.toBase64(owner.getBytes()), null, InstanceType.DEFAULT.name() /*i386*/,
        null, null, null, null);
    return desc;
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
    ec2.terminateInstances(instanceIDs);
  }
  
  /**
   * List available AMIs.
   * 
   * @return a List of elements of type ImageDescription
   * @throws Exception
   */
  public List<ImageDescription> listAvailableAMIs() throws Exception{
    List<ImageDescription>  list = new ArrayList();
    List params = new ArrayList();
    List<ImageDescription>  images = ec2.describeImages(params);
    if(images==null){
      return null;
    }
    Debug.debug("Finding available images", 3);
    ImageDescription img = null;
    for(Iterator<ImageDescription>  it=images.iterator(); it.hasNext();){
      img = it.next();
      if(img.imageState.equals("available")){
        list.add(img);
      }
    }
    return list;
  }

  /**
   * List reservations.
   * 
   * @return a List of elements of type ReservationDescription
   * @throws Exception
   */
  public List listReservations() throws Exception {
    GridPilot.getClassMgr().getSSL().activateSSL();
    List list = new ArrayList();
    List params = new ArrayList();
    List reservations = ec2.describeInstances(params);
    if(reservations==null){
      return null;
    }
    Debug.debug("Finding reservations", 3);
    ReservationDescription res = null;
    for(Iterator it=reservations.iterator(); it.hasNext();){
      res = (ReservationDescription) it.next();
      if(res!=null){
        list.add(res);
      }
    }
    return list;
  }
  
  /**
   * List instances.
   * 
   * @return a List of elements of type Instance
   * @throws Exception
   */
  public List listInstances(ReservationDescription res) throws Exception{
    Debug.debug("Finding running instances", 3);
    List instances = new ArrayList();
    Instance inst = null;
    for(Iterator it=res.instances.iterator(); it.hasNext();){
      inst = (Instance) it.next();
      // We only consider instances started with GridPilot
      if(inst.keyName.equals(KEY_NAME)){
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
  public List listKeyPairs() throws Exception{
    List list = new ArrayList();
    List params = new ArrayList();
    List keypairs = ec2.describeKeyPairs(params);
    if(keypairs==null){
      return null;
    }
    Debug.debug("Finding keypairs", 3);
    KeyPairInfo keyInfo = null;
    for(Iterator it=keypairs.iterator(); it.hasNext();){
      keyInfo = (KeyPairInfo) it.next();
      Debug.debug(keyInfo.keyName+"-->"+keyInfo.keyFingerprint, 3);
      if(keyInfo!=null){
        list.add(keyInfo);
      }
    }
    return list;
  }

}
