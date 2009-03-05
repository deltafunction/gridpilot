package gridpilot.csplugins.ec2;

import gridfactory.common.Debug;
import gridfactory.common.LocalStaticShell;
import gridpilot.GridPilot;
import gridpilot.MyLogFile;
import gridpilot.MyTransferControl;
import gridpilot.MyUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.globus.gsi.GlobusCredentialException;
import org.ietf.jgss.GSSException;

import com.xerox.amazonws.ec2.AttachmentInfo;
import com.xerox.amazonws.ec2.EC2Exception;
import com.xerox.amazonws.ec2.InstanceType;
import com.xerox.amazonws.ec2.Jec2;
import com.xerox.amazonws.ec2.ImageDescription;
import com.xerox.amazonws.ec2.KeyPairInfo;
import com.xerox.amazonws.ec2.LaunchConfiguration;
import com.xerox.amazonws.ec2.ReservationDescription;
import com.xerox.amazonws.ec2.SnapshotInfo;
import com.xerox.amazonws.ec2.VolumeInfo;
import com.xerox.amazonws.ec2.ReservationDescription.Instance;

/**
 * This class contains methods for managing virtual machines in the
 * Amazon Elastic Compute Cloud (EC2).
 * It relies on the Typica library http://code.google.com/p/typica/wiki/TypicaSampleCode)
 */
public class EC2Mgr {
  
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
   * Construct an EC2Mgr.
   * @param server the IP name or address of the server
   * @param port port number of the service
   * @param path path of the service
   * @param secure whether to use HTTPS or HTTP
   * @param accessKey AWS access key
   * @param secretKey AWS secret key
   * @param _subnet subnet from which to allow access
   * @param _owner owner label
   * @param _runDir run directory
   * @param _transferControl TransferControl object to get remote files
   */
  public EC2Mgr(String server, int port, String path, boolean secure,
      String accessKey, String secretKey, String _subnet, String _owner,
      String _runDir, MyTransferControl _transferControl) {
    if(secure){
      try{
        GridPilot.getClassMgr().getSSL().activateSSL();
      }
      catch(Exception e){
        e.printStackTrace();
        secure = false;
      }
    }
    ec2 = new Jec2(accessKey, secretKey, secure);
    //ec2 = new Jec2(accessKey, secretKey, secure, server, port, path);
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
      catch(EC2Exception e){
      }
      if(groupList==null || groupList.isEmpty()){
        ec2.createSecurityGroup(GROUP_NAME, description);
        groupList = ec2.describeSecurityGroups(new String [] {GROUP_NAME});
        // Allow ssh access
        ec2.authorizeSecurityGroupIngress(GROUP_NAME, "tcp", 22, 22, subnet);
      }
    }
    catch(EC2Exception e){
      logFile.addMessage("ERROR: Could not add inbound ssh access to AWS nodes.", e);
      e.printStackTrace();
    }
  }
  
  /**
   * Create a keypair with name GridPilot_EC2_TMP_KEY
   * and save the privte key locally and in the grid homedir
   * is possible.
   * 
   * @throws EC2Exception
   * @throws Exception
   */
  private KeyPairInfo createKeyPair() throws EC2Exception, Exception{
    Debug.debug("Generating new keypair "+KEY_NAME, 2);
    // Generate keypair
    KeyPairInfo keyInfo = ec2.createKeyPair(KEY_NAME);
    // Save secret key to file
    keyFile = new File(runDir, KEY_NAME+"-"+
        keyInfo.getKeyFingerprint().replaceAll(":", ""));
    Debug.debug("Writing private key to "+runDir, 1);
    LocalStaticShell.writeFile(keyFile.getAbsolutePath(), keyInfo.getKeyMaterial(), false);
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
    catch (EC2Exception e1) {
       e1.printStackTrace();
       return;
    }
    KeyPairInfo keyInfo = null;
    KeyPairInfo tmpInfo = null;
    for(Iterator it=keypairs.iterator(); it.hasNext();){
      tmpInfo = (KeyPairInfo) it.next();
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
        for(Iterator itt=res.getInstances().iterator(); itt.hasNext();){
          inst = (Instance) itt.next();
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
      ec2.deleteKeyPair(KEY_NAME);
    }
    catch (EC2Exception e1) {
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
   * @throws EC2Exception
   * @throws IOException 
   * @throws FileNotFoundException 
   */
  public KeyPairInfo getKey() throws Exception{
    List keypairs = listKeyPairs();
    KeyPairInfo keyInfo = null;
    KeyPairInfo tmpInfo = null;
    for(Iterator it=keypairs.iterator(); it.hasNext();){
      tmpInfo = (KeyPairInfo) it.next();
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
        return new KeyPairInfo(KEY_NAME, keyInfo.getKeyFingerprint(),
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
          return new KeyPairInfo(KEY_NAME, keyInfo.getKeyFingerprint(),
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
          for(Iterator itt=res.getInstances().iterator(); itt.hasNext();){
            inst = (Instance) itt.next();
            if((inst.isPending() || inst.isRunning()) && inst.getKeyName().equals(KEY_NAME)){
              throw new EC2Exception("The keypair "+KEY_NAME+" is in use by the instance "+
                  inst.getInstanceId()+". But the secret key is not available. " +
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
    LaunchConfiguration lc = new LaunchConfiguration(amiID, instances, instances);
    lc.setSecurityGroup(groupList);
    lc.setInstanceType(InstanceType.DEFAULT /*i386*/);
    lc.setKeyName(keypair.getKeyName());
    lc.setUserData(owner.getBytes());
    ReservationDescription desc =  ec2.runInstances(lc);
    return desc;
  }
  
  /**
   * Terminate running instances.
   * 
   * @param instanceIDs IDs of the instances to terminate.
   * @throws EC2Exception 
   */
  public void terminateInstances(String [] instanceIDs) throws EC2Exception{
    Debug.debug("Terminating instance(s) "+MyUtil.arrayToString(instanceIDs), 1);
    if(instanceIDs.length==0){
      return;
    }
    ec2.terminateInstances(instanceIDs);
  }
  
  /**
   * List available AMIs.
   * 
   * @param onlyPublicAMIs list only AMIs owned by me
   * @param onlyGridPilotAMIs list only AMIs in the designated AMI bucket
   * @return a List of elements of type ImageDescription
   * @see #AMI_BUCKET
   * @throws EC2Exception
   */
  public List<ImageDescription> listAvailableAMIs(boolean onlyPublicAMIs, boolean onlyGridPilotAMIs) throws EC2Exception{
    List list = new ArrayList();
    List params = new ArrayList();
    List images = ec2.describeImages(params);
    if(images==null){
      return null;
    }
    Debug.debug("Finding available images", 3);
    ImageDescription img = null;
    for(Iterator<ImageDescription> it=images.iterator(); it.hasNext();){
      img = it.next();
      if((!onlyGridPilotAMIs || isGridPilotAMI(img.getImageLocation())) &&
          (!onlyPublicAMIs || img.isPublic()) &&
          img.getImageState().equals("available")){
        list.add(img);
      }
    }
    return list;
  }
  
  private boolean isGridPilotAMI(String imageLocation) {
    return imageLocation.toLowerCase().startsWith(EC2ComputingSystem.AMI_PREFIX);
  }

  public ImageDescription getImageDescription(String imageId) throws EC2Exception{
    ImageDescription img = null;
    for(Iterator<ImageDescription> it=listAvailableAMIs(false, false).iterator(); it.hasNext();){
      img = it.next();
      if(img.getImageId().equals(imageId)){
        return img;
      }
    }
    throw new EC2Exception("No image with ID "+imageId);
  }

  /**
   * List reservations.
   * 
   * @return a List of elements of type ReservationDescription
   * @throws EC2Exception
   * @throws GSSException 
   * @throws GeneralSecurityException 
   * @throws IOException 
   * @throws GlobusCredentialException 
   */
  public List<ReservationDescription> listReservations() throws EC2Exception, GlobusCredentialException, IOException, GeneralSecurityException, GSSException{
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
   * @throws EC2Exception
   */
  public List listInstances(ReservationDescription res) throws EC2Exception{
    Debug.debug("Finding running instances", 3);
    List instances = new ArrayList();
    Instance inst = null;
    for(Iterator it=res.getInstances().iterator(); it.hasNext();){
      inst = (Instance) it.next();
      // We only consider instances started with GridPilot
      if(inst.getKeyName()!=null && inst.getKeyName().equals(KEY_NAME)){
        instances.add(inst);
      }
    }
    return instances;
  }
  
  /**
   * List keypairs.
   * 
   * @return a List of elements of type KeyPairInfo
   * @throws EC2Exception
   */
  public List listKeyPairs() throws EC2Exception{
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
      Debug.debug(keyInfo.getKeyName()+"-->"+keyInfo.getKeyFingerprint(), 3);
      if(keyInfo!=null){
        list.add(keyInfo);
      }
    }
    return list;
  }

  /**
   * Create an EBS volume from a given snapshot and attach it to an AMI
   * instance.
   * @param inst the AMI instance in question
   * @param _snapshotID ID of EBS snapshot
   * @param device device name
   * @return the device name of the attached volume
   * @throws EC2Exception 
   */
  public void attachVolumeFromSnapshot(Instance inst, String _snapshotID, String device) throws EC2Exception {
    String snapshotID = "'"+_snapshotID.replaceFirst("^'(.*)'$", "$1")+"'";
    List<SnapshotInfo> sis = ec2.describeSnapshots(new String[] {snapshotID});
    SnapshotInfo si = sis.get(0);
    List<VolumeInfo> vis = ec2.describeVolumes(new String[] {si.getVolumeId()});
    VolumeInfo vi = vis.get(0);
    VolumeInfo volumeInfo = ec2.createVolume(vi.getSize(), snapshotID, vi.getZone());
    AttachmentInfo ai = ec2.attachVolume(volumeInfo.getVolumeId(), inst.getInstanceId(), device);
    String status = ai.getStatus();
    logFile.addInfo("Attached volume: "+volumeInfo+"\n-->Status: "+status);
  }

}
