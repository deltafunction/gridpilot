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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.globus.gsi.GlobusCredentialException;
import org.ietf.jgss.GSSException;

import com.xerox.amazonws.ec2.AttachmentInfo;
import com.xerox.amazonws.ec2.EC2Exception;
import com.xerox.amazonws.ec2.GroupDescription;
import com.xerox.amazonws.ec2.InstanceType;
import com.xerox.amazonws.ec2.Jec2;
import com.xerox.amazonws.ec2.ImageDescription;
import com.xerox.amazonws.ec2.KeyPairInfo;
import com.xerox.amazonws.ec2.LaunchConfiguration;
import com.xerox.amazonws.ec2.ReservationDescription;
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
  private HashMap<String[], List<ImageDescription>> patternToIdCache;


  public final static String GROUP_NAME = "GridPilot";
  public final static String KEY_NAME = "GridPilot_EC2_TMP_KEY";
  public final static String [] INSTANCE_TYPES = new String[] {
     "t1.micro", "m1.small", "m1.large", "m1.xlarge",
     "c1.medium", "c1.xlarge",
     "m2.xlarge", "m2.2xlarge", "m2.4xlarge",
     "cc1.4xlarge", "cg1.4xlarge"
  };
  public HashMap<String, int[]> instanceTypes = new HashMap<String, int[]>();
  
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
    
    patternToIdCache = new HashMap<String[], List<ImageDescription>>();
    
    /*
     "t1.micro", "m1.small", "m1.large", "m1.xlarge",
     "c1.medium", "c1.xlarge",
     "m2.xlarge", "m2.2xlarge", "m2.4xlarge",
     "cc1.4xlarge", "cg1.4xlarge"
    */
    instanceTypes.put(EC2Mgr.INSTANCE_TYPES[0], new int [] {1, 613, 0});//1xi386/i386_64, 613 MB, 0 GB /mnt, low I/O
    instanceTypes.put(EC2Mgr.INSTANCE_TYPES[1], new int [] {1, 1700, 150000});//1xi386, 1.7 GB, 150 GB /mnt, moderate I/O
    instanceTypes.put(EC2Mgr.INSTANCE_TYPES[2], new int [] {2, 7500, 420000});//2xi386_64, 1.7 GB, 2x420 GB /mnt, high I/O
    instanceTypes.put(EC2Mgr.INSTANCE_TYPES[3], new int [] {4, 15000, 420000});//4xi386_64, 1.7 GB, 4x420 GB /mnt, high I/O
    instanceTypes.put(EC2Mgr.INSTANCE_TYPES[4], new int [] {2, 1700, 340000});//2xi386, 1.7 GB, 340 GB /mnt, moderate I/O
    instanceTypes.put(EC2Mgr.INSTANCE_TYPES[5], new int [] {8, 7000, 420000});//8xi386_64, 1.7 GB, 4x420 GB /mnt, high I/O
    instanceTypes.put(EC2Mgr.INSTANCE_TYPES[6], new int [] {2, 17100, 420000});//2xi386_64, 17.1 GB, 420 GB /mnt, moderate I/O
    instanceTypes.put(EC2Mgr.INSTANCE_TYPES[7], new int [] {4, 34200, 850000});//4xi386_64, 34.2 GB, 826 GB /mnt, high I/O
    instanceTypes.put(EC2Mgr.INSTANCE_TYPES[8], new int [] {8, 68400, 1690000});//8xi386_64, 68.4 GB, 1690 GB /mnt, high I/O
    instanceTypes.put(EC2Mgr.INSTANCE_TYPES[9], new int [] {8, 23000, 1690000});//8xi386_64(Xeon Nehalem), 23 GB, 1690 GB /mnt, very high I/O
    instanceTypes.put(EC2Mgr.INSTANCE_TYPES[10], new int [] {8, 22000, 1690000});//8xi386_64 + 2xNvidia Tesla GPUs, 22 GB, 1690 GB /mnt, very high I/O

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
      List<GroupDescription> groupList = null;
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
    List<KeyPairInfo> keypairs;
    try{
      keypairs = listKeyPairs();
    }
    catch (EC2Exception e1) {
       e1.printStackTrace();
       return;
    }
    KeyPairInfo keyInfo = null;
    KeyPairInfo tmpInfo = null;
    for(Iterator<KeyPairInfo> it=keypairs.iterator(); it.hasNext();){
      tmpInfo = it.next();
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
    List<ReservationDescription> reservations;
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
      for(Iterator<ReservationDescription> it=reservations.iterator(); it.hasNext();){
        res = it.next();
        for(Iterator<Instance> itt=res.getInstances().iterator(); itt.hasNext();){
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
    List<KeyPairInfo> keypairs = listKeyPairs();
    KeyPairInfo keyInfo = null;
    KeyPairInfo tmpInfo = null;
    for(Iterator<KeyPairInfo> it=keypairs.iterator(); it.hasNext();){
      tmpInfo = it.next();
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
      String downloadUrl = GridPilot.GRID_HOME_URL + (GridPilot.GRID_HOME_URL.endsWith("/")?"":"/")+
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
      List<ReservationDescription> reservations = listReservations();
      ReservationDescription res = null;
      Instance inst = null;
      if(reservations!=null){
        for(Iterator<ReservationDescription> it=reservations.iterator(); it.hasNext();){
          res = it.next();
          for(Iterator<Instance> itt=res.getInstances().iterator(); itt.hasNext();){
            inst = itt.next();
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
   * @param type instance type - one of: "t1.micro", "m1.small", "m1.large", "m1.xlarge",
     "c1.medium", "c1.xlarge",
     "m2.xlarge", "m2.2xlarge", "m2.4xlarge",
     "cc1.4xlarge", "cg1.4xlarge"
   * @return a List of elements of type ReservationDescription
   * @throws Exception 
   */
  public ReservationDescription launchInstances(String amiID, int instances, String type) throws Exception{
    KeyPairInfo keypair = getKey();
    createSecurityGroup();
    List<String> groupList = new ArrayList<String>();
    groupList.add(GROUP_NAME);
    LaunchConfiguration lc = new LaunchConfiguration(amiID, instances, instances);
    lc.setSecurityGroup(groupList);
    lc.setInstanceType(InstanceType.getTypeFromString(type) /*default: m1.small*/);
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
    if(instanceIDs==null || instanceIDs.length==0){
      return;
    }
    ec2.terminateInstances(instanceIDs);
    // Detach and delete all software volumes.
    List<VolumeInfo> volumes = ec2.describeVolumes(new String[] {});
    VolumeInfo volume = null;
    List <AttachmentInfo> ais;
    AttachmentInfo ai;
    for(Iterator<VolumeInfo> it=volumes.iterator(); it.hasNext();){
      try{
        volume = it.next();
        ais = volume.getAttachmentInfo();
        for(Iterator<AttachmentInfo> itt=ais.iterator(); itt.hasNext();){
          ai = itt.next();
          if(MyUtil.arrayContains(instanceIDs, ai.getInstanceId())){
            Debug.debug("Deleting volume "+volume.getVolumeId(), 2);
            if(ai.getStatus().equalsIgnoreCase("attached")){
              ec2.detachVolume(volume.getVolumeId(), ai.getInstanceId(), ai.getDevice(), true);
            }
            // If the volume was not created from a snapshot, don't delete it.
            if(volume.getSnapshotId()!=null && !volume.getSnapshotId().equals("")){
              continue;
            }
            Exception ee = null;
            int i;
            // Try 10 times with 3 seconds in between to delete volume, then give up.
            for(i=0; i<10; ++i){
              try{
                Thread.sleep(2000);
                ec2.deleteVolume(volume.getVolumeId());
                Debug.debug("Volume "+volume.getVolumeId()+" deleted.", 3);
                break;
              }
              catch(Exception eee){
                ee = eee;
                continue;
              }
            }
            if(i==9 && ee!=null){
              throw ee;
            }
          }
        }
      }
      catch(Exception e){
        MyUtil.showError("Could not delete volume "+volume+". Please do so by manually.");
        e.printStackTrace();
      }
    }
  }
  
  /**
   * Clear the cache kept by listAvailableAMIs();
   */
  public void clearCache(){
    patternToIdCache.clear();
  }
  
  /**
   * List available AMIs.
   * 
   * @param onlyPublicAMIs list only AMIs owned by me
   * @param patterns list only AMIs matching one of these patterns
   * @return a List of elements of type ImageDescription
   * @see #AMI_BUCKET
   * @throws EC2Exception
   */
  public List<ImageDescription> listAvailableAMIs(boolean onlyPublicAMIs, String[] patterns) throws EC2Exception{
    if(patternToIdCache.containsKey(patterns)){
      return patternToIdCache.get(patterns);
    }
    List<ImageDescription> list = new ArrayList<ImageDescription>();
    List<String> params = new ArrayList<String>();
    List<ImageDescription> images = ec2.describeImages(params);
    if(images==null){
      return null;
    }
    Debug.debug("Finding available images", 3);
    ImageDescription img = null;
    for(Iterator<ImageDescription> it=images.iterator(); it.hasNext();){
      img = it.next();
      if((matchAMI(img, patterns)) &&
          (!onlyPublicAMIs || img.isPublic()) &&
          img.getImageState().equals("available")){
        list.add(img);
      }
    }
    patternToIdCache.put(patterns, list);
    return list;
  }
  
  private boolean matchAMI(ImageDescription desc, String[] patterns) {
    if(patterns==null || patterns.length==0){
      return desc.getImageType().equalsIgnoreCase("machine");
    }
    for(int i=0; i<patterns.length; ++i){
      if((desc.getImageLocation().matches("(?i).*"+patterns[i]+".*") ||
          desc.getImageId().matches("(?i).*"+patterns[i]+".*") ||
          desc.getImageOwnerId().matches("(?i).*"+patterns[i]+".*")) &&
          desc.getImageType().equalsIgnoreCase("machine")){
        return true;
      }
    }
    return false;
  }

  public ImageDescription getImageDescription(String imageId) throws EC2Exception{
    ImageDescription img = null;
    for(Iterator<ImageDescription> it=listAvailableAMIs(false, null).iterator(); it.hasNext();){
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
    List<ReservationDescription> list = new ArrayList<ReservationDescription>();
    List<String> params = new ArrayList<String>();
    List<ReservationDescription> reservations = ec2.describeInstances(params);
    if(reservations==null){
      return null;
    }
    Debug.debug("Finding reservations", 3);
    ReservationDescription res = null;
    for(Iterator<ReservationDescription> it=reservations.iterator(); it.hasNext();){
      res = it.next();
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
  public List<Instance> listInstances(ReservationDescription res) throws EC2Exception{
    Debug.debug("Finding running instances", 3);
    List<Instance> instances = new ArrayList<Instance>();
    Instance inst = null;
    for(Iterator<Instance> it=res.getInstances().iterator(); it.hasNext();){
      inst = it.next();
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
  public List<KeyPairInfo> listKeyPairs() throws EC2Exception{
    List<KeyPairInfo> list = new ArrayList<KeyPairInfo>();
    List<String> params = new ArrayList<String>();
    List<KeyPairInfo> keypairs = ec2.describeKeyPairs(params);
    if(keypairs==null){
      return null;
    }
    Debug.debug("Finding keypairs", 3);
    KeyPairInfo keyInfo = null;
    for(Iterator<KeyPairInfo> it=keypairs.iterator(); it.hasNext();){
      keyInfo = it.next();
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
   * @param snapshotID ID of EBS snapshot
   * @param device device name
   * @return the device name of the attached volume
   * @throws EC2Exception 
   */
  public String attachVolumeFromSnapshot(Instance inst, String snapshotID, String device) throws EC2Exception {
    Debug.debug("Attaching volume from snapshot "+snapshotID, 2);
    VolumeInfo volumeInfo = ec2.createVolume(/*vi.getSize()*/null, snapshotID, /*vi.getZone()*/inst.getAvailabilityZone());
    AttachmentInfo ai = ec2.attachVolume(volumeInfo.getVolumeId(), inst.getInstanceId(), device);
    String status = ai.getStatus();
    logFile.addInfo("Attached volume "+volumeInfo.getVolumeId()+" on " +device+
        "\n-->Status: "+status);
    return volumeInfo.getVolumeId();
  }

}
