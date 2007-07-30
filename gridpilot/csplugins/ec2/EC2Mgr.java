package gridpilot.csplugins.ec2;

import gridpilot.Debug;
import gridpilot.GridPilot;
import gridpilot.LocalStaticShellMgr;
import gridpilot.LogFile;
import gridpilot.TransferControl;
import gridpilot.Util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.xerox.amazonws.ec2.EC2Exception;
import com.xerox.amazonws.ec2.Jec2;
import com.xerox.amazonws.ec2.ImageDescription;
import com.xerox.amazonws.ec2.KeyPairInfo;
import com.xerox.amazonws.ec2.ReservationDescription;
import com.xerox.amazonws.ec2.ReservationDescription.Instance;

/**
 * This class contains methods for managing virtual machines in the
 * Amazon Elastic Compute Cloud (EC2).
 * I relyes on the Typica library http://code.google.com/p/typica/wiki/TypicaSampleCode)
 */
public class EC2Mgr {
  
  private Jec2 ec2 = null;
  private String subnet = null;
  private LogFile logFile = null;
  private String owner = null;
  private String runDir = null;

  private static String GROUP_NAME = "GridPilot";
  private static String KEY_NAME = "GridPilot_EC2_TMP_KEY";
  
  protected File keyFile = null;

  public EC2Mgr(String accessKey, String secretKey, String _subnet, String _owner,
      String _runDir) {    
    ec2 = new Jec2(accessKey, secretKey);
    subnet = _subnet;
    owner = _owner;
    runDir = _runDir;
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
    LocalStaticShellMgr.writeFile(keyFile.getAbsolutePath(), keyInfo.getKeyMaterial(), false);
    if(GridPilot.gridHomeURL!=null && !GridPilot.gridHomeURL.equals("") && Util.urlIsRemote(GridPilot.gridHomeURL)){
      String uploadUrl = GridPilot.gridHomeURL + (GridPilot.gridHomeURL.endsWith("/")?"":"/");
      Debug.debug("Uploading private key to "+uploadUrl, 1);
      try{
        TransferControl.upload(keyFile, uploadUrl, null);
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
  protected void exit(){
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
    File keyFile = new File(runDir, KEY_NAME+"-"+keyInfo.getKeyFingerprint().replaceAll(":", ""));
    String downloadUrl = GridPilot.gridHomeURL + (GridPilot.gridHomeURL.endsWith("/")?"":"/")+
      keyFile.getName();
    List reservations;
    try{
      reservations = listReservations();
    }
    catch (EC2Exception e1){
      e1.printStackTrace();
      return;
    }
    ReservationDescription res = null;
    Instance inst = null;
    if(reservations!=null){
      for(Iterator it=reservations.iterator(); it.hasNext();){
        res = (ReservationDescription) it.next();
        for(Iterator itt=res.getInstances().iterator(); it.hasNext();){
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
      LocalStaticShellMgr.deleteFile(keyFile.getAbsolutePath());
    }
    catch(Exception e){
    }
    try{
      TransferControl.deleteFiles(new String[] {downloadUrl});
    }
    catch(Exception e){
    }
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
  private KeyPairInfo getKey() throws Exception{
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
      File keyFile = new File(runDir, KEY_NAME+"-"+keyInfo.getKeyFingerprint().replaceAll(":", ""));
      if(keyFile.exists()){
        Debug.debug("Loading existing keypair "+KEY_NAME, 2);
        return new KeyPairInfo(KEY_NAME, keyInfo.getKeyFingerprint(),
            LocalStaticShellMgr.readFile(keyFile.getAbsolutePath()));
      }
      // See if we can download the private key
      String downloadUrl = GridPilot.gridHomeURL + (GridPilot.gridHomeURL.endsWith("/")?"":"/")+
        keyFile.getName();
      Debug.debug("Downloading private key from "+downloadUrl, 1);
      try{
        TransferControl.download(downloadUrl, keyFile, null);
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
          for(Iterator itt=res.getInstances().iterator(); it.hasNext();){
            inst = (Instance) itt.next();
            if(inst.getKeyName().equals(KEY_NAME)){
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
        LocalStaticShellMgr.deleteFile(keyFile.getAbsolutePath());
      }
      catch(Exception e){
      }
      try{
        TransferControl.deleteFiles(new String[] {downloadUrl});
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
    List groupSet = new ArrayList();
    groupSet.add(GROUP_NAME);
    ReservationDescription desc = ec2.runInstances(
        amiID,
        instances,
        instances,
        groupSet,
        /*userData*/owner,
        keypair.getKeyName(),
        /*public IP*/true);
    return desc;
  }
  
  /**
   * Terminate running instances.
   * 
   * @param instanceIDs IDs of the instances to terminate.
   * @throws EC2Exception 
   */
  public void terminateInstances(String [] instanceIDs) throws EC2Exception{
    Debug.debug("Terminating instance(s) "+Util.arrayToString(instanceIDs), 1);
    ec2.terminateInstances(instanceIDs);
  }
  
  /**
   * List available AMIs.
   * 
   * @return a List of elements of type ImageDescription
   * @throws EC2Exception
   */
  public List listAvailableAMIs() throws EC2Exception{
    List list = new ArrayList();
    List params = new ArrayList();
    List images = ec2.describeImages(params);
    if(images==null){
      return null;
    }
    Debug.debug("Finding available images", 3);
    ImageDescription img = null;
    for(Iterator it=images.iterator(); it.hasNext();){
      img = (ImageDescription) it.next();
      if(img.getImageState().equals("available")){
        list.add(img);
      }
    }
    return list;
  }

  /**
   * List reservations.
   * 
   * @return a List of elements of type ReservationDescription
   * @throws EC2Exception
   */
  public List listReservations() throws EC2Exception{
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

}
