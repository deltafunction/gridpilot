package gridpilot.csplugins.ec2;

import gridpilot.Debug;
import gridpilot.GridPilot;
import gridpilot.LogFile;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;

import com.xerox.amazonws.ec2.ConsoleOutput;
import com.xerox.amazonws.ec2.EC2Exception;
import com.xerox.amazonws.ec2.GroupDescription;
import com.xerox.amazonws.ec2.Jec2;
import com.xerox.amazonws.ec2.ImageDescription;
import com.xerox.amazonws.ec2.ImageListAttributeItem;
import com.xerox.amazonws.ec2.ImageListAttribute.ImageListAttributeItemType;
import com.xerox.amazonws.ec2.KeyPairInfo;
import com.xerox.amazonws.ec2.ReservationDescription;
import com.xerox.amazonws.ec2.ReservationDescription.Instance;

/**
 * This class contains methods for managing virtual machines in the
 * Amazon Elastic Compute Cloud (EC2).
 * I relyes on the Typica library 8http://code.google.com/p/typica/wiki/TypicaSampleCode)
 */
public class EC2Mgr {
  
  private Jec2 ec2 = null;
  private String subnet = null;
  private LogFile logFile = null;
  private String keyName = null;
  private String owner = null;

  private static String GROUP_NAME = "GridPilot";

  public EC2Mgr(String accessKey, String secretKey, String _subnet, String _owner) {    
    ec2 = new Jec2(accessKey, secretKey);
    subnet = _subnet;
    owner = _owner;
    logFile = GridPilot.getClassMgr().getLogFile();
  }
  
  private void createSecurityGroup(){
    String description = "Security group for use by GridPilot. SSH access from submitting machine.";
    // First check if the group "GridPilot" already exists.
    // If not, create it.
    try{
      List groupList = ec2.describeSecurityGroups(new String [] {GROUP_NAME});
      if(groupList==null || groupList.isEmpty()){
        ec2.createSecurityGroup(GROUP_NAME, description);
        groupList = ec2.describeSecurityGroups(new String [] {GROUP_NAME});
      }
      // Allow ssh access
      ec2.authorizeSecurityGroupIngress(GROUP_NAME, "tcp", 22, 22, subnet);
    }
    catch(EC2Exception e){
      logFile.addMessage("ERROR: Could not add inbound ssh access to AWS nodes.", e);
      e.printStackTrace();
    }
  }
  
  private void createKeyPair(){
    // Generate keys for this session
  }
  
  private void exit(){
    // If no instances are running, delete keypairs
  }

  public ReservationDescription launchAMIs(String amiID, int instances) throws EC2Exception{
    createSecurityGroup();
    List groupSet = new ArrayList();
    groupSet.add(GROUP_NAME);
    ReservationDescription desc = ec2.runInstances(
        amiID,
        instances,
        instances,
        groupSet,
        /*userData*/owner,
        keyName,
        /*public IP*/true);
    return desc;
  }
  
  /**
   * List available AMIs.
   */
  public List listAvailableAMIs() throws EC2Exception{
    List list = new ArrayList();
    List params = new ArrayList();
    List images = ec2.describeImages(params);
    Debug.debug("Finding available Images", 3);
    ImageDescription img = null;
    for(Iterator it=images.iterator(); it.hasNext();){
      img = (ImageDescription) it.next();
      if(img.getImageState().equals("available")){
        list.add(img);
      }
    }
    return list;
  }

}
