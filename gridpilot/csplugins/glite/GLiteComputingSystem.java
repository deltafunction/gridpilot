package gridpilot.csplugins.glite;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import gridpilot.ComputingSystem;
import gridpilot.ConfigFile;
import gridpilot.DBPluginMgr;
import gridpilot.Debug;
import gridpilot.JobInfo;
import gridpilot.LogFile;
import gridpilot.GridPilot;
import gridpilot.ShellMgr;
import gridpilot.Util;

import org.glite.wms.wmproxy.CredentialException;
import org.glite.wms.wmproxy.ServerOverloadedFaultException;
import org.glite.wms.wmproxy.ServiceException;
import org.glite.wms.wmproxy.ServiceURLException;
import org.glite.wms.wmproxy.WMProxyAPI;
import org.globus.mds.MDS;
import org.globus.mds.MDSException;
import org.globus.mds.MDSResult;
import org.safehaus.uuid.UUIDGenerator;

/**
 * Main class for the LSF plugin.
 */

public class GLiteComputingSystem implements ComputingSystem{

  public static final String LSF_STATUS_PEND = "PEND";
  public static final String LSF_STATUS_RUN = "RUN";
  public static final String LSF_STATUS_WAIT = "WAIT";
  public static final String LSF_STATUS_DONE = "DONE";
  public static final String LSF_STATUS_EXIT = "EXIT";
  public static final String LSF_STATUS_UNKWN = "UNKWN";

  public static final String LSF_STATUS_NOTFOUND = "NOT_FOUND";
  public static final String LSF_STATUS_ERROR = "ERROR";
  public static final String LSF_STATUS_UNAVAILABLE = "UNAVAILABLE";

  private String csName;
  private LogFile logFile;
  private ConfigFile configFile;
  private String error = "";
  private String [] runtimeDBs = null;
  private HashSet finalRuntimes = null;
  private String wmUrl = null;
  private WMProxyAPI vmProxyAPI = null;
  private String bdiiHost = null;
  private MDS mds = null;
  private String [] rteClusters = null;
  private String [] rteVos = null;
  private String [] rteTags = null;
  private HashSet rteScriptMappings = null;
  
  private static String BDII_PORT = "2170";
  private static String BDII_BASE_DN = "mds-vo-name=local,o=grid";

  public GLiteComputingSystem(String _csName){
    csName = _csName;
    logFile = GridPilot.getClassMgr().getLogFile();
    configFile = GridPilot.getClassMgr().getConfigFile();
    try{
      rteVos = GridPilot.getClassMgr().getConfigFile().getValues(
          csName, "runtime vos");
    }
    catch(Exception e){
      logFile.addMessage(
          "WARNING: runtime vos for "+csName+" not defined. Showing all RTEs", e);
    }
    try{
      rteTags = GridPilot.getClassMgr().getConfigFile().getValues(
          csName, "runtime tags");
    }
    catch(Exception e){
      logFile.addMessage(
          "WARNING: runtime tags for "+csName+" not defined. Showing all RTEs", e);
    }
    try{
      rteClusters = GridPilot.getClassMgr().getConfigFile().getValues(
          csName, "runtime clusters");
    }
    catch(Exception e){
      logFile.addMessage(
          "WARNING: runtime clusters for "+csName+" not defined." +
                " Querying all clusters for RTEs. This may take a LONG time...", e);
    }
    try{
      wmUrl = GridPilot.getClassMgr().getConfigFile().getValue(
          csName, "WMProxy URL");
      bdiiHost = GridPilot.getClassMgr().getConfigFile().getValue(
          csName, "BDII host");
      
      vmProxyAPI = new WMProxyAPI(wmUrl,
            Util.getProxyFile().getAbsolutePath(),
            GridPilot.getClassMgr().getCaCertsTmpDir());
      
      mds = new MDS(bdiiHost, BDII_PORT, BDII_BASE_DN);
      
      try{
        runtimeDBs = GridPilot.getClassMgr().getConfigFile().getValues(
            csName, "runtime databases");
      }
      catch(Exception e){
        Debug.debug("ERROR getting runtime database: "+e.getMessage(), 1);
      }
      if(runtimeDBs!=null && runtimeDBs.length>0){
        setupRuntimeEnvironments(csName);
      }
      
    }
    catch(Exception e){
      Debug.debug("ERROR initializing "+csName+". "+e.getMessage(), 1);
      e.printStackTrace();
    }
  }

  /**
   * The runtime environments are simply found from the
   * information system.
   */
  public void setupRuntimeEnvironments(String csName){
    for(int i=0; i<runtimeDBs.length; ++i){
      setupRuntimeEnvironments(csName, runtimeDBs[i]);
    }
  }

    /**
   * The runtime environments are simply found from the
   * information system.
   */
  public void setupRuntimeEnvironments(String csName, String runtimeDB){
    finalRuntimes = new HashSet();
    HashSet runtimes = new HashSet();
    
    GridPilot.splashShow("Discovering gLite runtime environments...");

    try{
      mds.connect();
      Hashtable clusterTable =
        mds.search(BDII_BASE_DN, "(GlueSubClusterName=*)",
            new String [] {"GlueSubClusterName"}, MDS.SUBTREE_SCOPE);
      Enumeration en = clusterTable.elements();
      Enumeration enn = null;
      Hashtable rteTable = null;
      MDSResult hostRes = null;
      MDSResult rteRes = null;
      String host = null;
      String rte = null;
      Debug.debug("rteClusters: "+rteClusters, 2);
      while(en.hasMoreElements()){
        hostRes = (MDSResult) en.nextElement();
        host = hostRes.getFirstValue("GlueSubClusterName").toString();
        // If runtime hosts are defined, ignore non-mathing hosts
        if(rteClusters!=null && !Arrays.asList(rteClusters).contains(host)){
          continue;
        }
        Debug.debug("host -> "+host, 2);
        rteTable = mds.search(BDII_BASE_DN, "(GlueSubClusterName="+host+")",
            new String [] {"GlueHostApplicationSoftwareRunTimeEnvironment"},
            MDS.SUBTREE_SCOPE);
        enn = rteTable.elements();
        while(enn.hasMoreElements()){
          rteRes = (MDSResult) enn.nextElement();
          for(int i=0; i<rteRes.size("GlueHostApplicationSoftwareRunTimeEnvironment"); ++i){
            rte = (String) rteRes.getValueAt("GlueHostApplicationSoftwareRunTimeEnvironment", i);
            // Ignore RTEs that don't belong to one of the defined VOs
            if(rteVos!=null){
              for(int j=0; j<rteVos.length; ++j){
                Debug.debug("checking "+rte.toLowerCase()+" <-> "+"vo-"+rteVos[j].toLowerCase(), 3);
                if(rteVos[j]!=null &&
                    rte.toLowerCase().startsWith("vo-"+rteVos[j].toLowerCase())){
                  runtimes.add(rte);
                  continue;
                }
              }
            }
            else{
              runtimes.add(rte);
            }
            Debug.debug("RTE ---> "+rte, 2);
          }
        }
      }
      mds.disconnect();
    }
    catch(MDSException e){
      logFile.addMessage("WARNING: could not list runtime environments.", e);
      e.printStackTrace();
    }
    
    if(runtimes!=null && runtimes.size()>0){
      String name = null;
      DBPluginMgr dbPluginMgr = null;      
      try{
        dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(runtimeDB);
      }
      catch(Exception e){
        Debug.debug("WARNING: could not load runtime DB "+runtimeDB, 1);
        return;
      }
      String [] runtimeEnvironmentFields =
        dbPluginMgr.getFieldNames("runtimeEnvironment");
      String [] rtVals = new String [runtimeEnvironmentFields.length];
      for(Iterator it=runtimes.iterator(); it.hasNext();){
        name = null;
        try{
          name = it.next().toString();       
        }
        catch(Exception e){
          e.printStackTrace();
        }
        if(name!=null && name.length()>0){
          // Write the entry in the local DB
          for(int i=0; i<runtimeEnvironmentFields.length; ++i){
            if(runtimeEnvironmentFields[i].equalsIgnoreCase("name")){
              rtVals[i] = name;
            }
            else if(runtimeEnvironmentFields[i].equalsIgnoreCase("computingSystem")){
              rtVals[i] = csName;
            }
            else if(runtimeEnvironmentFields[i].equalsIgnoreCase("initLines")){             
              rtVals[i] = mapRteNameToScriptPaths(name);
            }
            else{
              rtVals[i] = "";
            }
          }
          try{
            if(dbPluginMgr.createRuntimeEnvironment(rtVals)){
              finalRuntimes.add(name);
            }
          }
          catch(Exception e){
            e.printStackTrace();
          }
        }
      }
    }
    else{
      Debug.debug("WARNING: no runtime environments found", 1);
    }
  }
  
  private String mapRteNameToScriptPaths(String name){
    if(rteScriptMappings==null){
      // Try to find (guess...) the paths to the setup scripts
      rteScriptMappings = new HashSet();
      String [] mappings = null;
      if(rteTags!=null){
        for(int i=0; i<rteVos.length; ++i){
          mappings = null;
          try{
            mappings = GridPilot.getClassMgr().getConfigFile().getValues(
               csName, rteTags[i]);
          }
          catch(Exception e){
          }
          if(mappings!=null){
            rteScriptMappings.add(mappings);
          }
        }
      }
    }

    String [] patternAndReplacements = null;
    String ret = "";
    for(Iterator it=rteScriptMappings.iterator(); it.hasNext();){
      patternAndReplacements = (String []) it.next();
      if(patternAndReplacements!=null && patternAndReplacements.length>1 &&
          name.matches(patternAndReplacements[0])){
        for(int i=1; i<patternAndReplacements.length; ++i){
          if(i>1){
            ret += "\n";
          }
          ret += "source "+name.replaceFirst(patternAndReplacements[0], patternAndReplacements[i]);
        }
      }
    }
    return ret;
  }

  public boolean submit(JobInfo job){
    String id = UUIDGenerator.getInstance().generateTimeBasedUUID().toString();
    String proxy;
    try{
      proxy=vmProxyAPI.grstGetProxyReq(id);
      vmProxyAPI.grstPutProxy(id, proxy);
    }
    catch(Exception e){
      logFile.addMessage("ERROR: could not delegate credentials.", e);
      e.printStackTrace();
    }
    
    
    // TODO Auto-generated method stub
    return false;
  }

  public void updateStatus(Vector jobs){
    // TODO Auto-generated method stub

  }

  public boolean killJobs(Vector jobs){
    // TODO Auto-generated method stub
    return false;
  }

  public void clearOutputMapping(JobInfo job){
    // TODO Auto-generated method stub

  }

  public void exit(){
    // TODO Auto-generated method stub

  }

  public String getFullStatus(JobInfo job){
    // TODO Auto-generated method stub
    return null;
  }

  public String[] getCurrentOutputs(JobInfo job) throws IOException{
    // TODO Auto-generated method stub
    return null;
  }

  public String[] getScripts(JobInfo job){
    // TODO Auto-generated method stub
    return null;
  }

  public String getUserInfo(String csName){
    // TODO Auto-generated method stub
    return null;
  }

  public boolean postProcess(JobInfo job){
    // TODO Auto-generated method stub
    return false;
  }

  public boolean preProcess(JobInfo job){
    // TODO Auto-generated method stub
    return false;
  }

  public String getError(String csName){
    return error;
  }

}