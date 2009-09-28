package gridpilot.dbplugins.atlas;

import gridfactory.common.Debug;
import gridpilot.MyUtil;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Vector;

import javax.xml.rpc.ServiceException;

import org.glite.lfc.LFCConfig;
import org.glite.lfc.LFCServer;
import org.glite.lfc.internal.ReplicaDesc;

public class LFCLookupPFN extends LookupPFN {
  
  LFCConfig lfcConfig;
  LFCServer lfcServer;
  String host;
  DataLocationInterface dli;
  URL dliUrl;
  
  public LFCLookupPFN(ATLASDatabase db, LFCConfig _lfcConfig, String catalogServer,
      String lfn, boolean findAll) throws MalformedURLException, URISyntaxException {
    super(db, catalogServer, lfn, findAll);
    lfcServer = new LFCServer(lfcConfig, new URI(catalogServer));
    host = catalogUrl.getHost();
    /*e.g. "http://lfc-atlas.cern.ch:8085", "http://lxb1941.cern.ch:8085"
           "http://lfc-atlas-test.cern.ch:8085" */
    dli = new DataLocationInterfaceLocator();
    dliUrl = new URL("http://"+host+":8085");
  }
  
  public String [] lookup() throws Exception { 
    
    String path = catalogUrl.getPath()==null ? "" : catalogUrl.getPath();
    String basePath = "/"+path+(path.endsWith("/")?"":"/");
    String [] pfns = null;
    String [] ret = null;
    // If the LFN starts with "user." assume lfcUserBasePath
    if(lfn.startsWith("user.")){
      String atlasLPN = basePath+db.lfcUserBasePath+lfn;
      try{
        pfns = lfcLookup(atlasLPN);
      }
      catch(Exception e){
      }
    }
    for(int i=0; i<db.pathConventions; ++i){
      if(pfns!=null && pfns.length>0 || db.getStop() || !db.findPFNs){
        break;
      }
      String atlasLPN = basePath+db.makeAtlasPath(lfn);
      Debug.debug("LPN: "+atlasLPN, 2);
      try{
        pfns = lfcLookup(atlasLPN);
      }
      catch(Exception e){
        e.printStackTrace();
      }
      // if nothing is found, try another path convention (and remember this)
      if(pfns==null || pfns.length==0 ||
          pfns.length==1 && (pfns[0]==null || pfns[0].equals(""))){
        ++db.pathConvention;
        if(db.pathConvention>db.pathConventions){
          db.pathConvention = 1;
        }
      }
      else{
        if(!findAll && pfns!=null && pfns.length>1){
          pfns = new String [] {pfns[0]};
        }
        Debug.debug("PFNs: "+MyUtil.arrayToString(pfns), 2);
        break;
      }
    }
    if(pfns==null){
      pfns = new String [] {};
    }
    ret = new String [pfns.length+2];
    ret[0] = null;
    ret[1] = null;
    for(int i=0; i<pfns.length; ++i){
      ret[i+2] = pfns[i];
    }
    
    return ret;
    
  }

  private String[] lfcLookup(String atlasLPN) throws Exception {
    String [] pfns = null;
    Exception ee = null;
    try{
      Debug.debug("Connecting to LFN server at "+dliUrl.toExternalForm(), 2);
      lfcServer.connect();
      ArrayList<ReplicaDesc> replicas = lfcServer.getReplicasByPath(atlasLPN);
      pfns = new String[replicas.size()];
      for(int i=0; i<pfns.length; ++i){
        pfns[i] = replicas.get(i).getSfn();
      }
    }
    catch(Exception e){
      e.printStackTrace();
      ee = e;
    }
    if(pfns!=null){
      return pfns;
    }
    try{
      Debug.debug("Connecting to DLI web service at "+dliUrl.toExternalForm(), 2);
      pfns = dli.getDataLocationInterface(dliUrl).listReplicas(
          "lfn", atlasLPN);
    }
    catch(Exception e){
      e.printStackTrace();
      ee = e;
    }
    if(pfns!=null){
      return pfns;
    }
    if(ee!=null){
      throw ee;
    }
    return pfns;
  }
    
}
