package gridpilot.dbplugins.atlas;

import gridpilot.Debug;
import gridpilot.Util;

import java.net.MalformedURLException;
import java.net.URL;

public class LFCLookupPFN extends LookupPFN {
  
  public LFCLookupPFN(ATLASDatabase db, String catalogServer,
      String lfn, boolean findAll) throws MalformedURLException {
    super(db, catalogServer, lfn, findAll);
  }

  public String [] lookup() throws Exception {
    
    String path = catalogUrl.getPath()==null ? "" : catalogUrl.getPath();
    String host = catalogUrl.getHost();
    DataLocationInterface dli = new DataLocationInterfaceLocator();
    /*e.g. "http://lfc-atlas.cern.ch:8085", "http://lxb1941.cern.ch:8085"
           "http://lfc-atlas-test.cern.ch:8085" */
    String basePath = "/"+path+(path.endsWith("/")?"":"/");
    URL dliUrl = new URL("http://"+host+":8085");
    Debug.debug("Connecting to DLI web service at "+dliUrl.toExternalForm(), 2);
    String [] pfns = null;
    String [] ret = null;
    // If the LFN starts with "user." assume lfcUserBasePath
    if(lfn.startsWith("user.")){
      String atlasLPN = basePath+db.lfcUserBasePath+lfn;
      try{
        pfns = dli.getDataLocationInterface(dliUrl).listReplicas(
            "lfn", atlasLPN);
      }
      catch(Exception e){
      }
    }
    for(int i=0; i<db.pathConventions; ++i){
      if(db.getStop() || !db.findPFNs){
        return null;
      }
      String atlasLPN = basePath+db.makeAtlasPath(lfn);
      Debug.debug("LPN: "+atlasLPN, 2);
      try{
        pfns = dli.getDataLocationInterface(dliUrl).listReplicas(
            "lfn", atlasLPN);
      }
      catch(Exception e){
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
        Debug.debug("PFNs: "+Util.arrayToString(pfns), 2);
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
    
}
