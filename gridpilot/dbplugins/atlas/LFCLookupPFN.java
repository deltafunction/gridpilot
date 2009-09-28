package gridpilot.dbplugins.atlas;

import gridfactory.common.Debug;
import gridpilot.MyUtil;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;


import org.glite.lfc.LFCConfig;
import org.glite.lfc.LFCServer;
import org.glite.lfc.internal.ReplicaDesc;

public class LFCLookupPFN extends LookupPFN {
  
  String dsn;
  LFCServer lfcServer;
  String host;
  DataLocationInterface dli;
  URL dliUrl;
  private int pathConvention = 1;
  private int pathConventions = 5;
  private boolean tryDli;
  
  public LFCLookupPFN(ATLASDatabase db, LFCConfig lfcConfig, String catalogServer,
      String _dsn, String _lfn, boolean findAll, boolean _tryDli) throws MalformedURLException, URISyntaxException {
    super(db, catalogServer, _lfn, findAll);
    dsn = _dsn;
    tryDli = _tryDli;
    lfcServer = new LFCServer(lfcConfig, new URI(catalogServer));
    Debug.debug("Created new LFCServer from ID "+lfcServer.getConfig().globusCredential.getIdentity(), 1);
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
        e.printStackTrace();
      }
    }
    for(int i=0; i<pathConventions; ++i){
      if(pfns!=null && pfns.length>0 || db.getStop() || !db.findPFNs){
        break;
      }
      String atlasLPN = basePath+makeAtlasPath(lfn, dsn);
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
        ++pathConvention;
        if(pathConvention>pathConventions){
          pathConvention = 1;
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
      Debug.debug("Connecting to LFC server at "+catalogServer, 2);
      lfcServer.connect();
      Debug.debug("Looking up "+atlasLPN, 3);
      ArrayList<ReplicaDesc> replicas = lfcServer.getReplicasByPath(atlasLPN);
      lfcServer.disconnect();
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
    if(tryDli){
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
    }
    if(ee!=null){
      throw ee;
    }
    return pfns;
  }
  
    // Construct path following ATLAS conventions
  public String makeAtlasPath(String lfn, String dsn){
    
    String atlasLpn = null;
    String [] lfnMetaData = MyUtil.split(lfn, "\\.");
    String [] dsnMetaData = MyUtil.split(dsn, "\\.");
    String baseStr = null;
    String [] baseMetaData = null;
    Debug.debug("lfnMetaData: "+ lfnMetaData.length+":"+MyUtil.arrayToString(lfnMetaData), 2);
    
    switch(pathConvention){
    
    case 1:
      Debug.debug("Using dsn path convention", 2);
      // data08_1beammag.00087764.physics_BPTX.merge.AOD.o4_r653_r792_p47_tid084038/AOD.084038._000001.pool.root.4 -->
      // /grid/atlas/dq2/data08_1beammag/AOD/data08_1beammag.00087764.physics_BPTX.merge.AOD.o4_r653_r792_p47_tid084038/AOD.084038._000001.pool.root.4
      baseStr = dsnMetaData[0]+"/"+lfnMetaData[0];
      if(dsnMetaData.length==6){
        atlasLpn = "dq2/"+baseStr+"/"+dsn+"/"+lfn;
        break;
      }
      else{
        // Fall through
        ++pathConvention;
      }
      
    case 2:
      Debug.debug("Using very very new path convention", 2);
      // trig1_misal1_mc12.006384.PythiaH120gamgam.recon.AOD.v13003002_tid016421 -->
      // /grid/atlas/dq2/trig1_misal1_mc12/AOD/trig1_misal1_mc12.006384.PythiaH120gamgam.recon.AOD.v13003002_tid016421/AOD.016421._00002.pool.root.12
      baseStr = lfn.replaceFirst("^(.*)\\._[^\\.]+\\..*$", "$1");
      baseMetaData = MyUtil.split(baseStr, "\\.");
      Debug.debug("baseStr: "+baseStr, 2);
      Debug.debug("--> length: "+baseMetaData.length, 2);
      if(baseMetaData.length==6){
        atlasLpn = /*datafiles*/"dq2/"+lfnMetaData[0]+"/"+lfnMetaData[4];
        //atlasLPN += "/"+lfnMetaData[3];
        atlasLpn += "/"+baseStr;
        atlasLpn += "/"+lfn;
        break;
      }
      else{
        // Fall through
        ++pathConvention;
      }
      
    case 3:
      Debug.debug("Using very new path convention", 2);
      // trig1_misal1_mc11.007406.singlepart_singlepi7.recon.log.v12003103_tid003805._00003.job.log.tgz.6 ->
      // /grid/atlas/dq2/trig1_misal1_mc11/trig1_misal1_mc11.007406.singlepart_singlepi7.recon.log.v12003103_tid003805/trig1_misal1_mc11.007406.singlepart_singlepi7.recon.log.v12003103_tid003805._00003.job.log.tgz.6
      baseStr = lfn.replaceFirst("^(.*)\\._[^\\.]+\\..*$", "$1");
      baseMetaData = MyUtil.split(baseStr, "\\.");
      Debug.debug("baseStr: "+baseStr, 2);
      Debug.debug("--> length: "+baseMetaData.length, 2);
      if(baseMetaData.length==6){
        atlasLpn = /*datafiles*/"dq2/"+lfnMetaData[0];
        //atlasLPN += "/"+lfnMetaData[3];
        atlasLpn += "/"+baseStr;
        atlasLpn += "/"+lfn;
        break;
      }
      else{
        // Fall through
        ++pathConvention;
      }
      
     case 4:
       Debug.debug("Using old path convention", 2);
       // csc11.007062.singlepart_gamma_E50.recon.AOD.v11004103._00001.pool.root ->
       // /grid/atlas/dq2/csc11/csc11.007062.singlepart_gamma_E50.recon.AOD.v11004103/
       if(lfnMetaData.length==8 || lfnMetaData.length==9 || lfnMetaData.length==10){
         atlasLpn = /*datafiles*/"dq2/"+lfnMetaData[0];
         //atlasLPN += "/"+lfnMetaData[3];
         atlasLpn += "/"+lfnMetaData[0]+"."+lfnMetaData[1]+"."+lfnMetaData[2]+"."+
            lfnMetaData[3]+"."+lfnMetaData[4]+"."+lfnMetaData[5];
         atlasLpn += "/"+lfn;
         break;
       }
       else{
         // Fall through
         ++pathConvention;
       }
       
     case 5:
       Debug.debug("Using new path convention", 2);
       // New (or old?) convention:
       // csc11.007062.singlepart_gamma_E50.recon.AOD.v11004103._00001.pool.root ->
       // /grid/atlas/dq2/csc11/csc11.007062.singlepart_gamma_E50.recon.AOD.v11004103/AOD/
       if(lfnMetaData.length==8 || lfnMetaData.length==9 || lfnMetaData.length==10){
         atlasLpn = /*"datafiles/"+*/"dq2/"+lfnMetaData[0];
         atlasLpn += "/"+lfnMetaData[4];
         atlasLpn += "/"+lfnMetaData[0]+"."+lfnMetaData[1]+"."+lfnMetaData[2]+"."+
            lfnMetaData[3]+"."+lfnMetaData[4]+"."+lfnMetaData[5];
         atlasLpn += "/"+lfn;
       }
       else{
         atlasLpn = lfn;
       }
       break;
       
     default:
       Debug.debug("pathConvention not in range: "+pathConvention, 2);
       throw new IndexOutOfBoundsException("pathConvention not in range: "+pathConvention);

    }
    
    return atlasLpn;
  }
    
}
