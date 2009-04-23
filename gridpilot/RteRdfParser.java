package gridpilot;

import java.util.HashSet;
import java.util.Iterator;

import gridfactory.common.DBRecord;
import gridfactory.common.DBResult;
import gridfactory.common.Debug;
import gridfactory.common.jobrun.RTECatalog;
import gridfactory.common.jobrun.RTECatalog.BaseSystem;
import gridfactory.common.jobrun.RTECatalog.InstancePackage;
import gridfactory.common.jobrun.RTECatalog.MetaPackage;

/**
 * This class provides methods for parsing the KnowARC RDF/XML format
 * for RTE catalogs.
 */

public class RteRdfParser {
  
  /** VM RTEs have names that start with this prefix. */
  public static final String VM_PREFIX = "VM/";
  public String [] catalogURLs;
  private RTECatalog rteCatalog;
  
  public RteRdfParser(String [] _urls){
    catalogURLs = _urls;
    MyUtil.checkAndActivateSSL(catalogURLs);
    //rteCatalog = new RTECatalog(catalogURLs, null);
    rteCatalog = GridPilot.getClassMgr().getRTEMgr(GridPilot.RUNTIME_DIR, catalogURLs).getRTECatalog();
  }
  
  /**
   * Get the active RTECatalog.
   * @return the active RTECatalog
   */
  public RTECatalog getRteCatalog(){
    return rteCatalog;
  }
  
  private DBRecord mkRecFromMetaPackage(String[] fields, String nameField, String csName, MetaPackage pack) throws Exception{
    DBRecord rec = new DBRecord(fields, new String [fields.length]);
    rec.setValue(nameField,  pack.name);
    if(pack.provides!=null && pack.provides.length>0){
      rec.setValue("provides", (rec.getValue("provides")==null||rec.getValue("provides").equals("")?"":
        rec.getValue("provides")+" ")+MyUtil.arrayToString(pack.provides));
    }
    // We add tags, labels and VirtualMachine.os to the 'provides' field to improve chances of
    // matching a required RTE. TODO: reconsider
    if(pack.virtualMachine!=null && pack.virtualMachine.os!=null){
      rec.setValue("provides", (rec.getValue("provides")==null||rec.getValue("provides").equals("")?"":
        rec.getValue("provides")+" ")+pack.virtualMachine.os);
    }
    if(pack.tags!=null && pack.tags.length>0){
      rec.setValue("provides", (rec.getValue("provides")==null||rec.getValue("provides").equals("")?"":
        rec.getValue("provides")+" ")+MyUtil.arrayToString(pack.tags));
    }
    if(pack.labels!=null && pack.labels.length>0){
      rec.setValue("provides", (rec.getValue("provides")==null||rec.getValue("provides").equals("")?"":
        rec.getValue("provides")+" ")+MyUtil.arrayToString(pack.labels));
    }
    rec.setValue("lastModified", pack.lastupdate);
    rec.setValue("computingSystem", csName);
    rec.setValue("url", "");
    return rec;
  }
  
  /**
   * Parse each MetaPackage into a "runtimeEnvironment" DBRecord.
   * "url" is set to the URL of the instance TarPackage.
   * Each BaseSystem is also parsed into a "runtimeEnvironment" DBRecord.
   */
  public DBResult getDBResult(DBPluginMgr dbpluginMgr, String csName){
    /*
       The standard runtimeEnvironment fields are:
         identifier name computingSystem certificate url initLines depends provides created lastModified
         
       The MetaPackage fields are:
         id name homepage description lastupdate provides instances tags
         
       The Package fields are:
         id baseSystem depends
         
       The TarPackage fields are:
         id baseSystem depends url
         
       The BaseSystem fields are:
         id name homePage description lastUpdate immutable
     */

    String [] fields = dbpluginMgr.getFieldnames("runtimeEnvironment");
    MetaPackage pack = null;
    int i = 0;
    InstancePackage instPack = null;
    HashSet records = new HashSet();
    String dep;
    BaseSystem bs;
    for(Iterator it=rteCatalog.getMetaPackages().iterator(); it.hasNext();){
      pack = (MetaPackage) it.next();
      Debug.debug("Adding metaPackage "+pack.name, 2);

      try{
        for(int j=0; j<pack.instances.length; ++j){
          DBRecord rec = mkRecFromMetaPackage(fields, MyUtil.getNameField(dbpluginMgr.getDBName(), "runtimeEnvironment"),
              csName, pack);
          instPack = rteCatalog.getInstancePackage(pack.instances[j]);
          if(instPack!=null){
            Debug.debug("Instance: "+pack.instances[j]+"-->"+instPack.id, 3);
            // We always depend on the base system
            bs = rteCatalog.getBaseSystem(instPack.baseSystem);
            if(bs==null || bs.name==null || bs.name.equals("")){
              GridPilot.getClassMgr().getLogFile().addMessage("WARNING: instance "+pack.instances[j]+" has no BaseSystem defined");
              //continue;
            }
            if(bs!=null){
              rec.setValue("depends",
                  ((rec.getValue("depends")!=null?rec.getValue("depends"):"")+" "+bs.name
                      )/*.replaceAll("'([^']+)'", "$1")*/.trim());
            }
            // Optional other dependencies
            for(int k=0; k<instPack.depends.length; ++k){
              try{
                dep = instPack.depends[k];
                if(dep!=null && !dep.trim().equals("")){
                  try{
                    dep = rteCatalog.getName(dep);
                  }
                  catch(Exception e){
                    // dep was probably already a name - ignore
                  }
                  if(dep==null || dep.trim().equals("")){
                    dep = instPack.depends[k];
                  }
                  if(MyUtil.isNumeric(dep)){
                    GridPilot.getClassMgr().getLogFile().addInfo("WARNING: The package "+pack.name+
                        " is not installable. "+instPack);
                    continue;
                  }
                  Debug.debug("found depends: "+instPack.depends[k]+"-->"+dep, 2);
                  rec.setValue("depends",
                      ((rec.getValue("depends")!=null?rec.getValue("depends"):"")+" "+
                          dep)/*.replaceAll("'([^']+)'", "$1")*/.trim());
                }
              }
              catch(Exception e){
                e.printStackTrace();
              }
            }
            // For AMIPackages and EBSSnapshotPackages we use the url field to hold the manifest and shapshot ID respectively
            if(instPack.getClass().getCanonicalName().equals(RTECatalog.AMIPackage.class.getCanonicalName())){
              rec.setValue("url", ((RTECatalog.AMIPackage)instPack).manifest);
            }
            else if(instPack.getClass().getCanonicalName().equals(RTECatalog.EBSSnapshotPackage.class.getCanonicalName())){
              rec.setValue("url", ((RTECatalog.EBSSnapshotPackage)instPack).snapshotId);
            }
            rec.setValue("url", ((rec.getValue("url")!=null?rec.getValue("url"):"")+" "+
                (instPack.url!=null && (rec.getValue("url")==null ||
                    !instPack.url.equals(rec.getValue("url")))?instPack.url:"")).replaceAll("'([^']+)'", "$1").trim());
            
            if(rec.getValue("url")==null || rec.getValue("url").equals("")){
              GridPilot.getClassMgr().getLogFile().addInfo("WARNING: package "+pack.name+" has no URL defined.");
            }
            Debug.debug("Adding record to "+records.size()+" --> "+MyUtil.arrayToString(rec.fields)+
                " --> '"+MyUtil.arrayToString(rec.values, "', '")+"'", 2);
            records.add(rec);
            
          }
          else{
            GridPilot.getClassMgr().getLogFile().addInfo("WARNING: The package "+pack.name+
                " is not installable. "+instPack);
          }
        }
      }
      catch(Exception e){
        e.printStackTrace();
      }
      ++i;
    }
    
    Debug.debug("Returning "+records.size()+" records", 2);
    
    DBResult res = new DBResult(fields, new String [records.size()][fields.length]);
    i = 0;
    for(Iterator it=records.iterator(); it.hasNext();){
      res.values[i] = ((DBRecord) it.next()).values;
      ++i;
    }
    
    return res;
  }
  
}
