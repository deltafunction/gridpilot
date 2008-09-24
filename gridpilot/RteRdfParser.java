package gridpilot;

import java.util.HashSet;
import java.util.Iterator;

import gridfactory.common.DBRecord;
import gridfactory.common.DBResult;
import gridfactory.common.Debug;
import gridfactory.common.jobrun.RTECatalog;
import gridfactory.common.jobrun.RTECatalog.MetaPackage;
import gridfactory.common.jobrun.RTECatalog.TarPackage;


/**
 * This class provides methods for parsing the KnowARC RDF/XML format
 * for RTE catalogs.
 */

public class RteRdfParser {
  
  public String [] catalogURLs;
  private RTECatalog rteCatalog;
  
  public RteRdfParser(String [] _urls){
    catalogURLs = _urls;
    for(int i=0; i<catalogURLs.length; ++i){
      if(catalogURLs[i].toLowerCase().startsWith("https://")){
        try{
          GridPilot.getClassMgr().getSSL().activateSSL();
        }
        catch(Exception e){
          e.printStackTrace();
          GridPilot.getClassMgr().getLogFile().addMessage("WARNING: could not activate SSL.");
        }
        break;
      }
    }
    rteCatalog = new RTECatalog(_urls, null);
  }
  
  /**
   * Get the active RTECatalog.
   * @return the active RTECatalog
   */
  public RTECatalog getRteCatalog(){
    return rteCatalog;
  }
  
  /**
   * Parse each MetaPackage into a "runtimeEnvironment" DBRecord.
   * "url" is set to the URL of the instance TarPackage.
   * Each BaseSystem is also parsed into a "runtimeEnvironment" DBRecord.
   */
  public DBResult getDBResult(DBPluginMgr dbpluginMgr, String csName){
    // The standard runtimeEnvironment fields are:
    // identifier name computingSystem certificate url initLines depends provides created lastModified
    // The MetaPackage fields are:
    // id name homepage description lastupdate provides instances tags
    // The Package fields are:
    // id baseSystem depends
    // The TarPackage fields are:
    // id baseSystem depends url
    // The BaseSystem fields are:
    // id name homePage description lastUpdate immutable

    String [] fields = dbpluginMgr.getFieldnames("runtimeEnvironment");
    MetaPackage pack = null;
    int i = 0;
    TarPackage tarPack = null;
    HashSet records = new HashSet();
    String dep;
    for(Iterator it=rteCatalog.getMetaPackages().iterator(); it.hasNext();){
      pack = (MetaPackage) it.next();
      Debug.debug("Adding metaPackage "+pack.name+" with "+pack.instances.length+
          " instance(s)", 2);
      DBRecord rec = new DBRecord(fields, new String [fields.length]);
      try{
        rec.setValue(MyUtil.getNameField(dbpluginMgr.getDBName(), "runtimeEnvironment"),
            pack.name);
        if(pack.provides!=null && pack.provides.length>0){
          rec.setValue("provides", MyUtil.arrayToString(pack.provides));
        }
        if(pack.tags!=null && pack.tags.length>0){
          rec.setValue("provides", (rec.getValue("provides")==null?"":rec.getValue("provides"))+
              MyUtil.arrayToString(pack.provides));
        }
        rec.setValue("lastModified", pack.lastupdate);
        rec.setValue("computingSystem", csName);
        rec.setValue("url", "");
        for(int j=0; j<pack.instances.length; ++j){
          tarPack = rteCatalog.getInstancePackage(pack.instances[j]);
          if(tarPack!=null){
            // We always depend on the base system
            rec.setValue("depends",
                ((rec.getValue("depends")!=null?rec.getValue("depends"):"")+" "+
                (tarPack.baseSystem!=null?"\\'"+rteCatalog.getBaseSystem(tarPack.baseSystem).name+"\\'":""))/*.replaceAll("'([^']+)'", "$1")*/.trim());
            // Optional other dependencies
            for(int k=0; k<tarPack.depends.length; ++k){
              Debug.debug("depends: "+tarPack.depends[k], 2);
              try{
                dep = tarPack.depends[k];
                if(dep!=null){
                  try{
                    dep = rteCatalog.getName(dep);
                  }
                  catch(Exception e){
                    // dep was probably already a name - ignore
                  }
                  rec.setValue("depends",
                      ((rec.getValue("depends")!=null?rec.getValue("depends"):"")+" "+
                          dep)/*.replaceAll("'([^']+)'", "$1")*/.trim());
                }
              }
              catch(Exception e){
                e.printStackTrace();
              }
            }
            rec.setValue("url", ((rec.getValue("url")!=null?rec.getValue("url"):"")+" "+
                (tarPack.url!=null && (rec.getValue("url")==null ||
                    !tarPack.url.equals(rec.getValue("url")))?tarPack.url:"")).replaceAll("'([^']+)'", "$1").trim());
            
            if(rec.getValue("url")==null || rec.getValue("url").equals("")){
              Debug.debug("Skipping record (no URL): "+rec, 2);
            }
            else{
              Debug.debug("Adding record: "+records.size()+" --> "+rec, 2);
              records.add(rec);
            }
            
          }
          else{
            Debug.debug("WARNING: Only TarPackages are supported.  "+pack.instances[j]+
                " "+tarPack, 1);
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
