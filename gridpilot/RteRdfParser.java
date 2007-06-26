package gridpilot;


import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashSet;
import java.util.Iterator;


/**
 * This class provides methods for parsing the KnowARC RDF/XML format
 * for RTE catalogs.
 */

public class RteRdfParser {
  
  public String [] catalogURLs;
  public HashSet baseSystems;
  public HashSet metaPackages;
  public HashSet tarPackages;
  public HashSet debianPackages;
  private HashSet topTagSet;
  private StringBuffer unparsed = null;
  private static String [] TOP_TAGS = {"BaseSystem", "MetaPackage",
    "TarPackage", "DebianPackage"};
  private static String BASE_URL = "http://knowarc.eu/kb#knowarc_Instance_";
  
  public RteRdfParser(String [] _urls){
    catalogURLs = _urls;
    baseSystems = new HashSet();
    metaPackages = new HashSet();
    tarPackages = new HashSet();
    debianPackages = new HashSet();
    topTagSet = new HashSet();
    unparsed = new StringBuffer();
    for(int i=0; i<TOP_TAGS.length; ++i){
      topTagSet.add(TOP_TAGS[i]);
    }
    for(int i=0; i<catalogURLs.length; ++i){
      try{
        parseFile(catalogURLs[i]);
      }
      catch(Exception e){
        GridPilot.getClassMgr().getLogFile().addMessage("WARNING: could not parse " +
            "catalog URL "+catalogURLs[i], e);
      }
    }
  }
  
  /**
   * Parse each MetaPackage into a "runtimeEnvironment" DBRecord.
   * "url" is set to the URL of the instance TarPackage.
   * Each BaseSystem is also parsed into a "runtimeEnvironment" DBRecord.
   */
  public DBResult getDBResult(DBPluginMgr dbpluginMgr){
    // The standard runtimeEnvironment fields are:
    // identifier name computingSystem certificate url initLines depends created lastModified
    // The MetaPackage fields are:
    // id name homepage description lastupdate instances tags
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
    for(Iterator it=metaPackages.iterator(); it.hasNext();){
      pack = (MetaPackage) it.next();
      Debug.debug("Adding metaPackage "+pack.name+" with "+pack.instances.length+
          " instance(s)", 2);
      DBRecord rec = new DBRecord(fields, new String [fields.length]);
      try{
        rec.setValue(Util.getNameField(dbpluginMgr.getDBName(), "runtimeEnvironment"),
            pack.name);
        rec.setValue("lastModified", pack.lastupdate);
        rec.setValue("computingSystem", "GPSS");
        rec.setValue("url", "");
        for(int j=0; j<pack.instances.length; ++j){
          // We support only TarPackages
          tarPack = getTarPackage(pack.instances[j]);
          if(tarPack!=null){
            // We always depend on the base system
            rec.setValue("depends",
                ((rec.getValue("depends")!=null?rec.getValue("depends"):"")+" "+
                (tarPack.baseSystem!=null?tarPack.baseSystem:"")).replaceAll("'([^']+)'", "$1").trim());
            // Optional other dependencies
            for(int k=0; k<tarPack.depends.length; ++k){
              try{
                rec.setValue("depends",
                    ((rec.getValue("depends")!=null?rec.getValue("depends"):"")+" "+
                    (getMetaPackageName(tarPack.depends[k])!=null?getMetaPackageName(tarPack.depends[k]):"")).replaceAll("'([^']+)'", "$1").trim());
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
              Debug.debug("Adding record: "+":"+records.size()+" --> "+rec, 2);
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
  
  private String [] parseFile(String _src) throws IOException{
    String src = _src;
    String [] res = new String [] {};
    if(src==null || src.equals("")){
      return res;
    }
    File tmpFile = null;
    if(!Util.urlIsRemote(src)){
      src = "file:"+Util.clearTildeLocally(Util.clearFile(src));
    }
    else if(!src.startsWith("http://")){
      tmpFile = File.createTempFile("GridPilot-", "");
      try{
        TransferControl.download(src, tmpFile, null);
        src = "file:"+tmpFile.getAbsolutePath();
      }
      catch(Exception e) {
        e.printStackTrace();
        throw new IOException("Could not download catalog to temp file.");
      }
    }
    URL url = new URL(src);
    InputStream is = null;
    DataInputStream dis = null;
    StringBuffer str = new StringBuffer("");
    String startPattern = null;
    String fullPattern = null;
    String shortPattern = null;
    String pattern = null;
    try{
      is = url.openStream();
      dis = new DataInputStream(new BufferedInputStream(is));
      int data = -1;
      String type = null;
      for(;;){
        data = dis.read();
        // Check for EOF
        if(data==-1){
          break;
        }
        else if(((char) data)=='\n' || ((char) data)=='\r'){
          continue;
        }
        else{
          str.append((char) data);
          startPattern = ".*<kb:([A-Z]\\w+) .*";
          if(((char) data)=='<' && str.toString().matches(startPattern)){
            type = str.toString().replaceFirst(startPattern, "$1");
            if(!topTagSet.contains(type)){
              type = "!"+type;
            }
          }
          fullPattern = ".*(<kb:"+type+") (.*<\\/kb:"+type+">)(.*)";
          shortPattern = ".*(<kb:"+type+") ([^<^>]*\\/>)(.*)";
          if(((char) data)=='>'){
            if(str.toString().matches(fullPattern)){
              pattern = fullPattern;
            }
            else if(str.toString().matches(shortPattern)){
              pattern = shortPattern;
            }
            else{
              continue;
            }
            String head = str.toString().replaceFirst(pattern, "$1");
            String body = str.toString().replaceFirst(pattern, "$2");
            Debug.debug(head+": "+body, 3);
            if(type.equals("BaseSystem")){
              BaseSystem bs = parseBaseSystem(body);
              baseSystems.add(bs);
              Debug.debug("Parsed BaseSystem "+bs.toString(), 3);
            }
            else if(type.equals("MetaPackage")){
              MetaPackage mp = parseMetaPackage(body);
              metaPackages.add(mp);
              Debug.debug("Parsed MetaPackage "+mp.toString(), 3);
            }
            else if(type.equals("TarPackage")){
              TarPackage tp = parseTarPackage(body);
              tarPackages.add(tp);
              Debug.debug("Parsed TarPackage "+tp.toString(), 3);
            }
            else{
              unparsed.append("\n"+head+" "+body);
            }
            str = new StringBuffer(str.toString().replaceFirst(pattern, "$3"));
          }
        }
      }
    }
    catch(IOException e){
      String error = "Could not open "+url;
      //e.printStackTrace();
      Debug.debug(error, 2);
      throw e;
    }
    finally{
      if(tmpFile!=null){
        try{
          tmpFile.delete();
        }
        catch(Exception ee){
          ee.printStackTrace();
        }
      }
    }
    return res;
  }
  
  public String getUnparsed(){
    return unparsed.toString();
  }
  
  private BaseSystem parseBaseSystem(String body){
    String [] labels = getValues(body, "kb:label");
    String id = body.replaceFirst(".*rdf:about=\"[^\"]*knowarc_Instance_(\\d+)\\D+.*", "$1");
    if(id.equals(body)){
      id = "";
    }
    String name = getValue(body, "kb:name");
    String url = getValue(body, "kb:url");
    String distribution = getValue(body, "kb:distribution");
    String description = getValue(body, "kb:description");
    String lastupdate = getValue(body, "kb:lastupdate");
    String immutable = getValue(body, "kb:immutable");

    return new BaseSystem(
        id.replaceAll("'([^']+)'", "$1").trim(),
        name.replaceAll("'([^']+)'", "$1").trim(),
        url.replaceAll("'([^']+)'", "$1").trim(),
        description.replaceAll("'([^']+)'", "$1").trim(),
        distribution.replaceAll("'([^']+)'", "$1").trim(),
        lastupdate.replaceAll("'([^']+)'", "$1").trim(),
        immutable.replaceAll("'([^']+)'", "$1").trim(),
        labels);
  }
  
  private MetaPackage parseMetaPackage(String body){
    String [] tags = getValues(body, "kb:tag");
    String [] labels = getValues(body, "kb:label");
    HashSet instanceSet = new HashSet();
    String instancePattern = "<kb:instance rdf:resource=\"[^\"]*knowarc_Instance_(\\d+)\"\\/>";
    String instanceTakeoutPattern = "<kb:instance rdf:resource=\"[^\"]*knowarc_Instance_\\d+\"\\/>";
    String instanceStr = body.replaceFirst(".*"+instancePattern+".*", "$1");
    String instanceTakeOutStr = null;
    int ind = -1;
    while(true){
      instanceTakeOutStr = body.replaceFirst(".*("+instanceTakeoutPattern+").*", "$1");
      if(body.equals(instanceTakeOutStr)){
        break;
      }
      ind = body.indexOf(instanceTakeOutStr);
      if(ind<0){
        break;
      }
      instanceStr = instanceTakeOutStr.replaceFirst(".*"+instancePattern+".*", "$1");
      body = body.substring(0, ind)+body.substring(ind+instanceTakeOutStr.length());
      instanceSet.add(instanceStr);
    }
    String [] instances = new String[instanceSet.size()];
    int i = 0;
    for(Iterator it=instanceSet.iterator(); it.hasNext();){
      instances[i] = (String) it.next();
      ++i;
    }
    String id = body.replaceFirst(".*rdf:about=\"[^\"]*knowarc_Instance_(\\d+)\\D+.*", "$1");
    if(id.equals(body)){
      id = "";
    }
    Debug.debug("Parsed id --> "+id, 3);
    String name = getValue(body, "kb:name");
    Debug.debug("Parsed name --> "+name, 3);
    String homepage = getValue(body, "kb:homepage");
    Debug.debug("Parsed homepage --> "+homepage, 3);
    String description = getValue(body, "kb:description");
    Debug.debug("Parsed description --> "+description, 3);
    String lastupdate = getValue(body, "kb:lastupdated");
    Debug.debug("Parsed lastupdate --> "+lastupdate, 3);
    return new MetaPackage(id.replaceAll("'([^']+)'", "$1").trim(), name.replaceAll("'([^']+)'", "$1").trim(),
        homepage.replaceAll("'([^']+)'", "$1").trim(), description.replaceAll("'([^']+)'", "$1").trim(),
        lastupdate.replaceAll("'([^']+)'", "$1").trim(), instances, tags, labels);
  }
  
  private TarPackage parseTarPackage(String body){
    String [] labels = getValues(body, "kb:label");
    HashSet dependsSet = new HashSet();
    String dependsPattern = "<kb:depends rdf:resource=\"[^\"]*knowarc_Instance_(\\d+)\"\\/>";
    String dependsTakeoutPattern = "<kb:depends rdf:resource=\"[^\"]*knowarc_Instance_\\d+\"\\/>";
    String dependsStr = body.replaceFirst(".*"+dependsPattern+".*", "$1");
    String dependsTakeOutStr = null;
    int ind = -1;
    while(true){
      dependsTakeOutStr = body.replaceFirst(".*("+dependsTakeoutPattern+").*", "$1");
      if(body.equals(dependsTakeOutStr)){
        break;
      }
      ind = body.indexOf(dependsTakeOutStr);
      if(ind<0){
        break;
      }
      dependsStr = dependsTakeOutStr.replaceFirst(".*"+dependsPattern+".*", "$1");
      body = body.substring(0, ind)+body.substring(ind+dependsTakeOutStr.length());
      dependsSet.add(dependsStr);
    }
    String [] depends = new String[dependsSet.size()];
    int i = 0;
    for(Iterator it=dependsSet.iterator(); it.hasNext();){
      depends[i] = (String) it.next();
      ++i;
    }
    String id = body.replaceFirst(".*rdf:about=\"[^\"]*knowarc_Instance_(\\d+)\\D+.*", "$1");
    if(id.equals(body)){
      id = "";
    }
    Debug.debug("Parsed id --> "+id, 3);
    String baseSystem = body.replaceFirst(".*<kb:basesystem rdf:resource=\"[^\"]*knowarc_Instance_(\\d+)\"\\/>.*", "$1");
    if(baseSystem.equals(body)){
      baseSystem = "";
    }
   Debug.debug("Parsed baseSystem --> "+baseSystem, 3);
    String url = getValue(body, "kb:url");
    Debug.debug("Parsed url --> "+url, 3);
    return new TarPackage(id.replaceAll("'([^']+)'", "$1").trim(),
        baseSystem.replaceAll("'([^']+)'", "$1").trim(),
        depends, url.replaceAll("'([^']+)'", "$1").trim(),
        labels);
  }
  
  public TarPackage getTarPackage(String id){
    TarPackage pack = null;
    for(Iterator it=tarPackages.iterator(); it.hasNext();){
      pack = (TarPackage) it.next();
      Debug.debug("Looking for "+id+" "+pack.id, 3);
      if(pack.id.equals(id)){
        return pack;
      }
    }
    return null;
  }
  
  public MetaPackage getMetaPackage(String name){
    MetaPackage pack = null;
    for(Iterator it=metaPackages.iterator(); it.hasNext();){
      pack = (MetaPackage) it.next();
      Debug.debug("Looking for "+name+" "+pack.name, 3);
      if(pack.name.equals(name)){
        return pack;
      }
    }
    return null;
  }
  
  /**
   * Gets the values of the specified attribute in body.
   * The attribute settings are truncated off body.
   */
  private String [] getValues(String body, String attribute){
    int ind = -1;
    HashSet tagSet = new HashSet();
    String tagPattern = "<"+attribute+">([^<]+)<\\/"+attribute+">";
    String tagPattern1 = ""+attribute+"=\"([^\"]+)\"";
    String tagTakeoutPattern = "<"+attribute+">[^<]+<\\/"+attribute+">";
    String tagTakeoutPattern1 = ""+attribute+"=\"[^\"]+\"";
    String tagTakeOutStr = null;
    String tagStr = body.replaceFirst(".*"+tagPattern+".*", "$1");
    while(true){
      tagTakeOutStr = body.replaceFirst(".*("+tagTakeoutPattern+").*", "$1");
      if(body.equals(tagTakeOutStr)){
        break;
      }
      ind = body.indexOf(tagTakeOutStr);
      if(ind<0){
        break;
      }
      tagStr = tagTakeOutStr.replaceFirst(".*"+tagPattern+".*", "$1");
      body = body.substring(0, ind)+body.substring(ind+tagTakeOutStr.length());
      tagSet.add(tagStr);
    }
    tagStr = body.replaceFirst(".*"+tagPattern1+".*", "$1");
    while(true){
      tagTakeOutStr = body.replaceFirst(".*("+tagTakeoutPattern1+").*", "$1");
      if(body.equals(tagTakeOutStr)){
        break;
      }
      ind = body.indexOf(tagTakeOutStr);
      if(ind<0){
        break;
      }
      tagStr = tagTakeOutStr.replaceFirst(".*"+tagPattern1+".*", "$1");
      body = body.substring(0, ind)+body.substring(ind+tagTakeOutStr.length());
      tagSet.add(tagStr);
    }
    String [] tags = new String[tagSet.size()];
    int i = 0;
    for(Iterator it=tagSet.iterator(); it.hasNext();){
      tags[i] = (String) it.next();
      ++i;
    }
    if(tagStr.equals(body)){
      tags = new String[] {};
    }
    return tags;
  }
  
  /**
   * Gets the value of the specified attribute in body or "";
   * The attribute setting is truncated off body.
   */
  private String getValue(String body, String attribute){
    int ind = -1;
    HashSet tagSet = new HashSet();
    String tagPattern = "<"+attribute+">([^<]+)<\\/"+attribute+">";
    String tagPattern1 = ""+attribute+"=\"([^\"]+)\"";
    String tagTakeoutPattern = "<"+attribute+">[^<]+<\\/"+attribute+">";
    String tagTakeoutPattern1 = ""+attribute+"=\"[^\"]+\"";
    String tagTakeOutStr = null;
    String tagStr = body.replaceFirst(".*"+tagPattern+".*", "$1");
    while(true){
      tagTakeOutStr = body.replaceFirst(".*("+tagTakeoutPattern+").*", "$1");
      if(body.equals(tagTakeOutStr)){
        break;
      }
      ind = body.indexOf(tagTakeOutStr);
      if(ind<0){
        break;
      }
      tagStr = tagTakeOutStr.replaceFirst(".*"+tagPattern+".*", "$1");
      body = body.substring(0, ind)+body.substring(ind+tagTakeOutStr.length());
      tagSet.add(tagStr);
    }
    tagStr = body.replaceFirst(".*"+tagPattern1+".*", "$1");
    while(true){
      tagTakeOutStr = body.replaceFirst(".*("+tagTakeoutPattern1+").*", "$1");
      if(body.equals(tagTakeOutStr)){
        break;
      }
      ind = body.indexOf(tagTakeOutStr);
      if(ind<0){
        break;
      }
      tagStr = tagTakeOutStr.replaceFirst(".*"+tagPattern1+".*", "$1");
      body = body.substring(0, ind)+body.substring(ind+tagTakeOutStr.length());
      tagSet.add(tagStr);
    }
    if(tagSet.isEmpty()){
      return "";
    }
    else if(tagSet.size()>1){
      Debug.debug("WARNING: more than one value of "+attribute+" found: "+
          Util.arrayToString(tagSet.toArray()), 2);
      return Util.arrayToString(tagSet.toArray());
    }
    else{
      return (String) tagSet.iterator().next();
    }
  }
  
  String getMetaPackageName(String tarPackageId){
    MetaPackage pack = null;
    for(Iterator it=metaPackages.iterator(); it.hasNext();){
      pack = (MetaPackage) it.next();
      for(int i=0; i<pack.instances.length; ++i){
        if(pack.instances[i].equals(tarPackageId)){
          return pack.name;
        }
      }
    }
    return null;
  }
  
  public class BaseSystem {
    public String id;
    public String name;
    public String url;
    public String description;
    public String distribution;
    public String lastupdate;
    public String immutable;
    public String [] labels;
    public BaseSystem(String _id, String _name, String _url, String _description,
        String _distribution, String _lastupdate, String _immutable, String [] _labels){
      name = _name;
      id = _id;
      description = _description;
      distribution = _distribution;
      lastupdate = _lastupdate;
      url = _url;
      immutable = _immutable;
      labels = _labels;
    }
    public String toString(){
      return
         "id: "+id+"\n"+
         "name: "+name+"\n"+
         "url: "+url+"\n"+
         "description: "+description+"\n"+
         "lastupdate: "+lastupdate+"\n"+
         "immutable: "+immutable+"\n";
    }
    public String toXML(){
      String xml = "<kb:BaseSystem " +
          (id.equals("")?"":"rdf:about=\""+BASE_URL+""+id+"\" ") +
          (distribution==null||distribution.equals("")?"":"kb:distribution=\""+distribution+"\" ") +
          (url==null||url.equals("")?"":"kb:url=\""+url+"\" ") +
          (lastupdate==null||lastupdate.equals("")?"":"kb:lastupdated=\""+lastupdate+"\" ") +
          (immutable==null||immutable.equals("")?"":"kb:immutable=\""+immutable+"\" ") +
          (name.equals("")?"":"kb:name=\""+name+"\"") +
           ">";
      xml += (description==null||description.equals("")?"":"\n  <kb:description>"+description+"</kb:description>");
      for(int i=0; i<(labels==null?0:labels.length); ++i){
        xml += "\n  <rdfs:label>"+labels[i]+"</rdfs:label>";
      }
      xml += "\n</kb:BaseSystem>";
      return xml;
    }
  }

  public class MetaPackage {
    public String id;
    public String name;
    public String homepage;
    public String description;
    public String lastupdate;
    public String [] instances;
    public String [] tags;
    public String [] labels;
    public MetaPackage(String _id, String _name, String _homepage, String _description,
        String _lastupdate, String [] _instances, String [] _tags, String [] _labels){
      name = _name;
      id = _id;
      description = _description;
      lastupdate = _lastupdate;
      homepage = _homepage;
      instances = _instances;
      tags = _tags;
      labels = _labels;
    }
    public String toString(){
      return
         "id: "+id+"\n"+
         "name: "+name+"\n"+
         "homepage: "+homepage+"\n"+
         "description: "+description+"\n"+
         "lastupdate: "+lastupdate+"\n"+
         "instances: "+Util.arrayToString(instances)+"\n"+
         "tags: "+Util.arrayToString(tags)+
         "labels: "+Util.arrayToString(labels)+"\n";
    }
    public String toXML(){
      String xml = "<kb:MetaPackage " +
      (id.equals("")?"":"rdf:about=\""+BASE_URL+""+id+"\" ") +
      (description==null||description.equals("")?"":"kb:description=\""+description+"\" ") +
      (lastupdate==null||lastupdate.equals("")?"":"kb:lastupdated=\""+lastupdate+"\" ") +
      (name.equals("")?"":"kb:name=\""+name+"\"") +
       ">";
      for(int i=0; i<(labels==null?0:labels.length); ++i){
      xml += "\n  <rdfs:label>"+labels[i]+"</rdfs:label>";
      }
      for(int i=0; i<(tags==null?0:tags.length); ++i){
        xml += "\n  <kb:tag>"+tags[i]+"</kb:tag>";
      }
      for(int i=0; i<(instances==null?0:instances.length); ++i){
        xml += "\n  <kb:instance rdf:resource=\""+BASE_URL+""+instances[i]+"\"/>";
      }
      xml += "\n</kb:MetaPackage>";
      return xml;
    }
  }
  
  public class Package {
    public String id;
    public String baseSystem;
    public String [] depends;
    public String [] labels;
  }
  
  public class TarPackage extends Package{
    public String url;
    public TarPackage(String _id, String _baseSystem, String [] _depends, String _url,
        String [] _labels){
      id = _id;
      baseSystem = _baseSystem;
      depends = _depends;
      url = _url;
      labels = _labels;
    }
    public String toString(){
      return
         "id: "+id+"\n"+
         "baseSystem: "+baseSystem+"\n"+
         "depends: "+Util.arrayToString(depends)+"\n"+
         "url: "+url+"\n"+
         "labels: "+Util.arrayToString(labels);
    }
    public String toXML(){
      String xml = "<kb:TarPackage " +
          (id.equals("")?"":"rdf:about=\""+BASE_URL+""+id+"\" ") +
          (url.equals("")?"":"kb:url=\""+url+"\" ") +
           ">";
      for(int i=0; i<(labels==null?0:labels.length); ++i){
        xml += "\n  <rdfs:label>"+labels[i]+"</rdfs:label>";
      }
      xml += (baseSystem.equals("")?"":"<kb:basesystem rdf:resource=\""+BASE_URL+""+baseSystem+"\"/> ");
      for(int i=0; i<(depends==null?0:depends.length); ++i){
        xml += "\n  <kb:depends rdf:resource=\""+BASE_URL+""+depends[i]+"\"/>"; 
      }
      xml += "\n</kb:TarPackage>";
      return xml;
    }
  }
  
  public class DebianPackage extends Package{
    // TODO
  }
}
