package gridpilot.dbplugins.atlas;

import gridfactory.common.Debug;
import gridpilot.GridPilot;
import gridpilot.MyUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;

public class TiersOfAtlas {

  private File toaFile;
  private String toaLocation;
  // Hash of catalog server mappings found in TiersOfAtlas
  private HashMap fileCatalogs = new HashMap();
  private HashMap httpFileCatalogs = new HashMap();

  public TiersOfAtlas(String _toaLocation) {
    try{
      toaLocation = _toaLocation;
      URL toaURL = null;
      toaFile = File.createTempFile(/*prefix*/"GridPilot-TOA", /*suffix*/"");
      toaFile.delete();
      toaLocation = MyUtil.clearFile(toaLocation);
      if(toaLocation.startsWith("~")){
        toaLocation = System.getProperty("user.home") + File.separator +
        toaLocation.substring(1);
      }
      if(toaLocation.matches("\\w:.*") || toaLocation.indexOf(":")<0){
        toaURL = (new File(toaLocation)).toURI().toURL();
      }
      else{
        toaURL = new URL(toaLocation);
      }
      BufferedReader in = new BufferedReader(new InputStreamReader(toaURL.openStream()));
      PrintWriter out = new PrintWriter(
          new FileWriter(toaFile)); 
      String line = null;
      while((line = in.readLine())!=null){
        out.println(line);
      }
      in.close();
      out.close();
      // hack to have the diretory deleted on exit
      GridPilot.tmpConfFile.put(toaFile.getName(), toaFile);
    }
    catch(Exception e){
      String error = "WARNING: could not load tiers of atlas. File catalog lookups " +
          "are disabled";    
      GridPilot.getClassMgr().getLogFile().addMessage(error, e);
    }
  }

  protected void clear(){
    fileCatalogs.clear();
    httpFileCatalogs.clear();
  }
  
  /**
   * @param name the site acronym
   * @param preferHttp only relevant for LRC catalogs: whether or not to use the http
   *        interface instead of direct mysql
   * @return the catalog URL
   */
  protected String getFileCatalogServer(String name, boolean preferHttp) throws MalformedURLException, IOException {
    
    if(!preferHttp && fileCatalogs.containsKey(name)){
      return (String) fileCatalogs.get(name);
    }
    if(preferHttp && httpFileCatalogs.containsKey(name)){
      return (String) httpFileCatalogs.get(name);
    }
    
    String catalogSite = null;
    String catalogServer = null;
    String catalogName = null;
    String parentSite = null;
    // Parse TOA file
    BufferedReader in = null;
    String line = null;
    String inLine = null;
    int count = 0;
    
    Debug.debug("Trying to match "+name, 3);
    while(catalogServer==null && count<5){
      boolean chkSite = false;
      ++count;
      in = new BufferedReader(new InputStreamReader((toaFile.toURI().toURL()).openStream()));
      StringBuffer lb = new StringBuffer();
      while((inLine = in.readLine())!=null){
        // take care of "lines" split on multiple lines
        if(inLine.endsWith("',")){
          lb.append(inLine);
          continue;
        }
        else if(inLine.matches("^\\s*\\#.*")){
          continue;
        }
        else if(lb.length()>0){
          lb.append(inLine);
          line = lb.toString();
          lb.setLength(0);
        }
        else{
          line = inLine;
        }
        // Say, we have a name 'CSCS'; first look for lines like
        // 'FZKSITES': [ 'FZK', 'FZU', 'CSCS', 'CYF', 'DESY-HH', 'DESY-ZN', 'UNI-FREIBURG', 'WUP' ],
        // 'FZK': [ 'FZKDISK', 'FZKTAPE' ],
        if(parentSite!=null &&
            line.matches("^\\W*'(\\w*)':\\s*\\[.*\\W+"+parentSite+"\\W+.*") ||
            parentSite==null && line.matches("^\\W*'(\\w*)':\\s*\\[.*\\W+"+name+"\\W+.*")){
          catalogSite = line.replaceFirst("^\\W*'(\\w*)':.*", "$1");
          Debug.debug("Catalog site: "+catalogSite, 3);
        }
        if(line.indexOf("SITES")<0 && line.indexOf("'TIER1S': [")<0 && 
            line.indexOf("'ALL': [")<0  && line.indexOf("'alternateName' : [")<0 && 
            line.indexOf("LRC")<0 && line.indexOf("LFC")<0 &&
            line.indexOf("'alternateName' : [")<0&& (
                parentSite!=null &&
                line.matches("^\\W*(\\w*)\\W*\\s*:\\s*\\[.*\\W+"+parentSite+"\\W+.*") ||
                line.matches("^\\W*(\\w*)\\W*\\s*:\\s*\\[.*\\W+"+name+"\\W+.*"))
            ){
          parentSite = line.replaceFirst("^\\W*(\\w*)\\W*\\s*:.*",
              "$1");
          Debug.debug("Parent site: "+parentSite, 3);
        }
        if(!chkSite){
          Debug.debug("Checking: "+catalogSite, 3);
          chkSite = true;
        }
        // Now look for
        // FZKLFC = 'lfc://lfc-fzk.gridka.de:/grid/atlas/'
        if(catalogSite!=null &&
            line.matches("^\\s*\\S+LFC\\s*:.*'"+catalogSite+"'.*")){
          catalogName = line.replaceFirst("^\\s*(\\S+LFC)\\s*:.*'"+catalogSite+"'.*", "$1");
          Debug.debug("Catalog name: "+catalogName, 3);
        }
        else if(catalogSite!=null &&
            line.matches("^\\s*\\S+LRC\\s*:.*'"+catalogSite+"'.*")){
          catalogName = line.replaceFirst("^\\s*(\\S+LRC)\\s*:.*'"+catalogSite+"'.*", "$1");
          Debug.debug("Catalog name: "+catalogName, 3);
        }
        else if(catalogSite!=null &&
            line.matches("^\\s*\\S+LFC\\s*:.*'"+name+"'.*")){
          catalogName = line.replaceFirst("^\\s*(\\S+LFC)\\s*:.*'"+name+"'.*", "$1");
          Debug.debug("Catalog name: "+catalogName, 3);
        }
        else if(catalogSite!=null &&
            line.matches("^\\s*\\S+LRC\\s*:.*'"+name+"'.*")){
          catalogName = line.replaceFirst("^\\s*(\\S+LRC)\\s*:.*'"+name+"'.*", "$1");
          Debug.debug("Catalog name: "+catalogName, 3);
        }
        else if(!preferHttp && catalogName!=null &&
            line.matches("^\\s*"+catalogName+"\\s*=\\s*'(.+)'.*")){
          catalogServer = line.replaceFirst("^\\s*"+catalogName+
              "\\s*=\\s*'(.+)'.*", "$1");
          Debug.debug("Catalog server: "+catalogServer, 3);
          fileCatalogs.put(name, catalogServer);
        }
        else if(preferHttp && catalogName!=null &&
            line.matches("^\\s*"+catalogName+"HTTP\\s*=\\s*'(.+)'.*")){
          catalogServer = line.replaceFirst("^\\s*"+catalogName+
              "HTTP\\s*=\\s*'(.+)'.*", "$1");
          Debug.debug("Catalog server: "+catalogServer, 3);
          httpFileCatalogs.put(name, catalogServer);
        }
      }
      in.close();
    }
    return catalogServer;

  }

  
}
