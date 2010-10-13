package gridpilot.dbplugins.atlas;

import gridfactory.common.Debug;
import gridfactory.common.Util;
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
  private HashMap<String, String> fileCatalogs = new HashMap<String, String>();
  private HashMap<String, String> httpFileCatalogs = new HashMap<String, String>();

  /**
   * Create TiersOfAtlas object.
   * @param _toaLocation URL of the tiers of ATLAS file
   * @param _localCacheFile local file to use for caching - can be null,
   *                        in which case caching is not persistent across sessions
   */
  public TiersOfAtlas(String _toaLocation, String _localCacheFile) {
    try{
      toaLocation = _toaLocation;
      URL toaURL = null;
      File tmpFile = File.createTempFile(/*prefix*/"GridPilot-TOA", /*suffix*/"");
      tmpFile.delete();
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
      // Check if the URL is available and write to tmp file - if not, don't overwrite cache.
      try{
        BufferedReader in = new BufferedReader(new InputStreamReader(toaURL.openStream()));
        PrintWriter out = new PrintWriter(new FileWriter(tmpFile)); 
        String line = null;
        while((line = in.readLine())!=null){
          out.println(line);
        }
        in.close();
        out.close();
      }
      catch(Exception ee){
        ee.printStackTrace();
      }
      if(_localCacheFile==null || _localCacheFile.trim().equals("")){
        if(!tmpFile.exists() || tmpFile.length()<1000){
          throw new IOException("ERROR: "+toaURL+" not available and no old cache file present.");
        }
        toaFile = tmpFile;
      }
      else{
        toaFile = new File(Util.clearTildeLocally(Util.clearFile(_localCacheFile)));
        if(!tmpFile.exists() || tmpFile.length()<1000){
          GridPilot.getClassMgr().getLogFile().addMessage("WARNING: "+toaURL+" not available. Using old cache file.");
        }
        else{
          toaFile.delete();
          tmpFile.renameTo(toaFile);
        }
      }
      Debug.debug("Wrote cache of TiersOfATLAS in "+toaFile.getAbsolutePath(), 2);
      // have the file deleted on exit
      GridPilot.addTmpFile(toaFile.getName(), toaFile);
    }
    catch(Exception e){
      String error = "WARNING: could not load tiers of ATLAS file. File catalog lookups " +
          "will not work.";    
      GridPilot.getClassMgr().getLogFile().addMessage(error, e);
      MyUtil.showError(error+" "+e.getMessage());
    }
  }

  protected void clear(){
    fileCatalogs.clear();
    httpFileCatalogs.clear();
  }
  
  /**
   * @param siteAcronym the site acronym
   * @param preferHttp only relevant for LRC catalogs: whether or not to use the http
   *        interface instead of direct mysql
   * @return the catalog URL
   */
  protected String getFileCatalogServer(String siteAcronym, boolean preferHttp) throws MalformedURLException, IOException {
    
    if(!preferHttp && fileCatalogs.containsKey(siteAcronym)){
      return fileCatalogs.get(siteAcronym);
    }
    if(preferHttp && httpFileCatalogs.containsKey(siteAcronym)){
      return httpFileCatalogs.get(siteAcronym);
    }
    
    Debug.debug(siteAcronym+" NOT in cache "+MyUtil.arrayToString(fileCatalogs.keySet().toArray())+
        " : "+preferHttp, 3);
    
    String catalogSite = null;
    String catalogServer = null;
    String httpCatalogServer = null;
    String catalogName = null;
    String parentSite = null;
    // Parse TOA file
    BufferedReader in = null;
    String line = null;
    String inLine = null;
    int count = 0;
    String tmp;
    Debug.debug("Trying to match "+siteAcronym, 3);
    while(catalogServer==null && httpCatalogServer==null && count<5){
      ++count;
      in = new BufferedReader(new InputStreamReader((toaFile.toURI().toURL()).openStream()));
      StringBuffer lb = new StringBuffer();
      while((inLine = in.readLine())!=null){
        inLine = inLine.trim();
        // take care of "lines" split on multiple lines
        if(inLine.length()==0){
          continue;
        }
        else if(inLine.endsWith("',") || inLine.endsWith("': [")){
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
           line.matches("^\\W*'(\\w*)':\\s*\\[.*\\W+'"+parentSite+"'\\W+.*") ||
           parentSite==null &&
           line.matches("^\\W*'(\\w*)':\\s*\\[.*\\W+'"+siteAcronym+"'\\W+.*")){
          catalogSite = line.replaceFirst("^\\W*'(\\w*)':.*", "$1");
          Debug.debug("catalogSite --> "+catalogSite, 3);
        }
        if(/*line.indexOf("SITES")<0 && line.indexOf("'TIER1S': [")<0 && */
            line.indexOf("'ALL': [")<0  && line.indexOf("'alternateName' : [")<0 && 
            line.indexOf("LRC")<0 && line.indexOf("LFC")<0 &&
            line.indexOf("'alternateName' : [")<0 && (
                parentSite!=null &&
                line.matches("^\\W*(\\w*)\\W*\\s*:\\s*\\[.*\\W+"+parentSite+"\\W+.*") ||
                line.matches("^\\W*(\\w*)\\W*\\s*:\\s*\\[.*\\W+"+siteAcronym+"\\W+.*") ||
                line.matches("^\\W*(\\w*)\\W*\\s*:\\s*\\[.*\\W+"+catalogSite+"\\W+.*")
              )
            ){
          tmp = line.replaceFirst("^\\W*(\\w*)\\W*\\s*:.*", "$1");
          if(!tmp.equals(catalogSite)){
            parentSite = tmp;
            Debug.debug("parentSite --> "+parentSite, 3);
          }
        }
        // Now look for
        // FZKLFC: [ 'FZKSITES' ]
        if(catalogSite!=null &&
            line.matches("^\\s*\\S+LFC\\s*:.*'"+catalogSite+"'.*")){
          catalogName = line.replaceFirst("^\\s*(\\S+LFC)\\s*:.*'"+catalogSite+"'.*", "$1");
          Debug.debug("Catalog name --> "+catalogName, 3);
        }
        else if(catalogSite!=null &&
            line.matches("^\\s*\\S+LRC\\s*:.*'"+catalogSite+"'.*")){
          catalogName = line.replaceFirst("^\\s*(\\S+LRC)\\s*:.*'"+catalogSite+"'.*", "$1");
          Debug.debug("Catalog name --> "+catalogName, 3);
        }
        else if(catalogSite!=null &&
            line.matches("^\\s*\\S+LFC\\s*:.*'"+siteAcronym+"'.*")){
          catalogName = line.replaceFirst("^\\s*(\\S+LFC)\\s*:.*'"+siteAcronym+"'.*", "$1");
          Debug.debug("Catalog name --> "+catalogName, 3);
        }
        else if(catalogSite!=null &&
            line.matches("^\\s*\\S+LRC\\s*:.*'"+siteAcronym+"'.*")){
          catalogName = line.replaceFirst("^\\s*(\\S+LRC)\\s*:.*'"+siteAcronym+"'.*", "$1");
          Debug.debug("Catalog name --> "+catalogName, 3);
        }
        else if(catalogSite!=null &&
            line.matches("^\\s*\\S+LFC\\s*:.*'"+parentSite+"'.*")){
          catalogName = line.replaceFirst("^\\s*(\\S+LFC)\\s*:.*'"+parentSite+"'.*", "$1");
          Debug.debug("Catalog name --> "+catalogName, 3);
        }
        else if(catalogSite!=null &&
            line.matches("^\\s*\\S+LRC\\s*:.*'"+parentSite+"'.*")){
          catalogName = line.replaceFirst("^\\s*(\\S+LRC)\\s*:.*'"+parentSite+"'.*", "$1");
          Debug.debug("Catalog name --> "+catalogName, 3);
        }
        // Now look for
        // FZKLFC = 'lfc://lfc-fzk.gridka.de:/grid/atlas/'
        else if(!preferHttp && catalogName!=null &&
            line.matches("^\\s*"+catalogName+"\\s*=\\s*'(.+)'.*")){
          catalogServer = line.replaceFirst("^\\s*"+catalogName+
              "\\s*=\\s*'(.+)'.*", "$1");
          Debug.debug("Catalog server --> "+siteAcronym+" --> "+catalogServer, 3);
          //fileCatalogs.put(siteAcronym, catalogServer);
        }
        else if(preferHttp && catalogName!=null &&
            line.matches("^\\s*"+catalogName+"HTTP\\s*=\\s*'(.+)'.*")){
          httpCatalogServer = line.replaceFirst("^\\s*"+catalogName+
              "HTTP\\s*=\\s*'(.+)'.*", "$1");
          Debug.debug("Catalog server --> "+siteAcronym+" --> "+httpCatalogServer, 3);
          //httpFileCatalogs.put(siteAcronym, httpCatalogServer);
        }
      }
      in.close();
    }
    fileCatalogs.put(siteAcronym, catalogServer);
    httpFileCatalogs.put(siteAcronym, httpCatalogServer);
    return catalogServer;
  }

  
}
