package gridpilot.wizards.manage_software;

import gridpilot.Debug;
import gridpilot.GPFrame;
import gridpilot.GridPilot;
import gridpilot.LocalStaticShellMgr;
import gridpilot.RteRdfParser;
import gridpilot.TransferControl;
import gridpilot.Util;

import java.awt.Color;
import java.awt.Component;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Rectangle;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.border.EtchedBorder;
import javax.swing.border.TitledBorder;

import org.safehaus.uuid.UUID;
import org.safehaus.uuid.UUIDGenerator;

public class CreateSoftwarePackageWizard extends GPFrame{
  
  private static final long serialVersionUID = 493635130266263014L;

  private static int TEXTFIELDWIDTH = 20;
  private static String title = "Create new software package (runtime environment)";
  
  private JPanel namePanel = null;
  private JPanel dirPanel = null;
  private JPanel scriptsPanel = null;
  private JPanel uploadPanel = null;
  private JButton cancelButton = new JButton("Cancel");
  private JScrollPane sp = new JScrollPane();
  private JPanel jp = new JPanel(new GridBagLayout());
  private CreateSoftwarePackageWizard thisFrame = this;
  
  private String name = null;
  private String shortName = null;
  private File dir = null;
  private File tmpDir = null;
  private String catalogUrl = null;
  private String tarballUrl = null;
  private String baseSystem = null;
  private MultiPicker rteBox = null;
  private MultiPicker bsBox = null;
  private RteRdfParser rteRdfParser = null;
  HashMap baseSystemMap = null;

  private JTextField jtfInstall =  new JTextField(TEXTFIELDWIDTH);
  private JTextField jtfRuntime =  new JTextField(TEXTFIELDWIDTH);
  private JTextField jtfRemove =  new JTextField(TEXTFIELDWIDTH);


  /**
   *                    "Create software package" : - ask for name
   *                                                - tell to copy all file to one
   *                                                    directory, ...
   *                                                - create install, runtime, remove scripts
   *                                                -->make tarball
   *                                                - ask for upload URL + publish catalog
   */
  public CreateSoftwarePackageWizard(){
    jp.add(welcomePanel(), new GridBagConstraints(0, 0, 2, 1, 0.0, 0.0,
        GridBagConstraints.WEST, GridBagConstraints.BOTH,
        new Insets(5, 5, 5, 5), 0, 0));
    namePanel = getNamePanel();
    jp.add(namePanel, new GridBagConstraints(0, 1, 2, 1, 0.0, 0.0,
        GridBagConstraints.WEST, GridBagConstraints.BOTH,
        new Insets(5, 5, 5, 5), 0, 0));
    dirPanel = getDirPanel();
    jp.add(dirPanel, new GridBagConstraints(0, 2, 2, 1, 0.0, 0.0,
        GridBagConstraints.WEST, GridBagConstraints.BOTH,
        new Insets(5, 5, 5, 5), 0, 0));
    scriptsPanel = mkScriptsPanel();
    jp.add(scriptsPanel, new GridBagConstraints(0, 3, 2, 1, 0.0, 0.0,
        GridBagConstraints.WEST, GridBagConstraints.BOTH,
        new Insets(5, 5, 5, 5), 0, 0));
    uploadPanel = uploadPanel();
    jp.add(uploadPanel, new GridBagConstraints(0, 4, 2, 1, 0.0, 0.0,
        GridBagConstraints.WEST, GridBagConstraints.BOTH,
        new Insets(5, 5, 5, 5), 0, 0));
    sp.getViewport().add(jp);
    add(sp);
    
    updateComponentTreeUI0(dirPanel, false);
    updateComponentTreeUI0(scriptsPanel, false);
    updateComponentTreeUI0(uploadPanel, false);
    
    int maxHeight = Toolkit.getDefaultToolkit().getScreenSize().height-10;
    int maxWidth = Toolkit.getDefaultToolkit().getScreenSize().width-10;
    this.setMaximumSize(new Dimension(maxWidth, maxHeight>400?400:maxHeight));
    this.setPreferredSize(new Dimension(660, 420));
    this.pack();
  }
  
  private JPanel welcomePanel(){
    JPanel panel = new JPanel();
    panel.setLayout(new GridBagLayout());
    setTitle(title);
    panel.setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white, new Color(165, 163, 151)), "Welcome"));
    String msg = 
            "Welcome to the software package creation wizard.\n\n" +
            "You will now be guided through 4 simple steps to create\n" +
            "and publish a software package AKA a runtime environment.\n\n" +
            "You can exit this wizard at any time by clicking \"Cancel\"";
    JLabel jlDirInstructions = new JLabel("<html>"+msg.replaceAll("\n", "<br>")+"</html>");

    GridBagConstraints ct = new GridBagConstraints();
    ct.fill = GridBagConstraints.BOTH;
    ct.anchor = GridBagConstraints.WEST;
    ct.weightx = 0;
    ct.insets = new Insets(7, 7, 7, 7);
    
    ct.gridx = 0;
    ct.gridy = 0;   
    ct.gridwidth = 2;
    ct.gridheight = 1;
    panel.add(jlDirInstructions, ct);
    
    cancelButton.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        try{
          exit();
        }
        catch(Throwable e1){
          e1.printStackTrace();
        }
      }
    }
    );
    ct.gridwidth = 1;
    ct.gridy = ct.gridy+1;
    ct.ipady = 0;
    JPanel cancelPanel = new JPanel();
    cancelPanel.add(cancelButton, ct);
    panel.add(cancelPanel, ct);
    ct.weightx = 10;
    ct.gridx = 1;
    panel.add(new JLabel(" "), ct);    
    return panel;
  }
  
  private void exit(){
    try{
      LocalStaticShellMgr.deleteDir(tmpDir);
    }
    catch(Exception e){
      e.printStackTrace();
    }
    thisFrame.dispose();
  }
  
  private JPanel getNamePanel(){
    JPanel panel = new JPanel();
    panel.setLayout(new GridBagLayout());
    setTitle(title);
    panel.setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white, new Color(165, 163, 151)), "Step 1/4: choose name"));
    String msg = 
            "First, you have to choose a name for the software package.\n" +
            "If you plan to publish it in a runtime environment catalog, you should follow the convention of\n" +
            "of scoping the name and giving it a versions number. E.g. version 12.0.6 of the ATLAS software\n" +
            "package used in High Energy Physics is named \"APPS/HEP/ATLAS/ATLAS-12.0.6\"\n\n";
    JLabel jlDirInstructions = new JLabel("<html>"+msg.replaceAll("\n", "<br>")+"</html>");
    final JTextField jtf = new JTextField(TEXTFIELDWIDTH);
    JPanel textPanel = new JPanel();
    textPanel.add(jtf);

    GridBagConstraints ct = new GridBagConstraints();
    ct.fill = GridBagConstraints.BOTH;
    ct.anchor = GridBagConstraints.WEST;
    ct.weightx = 0;
    ct.insets = new Insets(7, 7, 7, 7);
    
    ct.gridx = 0;
    ct.gridy = 0;
    ct.gridwidth = 4;
    ct.gridheight = 1;
    panel.add(jlDirInstructions, ct);
    
    ct.gridy = ct.gridy+1;
    ct.gridwidth = 1;
    ct.gridheight = 2;
    panel.add(new JLabel("Name"), ct);
    ct.gridx = 1;
    ct.gridheight = 1;
    ct.ipady = -10;
    panel.add(textPanel, ct);
    ct.ipady = -5;
    
    JButton continueButton = new JButton("Continue");
    continueButton.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        if(jtf.getText()==null || jtf.getText().equals("")){
          Util.showError("You must give a name");
          return;
        }
        name = jtf.getText().trim();
        name = name.replaceFirst("^/", "");
        name = name.replaceFirst("/$", "");
        updateComponentTreeUI0(dirPanel, true);
        updateComponentTreeUI0(namePanel, false);
        //int x = jp.getPreferredSize().width;
        //int y = sp.getViewport().getViewPosition().y;
        //sp.getViewport().setViewPosition(new Point(x, y+340));
        scrollToComponent(dirPanel);
      }
    }
    );
    ct.gridx = 2;
    JPanel pCont = new JPanel();
    pCont.add(continueButton);
    panel.add(pCont, ct);

    ct.gridx = 3;
    ct.weightx = 10;
    panel.add(new JLabel(" "), ct);
    
    return panel;
  }
  
  private JPanel getDirPanel(){
    JPanel panel = new JPanel();
    panel.setLayout(new GridBagLayout());
    setTitle(title);
    panel.setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white, new Color(165, 163, 151)), "Step 2/4: consolidate all files in a directory"));
    String msg = "If all files this software needs are not already in a single directory,\n" +
            "please copy them to one directory. Below, give the path to this directory.\n\n";
    JLabel jlDirInstructions = new JLabel("<html>"+msg.replaceAll("\n", "<br>")+"</html>");
    final JTextField jtf =  new JTextField(TEXTFIELDWIDTH);
    JPanel textPanel = new JPanel();
    textPanel.add(jtf);

    GridBagConstraints ct = new GridBagConstraints();
    ct.fill = GridBagConstraints.BOTH;
    ct.anchor = GridBagConstraints.WEST;
    ct.weightx = 0;
    ct.insets = new Insets(7, 7, 7, 7);
    
    ct.gridx = 0;
    ct.gridy = 0;   
    ct.gridwidth = 4;
    ct.gridheight = 1;
    panel.add(jlDirInstructions, ct);
    
    ct.gridy = ct.gridy+1;;
    ct.gridwidth = 1;
    ct.gridheight = 2;
    JPanel checkPanel = Util.createCheckPanel1(this, "Directory containing software", jtf, true, true, true);
    panel.add(checkPanel, ct);    
    ct.gridx = 1;
    ct.gridheight = 1;    panel.add(textPanel, ct);    
    JButton continueButton = new JButton("Continue");
    continueButton.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        if(jtf.getText()==null || jtf.getText().equals("")){
          Util.showError("You must specify a directory");
          return;
        }
        String dirStr = Util.clearFile(jtf.getText().trim());
        if(!(new File(dirStr)).exists() || !(new File(dirStr)).isDirectory()){
          Util.showError("Directory does not exist");
          dir = null;
          return;
        }
        dir = new File(dirStr);
        try{
          mkTmpDir();
        }
        catch(Exception ee){
          ee.printStackTrace();
          Util.showError("Could not create tmp dir. "+ee.getMessage());
          exit();
        }
        try{
          createTemplateScripts();
        }
        catch(Exception ee){
          ee.printStackTrace();
          Util.showError("Could not create template scripts. "+ee.getMessage());
          exit();
        }
        updateComponentTreeUI0(scriptsPanel, true);
        updateComponentTreeUI0(dirPanel, false);
        //int x = jp.getPreferredSize().width;
        //int y = sp.getViewport().getViewPosition().y;
        //sp.getViewport().setViewPosition(new Point(x, y+190));
        scrollToComponent(scriptsPanel);
      }
    }
    );
    
    ct.gridx = 2;
    JPanel pCont = new JPanel();
    pCont.add(continueButton);
    panel.add(pCont, ct);
    ct.gridx = 3;
    ct.weightx = 10;
    panel.add(new JLabel(" "), ct);
    return panel;
  }
  
  private void mkTmpDir() throws IOException {
    String cacheDir = GridPilot.getClassMgr().getConfigFile().getValue("GridPilot", "Pull cache directory");
    if(cacheDir==null || cacheDir.equals("")){
      throw new IOException("No cache directory defined.");
    }
    UUIDGenerator uuidGen = UUIDGenerator.getInstance();
    UUID nameSpaceUUID = new UUID(UUID.NAMESPACE_URL);
    String tmpName = name+uuidGen.generateNameBasedUUID(nameSpaceUUID, name);
    String [] tmpNames = Util.split(tmpName, "/");
    shortName =  tmpNames[tmpNames.length-1];
    tmpDir = new File(Util.clearTildeLocally(Util.clearFile(cacheDir)),shortName);
    tmpDir.mkdir();
    (new File(tmpDir, "control")).mkdir();
    // hack to have the diretory deleted on exit
    GridPilot.tmpConfFile.put(tmpDir.getAbsolutePath(), tmpDir);
  }

  private void packageInTmpDir() throws IOException {
    // copy data files to tmp dir
    LocalStaticShellMgr.copyDirectory(dir, new File(tmpDir, "data"));
    File tarFile = new File(tmpDir, shortName+".tar");
    File gzipFile = new File(tmpDir, shortName+".tar.gz");
    Util.tar(tarFile, tmpDir);
    Util.gzip(tarFile.getCanonicalPath(), gzipFile.getCanonicalPath());
    tarFile.delete();
  }
  
  private void registerInCatalog(String system, String [] depends) throws Exception {
    
    // TODO: add support for lastupdate, tags, homePage, description, etc.
        
    // Download temporary copy of the catalog
    //TransferControl.download(catalogUrl, tmpDir, this);
    
    // Add the entry to the catalog: name, url, depends.
    RteRdfParser rteRdfParser = new RteRdfParser(new String [] {catalogUrl});
    
    // Check if name is already in catalog. At the same time, find highest ID
    long highestID = 0;
    long tmpID = 0;
    RteRdfParser.MetaPackage mp = null;
    for(Iterator it=rteRdfParser.metaPackages.iterator(); it.hasNext();){
      mp = (RteRdfParser.MetaPackage) it.next();
      if(mp.name.equalsIgnoreCase(name)){
        throw new Exception("RTE "+name+" already exists in catalog. Cannot proceed.");
      }
      tmpID = Long.parseLong(mp.id);
      if(tmpID>highestID){
        highestID = tmpID;
      }
    }
    // Try to match the base system
    String baseSystem = "";
    String baseSystemID = "";
    RteRdfParser.BaseSystem bs = null;
    for(Iterator it=rteRdfParser.baseSystems.iterator(); it.hasNext();){
      bs = (RteRdfParser.BaseSystem) it.next();
      if(bs.name.matches(".*"+system+".*") &&
          (bs==null || bs.name.length()>baseSystem.length())){
        baseSystem = bs.name;
        baseSystemID = bs.id;
      }
      tmpID = Long.parseLong(bs.id);
      if(tmpID>highestID){
        highestID = tmpID;
      }
    }
    if(baseSystemID.equals("")){
      // TODO: we assume Debian Etch has ID 0...
      baseSystemID = "0";
    }
    // This is just to be sure to have the highest id
    // TODO: propose Daniel to use UUIDs instead of incremented integers.
    //       This in order to use several or distributed catalogs
    RteRdfParser.TarPackage tp = null;
    for(Iterator it=rteRdfParser.tarPackages.iterator(); it.hasNext();){
      tp = (RteRdfParser.TarPackage) it.next();
      tmpID = Long.parseLong(tp.id);
      if(tmpID>highestID){
        highestID = tmpID;
      }
    }
    
    // The TarPackage instance
    RteRdfParser.TarPackage tarPackage =
      rteRdfParser.new TarPackage(/*id*/Long.toString(highestID+1), /*baseSystem*/baseSystemID, /*depends*/depends,
           /*url*/tarballUrl, /*labels*/null);
    rteRdfParser.tarPackages.add(tarPackage);
    
    RteRdfParser.MetaPackage metaPackage =
      rteRdfParser.new MetaPackage(/*id*/Long.toString(highestID+2), name, /*homepage*/null,  /*description*/"",
       /*lastupdate*/null, /*instances*/new String []{tarPackage.id},
       /*tags*/null, /*labels*/null);
    rteRdfParser.metaPackages.add(metaPackage);
    
    // Generate the new catalog
    String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"+
   "<!DOCTYPE rdf:RDF [\n"+
   "<!ENTITY rdf 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'>\n"+
   "<!ENTITY kb 'http://knowarc.eu/kb#'>\n"+
   "<!ENTITY rdfs 'http://www.w3.org/2000/01/rdf-schema#'>]>\n"+
   "<rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" " +
   "xmlns:kb=\"http://knowarc.eu/kb#\" xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\">\n\n";
    
    for(Iterator it=rteRdfParser.baseSystems.iterator(); it.hasNext();){
      bs = (RteRdfParser.BaseSystem) it.next();
      xml += mp.toXML()+"\n";
    }    
    for(Iterator it=rteRdfParser.metaPackages.iterator(); it.hasNext();){
      mp = (RteRdfParser.MetaPackage) it.next();
      xml += mp.toXML()+"\n";
    }    
    for(Iterator it=rteRdfParser.tarPackages.iterator(); it.hasNext();){
      tp = (RteRdfParser.TarPackage) it.next();
      xml += mp.toXML()+"\n";
    }
    
    xml += rteRdfParser.getUnparsed() + "\n\n</rdf:RDF>";
    
    // Upload the catalog
    LocalStaticShellMgr.writeFile((new File(tmpDir, "newCatalog.rdf")).getCanonicalPath(), xml, false);
    // TODO
    
  }

  private void createTemplateScripts() throws IOException {
    String installStr =
      "#! /bin/sh\n" +
      "## This is in case your software directory contains a zip file,  my_software.zip,\n" +
      "## that needs to be unpacked in order for the software to run." +
      "set -e\n" +
      "MY_ZIP=\"my_software.zip\"\n" +
      "unzip $MY_ZIPP\n" +
      "rm -f $MY_ZIP\n" +
      "#eof";
    String runtimeStr = "#!/bin/sh\n"+
    "\n"+
    "# this file is sourced with argument \"0\" just before the job submission script\n"+
    "# is written. The job submission script sources it with \"1\" just before job\n"+
    "# execution and with \"2\" after job termination.\n"+
    "\n" +
    "## This is an example of setting system paths to include the installed software.\n" +
    "\n"+
    "MY_JAR=\"my_software/myjar.jar\"\n"+
    "MY_BIN_PATH=\"my_software/bin\"\n"+
    "\n"+
    "case \"$1\" in\n"+
    " 0)\n"+
    "   #none\n"+
    " ;;\n"+
    "\n"+
    " 1)\n"+
    "   # Initialize the java environment\n"+
    "   PATH=\"%BASEDIR%/${MY_BIN_PATH}:${PATH}\"\n"+
    "   export PATH\n"+
    "   CLASSPATH=\"%BASEDIR%/${MY_JAR}:${CLASSPATH}\"\n"+
    "   export CLASSPATH\n"+
    " ;;\n"+
    "\n"+
    " 2)\n"+
    "   #none\n"+
    " ;;\n"+
    "\n"+
    " *)\n"+
    "   return 1\n"+
    " ;;\n"+
    "esac \n"+
    "\n"+
    "# eof";
    String removeStr = "";
    
    File controlDir = new File(tmpDir, "control");
    
    LocalStaticShellMgr.writeFile((new File(controlDir, "install")).getCanonicalPath(), installStr, false);
    LocalStaticShellMgr.writeFile((new File(controlDir, "runtime")).getCanonicalPath(), runtimeStr, false);
    LocalStaticShellMgr.writeFile((new File(controlDir, "remove")).getCanonicalPath(), removeStr, false);

    jtfInstall.setText((new File(controlDir, "install")).getCanonicalPath());
    jtfRuntime.setText((new File(controlDir, "runtime")).getCanonicalPath());
    jtfRemove.setText((new File(controlDir, "remove")).getCanonicalPath());
  
  }
  
  private JPanel mkScriptsPanel(){
    JPanel panel = new JPanel();
    panel.setLayout(new GridBagLayout());
    String title = "Create scripts";
    setTitle(title);
    panel.setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white, new Color(165, 163, 151)), "Step 3/4: create install, runtime and remove scripts"));
    String msg = "You need to create three scripts that will be packed together with your software:\n\n" +
        "<ul>" +
        "<li><b>an install script:</b> installs the sofware" +
        "<li><b>a runtime script:</b> sets up the environment of a job so it can use the installed software.\n" +
        "The string \"%BASEDIR%\" will be replaced by the actual installation directory when\n" +
        "the software is installed" +
        "<li><b>a removal script:</b> does any extra cleanup - the software will be removed automatically.\n" +
        "Typically this script is empty" +
        "</ul>" +
        "For each script, a template is provided. By clicking on the buttons below, you can edit these\n" +
        "to match your specific software." +
        "\n\n";
    JLabel jlInstructions = new JLabel("<html>"+msg.replaceAll("\n", "<br>")+"</html>");
    JPanel tInstall = new JPanel();
    tInstall.add(jtfInstall);
    JPanel tRuntime = new JPanel();
    tRuntime.add(jtfRuntime);
    JPanel tRemove = new JPanel();
    tRemove.add(jtfRemove);
    JPanel jpInstall = Util.createCheckPanel1(this, "Install script", jtfInstall, true, false, false);
    JPanel jpRuntime = Util.createCheckPanel1(this, "Runtime script", jtfRuntime, true, false, false);
    JPanel jpRemove = Util.createCheckPanel1(this, "Remove script", jtfRemove, true, false, false);

    GridBagConstraints ct = new GridBagConstraints();
    ct.fill = GridBagConstraints.BOTH;
    ct.anchor = GridBagConstraints.WEST;
    ct.insets = new Insets(7, 7, 7, 7);
    ct.weightx = 0;
    
    ct.gridx = 0;
    ct.gridy = 0;   
    ct.gridwidth = 4;
    ct.gridheight = 1;
    panel.add(jlInstructions, ct);
    
    ct.gridy = ct.gridy+1;
    ct.gridwidth = 1;
    ct.gridheight = 2;
    panel.add(jpInstall, ct);    
    ct.gridx = 1;
    ct.gridheight = 1;
    panel.add(tInstall, ct); 
    ct.gridx = 2;
    panel.add(new JLabel(" "), ct);    
    
    ct.gridy = ct.gridy+2;
    ct.gridx = 0;
    ct.gridwidth = 1;
    ct.gridheight = 2;
    panel.add(jpRuntime, ct);    
    ct.gridx = 1;
    ct.gridheight = 1;
    panel.add(tRuntime, ct); 
    ct.gridx = 2;
    panel.add(new JLabel(" "), ct);    
    
    ct.gridy = ct.gridy+2;
    ct.gridx = 0;
    ct.gridwidth = 1;
    ct.gridheight = 2;
    panel.add(jpRemove, ct);    
    ct.gridx = 1;
    ct.gridheight = 1;
    panel.add(tRemove, ct);
        
    JButton continueButton = new JButton("Continue");
    continueButton.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        
        try{
          packageInTmpDir();
        }
        catch(IOException e1){
          e1.printStackTrace();
          Util.showError("Could not package files. "+e1.getMessage());
          return;
        }
        
        updateComponentTreeUI0(uploadPanel, true);
        updateComponentTreeUI0(scriptsPanel, false);
        //int x = jp.getPreferredSize().width;
        //int y = sp.getViewport().getViewPosition().y;
        //sp.getViewport().setViewPosition(new Point(x, y+200));
        scrollToComponent(uploadPanel);
      }
    }
    );
    ct.gridx = 2;
    JPanel pCont = new JPanel();
    pCont.add(continueButton);
    panel.add(pCont, ct);
    
    ct.gridx = 3;
    ct.weightx = 10;
    panel.add(new JLabel(" "), ct);
    
    return panel;
  }
  
  private JPanel uploadPanel(){
    JPanel panel = new JPanel();
    panel.setLayout(new GridBagLayout());
    String title = "Upload and publish runtime environment";
    setTitle(title);
    panel.setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white, new Color(165, 163, 151)), "Step 4/4: upload runtime environment and publish the URL in a catalog"));
    String msg =
        "The runtime environment has now been packaged and is ready to be uploaded to a\n" +
        "remote URL. This URL will then be published in the catalog you choose.\n\n" +
        "You must specify which base system the software is to run on. Optionally, you\n" +
        "can also specify if the runtime environment depends on other runtime environments\n\n";
    JLabel jlInstructions = new JLabel("<html>"+msg.replaceAll("\n", "<br>")+"</html>");
    String homeURL = GridPilot.getClassMgr().getConfigFile().getValue("GridPilot", "Grid home url");
    final JTextField jtfUrl =  new JTextField(TEXTFIELDWIDTH);
    if(Util.urlIsRemote(homeURL)){
      jtfUrl.setText(homeURL);
    }
    JPanel tUrl = new JPanel();
    tUrl.add(jtfUrl);
    final JTextField jtfCatalog =  new JTextField(TEXTFIELDWIDTH);
    String catalogUrlsString = GridPilot.getClassMgr().getConfigFile().getValue("GridPilot", "Runtime catalog URLs");
    if(catalogUrlsString!=null && !catalogUrlsString.equals("")){
      String[] catalogUrls;
      catalogUrls = null;
      try{
        catalogUrls = Util.splitUrls(catalogUrlsString);
      }
      catch (Exception e1){
        e1.printStackTrace();
        try{
          catalogUrls = Util.split(catalogUrlsString);
        }
        catch (Exception e2){
          e2.printStackTrace();
          catalogUrls = new String [] {catalogUrlsString};
        }
      }
      // Fill in the first of the configured runtime catalog URLs
      jtfCatalog.setText(catalogUrls[0]);
    }
    JPanel tCatalog = new JPanel();
    tCatalog.add(jtfCatalog);
    JPanel jpUrl = Util.createCheckPanel1(this, "URL to upload tarball", jtfUrl, true, true, false);
    JPanel jpCatalog = Util.createCheckPanel1(this, "Catalog URL", jtfCatalog, true, true, false);
    JButton bRefreshRTEs = new JButton("Refresh list");
    JButton bRefreshBSs = new JButton("Refresh list");
    bRefreshBSs.setToolTipText("Refresh the list of base systems");
    bRefreshBSs.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        // refresh the list on bsBox
        refreshBSs(catalogUrl);
      }
    }
    );
    bRefreshRTEs.setToolTipText("Refresh the list of base systems");
    bRefreshRTEs.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        // refresh the list on rteBox
        refreshRTEs(catalogUrl);
      }
    }
    );
    
    GridBagConstraints ct = new GridBagConstraints();
    ct.fill = GridBagConstraints.BOTH;
    ct.anchor = GridBagConstraints.WEST;
    ct.insets = new Insets(7, 7, 7, 7);
    ct.weightx = 0;
    
    ct.gridx = 0;
    ct.gridy = 0;   
    ct.gridwidth = 3;
    ct.gridheight = 1;
    panel.add(jlInstructions, ct);
    
    ct.gridy = ct.gridy+1;
    ct.gridwidth = 1;
    ct.gridheight = 2;
    panel.add(jpUrl, ct);    
    ct.gridx = 1;
    ct.gridheight = 1;
    panel.add(tUrl, ct); 
    ct.gridx = 2;
    panel.add(new JLabel(" "), ct);    
    
    ct.gridy = ct.gridy+2;
    ct.gridx = 0;
    ct.gridwidth = 1;
    ct.gridheight = 2;
    panel.add(jpCatalog, ct);    
    ct.gridx = 1;
    ct.gridheight = 1;
    panel.add(tCatalog, ct); 
    ct.gridx = 2;
    panel.add(new JLabel(" "), ct);
    
    ct.gridy = ct.gridy+2;
    ct.gridx = 0;
    ct.gridwidth = 1;
    ct.gridheight = 3;
    panel.add(new JLabel("Base system"), ct);    
    ct.gridx = 1;
    JPanel pRefreshBSs = new JPanel();
    pRefreshBSs.add(bRefreshBSs);
    bsBox = new MultiPicker();
    bsBox.setListData(new String [] {});
    JScrollPane spBSs = new JScrollPane();
    spBSs.setPreferredSize(new Dimension(160, 50));
    spBSs.getViewport().add(bsBox);
    pRefreshBSs.add(spBSs);    
    panel.add(pRefreshBSs, ct);
    ct.gridx = 2;
    panel.add(new JLabel(" "), ct);
        
    ct.gridy = ct.gridy+3;
    ct.gridx = 0;
    ct.gridwidth = 1;
    ct.gridheight = 3;
    panel.add(new JLabel("Dependencies"), ct);
    ct.gridx = 1;
    JPanel pRefreshRTEs = new JPanel();
    pRefreshRTEs.add(bRefreshRTEs);
    rteBox = new MultiPicker();
    rteBox.setListData(new String [] {});
    JScrollPane spRtes = new JScrollPane();
    spRtes.setPreferredSize(new Dimension(160, 80));
    spRtes.getViewport().add(rteBox);
    pRefreshRTEs.add(spRtes);    
    panel.add(pRefreshRTEs, ct);
    ct.gridx = 2;        
    JButton continueButton = new JButton("Continue");
    continueButton.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        try{
          if(jtfUrl.getText()==null || jtfUrl.getText().equals("")){
            Util.showError("You must specify an upload URL");
            return;
          }
          if(jtfCatalog.getText()==null || jtfCatalog.getText().equals("")){
            Util.showError("You must specify a catalog URL");
            return;
          }
          if(bsBox.getSelectedIndices()==null || bsBox.getSelectedIndices().length==0){
            Util.showError("You must specify a base system");
            return;
          }
          if(bsBox.getSelectedIndices()!=null && bsBox.getSelectedIndices().length>1){
            Util.showError("Only one base system supported");
            return;
          }
          // Upload the tarball
          File gzipFile = new File(tmpDir, shortName+".tar.gz");
          TransferControl.upload(gzipFile, jtfUrl.getText(), thisFrame);
          tarballUrl = jtfUrl.getText();
          catalogUrl = jtfCatalog.getText();
          baseSystem = (String) baseSystemMap.get((String) bsBox.getSelectedValue());
          String [] depends = new String[rteBox.getSelectedValues().length];
          for(int j=0; j<depends.length; ++j){
            depends[j] = (String) rteBox.getSelectedValues()[j];
          }
          registerInCatalog(baseSystem, depends);
        }
        catch(Exception e1){
          e1.printStackTrace();
          Util.showError("Could upload package files. "+e1.getMessage());
        }
        updateComponentTreeUI0(uploadPanel, false);
        cancelButton.setEnabled(false);
      }
    }
    );
    JPanel pCont = new JPanel();
    pCont.add(continueButton);
    panel.add(pCont, ct);
        
    return panel;
  }
  
  private void parseCatalogs(){
    int i = 0;
    String [] rteCatalogUrls =
      GridPilot.getClassMgr().getConfigFile().getValues("GridPilot", "runtime catalog URLs");
    HashSet catalogUrlsSet = new HashSet();
    if(rteCatalogUrls!=null){
      Collections.addAll(catalogUrlsSet, rteCatalogUrls);
    }
    catalogUrlsSet.add(catalogUrl);
    String [] catalogUrls = new String [catalogUrlsSet.size()];
    for(Iterator it=catalogUrlsSet.iterator(); it.hasNext();){
      catalogUrls[i] = (String) it.next();
      Debug.debug("Adding catalog "+catalogUrls[i], 2);
      ++i;
    }
    rteRdfParser = new RteRdfParser(catalogUrls);
  }
  
  private void refreshBSs(String catalogUrl){
    int i = 0;
    if(rteRdfParser==null){
      parseCatalogs();
    }
    String [] listData = new String [rteRdfParser.baseSystems.size()];
    i = 0;
    RteRdfParser.BaseSystem bs = null;
    baseSystemMap = new HashMap();
    for(Iterator it=rteRdfParser.baseSystems.iterator(); it.hasNext();){
      bs = ((RteRdfParser.BaseSystem) it.next());
      listData[i] = bs.name;
      baseSystemMap.put(bs.name, bs.id);
      ++i;
    }
    Debug.debug("Setting list data "+Util.arrayToString(listData), 2);
    bsBox.setListData(listData);
    bsBox.updateUI();
  }
  
  private void refreshRTEs(String catalogUrl){
    int i = 0;
    if(rteRdfParser==null){
      parseCatalogs();
    }
    String [] listData = new String [rteRdfParser.metaPackages.size()];
    i = 0;
    for(Iterator it=rteRdfParser.metaPackages.iterator(); it.hasNext();){
      listData[i] = ((RteRdfParser.MetaPackage) it.next()).name;
      ++i;
    }
    Debug.debug("Setting list data "+Util.arrayToString(listData), 2);
    rteBox.setListData(listData);
    rteBox.updateUI();
  }
  
  private static void updateComponentTreeUI0(Component c, boolean enabled){
    if(c instanceof JComponent){
      ((JComponent) c).setEnabled(enabled);
      ((JComponent) c).updateUI();
    }
    Component[] children = null;
    if(c instanceof JMenu){
      children = ((JMenu)c).getMenuComponents();
    }
    else if(c instanceof Container){
      children = ((Container)c).getComponents();
    }
    if(children!=null){
      for(int i=0; i<children.length; i++){
        updateComponentTreeUI0(children[i], enabled);
      }
    }
  }

  private void scrollToComponent(Component comp){
    Rectangle currentRect = jp.getVisibleRect();
    int newY = comp.getBounds().y-currentRect.y;
    sp.getViewport().scrollRectToVisible(new Rectangle(0, newY, 100, 300));
  }


}
