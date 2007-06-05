package gridpilot.wizards.beginning;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;

import gridpilot.ConfigFile;
import gridpilot.ConfirmBox;
import gridpilot.Debug;
import gridpilot.GridPilot;
import gridpilot.LocalStaticShellMgr;
import gridpilot.Util;

import javax.swing.ImageIcon;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;

public class BeginningWizard{

  private ImageIcon icon = null;
  private ConfigFile configFile = null;
  private boolean changes = false;
  private static int TEXTFIELDWIDTH = 32;

  public BeginningWizard(boolean firstRun){
    
    URL imgURL = null;
    changes = false;
    
    try{
      imgURL = GridPilot.class.getResource(GridPilot.resourcesPath + "aviateur.png");
      icon = new ImageIcon(imgURL);
    }
    catch(Exception e){
      try{
        imgURL = GridPilot.class.getResource("/resources/aviateur.png");
        icon = new ImageIcon(imgURL);
      }
      catch(Exception ee){
        ee.printStackTrace();
        Debug.debug("Could not find image "+ GridPilot.resourcesPath + "aviateur.png", 3);
        icon = null;
      }
    }
    
    try{
      if(welcome(firstRun)!=0){
        return;
      }
      
      if(checkDirs(firstRun)==2){
        return;
      }
      
      try{
        if(checkCertificate(firstRun)==2){
          return;
        }
      }
      catch(FileNotFoundException ee){
        showError(ee.getMessage());
        if(checkCertificate(firstRun)==2){
          return;
        }
      }
      
      // TODO: set grid homedir
      
      // TODO: Set grid file catalog - fallback to homegrid.dyndns.org
      
      // TODO: Set grid job DB - fallback to homegrid.dyndns.org
      
      // TODO: set ATLAS home site - present list - ask for mysql alias
      
      // TODO: set pull and GPSS DB - present list, fallback to homegrid.dyndns.org
      
      // TODO: ask on which grids the user is allowed
      
      // TODO: ask if an ssh pool should be configured
      
      // TODO: give report on which directories were created, etc.
      
      if(endGreeting(firstRun)==0 && changes){
        try{
          GridPilot.reloadConfigValues();
        }
        catch(Exception e1){
          e1.printStackTrace();
        }
      }

    }
    catch(Throwable e){
      e.printStackTrace();
      GridPilot.getClassMgr().getLogFile().addMessage("ERROR: could not run setup wizard", e);
      try{
        showError(e.getMessage());
      }
      catch(Exception e1){
        e1.printStackTrace();
      }
    }    
  }
  
  private int welcome(boolean firstRun) throws Exception{
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    String confirmString = "Welcome!\n\n" +
            (firstRun?"This appears to be the first time you run GridPilot.\n":"") +
            "On the next windows you will be guided through some simple steps to setup GridPilot.\n" +
            "You will be asked to confirm the creation of a configuration file, etc.\n\n" +
            "Click \"Continue\" to move on or \"Cancel\" to exit this wizard.\n\n" +
            "Notice that you can always run this wizard again by choosing it from " +
            "the \"Help\" menu.";
    JLabel confirmLabel = new JLabel();
    confirmLabel.setPreferredSize(new Dimension(520, 400));
    confirmLabel.setOpaque(true);
    confirmLabel.setText(confirmString);
    int choice = -1;
    choice = confirmBox.getConfirm("Starting with GridPilot",
        confirmString, new Object[] {"Continue", "Cancel"}, icon, Color.WHITE);
    return choice;
  }
  
  private int endGreeting(boolean firstRun) throws Exception{
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    String confirmString = "Configuring GridPilot is now done.\n" +
        "Your settings have been saved in " +configFile.getFile().getCanonicalPath()+
        ".\n\n"+
        "Please notice that only the most basic parameters,\n" +
            "necessary to get you up and running have been set.\n" +
            "You can modify these and set many others in \"Edit\" -> \"Preferences\"." +
            (firstRun?"\n\nThanks for using GridPilot and have fun!":"");
    int choice = -1;
    confirmBox.getConfirm("Setup completed!",
        confirmString, new Object[] {"OK",  "Cancel"}, icon, Color.WHITE);   
    return choice;
  }
  
  private void showError(String text) throws Exception{
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    String confirmString = "ERROR: could not set up GridPilot. "+text;
    confirmBox.getConfirm("Failed setting up GridPilot",
        confirmString, new Object[] {"OK"}, icon, Color.WHITE);
  }
  
  
  /**
   * Create the config file and some directories and set
   * config values.
   * @throws Throwable 
   */
  private int checkDirs(boolean firstRun) throws Throwable{
    
    if(firstRun){
      System.out.println("Creating new configuration file.");
      configFile = new ConfigFile(GridPilot.defaultConfFileName);
      // Make temporary config file
      File tmpConfFile = (File) GridPilot.tmpConfFile.get(GridPilot.defaultConfFileName);       
       // Copy over config file
       LocalStaticShellMgr.copyFile(tmpConfFile.getCanonicalPath(),
           GridPilot.userConfFile.getCanonicalPath());
       tmpConfFile.delete();
       try{
         configFile = new ConfigFile(GridPilot.userConfFile);
       }
       catch(Exception e){
         System.out.println("WARNING: could not create or load new configuration file!");
         e.printStackTrace();
       }
       GridPilot.getClassMgr().setConfigFile(configFile);
       // This is necessary to be able to create browser panels when clicking on
       // "browse"
       GridPilot.loadFTs();
    }
    else{
      configFile = GridPilot.getClassMgr().getConfigFile();
    }
    
    JPanel jPanel = new JPanel(new GridBagLayout());
    String [] names = new String [] {
        "Database directory",
        "Cache directory",
        "Working directory",
        "Software directory",
        "Transformations directory"
        };
    String dbDir = configFile.getValue("My_DB_local", "database");
    if(dbDir==null){
      dbDir = "~/GridPilot";
    }
    else{
      dbDir.replaceFirst("hsql://localhost/", "");
      dbDir.replaceFirst("/My_DB", "");
    }
    String cacheDir = configFile.getValue("GridPilot", "pull cache directory");
    String workingDir = configFile.getValue("Fork", "working directory");
    String runtimeDir = configFile.getValue("Fork", "runtime directory");
    String transDir = configFile.getValue("Fork", "transformation directory");
    String [] defDirs = new String [] {
        dbDir==null?dbDir:"~/GridPilot",
        cacheDir==null?dbDir+"/cache":cacheDir,
        workingDir==null?dbDir+"/jobs":workingDir,
        runtimeDir==null?dbDir+"/runtimeEnvironments":runtimeDir,
        transDir==null?dbDir+"/transformations":transDir
        };
    JTextField [] jtFields = new JTextField [defDirs.length];
    if(firstRun){
      jPanel.add(new JLabel("A configuration file "+GridPilot.userConfFile.getCanonicalPath()+
          " has been created."),
          new GridBagConstraints(0, 0, 2, 1, 0.0, 0.0,
              GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
              new Insets(5, 5, 5, 5), 0, 0)) ;
    }
    jPanel.add(new JLabel("The following directories will be created if they don't already exist: "),
        new GridBagConstraints(0, (firstRun?1:0), 2, 2, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    JPanel row = null;
    JPanel subRow = null;
    for(int i=0; i<defDirs.length; ++i){
      jtFields[i] = new JTextField(TEXTFIELDWIDTH);
      jtFields[i].setText(defDirs[i]);
      row = new JPanel(new BorderLayout());
      row.add(Util.createCheckPanel(
          JOptionPane.getRootFrame(),
          names[i], jtFields[i]), BorderLayout.WEST);
      subRow = new JPanel(new BorderLayout());
      subRow.add(jtFields[i], BorderLayout.CENTER);
      subRow.add(new JLabel("   "), BorderLayout.SOUTH);
      subRow.add(new JLabel("   "), BorderLayout.NORTH);
      row.add(subRow, BorderLayout.EAST);
      jPanel.add(row, new GridBagConstraints(0, i+(firstRun?5:4), 1, 1, 0.0, 0.0,
          GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
          new Insets(0, 0, 0, 0), 0, 0));
    }
    jPanel.validate();
    
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    int choice = -1;
    try{
      choice = confirmBox.getConfirm("Setting up GridPilot directories",
          jPanel, new Object[] {"Continue", "Skip", "Cancel"}, icon, Color.WHITE);
    }
    catch(Exception e){
      e.printStackTrace();
    }
    
    if(choice!=0){
      if(firstRun){
        // If no config file is present, there's no point in continuing.
        return 2;
      }
      return choice;
    }
    
    // Create missing directories
    String [] newDirs = new String [defDirs.length];
    File [] newDirFiles = new File[newDirs.length];
    for(int i=0; i<defDirs.length; ++i){
      newDirs[i] = jtFields[i].getText();
      Debug.debug("Checking directory "+newDirs[i], 2);
      newDirFiles[i] = new File(Util.clearTildeLocally(Util.clearFile(newDirs[i])));
      if(newDirs[i]!=null && !newDirs[i].equals("")){
        if(newDirFiles[i].exists()){
          if(!newDirFiles[i].isDirectory()){
            throw new IOException("The directory "+newDirFiles[i].getCanonicalPath()+" cannot be created.");
          }
        }
        else{
          Debug.debug("Creating directory "+newDirs[i], 2);
          newDirFiles[i].mkdir();
        }
      }
      newDirs[i] = Util.replaceWithTildeLocally(Util.clearFile(newDirs[i]));
    }
    
    // Set config entries
    if(!defDirs[0].equals(newDirs[0]) ||
       !defDirs[1].equals(newDirs[1]) ||
       !defDirs[2].equals(newDirs[2]) ||
       !defDirs[3].equals(newDirs[3]) ||
       !defDirs[4].equals(newDirs[4])){
      configFile.setAttributes(
          new String [] {"GridPilot", "My_DB_local", "Fork", "Fork", "Fork"},
          new String [] {"pull cache directory", "database", "working directory",
              "runtime directory", "transformation directory"},
          new String [] {
              newDirs[1],
              "hsql://localhost"+
                 (Util.clearFile(newDirs[0]).startsWith("/")?"":"/")+
                 Util.clearFile(newDirs[0])+
                 (Util.clearFile(newDirs[0]).endsWith("/")?"":"/")+
                 "My_DB",
              newDirs[2],
              newDirs[3],
              newDirs[4]}
      );
      changes = true;
    }
    
    return choice;
  }
  
  private int checkCertificate(boolean firstRun) throws Exception{
    String confirmString =
      "To access grid resources you need a valid X509 certificate.\n\n" +
      "If you don't have one, please get one from your grid certificate authority\n" +
      "(and run this wizard again).\n" +
      "GridPilot can still be started, but you can only run jobs and access files\n" +
      "on your local machine or machines on which you have an ssh account.\n\n" +
      "If you have a certificate, please indicate its path as well as the path of the\n" +
      "associated key and the directory where you want to store temporary\n" +
      "credentials (proxies).\n\n" +
      "Optionally, you can also specify a directory with the certificates of the\n" +
      "certificate authories (CAs) that you trust. This can safely be left unspecified\n" +
      "in which case a default set of CAs will be trusted.";
    JPanel jPanel = new JPanel(new GridBagLayout());
    String certPath = configFile.getValue("GridPilot", "key file");
    String keyPath = configFile.getValue("GridPilot", "certificate file");
    String proxyDir = configFile.getValue("GridPilot", "grid proxy directory");
    String caCertsDir = configFile.getValue("GridPilot", "ca certificates");
    String [] defDirs = new String [] {
        certPath,
        keyPath,
        proxyDir,
        caCertsDir
        };
    String [] names = new String [] {
        "Grid public certificate",
        "Grid private key",
        "Temporary credentials directory",
        "CA certificates directory"
        };
    JTextField [] jtFields = new JTextField [defDirs.length];
    jPanel.add(new JLabel("<html>"+confirmString.replaceAll("\n", "<br>")+"</html>"),
        new GridBagConstraints(0, (firstRun?1:0), 2, 2, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    JPanel row = null;
    JPanel subRow = null;
    for(int i=0; i<defDirs.length; ++i){
      jtFields[i] = new JTextField(TEXTFIELDWIDTH);
      jtFields[i].setText(defDirs[i]);
      row = new JPanel(new BorderLayout());
      row.add(Util.createCheckPanel(
          JOptionPane.getRootFrame(),
          names[i], jtFields[i]), BorderLayout.WEST);
      subRow = new JPanel(new BorderLayout());
      subRow.add(jtFields[i], BorderLayout.CENTER);
      subRow.add(new JLabel("   "), BorderLayout.SOUTH);
      subRow.add(new JLabel("   "), BorderLayout.NORTH);
      row.add(subRow, BorderLayout.EAST);
      jPanel.add(row, new GridBagConstraints(0, i+(firstRun?5:4), 1, 1, 0.0, 0.0,
          GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
          new Insets(0, 0, 0, 0), 0, 0));
    }
    jPanel.validate();
    
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    int choice = -1;
    try{
      choice = confirmBox.getConfirm("Setting up grid credentials",
          jPanel, new Object[] {"Continue", "Skip", "Cancel"}, icon, Color.WHITE);
    }
    catch(Exception e){
      e.printStackTrace();
    }
    
    if(choice!=0){
      return choice;
    }

    // Create missing directories
    String [] newDirs = new String[defDirs.length];
    File [] newDirFiles = new File[newDirs.length];
    for(int i=0; i<newDirs.length; ++i){
      newDirs[i] = jtFields[i].getText();
      if(i>1 && newDirs[i]!=null && !newDirs[i].equals("")){
        Debug.debug("Checking directory "+newDirs[i], 2);
        newDirFiles[i] = new File(Util.clearTildeLocally(Util.clearFile(newDirs[i])));
        if(newDirFiles[i].exists()){
          if(!newDirFiles[i].isDirectory()){
            throw new IOException("The directory "+newDirFiles[i].getCanonicalPath()+" cannot be created.");
          }
        }
        else{
          Debug.debug("Creating directory "+newDirs[i], 2);
          newDirFiles[i].mkdir();
        }
      }
      newDirs[i] = Util.replaceWithTildeLocally(Util.clearFile(newDirs[i]));
    }
    
    // Check if certificate and key exist
    File certFile = new File(Util.clearTildeLocally(Util.clearFile(newDirs[0])));
    if(!certFile.exists()){
      throw new FileNotFoundException(certFile.getCanonicalPath());
    }
    File keyFile = new File(Util.clearTildeLocally(Util.clearFile(newDirs[1])));
    if(!keyFile.exists()){
      throw new FileNotFoundException(keyFile.getCanonicalPath());
    }

    // Set config entries
    if(!defDirs[0].equals(newDirs[0]) ||
        !defDirs[1].equals(newDirs[1]) ||
        !defDirs[2].equals(newDirs[2]) ||
        newDirs[3]!=null && (defDirs[3]==null || !defDirs[3].equals(newDirs[3]))){
      configFile.setAttributes(
          new String [] {"GridPilot", "GridPilot", "GridPilot", "GridPilot"},
          new String [] {"key file", "certificate file", "grid proxy directory",
              "ca certificates"},
          new String [] {
              newDirs[0], newDirs[1], newDirs[2], newDirs[3]}
      );
      changes = true;
    }

    return choice;
  }
  

  
  
}
