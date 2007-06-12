package gridpilot.wizards.beginning;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
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
import javax.swing.JRadioButton;
import javax.swing.JTextField;

public class BeginningWizard{

  private ImageIcon icon = null;
  private ConfigFile configFile = null;
  private boolean changes = false;
  private JRadioButton [] jtbs = null;
  private static int TEXTFIELDWIDTH = 32;
  private static String HOMEGRID_URL = "https://homegrid.dyndns.org/";

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
      
      if(setGridHomeDir(firstRun)==2){
        return;
      }
      
      if(setGridJobDB(firstRun)==2){
        return;
      }
      
      if(setGridFileCatalog(firstRun)==2){
        return;
      }
      
      // TODO: Set grid file catalog - fallback to homegrid.dyndns.org
            
      //        --->  set ATLAS home site - present list - ask for mysql alias
      
      // TODO: ask on which computing systems should be enabled
      //       - where the user is allowed
      
      //        --->  set NG clusters

      //        --->  set gLite VO (virtual organization, runtime vos)

      //        --->  set gLite clusters to scan for VO software

      //        --->  set GPSS DB - present list, fallback to homegrid.dyndns.org
            
      //        --->  configure ssh pool
      
      // TODO: set pull DB - present list, fallback to homegrid.dyndns.org

      // TODO: give report on which directories were created, etc.
      
      endGreeting(firstRun);
      
      if(changes){
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
        confirmString, new Object[] {"Continue", "Cancel"}, icon, Color.WHITE, false);
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
        confirmString, new Object[] {"OK"}, icon, Color.WHITE, false);   
    return choice;
  }
  
  private void showError(String text) throws Exception{
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    String confirmString = "ERROR: could not set up GridPilot. "+text;
    confirmBox.getConfirm("Failed setting up GridPilot",
        confirmString, new Object[] {"OK"}, icon, Color.WHITE, false);
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
    String dbDir = configFile.getValue("My_DB_local", "Database");
    if(dbDir==null){
      dbDir = "~/GridPilot";
    }
    else{
      dbDir.replaceFirst("hsql://localhost/", "");
      dbDir.replaceFirst("/My_DB", "");
    }
    String cacheDir = configFile.getValue("GridPilot", "Pull cache directory");
    String workingDir = configFile.getValue("Fork", "Working directory");
    String runtimeDir = configFile.getValue("Fork", "Runtime directory");
    String transDir = configFile.getValue("Fork", "Transformation directory");
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
      row = new JPanel(new BorderLayout(8, 0));
      row.add(Util.createCheckPanel(JOptionPane.getRootFrame(),
          names[i], jtFields[i]), BorderLayout.WEST);
      subRow = new JPanel(new BorderLayout(8, 0));
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
          jPanel, new Object[] {"Continue", "Skip", "Cancel"}, icon, Color.WHITE, false);
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
      "in which case a default set of CAs will be trusted.\n\n" +
      "Specified, but non-existing directories will be created.\n\n";
    JPanel jPanel = new JPanel(new GridBagLayout());
    String certPath = configFile.getValue("GridPilot", "Key file");
    String keyPath = configFile.getValue("GridPilot", "Certificate file");
    String proxyDir = configFile.getValue("GridPilot", "Grid proxy directory");
    String caCertsDir = configFile.getValue("GridPilot", "CA certificates");
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
      row = new JPanel(new BorderLayout(8, 0));
      row.add(Util.createCheckPanel(JOptionPane.getRootFrame(),
          names[i], jtFields[i]), BorderLayout.WEST);
      subRow = new JPanel(new BorderLayout(8, 0));
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
          jPanel, new Object[] {"Continue", "Skip", "Cancel"}, icon, Color.WHITE, false);
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
          new String [] {"Key file", "Certificate file", "Grid proxy directory",
              "CA certificates"},
          new String [] {
              newDirs[0], newDirs[1], newDirs[2], newDirs[3]}
      );
      changes = true;
    }
  
    return choice;
  }
  
  /**
   * If a remote server is specified, assume that a database 'production'
   * exists on the specified server and is writeable;
   * enable Regional_DB with database 'replicas'; disable GP_DB.
   * If the default remote database is chosen, disable Regional_DB;
   * enable GP_DB with database 'production'; this will then be the default file catalog
   * (while perhaps also the default job database).
   */
  private int setGridFileCatalog(boolean firstRun) throws Exception{
    String confirmString =
      "The files you produce will be registered in the job database you chose in the\n" +
      "previous step. You may want to register them also in a 'real' file catalog,\n" +
      "which is readable by other clients than GridPilot.\n\n" +
      "Your local database is already such a file catalog and if you have write access\n" +
      "to a file catalog, you can use this too.\n\n" +
      "If you choose to use a remote database, you must specify the name of the server\n" +
      "hosting it. Please notice that the database must be a GridPilot-enabled MySQL database.\n\n" +
      "If you choose to use the default remote database, please notice that anything you\n" +
      "write there is world readable and that the service is provided by gridpilot.org with\n" +
      "absolutely no guarantee that data will not be deleted at any time.\n\n" +
      "You also have the option to enable the 'ATLAS' database plugin. If you don't work\n" +
      "in the ATLAS collaboration of CERN, this is probably of no relevance to you and you\n" +
      "can leave it disabled\n\n.";
    JPanel jPanel = new JPanel(new GridBagLayout());
    String remoteDB = configFile.getValue("Regional_DB", "Database");
    String host = remoteDB.replaceFirst(".*mysql://(.*)/.*","$1");
    String [] defDirs = new String [] {"",
                                       "db.gridpilot.dk",
                                       host};
    String [] names = new String [] {"Use local database",
                                     "Use default remote database host",
                                     "Use custom remote database host:"};
    JTextField [] jtFields = new JTextField [defDirs.length];
    jPanel.add(new JLabel("<html>"+confirmString.replaceAll("\n", "<br>")+"</html>"),
        new GridBagConstraints(0, (firstRun?1:0), 2, 2, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    JPanel row = null;
    JPanel subRow = null;
    jtbs = new JRadioButton[defDirs.length];
    RadioListener myListener = new RadioListener();
    for(int i=0; i<defDirs.length; ++i){
      jtFields[i] = new JTextField(TEXTFIELDWIDTH);
      jtFields[i].setText(defDirs[i]);
      jtbs[i] = new JRadioButton();
      jtbs[i].addActionListener(myListener);
      row = new JPanel(new BorderLayout(8, 0));
      row.add(jtbs[i], BorderLayout.WEST);
      if(i==2){
        row.add(new JLabel(names[i]), BorderLayout.CENTER);
      }
      else{
        row.add(new JLabel(names[i]), BorderLayout.CENTER);
        jtFields[i].setEditable(false);
      }
      subRow = new JPanel(new BorderLayout(8, 0));
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
    boolean goOn = false;
    int sel = -1;
    String [] newDirs = new String[defDirs.length];
    String title = "Setting up file catalog";
    while(!goOn){
      if(goOn){
        break;
      }
      try{
        choice = confirmBox.getConfirm(title, jPanel,
            new Object[] {"Continue", "Skip", "Cancel"}, icon, Color.WHITE, false);

        if(choice==0){
          // Get field values
          for(int i=0; i<newDirs.length; ++i){
            if(jtbs[i].isSelected()){
              sel = i;
              newDirs[i] = jtFields[i].getText();
              break;
            }
            else{
              continue;
            }
          }
          if(sel!=1 || newDirs[sel]!=null && !newDirs[sel].equals("")){
            goOn = true;
          }
          else{
            confirmBox.getConfirm(title, "Please fill in the host name of the remote file catalog server",
                new Object[] {"OK"}, icon, Color.WHITE, false);
          }
        }
        else{
          goOn = true;
        }
      }
      catch(Exception e){
        goOn = true;
        e.printStackTrace();
      }
    }
    
    if(choice!=0){
      return choice;
    }
  
    if(sel==0 && !firstRun){
      // If this is the first run, this should already be the set

      // Local DB, enable My_DB_Local, disable Regional_DB and GP_DB.
       configFile.setAttributes(
          new String [] {"My_DB_Local", "Regional_DB"},
          new String [] {"Enabled", "Enabled"},
          new String [] {"yes", "no"}
      );  
      changes = true;
    }
    else if(sel==1){
      // gridpilot.dk, enable GP_DB, My_DB_Local, disable Regional_DB.
      configFile.setAttributes(
          new String [] {"My_DB_Local", "Regional_DB", "GP_DB"},
          new String [] {"Enabled", "Enabled", "Enabled",},
          new String [] {"yes", "no", "yes"}
      );  
      changes = true;
    }
    else if(sel==2){
      // remote DB, enable Regional_DB, My_DB_Local, disable GP_DB.
      // Ask for Regional_DB:description
      String origDbDesc = configFile.getValue("Regional_DB", "Description");
      String dbDesc = Util.getName(
          "Please enter a short (~ 5 words) description of the remote file catalog", "");
      if(dbDesc==null || dbDesc.equals("")){
        dbDesc = origDbDesc;
      }
      configFile.setAttributes(
          new String [] {"My_DB_Local", "Regional_DB", "GP_DB",
              "My_DB_Remote", "My_DB_Remote"},
          new String [] {"Enabled", "Enabled", "Enabled",
              "Database", "Description"},
          new String [] {"yes", "yes", "no",
              "jdbc:mysql://"+newDirs[sel].trim()+":3306/production", dbDesc}
      );  
      changes = true;
    }
      
    return choice;
  }

  /**
   * If a remote server is specified, assume that a database with name corresponding
   * to the DN exists on the specified server and is writeable;
   * enable My_DB_Remote disable GP_DB.
   * If the default remote database is chosen, disable My_DB_Remote;
   * enable GP_DB with database 'production'; this will then be the job DB for remote CSs.
   */
  private int setGridJobDB(boolean firstRun) throws Exception{
    String confirmString =
      "GridPilot allows you to keep track of your grid life:\n" +
      "the jobs you have running or have run and the files you've produced.\n\n" +
      "You can keep this information in your local database or if you have write access\n" +
      "to a remote database, you can keep the information there.\n\n" +
      "If you choose to use a remote database, you must specify the name of the server\n" +
      "hosting it. Please notice that the database must be a GridPilot-enabled MySQL database.\n\n" +
      "If you choose to use the default remote database, please notice that anything you\n" +
      "write there is world readable and that the service is provided by gridpilot.org with\n" +
      "absolutely no guarantee that data will not be deleted at any time.\n\n";
    JPanel jPanel = new JPanel(new GridBagLayout());
    String remoteDB = configFile.getValue("My_DB_Remote", "Database");
    String host = remoteDB.replaceFirst(".*mysql://(.*)/.*","$1");
    String [] defDirs = new String [] {"",
                                       "db.gridpilot.dk",
                                       host};
    String [] names = new String [] {"Use local database",
                                     "Use default remote database host",
                                     "Use custom remote database host:"};
    JTextField [] jtFields = new JTextField [defDirs.length];
    jPanel.add(new JLabel("<html>"+confirmString.replaceAll("\n", "<br>")+"</html>"),
        new GridBagConstraints(0, (firstRun?1:0), 2, 2, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    JPanel row = null;
    JPanel subRow = null;
    jtbs = new JRadioButton[defDirs.length];
    RadioListener myListener = new RadioListener();
    for(int i=0; i<defDirs.length; ++i){
      jtFields[i] = new JTextField(TEXTFIELDWIDTH);
      jtFields[i].setText(defDirs[i]);
      jtbs[i] = new JRadioButton();
      jtbs[i].addActionListener(myListener);
      row = new JPanel(new BorderLayout(8, 0));
      row.add(jtbs[i], BorderLayout.WEST);
      if(i==2){
        row.add(new JLabel(names[i]), BorderLayout.CENTER);
      }
      else{
        row.add(new JLabel(names[i]), BorderLayout.CENTER);
        jtFields[i].setEditable(false);
      }
      subRow = new JPanel(new BorderLayout(8, 0));
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
    boolean goOn = false;
    int sel = -1;
    String [] newDirs = new String[defDirs.length];
    String title = "Setting up job database";
    while(!goOn){
      if(goOn){
        break;
      }
      try{
        choice = confirmBox.getConfirm(title, jPanel,
            new Object[] {"Continue", "Skip", "Cancel"}, icon, Color.WHITE, false);

        if(choice==0){
          // Get field values
          for(int i=0; i<newDirs.length; ++i){
            if(jtbs[i].isSelected()){
              sel = i;
              newDirs[i] = jtFields[i].getText();
              break;
            }
            else{
              continue;
            }
          }
          if(sel!=1 || newDirs[sel]!=null && !newDirs[sel].equals("")){
            goOn = true;
          }
          else{
            confirmBox.getConfirm(title, "Please fill in the host name of the remote database server",
                new Object[] {"OK"}, icon, Color.WHITE, false);
          }
        }
        else{
          goOn = true;
        }
      }
      catch(Exception e){
        goOn = true;
        e.printStackTrace();
      }
    }
    
    if(choice!=0){
      return choice;
    }
  
    if(sel==0 && !firstRun){
      // If this is the first run, this should already be the set

      // Local DB, enable My_DB_Local, disable My_DB_Remote and GP_DB.
      // Set GPSS:remote database = GP_DB
      // Set Fork:remote pull database = GP_DB
      // Set [Fork|GPSS|SSH|SSH_POOL|NG|GLite]:runtime databases = My_DB_Local
       configFile.setAttributes(
          new String [] {"My_DB_Local", "My_DB_Remote", "GP_DB",
              "GPSS", "Fork",
              "Fork", "GPSS", "SSH", "SSH_POOL", "NG", "GLite"},
          new String [] {"Enabled", "Enabled", "Enabled",
              "Remote database", "Remote pull database",
              "Runtime databases", "Runtime databases", "Runtime databases", "Runtime databases", "Runtime databases", "Runtime databases"},
          new String [] {"yes", "no", "no",
              "GP_DB", "GP_DB",
              "My_DB_Local", "My_DB_Local", "My_DB_Local", "My_DB_Local", "My_DB_Local", "My_DB_Local"}
      );  
      changes = true;
    }
    else if(sel==1){
      // gridpilot.dk, enable GP_DB, My_DB_Local, disable My_DB_Remote.
      // Set GPSS:remote database = GP_DB
      // Set GP_DB:database = production
      // Set Fork:remote pull database = GP_DB
      // Set [Fork|GPSS|SSH|SSH_POOL|NG|GLite]:runtime databases = GP_DB My_DB_Local
      configFile.setAttributes(
          new String [] {"My_DB_Local", "My_DB_Remote", "GP_DB",
              "GPSS", "Fork", "GP_DB",
              "Fork", "GPSS", "SSH", "SSH_POOL", "NG", "GLite"},
          new String [] {"Enabled", "Enabled", "Enabled",
              "Remote database", "Remote pull database", "Database",
              "Runtime databases", "Runtime databases", "Runtime databases", "Runtime databases", "Runtime databases", "Runtime databases"},
          new String [] {"yes", "no", "yes",
              "GP_DB", "GP_DB", "jdbc:mysql://db.gridpilot.dk:3306/production",
              "My_DB_Local", "GP_DB My_DB_Local", "My_DB_Local", "My_DB_Local", "GP_DB My_DB_Local", "GP_DB My_DB_Local"}
      );  
      changes = true;
    }
    else if(sel==2){
      // remote DB, enable My_DB_Remote, My_DB_Local, disable GP_DB.
      // Ask for My_DB_Remote:description
      // Set GPSS:remote database = My_DB_Remote
      // Set Fork:remote pull database = My_DB_Remote
      // Set [Fork|GPSS|SSH|SSH_POOL|NG|GLite]:runtime databases = My_DB_Remote My_DB_Local
      String origDbDesc = configFile.getValue("My_DB_Remote", "Description");
      String dbDesc = Util.getName(
          "Please enter a short (~ 5 words) description of the remote database", "");
      if(dbDesc==null || dbDesc.equals("")){
        dbDesc = origDbDesc;
      }
      configFile.setAttributes(
          new String [] {"My_DB_Local", "My_DB_Remote", "GP_DB",
              "GPSS", "Fork", "My_DB_Remote", "My_DB_Remote",
              "Fork", "GPSS", "SSH", "SSH_POOL", "NG", "GLite"},
          new String [] {"Enabled", "Enabled", "Enabled",
              "Remote database", "Remote pull database", "Database", "Description",
              "Runtime databases", "Runtime databases", "Runtime databases", "Runtime databases", "Runtime databases", "Runtime databases"},
          new String [] {"yes", "yes", "no",
              "My_DB_Remote", "My_DB_Remote", "jdbc:mysql://"+newDirs[sel].trim()+":3306/", dbDesc,
              "My_DB_Local", "My_DB_Remote My_DB_Local", "My_DB_Local", "My_DB_Local", "My_DB_Remote My_DB_Local", "My_DB_Remote My_DB_Local"}
      );  
      changes = true;
    }
      
    return choice;
  }

  private int setGridHomeDir(boolean firstRun) throws Exception{
    String confirmString =
      "When running jobs on a grid it is useful to have the jobs upload output files to \n" +
      "a directory on a server that's always on.\n\n" +
      "For this to be possible GridPilot needs to know a gridftp or https URL where you\n" +
      "have read/write permission with the X509 certificate you specified previously.\n\n" +
      "If you don't know any such URL or you don't understand the above, you may use\n" +
      "the default grid home URL given below. But please notice that this is but a temporary\n" +
      "solution and that the files on this location may be read, overwritten or deleted at\n" +
      "any time.\n\n"+
      "You may also choose a local directory, but in this case, output files will stay on the\n" +
      "resource where a job has run until GridPilot downloads them.\n\n"+
      "A specified, local, but non-existing directory will be created.\n\n";
    JPanel jPanel = new JPanel(new GridBagLayout());
    String homeUrl = configFile.getValue("GridPilot", "Grid home url");
    String [] defDirs = new String [] {homeUrl,
                                       HOMEGRID_URL+Util.getGridDatabaseUser()+"/",
                                       homeUrl};
    String [] names = new String [] {"Use your own grid or local home URL",
                                     "Use default grid home URL",
                                     "Use default local home URL"};
    JTextField [] jtFields = new JTextField [defDirs.length];
    jPanel.add(new JLabel("<html>"+confirmString.replaceAll("\n", "<br>")+"</html>"),
        new GridBagConstraints(0, (firstRun?1:0), 2, 2, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    JPanel row = null;
    JPanel subRow = null;
    jtbs = new JRadioButton[defDirs.length];
    RadioListener myListener = new RadioListener();
    for(int i=0; i<defDirs.length; ++i){
      jtFields[i] = new JTextField(TEXTFIELDWIDTH);
      jtFields[i].setText(defDirs[i]);
      jtbs[i] = new JRadioButton();
      jtbs[i].addActionListener(myListener);
      row = new JPanel(new BorderLayout(8, 0));
      row.add(jtbs[i], BorderLayout.WEST);
      if(i==0){
        row.add(Util.createCheckPanel(JOptionPane.getRootFrame(),
            names[i], jtFields[i]), BorderLayout.CENTER);
      }
      else{
        row.add(new JLabel(names[i]), BorderLayout.CENTER);
        jtFields[i].setEditable(false);
      }
      subRow = new JPanel(new BorderLayout(8, 0));
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
      choice = confirmBox.getConfirm("Setting up home directory",
          jPanel, new Object[] {"Continue", "Skip", "Cancel"}, icon, Color.WHITE, false);
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
    int sel = -1;
    for(int i=0; i<newDirs.length; ++i){
      if(jtbs[i].isSelected()){
        sel = i;
      }
      else{
        continue;
      }
      newDirs[i] = jtFields[i].getText();
      if(newDirs[i]!=null && !newDirs[i].equals("") && !Util.urlIsRemote(newDirs[i])){
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
      
    // Set config entries
    if(sel>-1 && newDirs[sel]!=null &&
        (defDirs[0]==null || !defDirs[0].equals(newDirs[sel]))){
      Debug.debug("Setting "+sel+":"+newDirs[sel], 2);
      configFile.setAttributes(
          new String [] {"GridPilot"},
          new String [] {"Grid home url"},
          new String [] {newDirs[sel]}
      );
      changes = true;
    }
  
    return choice;
  }
  

  /** Listens to the radio buttons. */
  class RadioListener implements ActionListener { 
      public void actionPerformed(ActionEvent e) {
        for(int i=0; i<jtbs.length; ++i){
          if(e.getSource().equals(jtbs[i])){
            continue;
          }
          jtbs[i].setSelected(false);
        }
      }
  }
  
}
