package gridpilot.wizards.beginning;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Toolkit;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.Security;
import java.util.Collections;
import java.util.Vector;

import gridfactory.common.ConfigFile;
import gridfactory.common.ConfirmBox;
import gridfactory.common.Debug;
import gridfactory.common.FileTransfer;
import gridfactory.common.LocalStaticShell;

import gridpilot.GridPilot;
import gridpilot.JExtendedComboBox;
import gridpilot.MySSL;
import gridpilot.MyUtil;
import gridpilot.dbplugins.atlas.SecureWebServiceConnection;
import gridpilot.dbplugins.atlas.TiersOfAtlas;

import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComponent;
import javax.swing.JEditorPane;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTextField;

import org.globus.util.GlobusURL;

public class BeginningWizard{

  private ImageIcon icon = null;
  private ConfigFile configFile = null;
  private boolean changes = false;
  private JRadioButton [] jrbs = null;
  private JCheckBox [] jcbs = null;
  private boolean dirsOk = true;
  private boolean certAndKeyOk = true;
  private Dimension catalogPanelSize = null;
  private Dimension gridsPanelSize = null;
  private JComponent cvVomses;
	private String TOA_URL = "http://atlas.web.cern.ch/Atlas/GROUPS/DATABASE/project/ddm/releases/TiersOfATLASCache.py";

  private static int TEXTFIELDWIDTH = 32;
  private static String HOME_URL = "https://www.gridfactory.org/";
  private static String DOC_ROOT_URL = "http://www.gridfactory.org/";
  private static String MYSQL_HOWTO_URL = DOC_ROOT_URL+"documentation/#Using_MySQL_as_a_secure_file_catalog";
  private static String HTTPS_HOWTO_URL = DOC_ROOT_URL+"documentation/#Using_Apache_httpd_as_a_secure_file_server";
  private static String GF_SUBMIT_DIR = "gridfactory/jobs/";
  private static String[] GF_SUBMIT_URLS =
    new String [] {"https://www.gridfactory.org/"+GF_SUBMIT_DIR,
                   "https://gridfactory.nbi.dk/"+GF_SUBMIT_DIR,
                   "https://gridfactory.dyndns.org/"+GF_SUBMIT_DIR};
  private JExtendedComboBox gfSubmitUrlsBox = new JExtendedComboBox();
  
  public BeginningWizard(boolean firstRun){
    
    Debug.DEBUG_LEVEL = 3;
    
    dirsOk = true;
    URL imgURL = null;
    changes = false;
    
    // Make things look nice
    try{
      imgURL = GridPilot.class.getResource(GridPilot.RESOURCES_PATH + "aviateur-32x32.png");
      icon = new ImageIcon(imgURL);
    }
    catch(Exception e){
      try{
        imgURL = GridPilot.class.getResource("/resources/aviateur-32x32.png");
        icon = new ImageIcon(imgURL);
      }
      catch(Exception ee){
        ee.printStackTrace();
        Debug.debug("Could not find image "+ GridPilot.RESOURCES_PATH + "aviateur-32x32.png", 3);
        icon = null;
      }
    }
    
    int ret = -1;
    
    try{
      ret = welcome(firstRun);
      if(ret!=0){
        if(firstRun){
          GridPilot.USER_CONF_FILE.delete();
          System.out.println("Deleting new configuration file.");
          System.exit(-1);
        }
        else{
          return;
        }
      }
      
      ret = checkDirs(firstRun);
      if(ret==2 || ret==-1){
        if(firstRun){
          GridPilot.USER_CONF_FILE.delete();
          System.out.println("Deleting new configuration file.");
          System.exit(-1);
        }
        else{
          return;
        }
      }
      else if(ret==1){
        if(!dirsOk){
          MyUtil.showError("Without these directories setup cannot continue. Exiting wizard.");
          if(firstRun){
            GridPilot.USER_CONF_FILE.delete();
            System.out.println("Deleting new configuration file.");
            System.exit(-1);
          }
          else{
            return;
          }
        }
      }
      
      try{
        ret = checkCertificate(firstRun);
        if(ret==2 || ret==-1){
          ret = partialSetupMessage(firstRun);
          if(firstRun && ret==1){
            System.exit(-1);
          }
          else{
            return;
          }
        }
        else if(ret==1){
          // No grid credentials
          throw new IOException("Failed initializing SSL.");
        }
      }
      catch(Exception ee){
       ee.printStackTrace();
       certAndKeyOk = false;
       MyUtil.showError(
           "WARNING: without grid credentials you will not be able to authenticate with remote resources.");
      }
      
      try{
        ret = setGridHomeDir(firstRun);
        if(ret==2 || ret==-1){
          ret = partialSetupMessage(firstRun);
          if(firstRun && ret==1){
            System.exit(-1);
          }
          else{
            return;
          }
        }
      }
      catch(IOException e){
        e.printStackTrace();
      }
      
      if(certAndKeyOk){
        try{
          ret = setGridJobDB(firstRun);
          if(ret==2 || ret==-1){
            ret = partialSetupMessage(firstRun);
            if(firstRun && ret==1){
              System.exit(-1);
            }
            else{
              return;
            }
          }
        }
        catch(IOException e){
          e.printStackTrace();
        }
        
        try{
          ret = setGridFileCatalog(firstRun);
          if(ret==2 || ret==-1){
            ret = partialSetupMessage(firstRun);
            if(firstRun && ret==1){
              System.exit(-1);
            }
            else{
              return;
            }
          }
        }
        catch(IOException e){
          e.printStackTrace();
        }
      }
      
      try{
        ret = configureComputingSystems(firstRun);
        if(ret==2 || ret==-1){
          ret = partialSetupMessage(firstRun);
          if(firstRun && ret==1){
            System.exit(-1);
          }
          else{
            return;
          }
        }
      }
      catch(IOException e){
        e.printStackTrace();
      }
      
      endGreeting(firstRun);
      
      if(!firstRun && changes){
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
        MyUtil.showError(e.getMessage());
      }
      catch(Exception e1){
        e1.printStackTrace();
      }
    }    
  }
  
  private void setupCertAndKey(boolean firstRun) throws IOException, GeneralSecurityException{
    String certPath = configFile.getValue(GridPilot.TOP_CONFIG_SECTION, "Certificate file");
    String keyPath = configFile.getValue(GridPilot.TOP_CONFIG_SECTION, "Key file");
    String certDir = (new File(certPath)).getParent();
    boolean useTestCreds = false;
    if(LocalStaticShell.existsFile(certPath) && LocalStaticShell.existsFile(keyPath)){
      if(firstRun){
        String dn = MyUtil.getDN(certPath);
        if(!MySSL.TEST_CERTIFICATE_DN.equals(dn)){
          useTestCreds = askToUseTestCreds(dn);
          if(useTestCreds){
            moveCertAndKey(certPath, keyPath, dn);
          }
          else{
            return;
          }
        }
        else{
          GridPilot.KEY_PASSWORD = MySSL.TEST_KEY_PASSWORD;
          // Disable any VOMS server that may have been set in the preferences
          GridPilot.VOMS_SERVER_URL = null;
          return;
        }
      }
    }
    else if(LocalStaticShell.existsFile(certPath) || LocalStaticShell.existsFile(keyPath)){
      throw new IOException("You must have BOTH a certificate and a key file or none of the two.\n\n" +
      		"Please fix this and rerun this wizard.");
    }
    // Set up key and cert.
    if(MySSL.setupTestCredentials(certDir, useTestCreds, GridPilot.class) && useTestCreds){
      GridPilot.KEY_PASSWORD = MySSL.TEST_KEY_PASSWORD;
      // Disable any VOMS server that may have been set in the preferences
      GridPilot.VOMS_SERVER_URL = null;
    }
  }
  
  private void moveCertAndKey(String certPath, String keyPath, String dn) {
    String bkExtension = "."+Long.toString(MyUtil.getDateInMilliSeconds());
    LocalStaticShell.moveFile(certPath, certPath+bkExtension);
    LocalStaticShell.moveFile(keyPath, keyPath+bkExtension);
    MyUtil.showMessage("Credentials moved", "I've moved "+certPath+" to "+certPath+bkExtension+
        " and "+keyPath+" to "+keyPath+bkExtension+".\n\n" +
        "Once you've joined a VO as "+dn+" you should move them back.");
  }

  private static boolean askToUseTestCreds(String dn) {
    ConfirmBox confirmBox = new ConfirmBox();
    String msg = "You have credentials installed with DN\n\n"+
         dn+"\n\n" +
    		"Please confirm that these credentials permit you to use at least some computing resource(s) and file server(s).\n\n" +
    		"Such permission is usually granted by way of virtual organization (VO) membership.\n\n" +
    		"For more on this, see http://www.gridfactory.org/services/.\n\n" +
    		"If you haven't joined any VO(s) you can let me move your credential files temporarily out of the way.\n" +
    		"Then you will use the default test credentials, allowing you to try out some GridFactory test resources.\n\n" +
    		"What do you want to do?\n\n";
    int choice = -1;
    try{
      choice = confirmBox.getConfirm("Use test credentials?",
          msg, new Object[] {
          MyUtil.mkButton("cancel.png", "I'll use your test credentials", "Move installed credentials to [file].bk"),
          MyUtil.mkButton("ok.png", "My credentials are fine, just use them", "Leave my credentials untouched")}, 1);
    }
    catch(Exception e){
      e.printStackTrace();
    }
        
    if(choice==JOptionPane.YES_OPTION){
      return true;
    }
    
    return false;
  }

  private int welcome(boolean firstRun) throws Exception{
    ConfirmBox confirmBox = new ConfirmBox();
    String confirmString = "Welcome!\n\n" +
            (firstRun?"This appears to be the first time you run GridPilot.\n":"") +
            "On the next windows you will be guided through 6 steps to set up GridPilot.\n" +
            "Your configuration will be stored to a file in your home directory.\n\n" +
            "Click \"Continue\" to move on or \"Cancel\" to exit this wizard.\n\n" +
            "Notice that you can always run this wizard again by choosing it from " +
            "the \"Help\" menu.\n\n";
    JLabel confirmLabel = new JLabel();
    confirmLabel.setPreferredSize(new Dimension(520, 400));
    confirmLabel.setOpaque(true);
    confirmLabel.setText(confirmString);
    int choice = -1;
    choice = confirmBox.getConfirmPlainText("Configure GridPilot",
        confirmString, new Object[] {"Continue", "Cancel"}, icon, Color.WHITE, true, false);
    return choice;
  }
  
  private int endGreeting(boolean firstRun) throws Exception{
    ConfirmBox confirmBox = new ConfirmBox();
    String confirmString = "Configuring GridPilot is now done.\n" +
        "Your settings have been saved in\n"+
        configFile.getFile().getAbsolutePath()+
        ".\n\n"+
        "Notice that only the most basic parameters,\n" +
            "necessary to get you up and running, have been set.\n" +
            "You can modify these and many others in\n" +
            "\"Edit\" - \"Preferences\"." +
            (firstRun?"\n\nGridPilot will now initialize - this may take a while.\n\n" +
            		"Have fun!":"");
    int choice = -1;
    confirmBox.getConfirmPlainText("Setup completed!",
        confirmString, new Object[] {MyUtil.mkOkObject(confirmBox.getOptionPane())},
        icon, Color.WHITE, true, false);   
    return choice;
  }
  
  private int partialSetupMessage(boolean firstRun) throws Exception{
    ConfirmBox confirmBox = new ConfirmBox();
    String confirmString =
        "Configuring GridPilot is only partially done.\n" +
        "Your settings have been saved in\n"+
        configFile.getFile().getAbsolutePath()+
        ".\n\n"+
        "Notice that you can set the remaining configuration\n" +
        "parameters in \"Edit\" - \"Preferences\"." +
        (firstRun?"\n\n" +
        "Click \"OK\" to try to start GridPilot anyway or \"Cancel\"\n" +
         "to exit.\n\n" +
         "Thanks for using GridPilot!\n\n":"");
    int choice = -1;
    choice = confirmBox.getConfirmPlainText("Setup completed!", confirmString,
        firstRun?(new Object[] {MyUtil.mkOkObject(confirmBox.getOptionPane()), MyUtil.mkCancelObject(confirmBox.getOptionPane())}):
          (new Object[] {MyUtil.mkOkObject(confirmBox.getOptionPane())}),
        icon, Color.WHITE, true, false);   
    return choice;
  }  
  
  /**
   * Create the config file and some directories and set
   * config values.
   * @throws Throwable 
   */
  private int checkDirs(boolean firstRun) throws Throwable{
    
    if(firstRun){
      System.out.println("Creating new configuration file.");
      // Make temporary config file
      ConfigFile tmpConfigFile = new ConfigFile(GridPilot.DEFAULT_FILE_NAME_WINDOWS,
          GridPilot.TOP_CONFIG_SECTION, GridPilot.CONFIG_SECTIONS, GridPilot.class);
      tmpConfigFile.excludeItems = GridPilot.MY_EXCLUDE_ITEMS;
      // Copy over temporary to real config file
      LocalStaticShell.copyFile(tmpConfigFile.getFile().getAbsolutePath(),
           GridPilot.USER_CONF_FILE.getAbsolutePath());
      GridPilot.setConfigFile();
      // Read in global variables
      GridPilot.loadConfigValues();
      // Clean up
      tmpConfigFile.getFile().delete();
      // Construct ConfigFile object from the new file
      try{
        configFile = new ConfigFile(GridPilot.USER_CONF_FILE, GridPilot.TOP_CONFIG_SECTION,
            GridPilot.CONFIG_SECTIONS);
        configFile.excludeItems = GridPilot.MY_EXCLUDE_ITEMS;
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
        "Working directory",
        "Software directory",
        "Executables directory"
        };
    String dbDir;
    String db = configFile.getValue("My_DB_local", "Database");
    if(db==null){
      dbDir = "~/GridPilot";
    }
    else{
      dbDir = db.replaceFirst("hsql://localhost/", "");
      dbDir = dbDir.replaceFirst("/My_DB", "");

    }
    String workingDir = configFile.getValue("Fork", "Working directory");
    String runtimeDir = GridPilot.RUNTIME_DIR;
    String transDir = configFile.getValue("Fork", "Executable directory");
    String [] defDirs = new String [] {
        dbDir==null?dbDir:"~/GridPilot",
        workingDir==null?dbDir+"/jobs":workingDir,
        runtimeDir==null?dbDir+"/runtimeEnvironments":runtimeDir,
        transDir==null?dbDir+"/executables":transDir
        };
    JTextField [] jtFields = new JTextField [defDirs.length];
    if(firstRun){
      jPanel.add(new JLabel("A configuration file "+GridPilot.USER_CONF_FILE.getAbsolutePath()+
          " has been created."),
          new GridBagConstraints(0, 0, 2, 1, 0.0, 0.0,
              GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
              new Insets(5, 5, 5, 5), 0, 0)) ;
    }
    String msg = "GridPilot needs a few directories to store information. Below you see the default paths.\n" +
    "If you click 'skip' and the default directories don't exist, they will not be created and GridPilot\n" +
    "will not function properly. You should therefore usually click 'continue'.";
    jPanel.add(new JLabel("<html>"+msg.replaceAll("\n", "<br>")+"</html>" ),
        new GridBagConstraints(0, (firstRun?1:0), 2, 2, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    JPanel row = null;
    JPanel subRow = null;
    ConfirmBox confirmBox = new ConfirmBox();
    for(int i=0; i<defDirs.length; ++i){
      jtFields[i] = new JTextField(TEXTFIELDWIDTH);
      jtFields[i].setText(defDirs[i]);
      row = new JPanel(new BorderLayout(8, 0));
      row.add(MyUtil.createCheckPanel1((Window)confirmBox.getOwner(),
          names[i], jtFields[i], true, false, true, true), BorderLayout.WEST);
      subRow = new JPanel(new BorderLayout(8, 0));
      subRow.add(jtFields[i], BorderLayout.CENTER);
      subRow.add(new JLabel("   "), BorderLayout.EAST);
      subRow.add(new JLabel("   "), BorderLayout.SOUTH);
      subRow.add(new JLabel("   "), BorderLayout.NORTH);
      row.add(subRow, BorderLayout.EAST);
      jPanel.add(row, new GridBagConstraints(0, i+(firstRun?5:4), 1, 1, 0.0, 0.0,
          GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
          new Insets(0, 0, 0, 0), 0, 0));
    }
    jPanel.validate();
    
    int choice = -1;
    try{
      choice = confirmBox.getConfirmPlainText("Step 1/6: Setting up GridPilot directories",
          jPanel, new Object[] {"Continue", "Skip", "Cancel"}, icon, Color.WHITE, true, false);
    }
    catch(Exception e){
      e.printStackTrace();
    }
    
    String [] newDirs = new String [defDirs.length];
    File [] newDirFiles = new File[newDirs.length];

    boolean doCreate = true;
    if(choice==2){
      return choice;
    }
    else if(choice==1){
      doCreate = false;
    }
    
    // Create missing directories
    for(int i=0; i<defDirs.length; ++i){
      newDirs[i] = jtFields[i].getText();
      Debug.debug("Checking directory "+newDirs[i], 2);
      newDirFiles[i] = new File(MyUtil.clearTildeLocally(MyUtil.clearFile(newDirs[i])));
      if(newDirs[i]!=null && !newDirs[i].equals("")){
        if(newDirFiles[i].exists()){
          if(!newDirFiles[i].isDirectory()){
            dirsOk = false;
            throw new IOException(newDirFiles[i].getAbsolutePath()+" already exists and is not a directory.");
          }
        }
        else{
          if(doCreate){
            Debug.debug("Creating directory "+newDirs[i], 2);
            dirsOk = dirsOk && newDirFiles[i].mkdir();
          }
          else{
            dirsOk = false;
          }
        }
      }
      newDirs[i] = MyUtil.replaceWithTildeLocally(MyUtil.clearFile(newDirs[i]));
    }
    
    // Now copy over the software catalog - NOT needed
    /*URL fileURL = null;
    BufferedReader in = null;
    try{
      fileURL = GridPilot.class.getResource(GridPilot.RESOURCES_PATH+"rtes.xml");
      in = new BufferedReader(new InputStreamReader(fileURL.openStream()));
    }
    catch(Exception e){
      fileURL = GridPilot.class.getResource("/resources/rtes.xml");
      in = new BufferedReader(new InputStreamReader(fileURL.openStream()));
    }
    BufferedWriter out =  new BufferedWriter(new FileWriter(
        new File(MyUtil.clearTildeLocally(MyUtil.clearFile(newDirs[0])), "rtes.xml")));
    int c;
    while((c=in.read())!=-1){
      if(c!='\r'){
        out.write(c); 
      }
    }
    in.close();
    out.close();*/
    
    // Set config entries
    boolean diff = false;
    for(int i=0; i<defDirs.length; ++i){
      if(!defDirs[i].equals(newDirs[i])){
        diff = true;
        break;
      }
    }
    if(doCreate && diff){
      configFile.setAttributes(
          new String [] {"My_DB_Local", GridPilot.TOP_CONFIG_SECTION, "Fork",
          		"GridFactory", "NG", "GLite",
          		"Fork"},
          new String [] {"database", "runtime directory", "working directory",
          		"working directory", "working directory", "working directory",
          		"executable directory"},
          new String [] {
              "hsql://localhost"+
                 (MyUtil.clearFile(newDirs[0]).startsWith("/")?"":"/")+
                 MyUtil.clearFile(newDirs[0])+
                 (MyUtil.clearFile(newDirs[0]).endsWith("/")?"":"/")+
                 "My_DB",
              newDirs[2],
              newDirs[1],
              newDirs[1],
              newDirs[1],
              newDirs[1],
              newDirs[3]}
      );
      changes = true;
    }
    
    return choice;
  }
  
  private int checkCertificate(boolean firstRun) throws Exception{
    String confirmString =
      "To access grid resources you need a valid grid certificate and a corresponding key.\n\n" +
      "If you don't have one, please get one from a grid certificate authority (and run this wizard again).\n" +
      "GridPilot can still be started, but you can only run jobs and access files on your local machine or\n" +
      "machines to which you have ssh access.\n\n" +
      "The fields below are pre-filled with the standard locations of grid credentials on a Linux system. If your\n" +
      "credentials are stored in a non-standard location, please set the corresponding paths as well the path of the\n" +
      "directory where you want to store temporary credentials (proxies).\n\n" +
      "Optionally, you can also specify a directory with the certificates of the certificate authories (CAs)\n" +
      "that you trust. This can safely be left unspecified, in which case a default set of CAs will be trusted.\n\n" +
      "Specified, but non-existing directories will be created.\n\n";
    JPanel jPanel = new JPanel(new GridBagLayout());
    String certPath = configFile.getValue(GridPilot.TOP_CONFIG_SECTION, "Certificate file");
    String keyPath = configFile.getValue(GridPilot.TOP_CONFIG_SECTION, "Key file");
    String proxyDir = configFile.getValue(GridPilot.TOP_CONFIG_SECTION, "Proxy directory");
    String caCertsDir = configFile.getValue(GridPilot.TOP_CONFIG_SECTION, "CA certificates");
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
    ConfirmBox confirmBox = new ConfirmBox();
    for(int i=0; i<defDirs.length; ++i){
      jtFields[i] = new JTextField(TEXTFIELDWIDTH);
      jtFields[i].setText(defDirs[i]);
      row = new JPanel(new BorderLayout(8, 0));
      row.add(MyUtil.createCheckPanel1((Window)confirmBox.getOwner(),
          names[i], jtFields[i], true, false, false, true), BorderLayout.WEST);
      subRow = new JPanel(new BorderLayout(8, 0));
      subRow.add(jtFields[i], BorderLayout.CENTER);
      subRow.add(new JLabel("   "), BorderLayout.EAST);
      subRow.add(new JLabel("   "), BorderLayout.SOUTH);
      subRow.add(new JLabel("   "), BorderLayout.NORTH);
      row.add(subRow, BorderLayout.EAST);
      jPanel.add(row, new GridBagConstraints(0, i+(firstRun?5:4), 1, 1, 0.0, 0.0,
          GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
          new Insets(0, 0, 0, 0), 0, 0));
    }
    jPanel.validate();
    
    int choice = -1;
    try{
      choice = confirmBox.getConfirmPlainText("Step 2/6: Setting up grid credentials",
          jPanel, new Object[] {"Continue", "Skip", "Cancel"}, icon, Color.WHITE, true, false);
    }
    catch(Exception e){
      e.printStackTrace();
      choice = 1;
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
        newDirFiles[i] = new File(MyUtil.clearTildeLocally(MyUtil.clearFile(newDirs[i])));
        if(newDirFiles[i].exists()){
          if(!newDirFiles[i].isDirectory()){
            throw new IOException("The directory "+newDirFiles[i].getAbsolutePath()+" cannot be created.");
          }
        }
        else{
          Debug.debug("Creating directory "+newDirs[i], 2);
          newDirFiles[i].mkdir();
        }
      }
      newDirs[i] = MyUtil.replaceWithTildeLocally(MyUtil.clearFile(newDirs[i]));
    }
    
    try{
      // setupTestCredentials throws an exception if the user clicks cancel.
      // We treat as if skipping this step.
      setupCertAndKey(firstRun);
    }
    catch(Exception e){
      e.printStackTrace();
      return 1;
    }
    
    // No idea why we suddenly have to add this. It worked before - now an exception is thrown
    // java.security.NoSuchProviderException: No such provider: BC
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    if(GridPilot.KEY_PASSWORD==MySSL.TEST_KEY_PASSWORD){
      // With test credentials, most likely only standard https will be used.
      GridPilot.getClassMgr().getSSL().activateSSL();
      // Disable any VOMS server that may have been set in the preferences
      GridPilot.VOMS_SERVER_URL = null;
    }
    else{
      // If a the user has a certificate, chances are he will use grid stuff.
      // Better make him a fresh proxy.
      try{
        MySSL.getProxyFile().delete();
      }
      catch(Exception e){
      }
      GridPilot.getClassMgr().getSSL().activateProxySSL();
    }
    
    // Set config entries
    if(!defDirs[0].equals(newDirs[0]) ||
        !defDirs[1].equals(newDirs[1]) ||
        !defDirs[2].equals(newDirs[2]) ||
        newDirs[3]!=null && (defDirs[3]==null || !defDirs[3].equals(newDirs[3]))){
      configFile.setAttributes(
          new String [] {GridPilot.TOP_CONFIG_SECTION, GridPilot.TOP_CONFIG_SECTION, "Fork"},
          new String [] {"Proxy directory", "CA certificates", "Public certificate"},
          new String [] {newDirs[2], newDirs[3], newDirs[0]}
      );
      changes = true;
    }
    
    // Check if certificate and key exist
    File certFile = new File(MyUtil.clearTildeLocally(MyUtil.clearFile(GridPilot.CERT_FILE)));
    if(!certFile.exists()){
      return 1;
    }
    File keyFile = new File(MyUtil.clearTildeLocally(MyUtil.clearFile(GridPilot.KEY_FILE)));
    if(!keyFile.exists()){
      return 1;
    }
  
    return choice;
  }
  
  /**
   * If a remote server is specified, assume that a database 'C=DK|ST=Aarhus|L=Aarhus|O=CABO|CN=test_user'
   * exists on the specified server and is writeable;
   * enable Regional_DB with database 'replicas'; disable GP_DB.
   * If the default remote database is chosen, disable Regional_DB;
   * enable GP_DB with database 'gridpilot'; this will then be the default file catalog
   * (while perhaps also the default job database).
   */
  private int setGridFileCatalog(boolean firstRun) throws Exception{
    String confirmString =
      "The files you produce will be registered in the job database you chose in the previous step. You\n" +
      "may want to register them also in a 'real' file catalog, which is readable by other clients than\n" +
      "GridPilot.\n\n" +
      "Your local database is already such a file catalog. The same goes for the default remote job\n" +
      "database. If you have write access to a remote file catalog, you can use this instead.\n\n" +
      "If you choose to use a remote file catalog, you must specify the name of the server hosting it.\n" +
      "Please notice that the database must be a " +
      "<a href=\""+MYSQL_HOWTO_URL+"\">GridPilot-enabled MySQL database</a>.\n\n" +
      "If you choose to use the default remote database, please notice that anything you write there is\n" +
      "world readable and that the service is provided by gridpilot.dk with absolutely no guarantee that\n" +
      "data will not be deleted at any time.\n\n" +
      "You also have the option to enable the 'ATLAS' database plugin. If you don't work in the ATLAS\n" +
      "collaboration of CERN, this is probably of no relevance to you and you can leave it disabled.\n\n";
    final JPanel jPanel = new JPanel(new GridBagLayout());
    JEditorPane pane = new JEditorPane("text/html", "<html>"+confirmString.replaceAll("\n", "<br>")+"</html>");
    pane.setEditable(false);
    pane.setOpaque(false);
    MyUtil.addHyperLinkListener(pane);
    String remoteDB = configFile.getValue("Regional_DB", "Database");
    String host = remoteDB.replaceFirst(".*mysql://(.*)/.*","$1");
    // TODO: now we assume that mysql always runs on port 3306 - generalize.
    host = host.replaceFirst("(.*):\\d+", "$1");
    String lfcUser = GridPilot.getClassMgr().getSSL().getGridSubject().replaceFirst(".*CN=(\\w+)\\s+(\\w+)\\W.*", "$1$2");
    lfcUser = GridPilot.getClassMgr().getSSL().getGridSubject().replaceFirst(".*CN=([\\w ]+).*", "$1").replaceAll(" +", "");
    String lfcPath = "/users/"+lfcUser;
    JTextField tfLfcPath = new JTextField(TEXTFIELDWIDTH);
    String toaPath = "~/GridPilot/TiersOfATLASCache.txt";
    JComponent tfHomeSite = createAtlasSitesField();
    JTextField tfTOAPath = new JTextField(TEXTFIELDWIDTH);
    String [] defDirs = new String [] {"",
                                       "www.gridpilot.dk",
                                       host};
    String [] names = new String [] {"Use job database",
                                     "Use default remote database host",
                                     "Use custom remote database host:"};
    JTextField [] jtFields = new JTextField [defDirs.length];
    jPanel.add(pane,
        new GridBagConstraints(0, (firstRun?1:0), 2, 2, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    JPanel row = null;
    JPanel subRow = null;
    jrbs = new JRadioButton[defDirs.length];
    RadioListener myListener = new RadioListener();
    int i = 0;
    for(i=0; i<defDirs.length; ++i){
      jtFields[i] = new JTextField(TEXTFIELDWIDTH);
      jtFields[i].setText(defDirs[i]);
      jrbs[i] = new JRadioButton();
      jrbs[i].addActionListener(myListener);
      row = new JPanel(new BorderLayout(8, 0));
      row.add(jrbs[i], BorderLayout.WEST);
      if(i==2){
        row.add(new JLabel(names[i]), BorderLayout.CENTER);
      }
      else{
        row.add(new JLabel(names[i]), BorderLayout.CENTER);
        jtFields[i].setEditable(false);
      }
      subRow = new JPanel(new BorderLayout(8, 0));
      subRow.add(jtFields[i], BorderLayout.CENTER);
      subRow.add(new JLabel("   "), BorderLayout.EAST);
      subRow.add(new JLabel("   "), BorderLayout.SOUTH);
      subRow.add(new JLabel("   "), BorderLayout.NORTH);
      row.add(subRow, BorderLayout.EAST);
      jPanel.add(row, new GridBagConstraints(0, i+4, 1, 1, 0.0, 0.0,
          GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
          new Insets(0, 0, 0, 0), 0, 0));
    }
    jrbs[0].setSelected(true);
    
    final ConfirmBox confirmBox = new ConfirmBox();

    // Only add ATLAS options if we're using VOMS proxies
    // (VOMS_SERVER_URL is set to null if we're using test credentials).
    JCheckBox cbAtlas = new JCheckBox();
    cbAtlas.setSelected(false);
    if(GridPilot.VOMS_SERVER_URL!=null && !GridPilot.VOMS_SERVER_URL.equals("")){
      JPanel atlasRow = new JPanel(new BorderLayout(8, 0));
      atlasRow.add(cbAtlas, BorderLayout.WEST);
      atlasRow.add(new JLabel("Enable ATLAS dataset/file catalogs"), BorderLayout.CENTER);
      jPanel.add(new JLabel(" "), new GridBagConstraints(0, i+5, 1, 1, 0.0, 0.0,
          GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
          new Insets(0, 0, 0, 0), 0, 0));
      jPanel.add(atlasRow, new GridBagConstraints(0, i+6, 1, 1, 0.0, 0.0,
          GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
          new Insets(0, 0, 0, 0), 0, 0));
      final JPanel atlasDetails = createAtlasPanel(jPanel, tfHomeSite, tfLfcPath, lfcPath, tfTOAPath, toaPath, cbAtlas, confirmBox);
      jPanel.add(atlasDetails, new GridBagConstraints(0, i+7, 1, 1, 0.0, 0.0,
          GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
          new Insets(0, 0, 0, 0), 0, 0));
      atlasDetails.setVisible(false);
    }
            
    jPanel.validate();
    
    int choice = -1;
    boolean goOn = false;
    int sel = -1;
    String [] newDirs = new String[defDirs.length];
    String title = "Step 5/6: Setting up file catalog";
    while(!goOn){
      if(goOn){
        break;
      }
      try{
        choice = confirmBox.getConfirmPlainText(title, jPanel,
            new Object[] {"Continue", "Skip", "Cancel"}, icon, Color.WHITE, true, true);
  
        if(choice==0){
          // Get field values
          for(i=0; i<newDirs.length; ++i){
            if(jrbs[i].isSelected()){
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
            confirmBox.getConfirmPlainText(title, "Please fill in the host name of the remote file catalog server",
                new Object[] {MyUtil.mkOkObject(confirmBox.getOptionPane())}, icon, Color.WHITE, true, true);
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
      // Disable Regional_DB.
       configFile.setAttributes(
          new String [] {"Regional_DB"},
          new String [] {"Enabled"},
          new String [] {"no"}
      );  
      changes = true;
    }
    else if(sel==1){
      // gridpilot.dk, enable Regional_DB.
      configFile.setAttributes(
          new String [] {"Regional_DB"},
          new String [] {"Enabled"},
          new String [] {"yes"}
      );  
      changes = true;
    }
    else if(sel==2){
      // Alternative DB, enable Regional_DB.
      // Ask for Regional_DB:description
      String origDbDesc = configFile.getValue("Regional_DB", "Description");
      String dbDesc = MyUtil.getName(
          "Please enter a short (~ 5 words) description of the remote file catalog", "");
      if(dbDesc==null || dbDesc.equals("")){
        dbDesc = origDbDesc;
      }
      configFile.setAttributes(
          new String [] {"Regional_DB", "Regional_DB", "Regional_DB"},
          new String [] {"Enabled", "Database", "Description"},
          new String [] {"yes",
              "jdbc:mysql://"+newDirs[sel].trim()+":3306/gridpilot", dbDesc}
      );  
      changes = true;
    }
    
    /*
    home site = FZKDISK mysql://dq2user:dqpwd@grid00.unige.ch:3306/localreplicas
    */
    String initalPanels = configFile.getValue(GridPilot.TOP_CONFIG_SECTION, "Initial panels");
    if(cbAtlas.isSelected()){
      configFile.setAttributes(
          new String [] {GridPilot.TOP_CONFIG_SECTION, "ATLAS"},
          new String [] {"Initial panels", "Enabled"},
          new String [] {initalPanels+" ATLAS:dataset", "yes"}
          );
      String homeSite = MyUtil.getJTextOrEmptyString(tfHomeSite);
      if(homeSite!=null && !homeSite.trim().equals("")){
        configFile.setAttributes(
            new String [] {"ATLAS"},
            new String [] {"Home site"},
            new String [] {homeSite.trim()}
            );
      }
      configFile.setAttributes(
          new String [] {"ATLAS", "ATLAS"},
          new String [] {"User path", "Tiers of atlas"},
          new String [] {tfLfcPath.getText().trim().replaceFirst("^/grid/atlas", ""),
          		 TOA_URL+" "+tfTOAPath.getText().trim()}
          );
    }
      
    return choice;
  }

  private JPanel createAtlasPanel(JPanel jPanel, Component tfHomeSite, JTextField tfLfcPath,
      String lfcPath, JTextField tfTOAPath, String toaPath, final JCheckBox cbAtlas, final ConfirmBox confirmBox) {
    JPanel row = null;
    final JPanel atlasDetails = new JPanel(new GridBagLayout());
    String atlasString = "\n" +
    "When looking up and downloading files, the first site with a given file will be used.\n" +
    "In order to always give preference to one site, you can specify a \"home site\".\n" +
    "This should be one of the ATLAS site acronyms from the file " +
    "<a href=\"http://atlas.web.cern.ch/Atlas/GROUPS/DATABASE/project/ddm/releases/TiersOfATLASCache.py\">TiersOfATLAS</a>" +
    " - e.g. UNICPH-NBI_LOCALGROUPDISK.\n\n" +
    "In order to be able to register your own datasets, you must have write access to the\n" +
    "file catalog of your \"home site\" and  you must also specify the path in the file\n" +
    "catalog under which you want to save your datasets.\n\n" +
    "If you don't understand the above or don't have write access to a remote file catalog,\n" +
    "you can safely leave the fields untouched. Then you will have only read access.\n";
    JEditorPane atlasLabel = new JEditorPane("text/html", "<html>"+atlasString.replaceAll("\n", "<br>")+"</html>");
    atlasLabel.setEditable(false);
    atlasLabel.setOpaque(false);
    MyUtil.addHyperLinkListener(atlasLabel);
    atlasDetails.add(atlasLabel,
        new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    row = new JPanel(new BorderLayout(8, 0));
    row.add(new JLabel("Home catalog site: "), BorderLayout.WEST);
    row.add(new JLabel("   "), BorderLayout.EAST);
    row.add(tfHomeSite, BorderLayout.CENTER);
    atlasDetails.add(row,
        new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    row = new JPanel(new BorderLayout(8, 0));
    row.add(new JLabel("LFC path: "), BorderLayout.WEST);
    row.add(new JLabel("   "), BorderLayout.EAST);
    tfLfcPath.setText(lfcPath);
    row.add(tfLfcPath, BorderLayout.CENTER);
    atlasDetails.add(row,
        new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    row = new JPanel(new BorderLayout(8, 0));
    row.add(new JLabel("TiersOfATLAS local cache file: "), BorderLayout.WEST);
    row.add(new JLabel("   "), BorderLayout.EAST);
    tfTOAPath.setText(toaPath);
    row.add(tfTOAPath, BorderLayout.CENTER);
    atlasDetails.add(row,
        new GridBagConstraints(0, 3, 1, 1, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    
    cbAtlas.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        try{
          if(catalogPanelSize==null){
            catalogPanelSize = confirmBox.getDialog().getSize();
          }
          int maxHeight = Toolkit.getDefaultToolkit().getScreenSize().height-100;
          int maxWidth = Toolkit.getDefaultToolkit().getScreenSize().width-100;
          confirmBox.getDialog().getContentPane().setMaximumSize(
              new Dimension(maxWidth, maxHeight));
          Dimension currentSize = confirmBox.getDialog().getSize();
          if(cbAtlas.isSelected()){
            atlasDetails.setVisible(true);            
            int newHeight = currentSize.height+300;
            int newWidth = currentSize.width+100;
            confirmBox.getDialog().setSize(
                newWidth>maxWidth?maxWidth:newWidth,
                newHeight>maxHeight?maxHeight:newHeight);
          }
          else{
            int newHeight = currentSize.height-200;
            int newWidth = currentSize.width-100;
            atlasDetails.setVisible(false);
            confirmBox.getDialog().setSize(
                newWidth<catalogPanelSize.width?catalogPanelSize.width:newWidth,
                newHeight<catalogPanelSize.height?catalogPanelSize.height:newHeight);
          }
          //confirmBox.getDialog().pack();
        }
        catch(Exception ex){
          Debug.debug("Could not show details", 2);
          ex.printStackTrace();
        }
      }
    });
    return atlasDetails;
  }

  private JComponent createAtlasSitesField() {
    TiersOfAtlas toa = new TiersOfAtlas(TOA_URL, null);
    String[] sites;
    try{
      sites = toa.getAllLocalGroupDisks();
      JExtendedComboBox sitesBox = new JExtendedComboBox();
      sitesBox.setAutoscrolls(true);
      ((JExtendedComboBox) sitesBox).addItem("");
      for(int i=0; i<sites.length; ++i){
        ((JExtendedComboBox) sitesBox).addItem(sites[i]);
      }
      ((JExtendedComboBox) sitesBox).setEditable(true);
      sitesBox.updateUI();
      return sitesBox;
    }
    catch(Exception e){
      e.printStackTrace();
    }
    return new JTextField(TEXTFIELDWIDTH);
  }

  private JComponent createVomsesField(String vomsUrl) throws MalformedURLException {
    GlobusURL url = new GlobusURL(vomsUrl);
    String[] vomses;
    try{
      vomses = findVomses("https://"+url.getHost()+":8443/vomses");
      JExtendedComboBox sitesBox = new JExtendedComboBox();
      sitesBox.setAutoscrolls(true);
      ((JExtendedComboBox) sitesBox).addItem("");
      for(int i=0; i<vomses.length; ++i){
        ((JExtendedComboBox) sitesBox).addItem(vomses[i]);
      }
      ((JExtendedComboBox) sitesBox).setEditable(true);
      sitesBox.updateUI();
      return sitesBox;
    }
    catch(Exception e){
      e.printStackTrace();
    }
    return new JTextField(TEXTFIELDWIDTH);
  }

  private String[] findVomses(String _url) {
    // Pretty hacky - TODO: consider asking the info system directly
    Vector<String> ret = new Vector<String>();
    try{
      // https://lcg-voms.cern.ch:8443/vomses does not work, instead we
      // use https://voms.cern.ch:8443/vomses which is a mirror.
      String url = _url.replaceFirst("lcg-voms.cern.ch", "voms.cern.ch");
      URL monitorURL = new URL(url);
      // This is to trust all CAs
      new SecureWebServiceConnection(monitorURL.getHost(), monitorURL.getPort(), monitorURL.getPath());
      String line = null;
      String inLine = null;
      Debug.debug("Reading URL "+monitorURL.toExternalForm(), 2);
      BufferedReader in = new BufferedReader(new InputStreamReader(monitorURL.openStream()));
      StringBuffer lb = new StringBuffer();
      String voPattern =
        "(?i).*<div class=\"voLink\">\\s*<a href=\"\\S+\">\\s*([\\w\\-\\.]+)\\s*.*";
      String vo;
      while((inLine = in.readLine())!=null){
        inLine = inLine.trim();
        // take care of "lines" split on multiple lines
        if(inLine.matches(".*\">\\s*$")){
          lb.append(inLine);
          continue;
        }
        if(inLine.length()==0){
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
        Debug.debug("-->"+line, 3);
        if(line.matches(voPattern)){
          vo = line.replaceFirst(voPattern, "$1");
          if(!ret.contains(vo)){
            Debug.debug("Adding VO "+vo, 1);
            ret.add(vo);
          }
        }
      }
      Collections.sort(ret);
    }
    catch(Exception e){
      e.printStackTrace();
    }
    return ret.toArray(new String [ret.size()]);
  }

  private JComponent createNGClustersField() {
    String[] sites;
    try{
      sites = findAllNGSites();
      JExtendedComboBox sitesBox = new JExtendedComboBox();
      sitesBox.setAutoscrolls(true);
      ((JExtendedComboBox) sitesBox).addItem("");
      for(int i=0; i<sites.length; ++i){
        ((JExtendedComboBox) sitesBox).addItem(sites[i]);
      }
      ((JExtendedComboBox) sitesBox).setEditable(true);
      sitesBox.updateUI();
      return sitesBox;
    }
    catch(Exception e){
      e.printStackTrace();
    }
    return new JTextField(TEXTFIELDWIDTH);
  }

  private String[] findAllNGSites() {
    // Pretty hacky - TODO: consider asking the info system directly
    Vector<String> ret = new Vector<String>();
    try{
      URL monitorURL = new URL("http://www.nordugrid.org/monitor/");
      String line = null;
      String inLine = null;
      Debug.debug("Reading URL "+monitorURL.toExternalForm(), 2);
      BufferedReader in = new BufferedReader(new InputStreamReader(monitorURL.openStream()));
      StringBuffer lb = new StringBuffer();
      String sitePattern = ".*clusdes\\.php\\?host=([\\w\\-\\.]+)\\&.*";
      String site;
      while((inLine = in.readLine())!=null){
        inLine = inLine.trim();
        if(inLine.length()==0){
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
        if(line.matches(sitePattern)){
          site = line.replaceFirst(sitePattern, "$1");
          if(!ret.contains(site)){
            Debug.debug("Adding site "+site, 1);
            ret.add(site);
          }
        }
      }
      Collections.sort(ret);
    }
    catch(Exception e){
      e.printStackTrace();
    }
    return ret.toArray(new String [ret.size()]);
  }

  private JComponent createGridFactorySitesField() {
    try{
      gfSubmitUrlsBox.setAutoscrolls(true);
      ((JExtendedComboBox) gfSubmitUrlsBox).addItem("");
      for(int i=0; i<GF_SUBMIT_URLS.length; ++i){
        ((JExtendedComboBox) gfSubmitUrlsBox).addItem(GF_SUBMIT_URLS[i]);
      }
      ((JExtendedComboBox) gfSubmitUrlsBox).setEditable(true);
      gfSubmitUrlsBox.updateUI();
      return gfSubmitUrlsBox;
    }
    catch(Exception e){
      e.printStackTrace();
    }
    return new JTextField(TEXTFIELDWIDTH);
  }

  /* Configure computing systems
  
  --->  configure NG

        *> [NG] clusters

  --->  configure gLite
  
        *> [GLite] virtual organization = ATLAS              
        
        [GLite] runtime vos = ATLAS CMS
        [GLite] runtime clusters = ce01-lcg.projects.cscs.ch g03n02.pdc.kth.se
        
        *> We just set runtime vos = virtual organization and leave runtime clusters

  --->  configure SSH_POOL
        
        *> [SSH_POOL] hosts
        *> [SSH_POOL] users
        *> [SSH_POOL] passwords
        
        *> set [SSH] host = ([SSH_POOL] hosts)[0], ...

  --->  configure EC2
        
        *> [EC2] aws access key id
        *> [EC2] aws secret access key

  --->  configure GRIDFACTORY

        [GRIDFACTORY] allowed submission URL = https://www.gridfactory.org/gridfactory/jobs/
                      
*/
  private int configureComputingSystems(boolean firstRun) throws Exception{
    String confirmString =
      "GridPilot can run jobs on a number of remote backend systems. Here you can configure the systems you would like to use.\n\n" +
      "NG is an abbreviation for NorduGrid, which is a grid initiated and driven by universitites and computing centers\n" +
      "in the Nordic countries. This grid uses the middleware called ARC. If you're not yourself a member of the nordugrid\n" +
      "virtual organization or another virtual organization affiliated with one of the institutes participating in NorduGrid\n" +
      "or ARC, you will probably not be allowed to run jobs on this backend.\n\n" +
      "GLite is the middleware built by EGEE and used by the LCG grid. LCG is a grid servicing high energy physicists.\n" +
      "If you're not a member of an EGEE virtual organization, you will probably not be able to run jobs on this backend.\n\n" +
      "SSH_POOL is a backend that runs jobs on a pool of Linux machines accessed via ssh. The scheduling is done by\n" +
      "a very simplistic FIFO algorithm.\n\n" +
      "GridFactory is the native batch system of GridPilot. It is still experimental, but you're welcome to try\n" +
      "it out. The submission is done by uploading jobs scripts and input file(s) to a server from where they will be\n" +
      "picked up and run by GridWorkers.\n\n";
    final JPanel jPanel = new JPanel(new GridBagLayout());
    jPanel.add(new JLabel("<html>"+confirmString.replaceAll("\n", "<br>")+"</html>"),
        new GridBagConstraints(0, (firstRun?1:0), 2, 2, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    JPanel row = null;
    String [] names = new String [] {"NG", "GLite", "SSH_POOL", "EC2", "GridFactory"};
    JPanel [] csRows = new JPanel [names.length];
    final JPanel [] csPanels = new JPanel [names.length];
    jcbs = new JCheckBox[names.length];
    int i = 0;
    final ConfirmBox confirmBox = new ConfirmBox();
    
    // NorduGrid
    csPanels[0] = new JPanel(new GridBagLayout());
    String ngString =
      "To use NorduGrid you must have a certificate/key that is recognized by NorduGrid and you (i.e.\n" +
      "your certificate) must be member of one of the virtual organizations recognized by NorduGrid.\n\n" +
      "If you fill in the field 'clusters', you choose to submit only to a selected set of clusters. This will\n" +
      "typically save you a significant amount of time when submitting jobs. The field must be filled with\n" +
      "a space-separated list of front-end host names. If you leave it empty all available resources will be\n" +
      "queried on each job submission. You can find a list of participating clusters at " +
      "<a href=\"http://www.nordugrid.org/monitor/\">www.nordugrid.org</a>.\n"+
    "You can find a list of virtual organizations at " +
    "<a href=\"http://www.nordugrid.org/NorduGridVO/\">www.nordugrid.org/NorduGridVO/</a>.\n\n";
    JEditorPane pane = new JEditorPane("text/html", "<html>"+ngString.replaceAll("\n", "<br>")+"</html>");
    pane.setEditable(false);
    pane.setOpaque(false);
    MyUtil.addHyperLinkListener(pane);
    csPanels[0].add(pane,
        new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    row = new JPanel(new BorderLayout(8, 0));
    row.add(new JLabel("Clusters: "), BorderLayout.WEST);
    final JPanel jpClusters = new JPanel();
    JTextField tfClusters = new JTextField(TEXTFIELDWIDTH);
    tfClusters.setText("");
    jpClusters.add(tfClusters);
    JButton bClusters = new JButton("Get list");
    bClusters.setToolTipText("Query ARC information system for list of all cluster front-end names.");
    bClusters.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        jPanel.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
        (new Thread(){
          public void run(){
            JComponent cbClusters = createNGClustersField();
            if(cbClusters!=null){
              jpClusters.remove(0);
              jpClusters.add(cbClusters, 0);
              jpClusters.updateUI();
            }
            jPanel.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
          }
        }).start();
      }
    });
    jpClusters.add(bClusters);
    row.add(jpClusters, BorderLayout.CENTER);
    csPanels[0].add(row,
        new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    
    // gLite
    csPanels[1] = new JPanel(new GridBagLayout());
    String gLiteString =
      "To use GLite you must have a certificate/key that is recognized by EGEE and you (i.e. your certificate)\n" +
      "must be member of one of the virtual organizations of EGEE.\n\n" +
      "Filling in the field 'virtual organization' is mandatory. It must be filled in with the name of the\n" +
      "EGEE virtual organization whose resources you wish to use, e.g. ATLAS. If it is not filled in, you\n" +
      "will be able to load the computing system backend, but your jobs will be rejected on the resources.\n" +
      "You can find a list of virtual organizations at " +
      "<a href=\"http://cic.gridops.org/index.php?section=home&page=volist\">cic.gridops.org</a>.\n\n";
    pane = new JEditorPane("text/html", "<html>"+gLiteString.replaceAll("\n", "<br>")+"</html>");
    pane.setEditable(false);
    pane.setOpaque(false);
    MyUtil.addHyperLinkListener(pane);
    csPanels[1].add(pane,
        new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    row = new JPanel(new BorderLayout(8, 0));
    row.add(new JLabel("VOMS server: "), BorderLayout.WEST);
    final JTextField tfVomsServer = new JTextField(TEXTFIELDWIDTH);
    tfVomsServer.setText("https://lcg-voms.cern.ch:15001/DC=ch/DC=cern/OU=computers/CN=lcg-voms.cern.ch");
    row.add(tfVomsServer, BorderLayout.CENTER);
    csPanels[1].add(row,
        new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    row = new JPanel(new BorderLayout(8, 0));
    row.add(new JLabel("Virtual organization: "), BorderLayout.WEST);
    final JPanel jpVos = new JPanel();
    final JTextField tfVO = new JTextField(TEXTFIELDWIDTH);
    tfVO.setText("");
    jpVos.add(tfVO);
    JButton bVos = new JButton("Get list");
    bVos.setToolTipText("Query VOMS server for list of virtual organizations.");
    bVos.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        jPanel.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
        (new Thread(){
          public void run(){
            try {
              cvVomses = createVomsesField(tfVomsServer.getText());
              if(cvVomses!=null){
                jpVos.remove(0);
                jpVos.add(cvVomses, 0);
                jpVos.updateUI();
              }
            }
            catch(Exception e){
              e.printStackTrace();
            }
            jPanel.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
          }
        }).start();
      }
    });
    jpVos.add(bVos);
    row.add(jpVos, BorderLayout.CENTER);
    csPanels[1].add(row,
        new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    
    // ssh pool
    csPanels[2] = new JPanel(new GridBagLayout());
    String sshPoolString =
      "To use SSH_POOL you must have SSH login accounts on one or several machines.\n\n" +
      "The field 'hosts' must be filled in with a space-separated list of host names.\n" +
      "The field 'users names' must be filled in with a user name for each host.\n" +
      "The field 'passwords' must be filled in with a password for each host.\n\n" +
      "If 'user names' or 'passwords' is not filled in, you will be prompted for it when submitting jobs.\n\n" +
      "It is not recommended to fill in 'passwords' because the passwords will be stored in clear text.\n\n";
    pane = new JEditorPane("text/html", "<html>"+sshPoolString.replaceAll("\n", "<br>")+"</html>");
    pane.setEditable(false);
    pane.setOpaque(false);
    csPanels[2].add(pane,
        new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    row = new JPanel(new BorderLayout(8, 0));
    row.add(new JLabel("Hosts: "), BorderLayout.WEST);
    JTextField tfHosts = new JTextField(TEXTFIELDWIDTH);
    tfHosts.setText("");
    row.add(tfHosts, BorderLayout.CENTER);
    csPanels[2].add(row,
        new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    row = new JPanel(new BorderLayout(8, 0));
    row.add(new JLabel("User names: "), BorderLayout.WEST);
    JTextField tfUsers = new JTextField(TEXTFIELDWIDTH);
    tfUsers.setText("");
    row.add(tfUsers, BorderLayout.CENTER);
    csPanels[2].add(row,
        new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    row = new JPanel(new BorderLayout(8, 0));
    row.add(new JLabel("Passwords: "), BorderLayout.WEST);
    JTextField tfPasswords = new JTextField(TEXTFIELDWIDTH);
    tfPasswords.setText("");
    row.add(tfPasswords, BorderLayout.CENTER);
    csPanels[2].add(row,
        new GridBagConstraints(0, 3, 1, 1, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));    
    
    // EC2
    csPanels[3] = new JPanel(new GridBagLayout());
    String ec2String =
      "To use EC2 you must have registered with Amazon at <a href=\"http://aws.amazon.com/\">aws.amazon.com</a>\n" +
      "and obtained an access key.\n\n" +
      "The AWS access key ID and AWS secret access key must be filled in with the\n" +
      "values you have been provided with by Amazon (\"Access Identifiers\").\n\n";
    pane = new JEditorPane("text/html", "<html>"+ec2String.replaceAll("\n", "<br>")+"</html>");
    pane.setEditable(false);
    pane.setOpaque(false);
    MyUtil.addHyperLinkListener(pane);
    csPanels[3].add(pane,
        new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    row = new JPanel(new BorderLayout(8, 0));
    row.add(new JLabel("AWS access key ID: "), BorderLayout.WEST);
    JTextField tfAwsId = new JTextField(TEXTFIELDWIDTH);
    tfAwsId.setText("");
    row.add(tfAwsId, BorderLayout.CENTER);
    csPanels[3].add(row,
        new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    row = new JPanel(new BorderLayout(8, 0));
    row.add(new JLabel("AWS secret access key: "), BorderLayout.WEST);
    JTextField tfAwsKey = new JTextField(TEXTFIELDWIDTH);
    tfAwsKey.setText("");
    row.add(tfAwsKey, BorderLayout.CENTER);
    csPanels[3].add(row,
        new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));   
    
    // GRIDFACTORY
    csPanels[4] = new JPanel(new GridBagLayout());
    String gfString =
      "To use GridFactory, you (i.e. your certificate/key) must have write access to a GridFactory server.\n\n" +
      "Users who are just testing (i.e. using the supplied test key/certificate) may run a small number\n" +
      "of test jobs on our test servers listed below."+
      "\n\n";
    pane = new JEditorPane("text/html", "<html>"+gfString.replaceAll("\n", "<br>")+"</html>");
    pane.setEditable(false);
    pane.setOpaque(false);
    csPanels[4].add(pane,
        new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    row = new JPanel(new BorderLayout(8, 0));
    row.add(new JLabel("Submit URL: "), BorderLayout.WEST);
    JComponent tfGfUrl = createGridFactorySitesField();
    row.add(tfGfUrl, BorderLayout.CENTER);
    csPanels[4].add(row,
        new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    
    for(i=0; i<names.length; ++i){
      csRows[i] = new JPanel(new BorderLayout(8, 0));
      jcbs[i] = new JCheckBox();
      csRows[i].add(jcbs[i], BorderLayout.WEST);
      csRows[i].add(new JLabel("Enable "+names[i]), BorderLayout.CENTER);
      jPanel.add(csRows[i], new GridBagConstraints(0, 3*i+5, 1, 1, 0.0, 0.0,
          GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
          new Insets(0, 0, 0, 0), 0, 0));
      jPanel.add(csPanels[i], new GridBagConstraints(0, 3*i+6, 1, 1, 0.0, 0.0,
          GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
          new Insets(0, 0, 0, 0), 0, 0));
      jPanel.add(new JLabel(" "), new GridBagConstraints(0, 3*i+7, 1, 1, 0.0, 0.0,
          GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
          new Insets(0, 0, 0, 0), 0, 0));
      csPanels[i].setVisible(false);
      jcbs[i].setMnemonic(i);
      jcbs[i].addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          try{
            if(gridsPanelSize==null){
              gridsPanelSize = confirmBox.getDialog().getSize();
           }
            int maxHeight = Toolkit.getDefaultToolkit().getScreenSize().height-100;
            int maxWidth = Toolkit.getDefaultToolkit().getScreenSize().width-100;
            confirmBox.getDialog().getContentPane().setMaximumSize(
                new Dimension(maxWidth, maxHeight));
            Dimension currentSize = confirmBox.getDialog().getSize();
            if(((JCheckBox) e.getSource()).isSelected()){
              csPanels[((JCheckBox) e.getSource()).getMnemonic()].setVisible(true);
              int newHeight = currentSize.height+180;
              int newWidth = currentSize.width+5;
              confirmBox.getDialog().setSize(
                  newWidth>maxWidth?maxWidth:newWidth,
                  newHeight>maxHeight?maxHeight:newHeight);
            }
            else{
              int newHeight = currentSize.height-180;
              int newWidth = currentSize.width-5;
              csPanels[((JCheckBox) e.getSource()).getMnemonic()].setVisible(false);
              confirmBox.getDialog().setSize(
                  newWidth<gridsPanelSize.width?gridsPanelSize.width:newWidth,
                  newHeight<gridsPanelSize.height?gridsPanelSize.height:newHeight);
            }
            //confirmBox.getDialog().pack();
          }
          catch(Exception ex){
            Debug.debug("Could not show details", 2);
            ex.printStackTrace();
          }
        }
      });
    }
        
    // Get confirmation
    int choice = -1;
    String title = "Step 6/6: Setting up remote computing systems";
    choice = confirmBox.getConfirmPlainText(title, jPanel,
        new Object[] {"Continue", "Skip", "Cancel"}, icon, Color.WHITE, true, true);

    if(choice!=0){
      return choice;
    }

    // Set configuration values
    String cluster = null;
    try{
      cluster = MyUtil.getJTextOrEmptyString(((JComponent) jpClusters.getComponent(0)));
    }
    catch(Exception ee){
      ee.printStackTrace();
    }
    if(jcbs[0].isSelected()){
      configFile.setAttributes(
          new String [] {"NG", "NG"},
          new String [] {"Enabled", "Clusters"},
          new String [] {"yes", cluster==null?"":cluster.trim()}
          );
      if(cluster==null || cluster.trim().equals("")){
        MyUtil.showMessage("No clusters defined",
            "WARNING: you have not defined any NorduGrid  clusters.\n" +
            "This may cause submission to NorduGrid to be very slow as all clusters\n" +
            "will be queried on each submission.\n" +
            "You can change this in the preferences.");
      }
    }
    else{
      configFile.setAttributes(
          new String [] {"NG"},
          new String [] {"Enabled"},
          new String [] {"no"}
          );
    }
    String vo0 =  tfVO.getText();
    String vo1 = cvVomses==null?null:MyUtil.getJTextOrEmptyString(cvVomses);
    String vo = vo1!=null&&!vo1.trim().equals("")?vo1:vo0;
    if(jcbs[1].isSelected() && vo!=null && !vo.trim().equals("")){
      boolean reInitSSL = false;
      if(!vo.equals(configFile.getValue(GridPilot.TOP_CONFIG_SECTION, "Virtual organization")) ||
         !tfVomsServer.getText().trim().equals(configFile.getValue(GridPilot.TOP_CONFIG_SECTION, "Voms server"))){
        reInitSSL = true;
      }
      configFile.setAttributes(
          new String [] {"GLite", GridPilot.TOP_CONFIG_SECTION, GridPilot.TOP_CONFIG_SECTION, "GLite"},
          new String [] {"Enabled", "Virtual organization", "Voms server", "Runtime vos"},
          new String [] {"yes", vo, tfVomsServer.getText(), vo}
          );
      if(reInitSSL){
        GridPilot.getClassMgr().getSSL().activateProxySSL(null, true);
      }
    }
    else{
      configFile.setAttributes(
          new String [] {"GLite"},
          new String [] {"Enabled"},
          new String [] {"no"}
          );
    }
    if(jcbs[2].isSelected() && tfHosts.getText()!=null && !tfHosts.getText().equals("")){
      configFile.setAttributes(
          // We use the first of the given hosts as master host
          new String [] {"SSH_Pool", "SSH_Pool", "SSH_Pool"},
          new String [] {"Enabled", "Hosts", "Host"},
          new String [] {"yes", tfHosts.getText().trim(), MyUtil.split(tfHosts.getText())[0]}
          );
      if(tfUsers.getText()!=null && !tfUsers.getText().equals("")){
        configFile.setAttributes(
            // We use the first of the given hosts as master host
            new String [] {"SSH_Pool", "SSH_Pool"},
            new String [] {"Users", "User"},
            new String [] {tfUsers.getText().trim(), MyUtil.split(tfUsers.getText())[0]}
            );
      }
      if(tfPasswords.getText()!=null && !tfPasswords.getText().equals("")){
        configFile.setAttributes(
            // We use the first of the given hosts as master host
            new String [] {"SSH_Pool", "SSH_Pool"},
            new String [] {"Passwords", "Password"},
            new String [] {tfPasswords.getText().trim(), MyUtil.split(tfPasswords.getText())[0]}
            );
      }
    }
    else{
      configFile.setAttributes(
          new String [] {"SSH_Pool"},
          new String [] {"Enabled"},
          new String [] {"no"}
          );
    }
    if(jcbs[3].isSelected() && MyUtil.getJTextOrEmptyString(tfAwsId)!=null &&
        !MyUtil.getJTextOrEmptyString(tfAwsId).equals("")){
      configFile.setAttributes(
          new String [] {"EC2", "EC2", "EC2", "sss", "sss"},
          new String [] {"Enabled",
                         "AWS access key ID", "AWS secret access key",
                         "AWS access key ID", "AWS secret access key"},
          new String [] {"yes", MyUtil.getJTextOrEmptyString(tfAwsId).trim(),
                                MyUtil.getJTextOrEmptyString(tfAwsKey).trim(),
                                MyUtil.getJTextOrEmptyString(tfAwsId).trim(),
                                MyUtil.getJTextOrEmptyString(tfAwsKey).trim()}
          );
    }
    else{
      configFile.setAttributes(
          new String [] {"EC2"},
          new String [] {"Enabled"},
          new String [] {"no"}
          );
    }
    if(jcbs[4].isSelected()){
      if(MyUtil.getJTextOrEmptyString(tfGfUrl)!=null &&
          !MyUtil.getJTextOrEmptyString(tfGfUrl).trim().equals("")){
        String gfSubmitUrl = MyUtil.getJTextOrEmptyString(tfGfUrl).trim();
        configFile.setAttributes(
            new String [] {"GridFactory", "GridFactory"},
            new String [] {"Enabled", "Submission URLs"},
            new String [] {"yes", gfSubmitUrl}
            );
      }
      else{
        MyUtil.showMessage("No submission URL defined",
            "WARNING: You have not given any GridFactory submission URL.\n" +
        		"GridFactory will not be enabled.\n" +
        		"You can change this in the preferences.");
      }
    }
    else{
      configFile.setAttributes(
          new String [] {"GridFactory"},
          new String [] {"Enabled"},
          new String [] {"no"}
          );
    }
    
    return choice;
  }

  /**
   * If a remote server is specified, assume that a database with name corresponding
   * to the DN exists on the specified server and is writeable;
   * enable My_DB_Remote disable GP_DB.
   * If the default remote database is chosen, disable My_DB_Remote;
   * enable GP_DB with database 'gridpilot'; this will then be the job DB for remote CSs.
   */
  private int setGridJobDB(boolean firstRun) throws Exception{
    String confirmString =
      "GridPilot allows you to keep track of your grid life: the jobs you have running or have run and the files\n" +
      "you've produced.\n\n" +
      "You can keep this information in your local database or if you have write access to a remote database,\n" +
      "you can keep the information there.\n\n" +
      "If you choose to use a remote database, you must specify the name of the server hosting it. Please notice\n" +
      "that the database must be a <a href=\""+MYSQL_HOWTO_URL+"\">GridPilot-enabled MySQL database</a>.\n\n" +
      "If you choose to use the default remote database, please notice that anything you write there is\n" +
      "world readable and that the service is provided by gridpilot.dk with absolutely no guarantee that\n" +
      "data will not be deleted at any time.\n\n";
    JPanel jPanel = new JPanel(new GridBagLayout());
    JEditorPane pane = new JEditorPane("text/html", "<html>"+confirmString.replaceAll("\n", "<br>")+"</html>");
    pane.setEditable(false);
    pane.setOpaque(false);
    MyUtil.addHyperLinkListener(pane);
    String remoteDB = configFile.getValue("My_DB_Remote", "Database");
    String host = remoteDB.replaceFirst(".*mysql://(.*)/.*","$1");
    // TODO: now we assume that mysql always runs on port 3306 - generalize.
    host = host.replaceFirst("(.*):\\d+", "$1");
    String [] defDirs = new String [] {"",
                                       "www.gridpilot.dk",
                                       host};
    String [] names = new String [] {"Use local database",
                                     "Use default remote database host",
                                     "Use custom remote database host:"};
    JTextField [] jtFields = new JTextField [defDirs.length];
    jPanel.add(pane,
        new GridBagConstraints(0, (firstRun?1:0), 2, 2, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    JPanel row = null;
    JPanel subRow = null;
    jrbs = new JRadioButton[defDirs.length];
    RadioListener myListener = new RadioListener();
    for(int i=0; i<defDirs.length; ++i){
      jtFields[i] = new JTextField(TEXTFIELDWIDTH);
      jtFields[i].setText(defDirs[i]);
      jrbs[i] = new JRadioButton();
      jrbs[i].addActionListener(myListener);
      row = new JPanel(new BorderLayout(8, 0));
      row.add(jrbs[i], BorderLayout.WEST);
      if(i==2){
        row.add(new JLabel(names[i]), BorderLayout.CENTER);
      }
      else{
        row.add(new JLabel(names[i]), BorderLayout.CENTER);
        jtFields[i].setEditable(false);
      }
      subRow = new JPanel(new BorderLayout(8, 0));
      subRow.add(jtFields[i], BorderLayout.CENTER);
      subRow.add(new JLabel("   "), BorderLayout.EAST);
      subRow.add(new JLabel("   "), BorderLayout.SOUTH);
      subRow.add(new JLabel("   "), BorderLayout.NORTH);
      row.add(subRow, BorderLayout.EAST);
      jPanel.add(row, new GridBagConstraints(0, i+4, 1, 1, 0.0, 0.0,
          GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
          new Insets(0, 0, 0, 0), 0, 0));
    }
    jrbs[0].setSelected(true);
    jPanel.validate();
    
    ConfirmBox confirmBox = new ConfirmBox();
    int choice = -1;
    boolean goOn = false;
    int sel = -1;
    String [] newDirs = new String[defDirs.length];
    String title = "Step 4/6: Setting up job database";
    while(!goOn){
      if(goOn){
        break;
      }
      try{
        choice = confirmBox.getConfirmPlainText(title, jPanel,
            new Object[] {"Continue", "Skip", "Cancel"}, icon, Color.WHITE, true, false);

        if(choice==0){
          // Get field values
          for(int i=0; i<newDirs.length; ++i){
            if(jrbs[i].isSelected()){
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
            confirmBox.getConfirmPlainText(title, "Please fill in the host name of the remote database server",
                new Object[] {MyUtil.mkOkObject(confirmBox.getOptionPane())}, icon, Color.WHITE,
                true, false);
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
  
    if(sel==0){
      // Local DB, enable My_DB_Local, disable My_DB_Remote and GP_DB.
      // Set [Fork|GRIDFACTORY|SSH|SSH_POOL|NG|GLite|EC2]:runtime databases = My_DB_Local
       configFile.setAttributes(
          new String [] {GridPilot.TOP_CONFIG_SECTION, "My_DB_Local", "My_DB_Remote", "GP_DB",
              "Fork", "GRIDFACTORY", "SSH", "SSH_POOL", "NG", "GLite", "EC2"},
          new String [] {"Initial panels", "Enabled", "Enabled", "Enabled",
              "Runtime databases", "Runtime databases", "Runtime databases", "Runtime databases",
              "Runtime databases","Runtime databases","Runtime databases"},
          new String [] {"My_DB_Local:dataset", "yes", "no", "no",
              "My_DB_Local", "My_DB_Local", "My_DB_Local", "My_DB_Local", "My_DB_Local",
              "My_DB_Local", "My_DB_Local"}
      );  
      changes = true;
    }
    else if(sel==1){
      // gridpilot.dk, enable GP_DB, My_DB_Local, disable My_DB_Remote.
      // Set GP_DB:database = gridpilot
      // Set [Fork|GRIDFACTORY|SSH|SSH_POOL|NG|GLite|EC2]:runtime databases = GP_DB My_DB_Local
      configFile.setAttributes(
          new String [] {GridPilot.TOP_CONFIG_SECTION, "My_DB_Local", "My_DB_Remote", "GP_DB",
              "GP_DB", "Fork", "GRIDFACTORY", "SSH", "SSH_POOL", "NG", "GLite", "EC2"},
          new String [] {"Initial panels", "Enabled", "Enabled", "Enabled",
              "Database", "Runtime databases", "Runtime databases", "Runtime databases", "Runtime databases",
              "Runtime databases", "Runtime databases", "Runtime databases"},
          new String [] {"GP_DB:dataset My_DB_Local:dataset", "yes", "no", "yes",
              "jdbc:mysql://www.gridpilot.dk:3306/gridpilot", "My_DB_Local", "GP_DB My_DB_Local",
              "My_DB_Local", "My_DB_Local", "GP_DB My_DB_Local", "GP_DB My_DB_Local", "GP_DB My_DB_Local"}
      );  
      changes = true;
    }
    else if(sel==2){
      // remote DB, enable My_DB_Remote, My_DB_Local, disable GP_DB.
      // Ask for My_DB_Remote:description
      // Set [Fork|GRIDFACTORY|SSH|SSH_POOL|NG|GLite|EC2]:runtime databases = My_DB_Remote My_DB_Local
      String origDbDesc = configFile.getValue("My_DB_Remote", "Description");
      String dbDesc = MyUtil.getName(
          "Please enter a short (~ 5 words) description of the remote database", "");
      if(dbDesc==null || dbDesc.equals("")){
        dbDesc = origDbDesc;
      }
      configFile.setAttributes(
          new String [] {GridPilot.TOP_CONFIG_SECTION, "My_DB_Local", "My_DB_Remote", "GP_DB",
              "My_DB_Remote", "My_DB_Remote",
              "Fork", "GRIDFACTORY", "SSH", "SSH_POOL", "NG", "GLite", "EC2"},
          new String [] {"Initial panels", "Enabled", "Enabled", "Enabled",
             "Database", "Description",
              "Runtime databases", "Runtime databases", "Runtime databases", "Runtime databases",
              "Runtime databases", "Runtime databases", "Runtime databases"},
          new String [] {"My_DB_Remote:dataset My_DB_Local:dataset", "yes", "yes", "no",
              "jdbc:mysql://"+newDirs[sel].trim()+":3306/", dbDesc,
              "My_DB_Local", "My_DB_Remote My_DB_Local", "My_DB_Local", "My_DB_Local",
              "My_DB_Remote My_DB_Local", "My_DB_Remote My_DB_Local", "My_DB_Remote My_DB_Local"}
      );  
      changes = true;
    }
      
    return choice;
  }

  private int setGridHomeDir(boolean firstRun) throws Exception{
    GridPilot.PROXY_DIR = configFile.getValue(GridPilot.TOP_CONFIG_SECTION, "Proxy directory");
    GridPilot.CA_CERTS_DIR = GridPilot.getClassMgr().getConfigFile().getValue(GridPilot.TOP_CONFIG_SECTION,
       "ca certificates");
    GridPilot.RESOURCES_PATH =  GridPilot.getClassMgr().getConfigFile().getValue(GridPilot.TOP_CONFIG_SECTION, "resources");
    String confirmString;
    if(certAndKeyOk){
      confirmString =
        "When running jobs on a grid it is useful to have the jobs upload output files to a directory on a server\n" +
        "that's always on-line.\n\n" +
        "For this to be possible GridPilot needs to know a URL on a " +
        "gridftp or <a href=\""+HTTPS_HOWTO_URL+"\">https server</a> where you have\n" +
        "read/write permission with the certificate you specified previously.\n\n" +
        "If you don't know any such URL or you don't understand the above, you may use the default grid home URL\n" +
        "given below. But please notice that this is but a temporary solution and that the files on this location may\n" +
        "be read, overwritten or deleted at any time.\n\n"+
        "You may also choose a local directory, but in this case, output files will stay on the resource where a job\n" +
        "has run until GridPilot downloads them.\n\n"+
        "A specified, local, but non-existing directory will be created.\n\n";
    }
    else{
      confirmString =
        "Please choose a directory where to download the output files of your jobs.\n" +
        "Notice that output files will stay on the resource where a job as run until\n" +
        "GridPilot downloads them.\n\n"+
        "A specified, but non-existing directory will be created.\n\n";
    }
    JPanel jPanel = new JPanel(new GridBagLayout());
    JEditorPane pane = new JEditorPane("text/html", "<html>"+confirmString.replaceAll("\n", "<br>")+"</html>");
    pane.setEditable(false);
    pane.setOpaque(false);
    MyUtil.addHyperLinkListener(pane);
    String homeUrl = configFile.getValue(GridPilot.TOP_CONFIG_SECTION, "Grid home url");
    String [] defDirs;
    String [] names;
    if(certAndKeyOk){
      defDirs = new String [] {homeUrl,
          HOME_URL+"users/"+GridPilot.getClassMgr().getSSL().getGridDatabaseUser()+"/",
          homeUrl};
      names = new String [] {"Use your own grid or local home URL",
        "Use default grid home URL",
        "Use default local home URL"};
    }
    else{
      defDirs = new String [] {homeUrl};
      names = new String [] {"Choose home directory"};
    }
    JTextField [] jtFields = new JTextField [defDirs.length];
    jPanel.add(pane,
        new GridBagConstraints(0, (firstRun?1:0), 2, 2, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    JPanel row = null;
    JPanel subRow = null;
    jrbs = new JRadioButton[defDirs.length];
    RadioListener myListener = new RadioListener();
    ConfirmBox confirmBox = new ConfirmBox();
    for(int i=0; i<defDirs.length; ++i){
      jtFields[i] = new JTextField(TEXTFIELDWIDTH);
      jtFields[i].setText(defDirs[i]);
      jrbs[i] = new JRadioButton();
      jrbs[i].addActionListener(myListener);
      row = new JPanel(new BorderLayout(8, 0));
      if(certAndKeyOk){
        row.add(jrbs[i], BorderLayout.WEST);
      }
      if(i==0){
        row.add(MyUtil.createCheckPanel1((Window)confirmBox.getOwner(),
            names[i], jtFields[i], true, true, true, !certAndKeyOk), BorderLayout.CENTER);
      }
      else{
        row.add(new JLabel(names[i]), BorderLayout.CENTER);
        jtFields[i].setEditable(false);
      }
      subRow = new JPanel(new BorderLayout(8, 0));
      subRow.add(jtFields[i], BorderLayout.CENTER);
      subRow.add(new JLabel("   "), BorderLayout.EAST);
      subRow.add(new JLabel("   "), BorderLayout.SOUTH);
      subRow.add(new JLabel("   "), BorderLayout.NORTH);
      row.add(subRow, BorderLayout.EAST);
      jPanel.add(row, new GridBagConstraints(0, i+(firstRun?5:4), 1, 1, 0.0, 0.0,
          GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
          new Insets(0, 0, 0, 0), 0, 0));
    }
    jrbs[0].setSelected(true);
    jPanel.validate();
    
    int choice = -1;
    try{
      choice = confirmBox.getConfirmPlainText("Step 3/6: Setting up grid home directory",
          jPanel, new Object[] {"Continue", "Skip", "Cancel"}, icon, Color.WHITE, true, false);
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
      if(jrbs[i].isSelected()){
        sel = i;
      }
      else{
        continue;
      }
      newDirs[i] = jtFields[i].getText();
      if(newDirs[i]!=null && !newDirs[i].equals("") && !MyUtil.urlIsRemote(newDirs[i])){
        Debug.debug("Checking directory "+newDirs[i], 2);
        newDirFiles[i] = new File(MyUtil.clearTildeLocally(MyUtil.clearFile(newDirs[i])));
        if(newDirFiles[i].exists()){
          if(!newDirFiles[i].isDirectory()){
            throw new IOException("The directory "+newDirFiles[i].getAbsolutePath()+" cannot be created.");
          }
        }
        else{
          Debug.debug("Creating directory "+newDirs[i], 2);
          newDirFiles[i].mkdir();
        }
      }
      newDirs[i] = MyUtil.replaceWithTildeLocally(MyUtil.clearFile(newDirs[i]));
    }

    // Set config entries
    if(sel>-1 && newDirs[sel]!=null &&
        (defDirs[0]==null || !defDirs[0].equals(newDirs[sel]))){
      // Create the homedir if it doesn't exist
      if(MyUtil.urlIsRemote(newDirs[sel])){
        mkRemoteDir(newDirs[sel]);
      }
      else{
        if(!LocalStaticShell.mkdir(newDirs[sel])){
          throw new IOException("Could not create directory "+newDirs[sel]);
        }
      }
      Debug.debug("Setting "+sel+":"+newDirs[sel], 2);
      configFile.setAttributes(
          new String [] {GridPilot.TOP_CONFIG_SECTION},
          new String [] {"Grid home url"},
          new String [] {newDirs[sel]}
      );
      changes = true;
    }
  
    return choice;
  }
  
  private void mkRemoteDir(String url) throws Exception{
    if(!url.endsWith("/")){
      throw new IOException("Directory URL: "+url+" does not end with a slash.");
    }
    GlobusURL globusUrl= new GlobusURL(url);
    FileTransfer fileTransfer = GridPilot.getClassMgr().getFTPlugin(globusUrl.getProtocol());
    // First, check if directory already exists.
    try{
      fileTransfer.list(globusUrl, null);
      return;
    }
    catch(Exception e){
    }
    // If not, create it.
    Debug.debug("Creating directory "+globusUrl.getURL(), 2);
    fileTransfer.write(globusUrl, "");
  }
  

  /** Listens to the radio buttons. */
  class RadioListener implements ActionListener { 
      public void actionPerformed(ActionEvent e) {
        for(int i=0; i<jrbs.length; ++i){
          if(e.getSource().equals(jrbs[i])){
            continue;
          }
          jrbs[i].setSelected(false);
        }
      }
  }
  
}
