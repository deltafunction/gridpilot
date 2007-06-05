package gridpilot.wizards.beginning;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.io.File;
import java.io.IOException;
import java.net.URL;

import gridpilot.ConfigFile;
import gridpilot.ConfirmBox;
import gridpilot.Debug;
import gridpilot.GridPilot;
import gridpilot.LocalStaticShellMgr;
import gridpilot.Util;

import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;

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
      Debug.debug("Could not find image "+ GridPilot.resourcesPath + "aviateur.png", 3);
      icon = null;
    }
    
    try{
      if(!welcome(firstRun)){
        return;
      }
      
      if(!checkDirs(firstRun)){
        return;
      }
      
      // TODO: ask about certificate location. If not there, warn.
      
      // TODO: set grid homedir
      
      // TODO: Set grid file catalog - fallback to homegrid.dyndns.org
      
      // TODO: Set grid job DB - fallback to homegrid.dyndns.org
      
      // TODO: set ATLAS home site - present list - ask for mysql alias
      
      // TODO: set pull and GPSS DB - present list, fallback to homegrid.dyndns.org
      
      // TODO: ask on which grids the user is allowed
      
      // TODO: ask if an ssh pool should be configured
      
      // TODO: give report on which directories were created, etc.
      
      if(endGreeting(firstRun) && changes){
        try{
          GridPilot.reloadConfigValues();
        }
        catch(Exception e1){
          e1.printStackTrace();
        }
      }

    }
    catch(Exception e){
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
  
  private boolean welcome(boolean firstRun) throws Exception{
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
        confirmString, new Object[] {"Continue",  "Cancel"}, icon, Color.WHITE);
    
    if(choice!=0){
      return false;
    }
    return true;
  }
  
  private boolean endGreeting(boolean firstRun) throws Exception{
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    String confirmString = "Configuring GridPilot is now done.\n" +
        "Click \"OK\" to save your settings to " +configFile.getFile().getCanonicalPath()+
        ".\n\n"+
        "Please notice that only the most basic parameters,\n" +
            "necessary to get you up and running have been set.\n" +
            "You can modify these and set many others in \"Edit\" -> \"Preferences\"." +
            (firstRun?"\n\nThanks for using GridPilot and have fun!":"");
    int choice = -1;
    confirmBox.getConfirm("Setup completed!",
        confirmString, new Object[] {"OK",  "Cancel"}, icon, Color.WHITE);   
    if(choice!=0){
      return false;
    }
    return true;
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
   */
  private boolean checkDirs(boolean firstRun) throws IOException{
    JPanel jPanel = new JPanel(new GridBagLayout());
    String [] names = new String [] {
        "Database directory",
        "Cache directory",
        "Working directory",
        "Software directory",
        "Transformations directory"
        };
    String dbDir = "~/GridPilot";
    String cacheDir = configFile.getValue("GridPilot", "pull cache directory");
    String workingDir = configFile.getValue("Fork", "working directory");
    String runtimeDir = configFile.getValue("Fork", "runtime directory");
    String transDir = configFile.getValue("Fork", "transformation directory");
    String [] defDirs = new String [] {
        dbDir,
        cacheDir==null?dbDir+"/cache":cacheDir,
        workingDir==null?dbDir+"/jobs":workingDir,
        runtimeDir==null?dbDir+"/runtimeEnvironments":runtimeDir,
        transDir==null?dbDir+"/transformations":transDir
        };
    JTextField [] jtFields = new JTextField [defDirs.length];
    if(firstRun){
      jPanel.add(new JLabel("A configuration file "+GridPilot.userConfFile.getCanonicalPath()+
          " will be created."),
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
          (JFrame) SwingUtilities.getWindowAncestor(GridPilot.getClassMgr().getGlobalFrame().getRootPane()),
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
      choice = confirmBox.getConfirm("Setting up GridPilot",
          jPanel, new Object[] {"Continue",  "Cancel"}, icon, Color.WHITE);
    }
    catch(Exception e){
      e.printStackTrace();
    }
    
    if(choice!=0){
      return false;
    }
    
    // Create missing directories
    String [] newDirs = new String [defDirs.length];
    File [] newDirFiles = new File[newDirs.length];
    for(int i=0; i<defDirs.length; ++i){
      newDirs[i] = jtFields[i].getText();
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
      newDirs[i] = Util.replaceWithTildeLocally(Util.clearFile(newDirs[i]));
    }
    
    if(firstRun){
      configFile = new ConfigFile(GridPilot.defaultConfFileName);
      // Make temporary config file
      File tmpConfFile = (File) GridPilot.tmpConfFile.get(GridPilot.defaultConfFileName);       
       // Copy over config file
       LocalStaticShellMgr.copyFile(tmpConfFile.getCanonicalPath(), GridPilot.userConfFile.getCanonicalPath());
       tmpConfFile.delete();
    }
    else{
      configFile = GridPilot.getClassMgr().getConfigFile();
    }
    
    if(!defDirs[0].equals(newDirs[0]) ||
       !defDirs[1].equals(newDirs[1]) ||
       !defDirs[2].equals(newDirs[2]) ||
       !defDirs[3].equals(newDirs[3]) ||
       !defDirs[4].equals(newDirs[4])){
      // Set config entries
      /* [GridPilot] pull cache directory = ~/GridPilot/cache
       * [My_DB_local] database = hsql://localhost/~/GridPilot/My_DB
       * [Fork] working directory = ~/GridPilot/jobs
       * [Fork] runtime directory = ~/GridPilot/runtimeEnvironments
       * [Fork] transformation directory = ~/GridPilot/transformations
        
        userConfDir,
        userConfDir+"/cache",
        userConfDir+"/jobs",
        userConfDir+"/runtimeEnvironments",
        userConfDir+"/transformations"

       */
      configFile.setAttributes(
          new String [] {"GridPilot", "My_DB_local", "Fork", "Fork", "Fork"},
          new String [] {"pull cache directory", "database", "working directory", "runtime directory", "transformation directory"},
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
    
    return true;
  }
  
  
}
