package gridpilot;

import gridfactory.common.Debug;
import gridfactory.common.LocalStaticShell;
import gridfactory.common.Shell;

import javax.swing.JOptionPane;
import javax.swing.JProgressBar;
import javax.swing.SwingUtilities;

import javax.swing.*;
import java.awt.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * Shows dialog boxes with job outputs. <br>
 * <p><a href="ShowOutputsJobsDialog.java.html">see sources</a>
 */
public class ShowOutputsJobsDialog extends JOptionPane{

  private static final long serialVersionUID = 1L;
  private static Dimension WINDOW_DIMENSION = new Dimension(600, 600);
  
  /**
   * Shows an option dialog for each job in 'jobs', with button for each String 'options'
   * Dialog contains job stdOut, job stdErr, and if withValid is true, validation stdOut,
   * validation StdErr. <br>
   * If 'jobs' length is > 1, this dialog will contain a check box which let to
   * apply same choice for all following jobs.
   *
       * @return a int array (same length as 'jobs') with at i, the choice for jobs[i],
   * or -1 if windows was closed
   */
  public static int[] show(Component parent, Set<MyJobInfo> jobs, String[] options)
     throws InterruptedException, InvocationTargetException {

    JCheckBox cbForAll;
    if (jobs.size()>1){
      cbForAll = new JCheckBox("Apply choice for all jobs");
    }
    else{
      cbForAll = null;
    }
    int[] choices = new int[jobs.size()];
    MyJobInfo job;
    String[] files;
    Vector<String> vFiles;
    int i = 0;
    for(Iterator<MyJobInfo> it=jobs.iterator(); it.hasNext();){
      job = (MyJobInfo) it.next();
      vFiles = new Vector<String>();
      if(job.getOutTmp()!=null){
          vFiles.add(job.getOutTmp());
      }
      if(job.getErrTmp()!=null){
        vFiles.add(job.getErrTmp());
      }

      files = new String[vFiles.size()];
      for (int k=0; k<files.length; ++k){
        files[k] = vFiles.get(k).toString();
      }
      Shell shell = null;
      try{
        shell = GridPilot.getClassMgr().getShell(job);
      }
      catch(Exception e){
        Debug.debug("WARNING: no shell manager: "+e.getMessage(), 1);
        //e.printStackTrace();
      }

      choices[i] = showTabs(parent, "Job " + job.getName(),
         shell, files, options, cbForAll);
      
      // abort if window is closed
      if(choices[i]==CLOSED_OPTION){
        return null;
      }

      if(cbForAll!=null && cbForAll.isSelected() && jobs.size()>i+1){
        for(int j=i+1; j<jobs.size(); ++j){
          choices[j] = choices[i];
        }
        break;
      }
      ++i;
    }
    return choices;
  }

  public static void showTabs(Component parent, String header,
      final MyJobInfo job, final String[] filesPaths)
     throws InterruptedException, InvocationTargetException{
    showTabs(parent, header, job, filesPaths, null, null);
  }
  
  public static int showTabs(final Component parent, final String header,
      final Shell shell, final String[] filesPaths,
      final String[] options, final JCheckBox cb) throws InterruptedException,
      InvocationTargetException{
    if(SwingUtilities.isEventDispatchThread()){
      return doShowTabs(parent, header, shell, filesPaths, options, cb);
    }
    else{
      MyResThread rt = new MyResThread(){
        private int res;
        public void run(){
          try{
            res = doShowTabs(parent, header, shell, filesPaths, options, cb);
          }
          catch(Exception ex){
            Debug.debug("Could not create panel ", 1);
            ex.printStackTrace();
          }
        }
        public int getIntRes(){
          return res;
        }
      };
      SwingUtilities.invokeAndWait(rt);
      return rt.getIntRes();
    }
  }

  public static int doShowTabs(Component parent, String header,
                                  final Shell shell,
                                  final String[] filesPaths,
                                  String[] options,
                                  JCheckBox cb){
    JPanel mainPanel = new JPanel(new BorderLayout());
    mainPanel.setPreferredSize(WINDOW_DIMENSION);
    JTabbedPane outputs = new JTabbedPane();
    mainPanel.add(outputs, BorderLayout.CENTER);
    if(cb!=null){
      mainPanel.add(cb, BorderLayout.SOUTH);
    }
    Vector<Thread> threads = new Vector<Thread>();
    for(int i=0; i<filesPaths.length; ++i){
      if(filesPaths[i]!=null){
        final JPanel panel = new JPanel(new BorderLayout());
        JLabel label = new JLabel(filesPaths[i]);
        final JTextArea textArea = new JTextArea(){
          private static final long serialVersionUID=1L;
          public java.awt.Dimension getPreferredSize(){
            java.awt.Dimension dim = super.getPreferredSize();
            dim.setSize(0, dim.height);
            return dim;
          }
        };
        textArea.setLineWrap(true);
        textArea.setWrapStyleWord(true);
        textArea.setEditable(false);
        JScrollPane scrollPane = new JScrollPane(textArea);
        panel.add(scrollPane, BorderLayout.CENTER);
        panel.add(label, BorderLayout.SOUTH);
        String tabName = "";
        if(filesPaths[i].lastIndexOf("/")>0){
          tabName = filesPaths[i].substring(filesPaths[i].lastIndexOf("/") + 1);
        }
        else{
          tabName = filesPaths[i].substring(filesPaths[i].lastIndexOf(File.separator) + 1);
        }
        outputs.add(panel, tabName);
        final int finalI = i;
        Thread t = new Thread() {
          public void run() {
            JProgressBar pb = new JProgressBar();
            pb.setIndeterminate(true);
            textArea.setText("Please wait, reading...");
            panel.add(pb, BorderLayout.NORTH);
            String content;
            try{
              if(shell == null){
                content = LocalStaticShell.readFile(filesPaths[finalI]);
              }
              else{
                content = shell.readFile(filesPaths[finalI]);
              }
            }
            catch (FileNotFoundException e){
              content = "This file (" + filesPaths[finalI] + ") doesn't exist";
              textArea.setForeground(Color.gray);
            }
            catch(Exception e){
              content = "Exeption during reading of file " +
                  filesPaths[finalI] + " : " +
                  e.getMessage();
              textArea.setForeground(Color.gray);
            }
            textArea.setText(content);
            panel.remove(pb);
            panel.updateUI();
            Debug.debug("end of thread for " + filesPaths[finalI], 2);
          }
        };
        t.start();
        threads.add(t);
      }
    }

    outputs.setPreferredSize(WINDOW_DIMENSION);

    JOptionPane pane;
    if (options==null){
      pane = new JOptionPane(mainPanel, JOptionPane.INFORMATION_MESSAGE);
    }
    else{
      pane = new JOptionPane(mainPanel, QUESTION_MESSAGE, DEFAULT_OPTION, null,
                             options);
    }
    JDialog dialog = pane.createDialog(parent, header);
    dialog.setResizable(true);
    dialog.setVisible(true);
    dialog.dispose();
    for (int i=0; i<threads.size(); ++i) {
      Thread t = threads.get(i);
      t.interrupt();
    }
    if(options==null){
      return CLOSED_OPTION;
    }
    Object selectedValue = pane.getValue();
    if(selectedValue==null){
      Debug.debug("Window probably closed, returning "+CLOSED_OPTION, 3);
      return CLOSED_OPTION;
    }
    for(int i=0; i<options.length; ++i){
      if(options[i]==selectedValue){
        Debug.debug("Returning "+i, 3);
        return i;
      }
    }
    return CLOSED_OPTION;
  }

  public static int showTabs(final Component parent, final String header,
      final MyJobInfo job, final String[] filesPaths,
      final String[] options, final JCheckBox cb) throws InterruptedException,
      InvocationTargetException{
    if(SwingUtilities.isEventDispatchThread()){
      return doShowTabs(parent, header, job, filesPaths, options, cb);
    }
    else{
      MyResThread rt = new MyResThread(){
        private int res;
        public void run(){
          try{
            res =  doShowTabs(parent, header, job, filesPaths, options, cb);
          }
          catch(Exception ex){
            Debug.debug("Could not create panel ", 1);
            ex.printStackTrace();
          }
        }
        public int getIntRes(){
          return res;
        }
      };
      SwingUtilities.invokeAndWait(rt);
      return rt.getIntRes();
    }
  }
  
  private static JPanel createPanel(final String text){
    if(SwingUtilities.isEventDispatchThread()){
      return doCreatePanel(text);
    }
    MyResThread rt = new MyResThread(){
      JPanel panel;
      public void run(){
        panel = doCreatePanel(text);
      }
      public JPanel getJPanelRes(){
        return panel;
      }
    };
    try{
      SwingUtilities.invokeAndWait(rt);
    }
    catch(Exception e1){
      e1.printStackTrace();
    }
    return rt.getJPanelRes();
  }
  
  private static JPanel doCreatePanel(String text){
    JPanel panel = new JPanel(new BorderLayout());
    JLabel label = new JLabel(text);
    JTextArea textArea = new JTextArea();
    textArea.setLineWrap(true);
    textArea.setWrapStyleWord(true);
    textArea.setEditable(false);
    JScrollPane scrollPane = new JScrollPane(textArea);
    panel.add(scrollPane, BorderLayout.CENTER);
    panel.add(label, BorderLayout.SOUTH);
    JProgressBar pb = new JProgressBar();
    pb.setIndeterminate(true);
    textArea.setText("Please wait, I'm reading...");
    panel.add(pb, BorderLayout.NORTH);
    return panel;
  }
  
  /**
   * 'panel' must be a JPanel created with createPanel
   * @param jta
   * @param text
   * @param boolean err
   */
  private static void setText(final JPanel mainPanel, final String tabName,
      final JPanel panel, final String text, final boolean err){
    if(SwingUtilities.isEventDispatchThread()){
      doSetText(mainPanel, tabName, panel, text, err);
      return;
    }
    try{
      SwingUtilities.invokeAndWait(
        new Runnable(){
          public void run(){
            doSetText(mainPanel, tabName, panel, text, err);
          }
        }
      );
    }
    catch(Exception e){
      e.printStackTrace();
    }
  }
  
  private static void doSetText(JPanel mainPanel, String tabName,
      JPanel panel, String text, boolean err){
    JTextArea textArea = ((JTextArea) ((JScrollPane) panel.getComponent(0)).getViewport().getComponent(0));
    textArea.setText(text);
    panel.remove(2);
    panel.updateUI();
    if(err){
      textArea.setForeground(Color.gray);
    }
    JTabbedPane outputs = (JTabbedPane) mainPanel.getComponent(0);
    outputs.add(panel, tabName);
  }
  
  private static JPanel createMainPanel(final JCheckBox cb){
    if(SwingUtilities.isEventDispatchThread()){
      return doCreateMainPanel(cb);
    }
    MyResThread rt = new MyResThread(){
      JPanel mainPanel;
      public void run(){
        mainPanel = doCreateMainPanel(cb);
      }
      public JPanel getJPanelRes(){
        return mainPanel;
      }
    };
    try{
      SwingUtilities.invokeAndWait(rt);
    }
    catch(Exception e1){
      e1.printStackTrace();
    }
    return rt.getJPanelRes();
  }
  
  private static JPanel doCreateMainPanel(JCheckBox cb){
    JPanel mainPanel = new JPanel(new BorderLayout());
    mainPanel.setPreferredSize(WINDOW_DIMENSION);
    JTabbedPane outputs = new JTabbedPane();
    outputs.setPreferredSize(WINDOW_DIMENSION);
    mainPanel.add(outputs, BorderLayout.CENTER);
    if(cb!=null){
      mainPanel.add(cb, BorderLayout.SOUTH);
    }
    return mainPanel;
  }
  
  public static int doShowTabs(Component parent, String header,
      final MyJobInfo job, final String[] filesPaths,
      String[] options, JCheckBox cb){
    final JPanel mainPanel = createMainPanel(cb);
    Vector<Thread> threads = new Vector<Thread>();
    if(filesPaths==null){
      String content = "This job definition does not appear to have any associated scripts.";
      JPanel panel = createPanel("No scripts");
      setText(mainPanel, "No scripts", panel, content, true);
    }
    else{
      for(int i=0; i<filesPaths.length; ++i){
        if(filesPaths[i]!=null){
          final JPanel panel = createPanel(filesPaths[i]);
          final String tabName = filesPaths[i].substring(
              filesPaths[i].lastIndexOf("/") + 1);
          final int finalI = i;
          Thread t = new Thread(){
            public void run(){
              String content;
              boolean err = false;
              try{
                //GridPilot.getClassMgr().getCSPluginMgr().getCurrentOutputs(job);
                RandomAccessFile f = new RandomAccessFile(filesPaths[finalI], "r");
                byte [] b  = new byte [(int)f.length()];
                f.readFully(b);
                content = new String(b);
                f.close();
              }
              catch(FileNotFoundException e){
                content = "This file (" + filesPaths[finalI] + ") doesn't exist";
                err = true;
              }
              catch(IOException e){
                content = "IOExeption during reading of file " +
                    filesPaths[finalI] + " : " +
                    e.getMessage();
                err = true;
              }
              setText(mainPanel, tabName, panel, content, err);
              Debug.debug("end of thread for " + filesPaths[finalI], 2);
            }
          };
          t.start();
          threads.add(t);
        }
      }
    }


    JOptionPane pane;
    if (options==null){
      pane = new JOptionPane(mainPanel, JOptionPane.INFORMATION_MESSAGE);
    }
    else{
      pane = new JOptionPane(mainPanel, QUESTION_MESSAGE, DEFAULT_OPTION, null,
                             options);
    }
    JDialog dialog = pane.createDialog(parent, header);
    dialog.setResizable(true);
    dialog.setVisible(true);
    dialog.dispose();
    for (int i=0; i<threads.size(); ++i) {
      Thread t = threads.get(i);
      t.interrupt();
    }
    if(options==null){
      return CLOSED_OPTION;
    }
    Object selectedValue = pane.getValue();
    if(selectedValue==null){
      return CLOSED_OPTION;
    }
    for(int i=0; i<options.length; ++i){
      if(options[i]==selectedValue){
        return i;
      }
    }
    return CLOSED_OPTION;
  }

  public static void showTabs(final Component parent, final String header,
      final String[] filesPaths, final String[] filesContents){
    if(filesPaths==null || filesContents==null){
      Debug.debug(
          "fullPaths == null || filesContents == null", 3);
      return;
    }
    if (filesPaths.length!=filesContents.length) {
      Debug.debug(
          "fullPaths.length != filesContents", 3);
      return;
    }
    if(SwingUtilities.isEventDispatchThread()){
      doShowTabs(parent, header, filesPaths, filesContents);
    }
    else{
      SwingUtilities.invokeLater(
        new Runnable(){
          public void run(){
            try{
              doShowTabs(parent, header, filesPaths, filesContents);
            }
            catch(Exception ex){
              Debug.debug("Could not create panel ", 1);
              ex.printStackTrace();
            }
          }
        }
      );
    }
  }
  
  public static void doShowTabs(Component parent, String header,
     String[] filesPaths, String[] filesContents){
    final JPanel mainPanel = new JPanel(new BorderLayout());
    mainPanel.setPreferredSize(WINDOW_DIMENSION);
    JTabbedPane outputs = new JTabbedPane();
    mainPanel.add(outputs, BorderLayout.CENTER);
    for (int i=0; i<filesPaths.length; ++i) {
      if(filesPaths[i]==null){
        filesPaths[i] = "";
      }     
      JPanel panel = new JPanel(new BorderLayout());
      JLabel label = new JLabel(filesPaths[i]);
      JTextArea textArea = new JTextArea();
      textArea.setLineWrap(true);
      textArea.setWrapStyleWord(true);
      textArea.setEditable(false);
      textArea.setText(filesContents[i]);
      JScrollPane scrollPane = new JScrollPane(textArea);
      panel.add(scrollPane, BorderLayout.CENTER);
      panel.add(label, BorderLayout.SOUTH);
      String tabName = filesPaths[i].substring(
          filesPaths[i].lastIndexOf("/") + 1);
      outputs.add(panel, tabName);
    }
    showMessageDialog(parent, mainPanel, header, INFORMATION_MESSAGE);
  }
}