package gridpilot;

import javax.swing.JOptionPane;

import javax.swing.*;
import java.awt.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

/**
 * Shows dialog boxes with job outputs. <br>
 * <p><a href="ShowOutputsJobsDialog.java.html">see sources</a>
 */
public class ShowOutputsJobsDialog extends JOptionPane{

  private static final long serialVersionUID = 1L;
  private static Dimension winDim = new Dimension(600, 600);
  
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
  public static int[] show(Component parent, Vector jobs, String[] options) {

    JCheckBox cbForAll;
    if (jobs.size()>1){
      cbForAll = new JCheckBox("Apply my choice for all jobs");
    }
    else{
      cbForAll = null;
    }
    int[] choices = new int[jobs.size()];
    for(int i=0; i<jobs.size(); ++i){

      String[] files;
      Vector vFiles = new Vector();
      JobInfo job = (JobInfo) jobs.get(i);
      if(job.getStdOut()!=null){
          vFiles.add(job.getStdOut());
      }
      if(job.getStdErr()!=null){
        vFiles.add(job.getStdErr());
      }

      files = new String[vFiles.size()];
      for (int k=0; k<files.length; ++k){
        files[k] = vFiles.get(k).toString();
      }
      ShellMgr shell = null;
      try{
        shell = GridPilot.getClassMgr().getShellMgr(job);
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

      if(cbForAll!=null && cbForAll.isSelected()){
        int iSave = i;
        for (++i; i<jobs.size(); ++i){
          choices[i] = choices[iSave];
        }
      }
    }
    return choices;
  }

  public static void showTabs(Component parent, String header,
      final JobInfo job,
      final String[] filesPaths){
    showTabs(parent, header, job, filesPaths, null, null);
  }

  public static int showTabs(Component parent, String header,
                                  final ShellMgr shell,
                                  final String[] filesPaths,
                                  String[] options,
                                  JCheckBox cb){
    JPanel mainPanel = new JPanel(new BorderLayout());
    mainPanel.setPreferredSize(winDim);
    JTabbedPane outputs = new JTabbedPane();
    mainPanel.add(outputs, BorderLayout.CENTER);
    if(cb!=null){
      mainPanel.add(cb, BorderLayout.SOUTH);
    }
    Vector threads = new Vector();
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
                content = LocalStaticShellMgr.readFile(filesPaths[finalI]);
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

    outputs.setPreferredSize(winDim);

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
      Thread t = (Thread) threads.get(i);
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

  public static int showTabs(Component parent, String header,
      final JobInfo job,
      final String[] filesPaths,
      String[] options,
      JCheckBox cb){
    JPanel mainPanel = new JPanel(new BorderLayout());
    mainPanel.setPreferredSize(winDim);
    JTabbedPane outputs = new JTabbedPane();
    mainPanel.add(outputs, BorderLayout.CENTER);
    if(cb!=null){
      mainPanel.add(cb, BorderLayout.SOUTH);
    }
    Vector threads = new Vector();
    for(int i=0; i<filesPaths.length; ++i){
      if(filesPaths[i]!=null){
        final JPanel panel = new JPanel(new BorderLayout());
        JLabel label = new JLabel(filesPaths[i]);
        final JTextArea textArea = new JTextArea();
        textArea.setLineWrap(true);
        textArea.setWrapStyleWord(true);
        textArea.setEditable(false);
        JScrollPane scrollPane = new JScrollPane(textArea);
        panel.add(scrollPane, BorderLayout.CENTER);
        panel.add(label, BorderLayout.SOUTH);
        String tabName = filesPaths[i].substring(
            filesPaths[i].lastIndexOf("/") + 1);
        outputs.add(panel, tabName);
        final int finalI = i;
        Thread t = new Thread(){
          public void run(){
            JProgressBar pb = new JProgressBar();
            pb.setIndeterminate(true);
            textArea.setText("Please wait, I'm reading...");
            panel.add(pb, BorderLayout.NORTH);
            String content;
            try{
              //GridPilot.getClassMgr().getCSPluginMgr().getCurrentOutputs(job);
              RandomAccessFile f = new RandomAccessFile(filesPaths[finalI], "r");
              byte [] b  = new byte [(int)f.length()];
              f.readFully(b);
              content = new String(b);
              f.close();
            }
            catch (FileNotFoundException e){
              content = "This file (" + filesPaths[finalI] + ") doesn't exist";
              textArea.setForeground(Color.gray);
            }
            catch(IOException e){
              content = "IOExeption during reading of file " +
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

    outputs.setPreferredSize(winDim);

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
      Thread t = (Thread) threads.get(i);
      t.interrupt();
    }
    if (options==null){
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

  public static void showTabs(Component parent, String header,
                                   String[] filesPaths,
                                   String[] filesContents){
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
    JPanel mainPanel = new JPanel(new BorderLayout());
    mainPanel.setPreferredSize(winDim);
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