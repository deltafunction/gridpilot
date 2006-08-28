package gridpilot;

import javax.swing.*;

import gridpilot.GridPilot;
import gridpilot.Debug;

import java.awt.event.*;
import java.awt.*;
import java.net.URL;

/**
 * The <code>StatusBar</code> implements a status bar in two parts : a label on the left,
 * and a progress bar on the right. <p>
 * The label can be used in two mode : in the first one, the label is shown until another label
 * overwrite this label, in the second mode, the label is shown until a specified delay
 * is elapsed or another label is set. <br>
 * The progress bar has two mode as well : the first one is a "normal" progress bar which is
 * created outside this status bar, and the second one is an undeterminate progress bar.
 */
public class StatusBar extends JPanel {

  private static final long serialVersionUID = 1L;
  private JLabel label = new JLabel();
  private JProgressBar indeterminatePB = new JProgressBar();

  java.util.Stack stackProgressBar = new java.util.Stack();

  private Frame frame;
  private ImageIcon save = new ImageIcon();
  private ImageIcon waitingIcon;

  private Timer timerLabel = new Timer(0, new ActionListener(){
    public void actionPerformed(ActionEvent e){
      //removeLabel();
    }
  });

  public StatusBar(){
    setLayout(new BorderLayout());

    timerLabel.setRepeats(false);

    add(new JLabel(" "), BorderLayout.WEST);
    
    URL imgURL=null;
    try{
      imgURL = GridPilot.class.getResource(GridPilot.resourcesPath + "wait.png");
      waitingIcon = new ImageIcon(imgURL);
    }
    catch(Exception e){
      Debug.debug("Could not find image "+ GridPilot.resourcesPath + "wait.png", 3);
      waitingIcon = new ImageIcon();
    }
  }

  /**
   * Sets this JLabel on the left on this status bar
   */
  public synchronized void setLabel(JLabel _label){
    if(timerLabel.isRunning())
      timerLabel.stop();

    removeLabel();

    label = _label;

    add(label, BorderLayout.CENTER);

    updateUI();
  }

  /**
   * Sets this String on the left of this status bar
   */
  public synchronized void setLabel(String s){
    setLabel(new JLabel(s));
  }

  /**
   * Sets this JLabel on the left on this status bare during 'secTime' seconds
   */
  public synchronized void setLabel(JLabel _label, int secTime){
    setLabel(_label);

    timerLabel.setInitialDelay(secTime * 1000);
    timerLabel.start();
  }

  /**
   * Sets this String on the left of this status bar during 'secTime' seconds
   */
  public synchronized void setLabel(String _label, int secTime){
    setLabel(new JLabel(_label), secTime);

  }

  /**
   * Sets this JProgressBar on the rigth of this status bar. <br>
   */
  public synchronized void setProgressBar(JProgressBar pb) {
    if(!stackProgressBar.empty()){
      remove((JProgressBar) stackProgressBar.peek());
    }
    stackProgressBar.push(pb);
    add(pb, BorderLayout.EAST);
  }

  /**
   * Removes the current label
   */
  public synchronized void removeLabel(){
    remove(label);

    label = new JLabel(" ");
    add(label, BorderLayout.CENTER);
    updateUI();
  }

  /**
   * Removes the current progress bar. <p>
   * If another progress bar was shown before that <code>p</code> was set, the
   * previous progress bar is re-shown (if this old progress bar hasn't been removed)
   */
  public synchronized void removeProgressBar(JProgressBar p){
    if(p==null)
      return;
    remove(p);

    if(stackProgressBar.contains(p))
      stackProgressBar.remove(p);

    if(!stackProgressBar.isEmpty())
      setProgressBar((JProgressBar) stackProgressBar.pop());

    updateUI();
  }


  /**
   * Sets an undeterminate progress bar on the right of this progress bar
   */
  public synchronized void animateProgressBar(){
    setProgressBar(indeterminatePB);
    indeterminatePB.setIndeterminate(true);
    if(frame == null)
      frame = JFrame.getFrames()[1]; // ?? dangerous !!!

    save.setImage(frame.getIconImage());
    frame.setIconImage(waitingIcon.getImage());
  }

  /**
   * Removes this animated progress bar
   */
  public synchronized void stopAnimation(){
    indeterminatePB.setIndeterminate(false);
    removeProgressBar(indeterminatePB);
    indeterminatePB.removeAll();
    frame.setIconImage(save.getImage());
  }

  /**
   * Adds a MouseListener to the animated progres bar. <p>
   */
  public void addIndeterminateProgressBarMouseListener(MouseListener ml){
    indeterminatePB.addMouseListener(ml);
  }

  /**
   * Adds a ToolTip to animated progres bar. <p>
   */
  public void setIndeterminateProgressBarToolTip(String s){
    indeterminatePB.setToolTipText(s);
  }
}

