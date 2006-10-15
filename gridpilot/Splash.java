package gridpilot;

import javax.swing.*;

import java.awt.*;
import java.awt.event.*;
import java.net.URL;


/**
 * Little window shown during loading.
 */
public class Splash{

  JWindow win  = new JWindow();
  JProgressBar pb = new JProgressBar();

  private String resourcesPath;

  public Splash(String _resourcesPath){
    new Splash(resourcesPath, "splash.png");
  }
  
  public Splash(String _resourcesPath, String image){
    resourcesPath = _resourcesPath;

    URL imgURL=null;
    try{
      imgURL = GridPilot.class.getResource(resourcesPath + image);
    }
    catch(Exception e){
      Debug.debug("Could not find image "+ resourcesPath + image, 3);
    }
    JLabel label;
    if(imgURL != null){
      Debug.debug("Loading image "+imgURL, 3);
      label = new JLabel(new ImageIcon(imgURL));
    }
    else{
      Debug.debug("Couldn't find file: " + imgURL, 2);
      label = new JLabel(new ImageIcon("./" + resourcesPath + image));
    }
    JPanel panel = new JPanel(new BorderLayout());
    panel.add(label, BorderLayout.CENTER);


    pb.setIndeterminate(true);
    pb.setStringPainted(true);

    pb.addMouseListener(new MouseAdapter(){
      public void mouseClicked(MouseEvent e){
        System.out.println("Loading interrupted");
        System.exit(0);
      }
    });
    panel.add(pb, BorderLayout.SOUTH);
    panel.updateUI();

    win.getContentPane().add(panel);
    win.pack();

    Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
    win.setLocation(screenSize.width/2 - win.getSize().width/2,
                    screenSize.height/2 - win.getSize().height/2);

  }

  public void show(String msg){
    pb.setString(msg);

    win.setVisible(true);
  }

  public void show(){
    win.setVisible(true);
  }

  public void hide(){
    win.setVisible(false);
  }

  public void stopSplash(){
    win.dispose();
  }
}
