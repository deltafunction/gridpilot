package gridpilot;

import java.awt.Component;
import java.awt.Graphics;
import javax.swing.Icon;

/*
 Icon class to be used in Panels for the (X) image to close a monitoring panel
 */
public class IconProxy implements Icon {

    private int x;
    private int y;
    private int xMax;
    private int yMax;
    private int width;
    private int height;
    private Icon icon;
    
    public IconProxy(Icon icon) {
        this.icon = icon;
        width  = icon.getIconWidth();
        height = icon.getIconHeight();
    }
    
    public int getIconWidth() {
        return width;
    }
    
    public int getIconHeight() {
        return height;
    }
    
    public void paintIcon(Component c, Graphics g, int x, int y) {
        this.x = x;
        this.y = y;
        xMax = x + width;
        yMax = y + height;
        icon.paintIcon(c, g, x, y);
    }
    
    public boolean contains(int x, int y) {
        return x >= this.x && x <= xMax && y >= this.y && y <= yMax;
    }
}
