package simpledb;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.*;
import java.awt.image.BufferedImage;

public class Drawing extends Canvas {

    public static void main(String[] args) {
        String imagePath = "image.jpg";
        int width = 400;
        int height = 400;
        BufferedImage image = new BufferedImage(width,height,BufferedImage.TYPE_3BYTE_BGR);

        Graphics2D g = (Graphics2D) image.getGraphics();
        g.setStroke(new BasicStroke(3));
        g.setColor(Color.BLUE);
        g.drawRect(10,10,width-20,height-20);

        JLabel picLabel = new JLabel(new ImageIcon(image));
        JPanel jPanel = new JPanel();
        jPanel.add(picLabel);

        JFrame f = new JFrame();
        f.setSize(new Dimension(width, height));
        f.add(jPanel);
        f.setVisible(true);
    }
}
