package Problem1;

import java.io.*;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Random;

/**
 * Created by test on 2/16/17.
 */
public class generateData {
    private Random length = new Random();
    public String RandomCoordinates() {
        int x_axis = length.nextInt((10000 - 1) + 1) + 1;
        int y_axis = length.nextInt((10000 - 1) + 1) + 1;

        return Arrays.toString(new int[]{x_axis,y_axis});
    }

    public String RandomRectangle() {
        // up right
        int x_axis = length.nextInt((10000 - 1) + 1) + 1;
        int y_axis = length.nextInt((10000 - 1) + 1) + 1;
        String up_r = Arrays.toString(new int[]{x_axis,y_axis});
        // height & width
        int width = length.nextInt((x_axis - 1) + 1) + 1;
        int height = length.nextInt((y_axis - 1) + 1) + 1;
        // up left
        String up_l = Arrays.toString(new int[]{x_axis-width,y_axis});
        // down left
        String down_l = Arrays.toString(new int[]{x_axis-width,y_axis-height});
        // down right
        String down_r = Arrays.toString(new int[]{x_axis,y_axis-height});
        return down_l+","+down_r+","+up_l+","+up_r;
    }

    public static void main(String[] Args) throws IOException {
        generateData a = new generateData();
        File fout1 = new File("Project2/input/coordinates");
        FileOutputStream fos1 = new FileOutputStream(fout1);
        BufferedWriter bw1 = new BufferedWriter(new OutputStreamWriter(fos1));
        for (int i = 1; i <= 8000000; i++) {
            bw1.write(a.RandomCoordinates());
            bw1.newLine();
        }
        bw1.close();

        File fout2 = new File("Project2/input/rectangles");
        FileOutputStream fos2 = new FileOutputStream(fout2);
        BufferedWriter bw2 = new BufferedWriter(new OutputStreamWriter(fos2));
        for (int i = 1; i <= 2500000; i++) {
            bw2.write(a.RandomRectangle());
            bw2.newLine();
        }
        bw2.close();
    }
}
