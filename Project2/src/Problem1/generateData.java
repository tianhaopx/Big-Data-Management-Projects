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
        return  x_axis + "," + y_axis;
    }

    public String RandomRectangle(int n) {
        // up right
        int right = length.nextInt((10000 - 1) + 1) + 1;
        int up = length.nextInt((10000 - 1) + 1) + 1;
        // height & width
        // this part is a little bit tricky
        // if we set the range of the width and height to big
        // map will exceed the memory limits
        int width = length.nextInt((20 - 1) + 1) + 1;
        int height = length.nextInt((5 - 1) + 1) + 1;
        int left = right - width;
        int down = up - height;
        // final output like
        // rectangle#, down_l, down_r, up_l, up_r
        return "r" + n + "," + left + "," + down +","+right + "," + up;
    }

    public static void main(String[] Args) throws IOException {
        generateData a = new generateData();
        String fout1 = "Project2/input/coordinates";
        FileOutputStream fos1 = new FileOutputStream(fout1);
        BufferedWriter bw1 = new BufferedWriter(new OutputStreamWriter(fos1));
        // 100MB we need 11000000
        for (int i = 1; i <= 11000000; i++) {
            bw1.write(a.RandomCoordinates());
            bw1.newLine();
        }
        bw1.close();

        String fout2 = "Project2/input/rectangles";
        FileOutputStream fos2 = new FileOutputStream(fout2);
        BufferedWriter bw2 = new BufferedWriter(new OutputStreamWriter(fos2));
        // 100MB we need 4000000
        for (int i = 1; i <= 4000000; i++) {
            bw2.write(a.RandomRectangle(i));
            bw2.newLine();
        }
        bw2.close();
    }
}
