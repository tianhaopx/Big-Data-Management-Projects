package GenerateData;

import java.io.*;
import java.util.Random;

/**
 * Created by test on 2/16/17.
 */
public class generateP1Data {
    private Random length = new Random();

    public String RandomCoordinates() {
        int x_axis = length.nextInt((10000 - 1) + 1) + 1;
        int y_axis = length.nextInt((10000 - 1) + 1) + 1;
        return  x_axis + "," + y_axis;
    }

    public String RandomRectangle(int n) {
        // top left
        int left = length.nextInt((10000 - 1) + 1) + 1;
        int up = length.nextInt((10000 - 1) + 1) + 1;
        // height & width
        // this part is a little bit tricky
        // if we set the range of the width and height to big
        // map will exceed the memory limits
        int width = length.nextInt((20 - 1) + 1) + 1;
        int height = length.nextInt((5 - 1) + 1) + 1;
        // final output like
        // rectangle#, up_left_x, up_left_y, width, height
        return "r" + n + "," + left + "," + up +","+width + "," + height;
    }

    public static void main(String[] Args) throws IOException {
        generateP1Data a = new generateP1Data();
        String fout1 = "Project2/input/coordinates";
        FileOutputStream fos1 = new FileOutputStream(fout1);
        BufferedWriter bw1 = new BufferedWriter(new OutputStreamWriter(fos1));
        // 100MB we need 11000000
        for (int i = 1; i <= 110000; i++) {
            bw1.write(a.RandomCoordinates());
            bw1.newLine();
        }
        bw1.close();

        String fout2 = "Project2/input/rectangles";
        FileOutputStream fos2 = new FileOutputStream(fout2);
        BufferedWriter bw2 = new BufferedWriter(new OutputStreamWriter(fos2));
        // 100MB we need 4000000
        for (int i = 1; i <= 40000; i++) {
            bw2.write(a.RandomRectangle(i));
            bw2.newLine();
        }
        bw2.close();
    }
}
