package generateData;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;

/**
 * Created by test on 3/8/17.
 */
public class generateCoordinatesData {
    private Random length = new Random();

    public String RandomCoordinates() {
        int x_axis = length.nextInt((9999 - 0) + 1) + 0;
        int y_axis = length.nextInt((9999 - 0) + 1) + 0;
        return  x_axis + "," + y_axis;
    }
    public static void main(String[] Args) throws IOException {
        generateCoordinatesData a = new generateCoordinatesData();
        String fout1 = "Project3/input/coordinates";
        FileOutputStream fos1 = new FileOutputStream(fout1);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos1));
        //write headers in the first line
        bw.write(a.RandomCoordinates());
        bw.newLine();
        bw.close();
        // 100MB we need 11000000
        for (int i = 1; i <= 11000000; i++) {
            bw.write(a.RandomCoordinates());
            bw.newLine();
        }
        bw.close();
    }
}
