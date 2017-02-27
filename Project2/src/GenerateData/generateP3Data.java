package GenerateData;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;

/**
 * Created by test on 2/19/17.
 */
public class generateP3Data {
    private Random length = new Random();

    public String RandomCoordinates() {
        int x = length.nextInt((10000 - 1) + 1) + 1;
        int y = length.nextInt((10000 - 1) + 1) + 1;
        return  x + "," + y;
    }

    public static void main(String[] Args) throws IOException {
        generateP3Data a = new generateP3Data();
        String fout = "Project2/input/kmeans";
        FileOutputStream fos = new FileOutputStream(fout);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
        // 100MB we need 11000000
        for (int i = 1; i <= 1100; i++) {
            bw.write(a.RandomCoordinates());
            bw.newLine();
        }
        bw.close();
    }
}
