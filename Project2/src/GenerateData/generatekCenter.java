package GenerateData;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by test on 2/26/17.
 */
public class generatekCenter {
    public static List<String> getRandomCentroids(String f, Integer n) throws Exception{
        List<String> temp = new ArrayList<String>();
        List<String> ret = new ArrayList<String>();
        BufferedReader br = new BufferedReader(new FileReader(f));
        String line;
        while ((line = br.readLine())!=null) {
            temp.add(line);
        }
        Random centroids = new Random();
        for (int i=1;i<=n;i++) {
            int index = centroids.nextInt(temp.size());
            ret.add(temp.get(index));
        }
        temp = null;
        return ret;
    }

    public static void main(String[] args) throws Exception{
        if (args.length == 2) {
            List<String> a = getRandomCentroids(args[0],Integer.valueOf(args[1]));
            String fout = "Project2/input/kCenters";
            FileOutputStream fos = new FileOutputStream(fout);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
            for (int i = 1; i <= Integer.valueOf(args[1]); i++) {
                bw.write(a.get(i-1));
                bw.newLine();
            }
            bw.close();
        } else {
            System.exit(1);
        }
    }
}
