package data;

import java.io.*;
import java.util.Random;

public class GenerateTransactionData {
    static final String AB = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    private Random length = new Random();

    public String randomCustID(int min,int max) {
        int randomCustID = length.nextInt(max-min+1)+min;
        return Integer.toString(randomCustID);
    }

    public float randomTransTotal(float min, float max) {
        float range = max - min;
        float scaled = length.nextFloat() * range;
        float shifted = scaled + min;
        return shifted;
    }

    public String randomNumItems() {
        int randomNumItems = length.nextInt((10 - 1) + 1) + 1;
        return Integer.toString(randomNumItems);
    }

    public String randomDesc() {
        int randomLength = length.nextInt((50 - 20) + 1) + 10;
        String sb = "";
        for (int i = 0; i < randomLength; i++)
            sb += AB.charAt(length.nextInt(AB.length()));
        return sb;
    }

    public String randomTransactionInstance(int n) {
        String id = Integer.toString(n);
        String CustID = randomCustID(1,50000);
        float TransTotal = randomTransTotal(10f, 1000f);
        String NumItems = randomNumItems();
        String Desc = randomDesc();
        String str = id + ',' + CustID + ',' + TransTotal + ',' + NumItems + ',' + Desc;
        return str;
    }

    public static void main(String[] args) throws IOException {
        GenerateTransactionData a = new GenerateTransactionData();
        File fout = new File("Project1/input/transaction");
        FileOutputStream fos = new FileOutputStream(fout);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
        for (int i = 1; i <= 5000000; i++) {
            bw.write(a.randomTransactionInstance(i));
            bw.newLine();
        }
        bw.close();
    }
}
