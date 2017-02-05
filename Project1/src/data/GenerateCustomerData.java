package data;

import java.io.*;
import java.util.Random;

public class GenerateCustomerData {
    static final String AB = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    private Random length = new Random();

    public String randomName() {
        int randomLength = length.nextInt((20 - 10) + 1) + 10;
        String sb = "";
        for (int i = 0; i < randomLength; i++)
            sb += AB.charAt(length.nextInt(AB.length()));
        return sb;
    }

    public String randomAge() {
        int randomAge = length.nextInt((70 - 10) + 1) + 10;
        return Integer.toString(randomAge);
    }

    public String randomCountry() {
        int randomCountry = length.nextInt((10 - 1) + 1) + 1;
        return Integer.toString(randomCountry);
    }

    public String randomSalary() {
        float randomSalary = length.nextFloat() * ((10000.0f - 100.0f) + 1.0f) + 100.0f;
        return Float.toString(randomSalary);
    }

    public String randomCustomerInstance(int n) {
        String id = Integer.toString(n);
        String name = randomName();
        String age = randomAge();
        String country = randomCountry();
        String salary = randomSalary();
        String str = id + ',' + name + ',' + age + ',' + country + ',' + salary;
        return str;
    }

    public static void main(String[] args) throws IOException {
        GenerateCustomerData a = new GenerateCustomerData();
        File fout = new File("Project1/input/customer");
        FileOutputStream fos = new FileOutputStream(fout);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
        for (int i = 1; i <= 50000; i++) {
            bw.write(a.randomCustomerInstance(i));
            bw.newLine();
        }
        bw.close();
    }
}
