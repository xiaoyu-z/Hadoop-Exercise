import org.apache.commons.csv.*;
import org.apache.commons.lang.RandomStringUtils;

import java.io.*;
import java.nio.file.*;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class FileCreater {
    public static String file_name1 = "Customers.csv";
    public static String file_name2 = "Transactions.csv";

    public static void main(String[] args) throws IOException {

        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(file_name1));
            CSVPrinter csvWriter = new CSVPrinter(writer, CSVFormat.DEFAULT);
            for (int i = 1; i <= 50000; i++) {
                csvWriter.printRecord(customer(i));
            }
            csvWriter.flush();

            BufferedWriter trans_riter = new BufferedWriter(new FileWriter(file_name2));
            CSVPrinter transWriter = new CSVPrinter(trans_riter, CSVFormat.DEFAULT);
            for (int i = 1; i <= 5000000; i++) {
                transWriter.printRecord(transaction(i));
            }
            transWriter.flush();
        } catch (Exception e) {
            System.out.print(e);
        }

    }

    public static List<String> customer(int i) {
        Random random = new Random();
        int age = random.nextInt(61) + 10;
        int name_length = random.nextInt(11) + 10;
        String name = RandomStringUtils.random(name_length, true, false);
        String gender = "male";
        if (random.nextBoolean()) {
            gender = "female";
        }
        int country_code = random.nextInt(10) + 1;
        float salary_range = 9900.0f;
        DecimalFormat salary_format = new DecimalFormat("#.00");
        String salary = salary_format.format(random.nextFloat() * salary_range + 100.0f);
        //float salary = (float)(Math.round((random.nextFloat()*salary_range+100)*100)/100);
        return Arrays.asList(String.valueOf(i), name, String.valueOf(age), gender, String.valueOf(country_code), salary);

    }

    public static List<String> transaction(int j) {
        Random random = new Random();
        int cust_id = random.nextInt(50000) + 1;
        float transaction_range = 990.0f;
        DecimalFormat transaction_format = new DecimalFormat("#.00");
        String total_transaction = transaction_format.format(random.nextFloat() * transaction_range + 10.0f);
        int trans_num = random.nextInt(10) + 1;
        String text_list = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890. ";
        int text_length = random.nextInt(31) + 20;
        StringBuffer text = new StringBuffer();
        for (int k = 0; k < text_length; k++) {
            int index;
            if (k == 0 || k == text_length - 1) {
                index = random.nextInt(text_list.length() - 1);
            } else {
                index = index = random.nextInt(text_list.length());
            }
            text.append(text_list.charAt(index));
        }
        return Arrays.asList(String.valueOf(j), String.valueOf(cust_id), total_transaction, String.valueOf(trans_num), text.toString());
    }

}


