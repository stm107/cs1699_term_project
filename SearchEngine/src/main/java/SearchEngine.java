import java.io.*;
import java.util.*;
import com.jcraft.jsch.*;


public class SearchEngine{
    public static String option = "";

    public static void main(String[] args) throws Exception {
        printMenu();

    }
    private static void runCommand(String command){
        try {
            ProcessBuilder processBuilder = new ProcessBuilder().inheritIO();
            processBuilder.command(command);

        } catch (Exception e){
            System.out.println("EXCEPTION: " + e);
        }
    }

    private static void printMenu() throws Exception {
        System.out.println("Please from the following options: ");
        System.out.println("To search ONLY Victor Hugo, Press 1");
        System.out.println("To search ONLY Leo Tolstoy, Press 2");
        System.out.println("To search ONLY William Shakespeare, Press 3");
        System.out.println("To search all three, press 4");
        System.out.println("To exit, press 5");
        System.out.print("Please enter your selection: ");

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        option = reader.readLine();

        switch(option){
            case "1":
                index("Hugo");

            case "2":
                index("Tolstoy");

            case "3":
                index("shakespeare");

            case "4":
                index("*");

            case "5":
                System.out.println("Thank you for Searching! Bye!");
                System.exit(0);

            default:
                System.out.println("I didn't understand your input.");
                printMenu();
        }
    }

    private static void index(String author) throws Exception {
        System.out.println("Indexing Selected Author(s)...");
        JSch jsch = new JSch();
        Session session = jsch.getSession("sean.mizerski", "34.74.156.245", 22);
        String privateKey = "~/.ssh/id_rsa";
        jsch.addIdentity(privateKey);
        Properties prop = new Properties();
        prop.put("StrictHostKeyChecking", "no");
        session.setConfig(prop);
        session.connect();
        System.out.println("Connected to Spark Machine");
        System.out.println("Submitting Spark Job. Please wait...");

        long startTime = System.nanoTime();

        ChannelExec ce = (ChannelExec) session.openChannel("exec");

        ce.setCommand("spark-submit --class Driver gs://invertindex-data/invertedindex_2.11-1.0.jar -a " + author);
        ce.setErrStream(System.err);
        OutputStream out = ce.getOutputStream();
        ce.connect();

        BufferedReader reader = new BufferedReader(new InputStreamReader(ce.getInputStream()));
        String line;

        long endTime;

        // Select Search or TopN
        while ((line = reader.readLine()) != null) {
            endTime = System.nanoTime();
            System.out.println("Indexing took " + (endTime - startTime) + " ms!");
            System.out.println(line);
            break;
        }

        BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
        String inputStr = input.readLine();
        out.write((inputStr + "\n").getBytes());
        out.flush();

        // Select Term or N
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
            break;
        }
        input = new BufferedReader(new InputStreamReader(System.in));
        inputStr = input.readLine();
        out.write((inputStr + "\n").getBytes());
        out.flush();

        startTime = System.nanoTime();

        // Print the result!
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }

        endTime = System.nanoTime();
        System.out.println("Operation took " + (endTime - startTime) + " ms!");

        ce.disconnect();
        session.disconnect();

        printMenu();
    }
}