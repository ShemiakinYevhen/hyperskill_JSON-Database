package client;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.gson.Gson;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class Main {

    private static final String ADDRESS = "127.0.0.1";
    private static final int PORT = 34567;
    private static final Gson GSON = new Gson();
    private static final String REQUESTS_DIRECTORY_PATH = "/src/client/data/";

    @Parameter(names = "-t")
    static String commandType;
    @Parameter(names = "-k")
    static String key;
    @Parameter(names = "-v")
    static String value;
    @Parameter(names = "-in")
    static String filePath;

    public static void main(String[] args) {
        System.out.println("Client started!");

        Main main = new Main();
        JCommander.newBuilder().addObject(main).build().parse(args);

        try (Socket socket = new Socket(InetAddress.getByName(ADDRESS), PORT);
             DataInputStream input = new DataInputStream(socket.getInputStream());
             DataOutputStream output = new DataOutputStream(socket.getOutputStream())) {
            String outputMessage;

            if (filePath != null) {
                outputMessage = new String(Files.readAllBytes(Paths
                        .get(System.getProperty("user.dir") + REQUESTS_DIRECTORY_PATH + filePath)));
            } else {
                Map<String, String> commandData = new HashMap<>();
                commandData.put("type", commandType);
                switch (commandType) {
                    case "get":
                    case "delete":
                        commandData.put("key", String.valueOf(key));
                        break;
                    case "set":
                        commandData.put("key", String.valueOf(key));
                        commandData.put("value", value);
                        break;
                }
                outputMessage = GSON.toJson(commandData);
            }

            System.out.println("Sent: " + outputMessage);
            output.writeUTF(outputMessage);

            String inputMessage = GSON.fromJson(input.readUTF(), String.class);
            System.out.println("Received: " + inputMessage);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}