package server;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RequestProcessor extends Thread {

    private final String inputMessage;
    private final DataOutputStream output;

    private static final Gson GSON = new Gson();
    private static final String PATH_TO_DB_FILE = "/src/server/data/db.json";

    private static final ReadWriteLock LOCK = new ReentrantReadWriteLock();
    private static final Lock READ_LOCK = LOCK.readLock();
    private static final Lock WRITE_LOCK = LOCK.writeLock();

    public RequestProcessor(String inputMessage, DataOutputStream output) {
        this.inputMessage = inputMessage;
        this.output = output;
    }

    public void run() {
        System.out.println("Received: " + inputMessage);
        JsonElement commandData = GSON.fromJson(inputMessage, JsonObject.class);
        String outputMessage = "";
        JsonElement responseData = new JsonObject();

        try {
            switch (commandData.getAsJsonObject().get("type").toString().replaceAll("\"", "")) {
                case "set":
                    setCellValue(commandData);
                    responseData.getAsJsonObject().addProperty("response", "OK");
                    outputMessage = GSON.toJson(responseData);
                    break;
                case "get":
                    responseData.getAsJsonObject().addProperty("response", "OK");
                    responseData.getAsJsonObject().add("value", getCellValue(commandData));
                    outputMessage = GSON.toJson(responseData);
                    break;
                case "delete":
                    deleteCellValue(commandData);
                    responseData.getAsJsonObject().addProperty("response", "OK");
                    outputMessage = GSON.toJson(responseData);
                    break;
                case "exit":
                    responseData.getAsJsonObject().addProperty("response", "OK");
                    outputMessage = GSON.toJson(responseData);
                    throw new RuntimeException();
                default:
                    responseData.getAsJsonObject().addProperty("response", "ERROR");
                    responseData.getAsJsonObject().addProperty("reason", "Unknown command!");
                    outputMessage = GSON.toJson(responseData);
            }
        } catch (IllegalArgumentException e) {
            responseData.getAsJsonObject().addProperty("response", "ERROR");
            responseData.getAsJsonObject().addProperty("reason", e.getMessage());
            outputMessage = GSON.toJson(responseData);
        } finally {
            System.out.println("Sent: " + outputMessage);
            try {
                output.writeUTF(GSON.toJson(outputMessage));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void setCellValue(JsonElement commandData) {
        JsonElement database = getDBFromFileOrCreateNew();

        if (database.getAsJsonObject().keySet().size() == 1000) {
            throw new IllegalArgumentException("Storage is full!");
        } else {
            try {
                WRITE_LOCK.lock();

                if (commandData.getAsJsonObject().get("key").isJsonPrimitive()) {
                    database.getAsJsonObject().add(commandData.getAsJsonObject().get("key").getAsString(), commandData.getAsJsonObject().get("value"));
                } else if (commandData.getAsJsonObject().get("key").isJsonArray()) {
                    JsonArray keys = commandData.getAsJsonObject().get("key").getAsJsonArray();
                    String toAdd = keys.remove(keys.size() - 1).getAsString();
                    findObject(database, keys, true).getAsJsonObject().add(toAdd, commandData.getAsJsonObject().get("value"));
                } else {
                    throw new IllegalArgumentException("No such key");
                }

                writeDBToFile(database);
            } finally {
                WRITE_LOCK.unlock();
            }
        }
    }

    private static JsonElement getCellValue(JsonElement commandData) {
        JsonElement dbObject = getDBFromFileOrCreateNew();

        try {
            READ_LOCK.lock();
            if (commandData.getAsJsonObject().get("key").isJsonPrimitive() &&
                    dbObject.getAsJsonObject().has(commandData.getAsJsonObject().get("key").getAsString())) {
                return dbObject.getAsJsonObject().get(commandData.getAsJsonObject().get("key").getAsString());
            } else if (commandData.getAsJsonObject().get("key").isJsonArray()) {
                return findObject(dbObject, commandData.getAsJsonObject().get("key").getAsJsonArray(), false);
            } else {
                throw new IllegalArgumentException("No such key");
            }
        } finally {
            READ_LOCK.unlock();
        }
    }

    private static void deleteCellValue(JsonElement commandData) {
        JsonElement dbObject = getDBFromFileOrCreateNew();

        try {
            WRITE_LOCK.lock();
            if (commandData.getAsJsonObject().get("key").isJsonPrimitive() &&
                    dbObject.getAsJsonObject().has(commandData.getAsJsonObject().get("key").getAsString())) {
                dbObject.getAsJsonObject().remove(commandData.getAsJsonObject().get("key").getAsString());
            } else if (commandData.getAsJsonObject().get("key").isJsonArray()) {
                JsonArray keys = commandData.getAsJsonObject().get("key").getAsJsonArray();
                String toRemove = keys.remove(keys.size() - 1).getAsString();
                findObject(dbObject, keys, false).getAsJsonObject().remove(toRemove);

                writeDBToFile(dbObject);
            } else {
                throw new IllegalArgumentException("No such key");
            }
        } finally {
            WRITE_LOCK.unlock();
        }
    }

    private static JsonElement getDBFromFileOrCreateNew() {
        READ_LOCK.lock();

        String jsonString = "";

        try {
            jsonString = new String(Files.readAllBytes(Paths.get(System.getProperty("user.dir") + PATH_TO_DB_FILE)));
        } catch (IOException ignored) {}

        JsonElement dbObject;

        if (jsonString.length() > 0) {
            dbObject = GSON.fromJson(jsonString, JsonElement.class);
        } else {
            dbObject = new JsonObject();
        }

        READ_LOCK.unlock();
        return dbObject;
    }

    private static void writeDBToFile(JsonElement dbObject) {
        File dbFile = new File(System.getProperty("user.dir") + PATH_TO_DB_FILE);
        try (FileWriter fileWriter = new FileWriter(dbFile, false)) {
            WRITE_LOCK.lock();
            fileWriter.write(GSON.toJson(dbObject));
            fileWriter.flush();
            WRITE_LOCK.unlock();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static JsonElement findObject(JsonElement dbObject, JsonArray keys, boolean addIfAbsent) {
        JsonElement tempObject = dbObject;

        if (addIfAbsent) {
            for (JsonElement key: keys) {
                if (!tempObject.getAsJsonObject().has(key.getAsString())) {
                    tempObject.getAsJsonObject().add(key.getAsString(), new JsonObject());
                }
                tempObject = tempObject.getAsJsonObject().get(key.getAsString());
            }
        } else {
            for (JsonElement key: keys) {
                if (!key.isJsonPrimitive() || !tempObject.getAsJsonObject().has(key.getAsString())) {
                    throw new IllegalArgumentException("No such key");
                }
                tempObject = tempObject.getAsJsonObject().get(key.getAsString());
            }
        }

        return tempObject;
    }
}