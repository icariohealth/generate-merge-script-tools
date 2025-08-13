package com.tool;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class PropertiesLoader {
    public String propertiesFile;
    public List<String> requiredPros = Arrays.asList(
            "fileName",
            "schemaName",
            "tableName",
            "columns",
            "unitKeys"
    );

    public PropertiesLoader(String[] args) {
        if (args.length < 1) {
            System.err.println("Please provide the name of properties file");
            System.exit(1);
        } else if (args.length > 1) {
            System.err.println("Too many arguments");
            System.exit(1);
        }
        propertiesFile = args[0];
    }

    public Properties getProperties() {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(propertiesFile)) {
            props.load(fis);
            StringBuilder missingKeys = new StringBuilder();
            for (String key : requiredPros) {
                if (props.getProperty(key) == null || props.getProperty(key).trim().isEmpty()) {
                    missingKeys.append(" - ").append(key).append("\n");
                }
            }
            if (!missingKeys.isEmpty()) {
                System.err.println("Missing required properties:\n" + missingKeys);
                System.exit(1);
            }
        } catch (IOException e) {
            System.err.println("No such file:" + propertiesFile);
            System.exit(1);
        }
        if (props.isEmpty()) {
            System.err.println("Missing properties in file: " + propertiesFile);
            System.exit(1);
        }
        return props;
    }
}
