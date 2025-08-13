package com.tool;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

public class ScriptGenerator {

    private final Properties props;
    private String fileName;
    private String schemaName;
    private String tableName;
    private List<String> columns;
    private String unitKey;

    public ScriptGenerator(Properties properties) {
        this.props = properties;
    }

    public void generate() {
        fileName = props.getProperty("fileName").trim();
        schemaName = props.getProperty("schemaName").trim();
        tableName = props.getProperty("tableName").trim();
        columns = Arrays.stream(props.getProperty("columns")
                        .split(","))
                .map(String::trim)
                .collect(Collectors.toList());
        unitKey = props.getProperty("unitKeys").trim();
        generateScriptFile(false, "output/merge_into_inc_tables");
        generateScriptFile(true, "output/merge_into_target_tables");
    }

    private void generateScriptFile (boolean isTargetTable, String outputFolder) {
        Path outputDir = Paths.get(outputFolder);
        try {
            if (!Files.exists(outputDir)) {
                Files.createDirectories(outputDir);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create output directory", e);
        }

        Path filePath = outputDir.resolve(fileName + ".sql");
        String sql = generateProcedure(isTargetTable) ;
        try (BufferedWriter writer = Files.newBufferedWriter(filePath)) {
            writer.write(sql);
            System.out.println("SQL procedure generated successfully: " + filePath);
        } catch (IOException e) {
            System.err.printf("Error writing SQL file [%s]: %s%n", filePath, e.getMessage());
        }
    }

    private String generateUnitKeysCondition() {
        StringBuilder onClause = new StringBuilder("ON ");
        String[] unitKeys = unitKey.split("\\s*,\\s*");
        for (int i = 0; i < unitKeys.length; i++) {
            if (i > 0) {
                onClause.append(" AND ");
            }
            String col = unitKeys[i];
            onClause.append("tgt.").append(col).append(" = src.").append(col);
        }
        return onClause.toString();
    }

    private String generateProcedure(boolean isTargetTable) {
        String SOURCE_TIMESTAMPS = Optional.ofNullable(props.getProperty("source_timestamps"))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .orElse("source_timestamp_ms");
        String DW_CREATED_TIMESTAMPS = Optional.ofNullable(props.getProperty("created_timestamps"))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .orElse("dw_created_timestamp_ms");
        String DW_MODIFIED_TIMESTAMPS = Optional.ofNullable(props.getProperty("last_modified_timestamps"))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .orElse("dw_last_modified_timestamp_ms");
        String columnList = String.join(", ", columns);
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE OR REPLACE PROCEDURE ").append(schemaName).append(".").append(fileName).append("()\n")
                .append("LANGUAGE plpgsql\n")
                .append("AS $procedure$\n")
                .append("BEGIN\n\n");

        // Step 1
        sb.append("    -- Step 1: Insert records with NULL source_timestamp_ms into the main table\n")
                .append("    INSERT INTO ").append(schemaName).append((isTargetTable) ? "." : ".inc_").append(tableName).append(" (\n")
                .append("        ").append(columnList).append("\n")
                .append("    )\n")
                .append("    SELECT\n")
                .append("        ").append(columnList).append("\n")
                .append("    FROM ").append(schemaName).append(".stg_").append(tableName).append("\n")
                .append("    WHERE ").append(SOURCE_TIMESTAMPS).append(" IS NULL;\n\n");

        // Step 2
        sb.append("    -- Step 2: Archive NULL-source_timestamp_ms records\n")
                .append("    INSERT INTO ").append(schemaName).append(".archived_").append(tableName).append(" (\n")
                .append("        ").append(columnList).append(", ")
                .append(DW_CREATED_TIMESTAMPS).append("\n")
                .append("    )\n")
                .append("    SELECT\n")
                .append("        ").append(columnList).append(", ")
                .append(DW_CREATED_TIMESTAMPS).append("\n")
                .append("    FROM ").append(schemaName).append(".stg_").append(tableName).append("\n")
                .append("    WHERE ").append(SOURCE_TIMESTAMPS).append(" IS NULL;\n\n");

        // Step 3
        sb.append("    -- Step 3: Delete NULL-source_timestamp_ms records from staging\n")
                .append("    DELETE FROM ").append(schemaName).append(".stg_").append(tableName).append("\n")
                .append("    WHERE ").append(SOURCE_TIMESTAMPS).append(" IS NULL;\n\n");

        // Step 4
        String srcAlias = "src";
        String tgtAlias = "tgt";
        String srcColumnList = columns.stream()
                .map(c -> srcAlias + "." + c)
                .collect(Collectors.joining(", "));
        String updateSet = columns.stream()
                .filter(c -> Arrays.stream(unitKey.split(","))
                        .map(String::trim)
                        .noneMatch(cond -> cond.equalsIgnoreCase(c)))
                .map(c -> c + " = " + srcAlias + "." + c)
                .collect(Collectors.joining(",\n            "))
                + ",\n            " + DW_MODIFIED_TIMESTAMPS + " = CURRENT_TIMESTAMP(3) AT TIME ZONE 'UTC'";

        sb.append("    -- Step 4: Merge latest records by id based on operation type\n")
                .append("    WITH latest_ops AS (\n")
                .append("        SELECT\n")
                .append("            ").append(columnList).append(", rn\n")
                .append("        FROM (\n")
                .append("            SELECT\n")
                .append("            ").append(columnList).append(", ROW_NUMBER() OVER (PARTITION BY id ORDER BY ")
                .append(SOURCE_TIMESTAMPS).append(" DESC) AS rn\n")
                .append("            FROM ").append(schemaName).append(".stg_").append(tableName).append("\n")
                .append("            WHERE op IN ('+I', '+U', '-D')\n")
                .append("        ) sub\n")
                .append("        WHERE rn = 1\n")
                .append("    )\n")
                .append("    MERGE INTO ").append(schemaName).append((isTargetTable) ? "." : ".inc_").append(tableName).
                append(" AS ").append(tgtAlias).append("\n")
                .append("    USING latest_ops AS ").append(srcAlias).append("\n")
                .append("    ").append(generateUnitKeysCondition()).append("\n")
                .append("    WHEN MATCHED AND ").append(srcAlias).append(".op IN ('+I', '+U') THEN\n")
                .append("        UPDATE SET\n")
                .append("            ").append(updateSet).append("\n")
                .append("    WHEN MATCHED AND ").append(srcAlias).append(".op = '-D' THEN\n")
                .append("        DELETE\n")
                .append("    WHEN NOT MATCHED AND ").append(srcAlias).append(".op IN ('+I', '+U') THEN\n")
                .append("        INSERT (\n")
                .append("            ").append(columnList).append("\n")
                .append("        )\n")
                .append("        VALUES (\n")
                .append("            ").append(srcColumnList).append("\n")
                .append("        );\n\n");

        // Step 5
        sb.append("    -- Step 5: Archive all remaining staging records\n")
                .append("    INSERT INTO ").append(schemaName).append(".archived_").append(tableName).append(" (\n")
                .append("        ").append(columnList).append(", ")
                .append(DW_CREATED_TIMESTAMPS).append("\n")
                .append("    )\n")
                .append("    SELECT\n")
                .append("        ").append(columnList).append(", ")
                .append(DW_CREATED_TIMESTAMPS).append("\n")
                .append("    FROM ").append(schemaName).append(".stg_").append(tableName).append(";\n\n");

        // Step 6
        sb.append("    -- Step 6: Truncate staging table\n")
                .append("    DELETE FROM ").append(schemaName).append(".stg_").append(tableName).append(";\n\n");

        // Exception
        sb.append("EXCEPTION\n")
                .append("    WHEN OTHERS THEN\n")
                .append("        RAISE EXCEPTION 'Error in ").append(fileName).append(": %', SQLERRM;\n\n");

        sb.append("END;\n")
                .append("$procedure$\n")
                .append(";");

        return sb.toString();
    }
}
