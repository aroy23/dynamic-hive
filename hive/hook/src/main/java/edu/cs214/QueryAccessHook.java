package edu.cs214;

import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.plan.HiveOperation;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.Set;

public class QueryAccessHook implements ExecuteWithHookContext {

    private static final String LOG_PATH = "/opt/logs/access_log.jsonl";

    @Override
    public void run(HookContext hookContext) throws Exception {
        if (hookContext.getQueryPlan() == null) {
            return;
        }

        Set<ReadEntity> inputs = hookContext.getQueryPlan().getInputs();
        if (inputs == null || inputs.isEmpty()) {
            return;
        }

        HiveOperation operation = hookContext.getOperationName() != null
                ? HiveOperation.valueOf(hookContext.getOperationName())
                : null;
        String queryType = operation != null ? operation.name() : "UNKNOWN";
        String user = hookContext.getUgi() != null
                ? hookContext.getUgi().getShortUserName()
                : "unknown";
        String timestamp = Instant.now().toString();

        for (ReadEntity entity : inputs) {
            if (entity.getTable() != null) {
                String dbName = entity.getTable().getDbName();
                String tableName = entity.getTable().getTableName();
                String fullTable = dbName + "." + tableName;

                String jsonLine = String.format(
                        "{\"timestamp\":\"%s\",\"table\":\"%s\",\"query_type\":\"%s\",\"user\":\"%s\"}",
                        timestamp, fullTable, queryType, user
                );

                appendToLog(jsonLine);
            }
        }
    }

    // synchronized to prevent interleaved writes from concurrent hook invocations
    private synchronized void appendToLog(String line) {
        try (PrintWriter pw = new PrintWriter(new FileWriter(LOG_PATH, true))) {
            pw.println(line);
        } catch (IOException e) {
            System.err.println("QueryAccessHook: failed to write log - " + e.getMessage());
        }
    }
}
