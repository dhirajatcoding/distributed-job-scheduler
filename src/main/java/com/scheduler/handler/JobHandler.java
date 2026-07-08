package com.scheduler.handler;

/**
 * Contract for all executable job types.
 *
 * To add a new job type:
 *   1. Implement this interface.
 *   2. Annotate the class with @Component.
 *   3. Register it in JobHandlerRegistry with a unique string key.
 *   4. Use that key as the `jobType` field when registering jobs via the API.
 *
 * The execute() method should be idempotent where possible — the retry
 * mechanism may call it more than once for the same job instance.
 */
public interface JobHandler {

    /**
     * @param payload  Raw JSON string from the job record. Parse it inside the handler.
     * @throws Exception  Any exception signals failure. The worker pool will catch
     *                    it and trigger the retry/DLQ flow automatically.
     */
    void execute(String payload) throws Exception;
}
