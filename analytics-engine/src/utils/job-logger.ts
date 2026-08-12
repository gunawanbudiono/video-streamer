/**
 * In-memory log store for job processing.
 * Stores logs per job ID and supports SSE streaming.
 */

interface LogEntry {
    timestamp: string;
    message: string;
}

interface LogStream {
    logs: LogEntry[];
    listeners: Set<(entry: LogEntry) => void>;
    completed: boolean;
}

const jobLogs = new Map<string, LogStream>();

/** Initialize log store for a job. Pass clear=false to safely append to an existing running session log. */
export function initJobLog(jobId: string, clear: boolean = true) {
    if (clear || !jobLogs.has(jobId)) {
        jobLogs.set(jobId, { logs: [], listeners: new Set(), completed: false });
    } else {
        const stream = jobLogs.get(jobId)!;
        stream.completed = false; // Reset to allow more streaming
    }
}

/** Emit a log message for a job (also logs to console) */
export function emitLog(jobId: string, message: string) {
    console.log(`  ${message}`);
    const stream = jobLogs.get(jobId);
    if (!stream) return;

    const entry: LogEntry = {
        timestamp: new Date().toLocaleTimeString('id-ID', { timeZone: 'Asia/Jakarta', hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' }),
        message,
    };
    stream.logs.push(entry);
    stream.listeners.forEach(cb => cb(entry));
}

/** Mark a job's log stream as completed */
export function completeJobLog(jobId: string) {
    const stream = jobLogs.get(jobId);
    if (!stream) return;
    stream.completed = true;
    // Notify listeners that we're done
    const doneEntry: LogEntry = { timestamp: '', message: '__DONE__' };
    stream.listeners.forEach(cb => cb(doneEntry));
    stream.listeners.clear();
    // Clean up after 5 minutes
    setTimeout(() => jobLogs.delete(jobId), 5 * 60 * 1000);
}

/** Subscribe to a job's log stream (SSE). Returns cleanup function. */
export function subscribeJobLog(
    jobId: string,
    onEntry: (entry: LogEntry) => void,
    onDone: () => void,
): () => void {
    const stream = jobLogs.get(jobId);
    if (!stream) {
        onDone();
        return () => { };
    }

    // Send all existing logs first
    for (const entry of stream.logs) {
        onEntry(entry);
    }

    // If already completed, close
    if (stream.completed) {
        onDone();
        return () => { };
    }

    // Listen for new entries
    const listener = (entry: LogEntry) => {
        if (entry.message === '__DONE__') {
            onDone();
        } else {
            onEntry(entry);
        }
    };
    stream.listeners.add(listener);

    return () => { stream.listeners.delete(listener); };
}

/** Retrieve all accumulated logs for a specific job */
export function getJobLogs(jobId: string): LogEntry[] {
    return jobLogs.get(jobId)?.logs || [];
}
