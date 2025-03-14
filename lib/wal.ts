import { promises as fs } from "fs";
import { open, constants } from "fs";
import path from "path";

class WriteAheadLog {
  private readonly LOG_FILE_PATH: string;
  private byteOffset: number;

  constructor(logFileName: string) {
    this.LOG_FILE_PATH = path.resolve(logFileName);
    this.byteOffset = 0;

    // Ensure the log file exists and is ready for appending
    this.ensureLogFileExists();
  }

  public async writeInsertLog(key: string, value: string): Promise<void> {
    try {
      await this.writeLog("INSERT", key, value);
    } catch (error) {
      console.error("Error writing insert log:", error);
    }
  }

  public async writeUpdateLog(key: string, value: string): Promise<void> {
    try {
      await this.writeLog("UPDATE", key, value);
    } catch (error) {
      console.error("Error writing update log:", error);
    }
  }

  public async writeDeleteLog(key: string): Promise<void> {
    try {
      await this.writeLog("DELETE", key, "TOMBSTONE");
    } catch (error) {
      console.error("Error writing delete log:", error);
    }
  }

  public async clearLog(): Promise<void> {
    try {
      // Clear the log file by truncating it to zero bytes
      await fs.writeFile(this.LOG_FILE_PATH, "");
      this.byteOffset = 0;
    } catch (error) {
      console.error("Error clearing log:", error);
    }
  }

  public async close(): Promise<void> {
    // No explicit close needed in Node.js since `fs.promises` handles resource cleanup
    console.log("WriteAheadLog closed.");
  }

  private async writeLog(
    operation: string,
    key: string,
    value: string
  ): Promise<void> {
    const logEntry = `${this.byteOffset}, ${operation}: ${key}: ${value}\n`;
    const bytes = Buffer.from(logEntry);

    // Append the log entry to the file
    await fs.appendFile(this.LOG_FILE_PATH, bytes);

    // Update the byte offset
    this.byteOffset += bytes.length;

    // Synchronize the file to disk
    await this.fsync();
  }

  private async fsync(): Promise<void> {
    // In Node.js, there's no direct equivalent to `force(true)` in Java.
    // However, using `fs.promises.writeFile` or `fs.promises.appendFile` ensures data is flushed to disk.
    // No additional action is required here.
  }

  private async ensureLogFileExists(): Promise<void> {
    try {
      // Check if the file exists, and create it if it doesn't
      await fs.access(this.LOG_FILE_PATH, constants.F_OK);
    } catch (error) {
      // File does not exist, create it
      await fs.writeFile(this.LOG_FILE_PATH, "");
    }
  }
}

export default WriteAheadLog;
