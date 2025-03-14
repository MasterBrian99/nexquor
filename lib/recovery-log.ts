import { promises as fs } from "fs";
import path from "path";
import AVL from "./avl";

class RecoverLog {
  private avlTree: AVL;

  constructor(avlTree: AVL) {
    this.avlTree = avlTree;
  }

  /**
   * Recovers the AVL tree by reading and processing log entries from a file.
   * @param logFileName - The name of the log file to recover from.
   */
  public async recover(logFileName: string): Promise<void> {
    const logFilePath = path.resolve(logFileName);

    try {
      // Read the log file line by line
      const data = await fs.readFile(logFilePath, "utf-8");
      const lines = data.split("\n");

      for (const line of lines) {
        if (line.trim() === "") continue; // Skip empty lines

        // Parse the log entry
        const [offset, logPart] = line.split(", ");
        const [operation, key, value] = logPart.split(": ");

        // Perform the corresponding operation on the AVL tree
        switch (operation) {
          case "INSERT":
            this.avlTree.insert(key, value);
            break;
          case "UPDATE":
            this.avlTree.update(key, value);
            break;
          case "DELETE":
            this.avlTree.delete(key);
            break;
          default:
            console.warn(`Unknown operation: ${operation}`);
        }
      }
    } catch (error) {
      console.error(`Error recovering log from file: ${logFileName}`, error);
    }
  }
}

export default RecoverLog;
