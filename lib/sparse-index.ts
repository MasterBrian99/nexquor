import { promises as fs } from "fs";
import path from "path";

class SparseIndex {
  private index: Map<string, number>; // Using a Map to simulate TreeMap behavior

  constructor() {
    this.index = new Map<string, number>();
  }

  /**
   * Adds a key-value pair to the sparse index.
   * @param key - The key to add.
   * @param value - The offset value associated with the key.
   */
  public add(key: string, value: number): void {
    this.index.set(key, value);
  }

  /**
   * Retrieves the offset value for a given key.
   * @param key - The key to look up.
   * @returns The offset value if the key exists, otherwise undefined.
   */
  public get(key: string): number | undefined {
    return this.index.get(key);
  }

  /**
   * Finds the segment (key) that is less than or equal to the given key.
   * @param key - The key to search for.
   * @returns The key of the segment if found, otherwise null.
   */
  public findSegment(key: string): string | null {
    let floorKey: string | null = null;
    for (const [indexKey] of this.index.entries()) {
      if (indexKey <= key) {
        floorKey = indexKey;
      } else {
        break;
      }
    }
    return floorKey;
  }

  /**
   * Finds the next higher key and its offset, or calculates the last line offset if no higher key exists.
   * @param key - The key to search for.
   * @param filePath - The path to the file for which the last line offset is calculated.
   * @returns A key-value pair representing the next higher key and its offset.
   */
  public async highEntry(
    key: string,
    filePath: string
  ): Promise<[string, number]> {
    const higherKey = this.findHigherKey(key);
    if (higherKey === null) {
      const lastLineOffset = await this.findLastLineOffset(filePath);
      return [key, lastLineOffset];
    }
    return [higherKey, this.index.get(higherKey)!];
  }

  /**
   * Finds the next higher key in the index.
   * @param key - The key to search for.
   * @returns The next higher key if it exists, otherwise null.
   */
  private findHigherKey(key: string): string | null {
    for (const [indexKey] of this.index.entries()) {
      if (indexKey > key) {
        return indexKey;
      }
    }
    return null;
  }

  /**
   * Finds the offset of the last line in the file.
   * @param filePath - The path to the file.
   * @returns The offset of the last line.
   */
  private async findLastLineOffset(filePath: string): Promise<number> {
    try {
      const fileHandle = await fs.open(filePath, "r");
      const fileStats = await fileHandle.stat();
      let lastLineOffset = fileStats.size - 1;

      while (lastLineOffset >= 0) {
        const buffer = Buffer.alloc(1);
        await fileHandle.read(buffer, 0, 1, lastLineOffset);
        if (buffer[0] === 10) {
          // ASCII code for '\n'
          break;
        }
        lastLineOffset--;
      }

      await fileHandle.close();
      return Math.max(lastLineOffset, 0);
    } catch (error) {
      console.error("Error finding last line offset:", error);
      return 0;
    }
  }
}

export default SparseIndex;
