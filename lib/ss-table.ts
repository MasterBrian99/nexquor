import { promises as fs } from "fs";
import path from "path";
import AVL from "./avl";
import WriteAheadLog from "./wal";
import SparseIndex from "./sparse-index";
import BloomFilter from "./bloom-filter";
import Compaction from "./compaction";

class SSTable {
  private static fileNameAndSSTableMap: Map<string, SSTable> = new Map();
  private static readonly SSTABLE_DIRECTORY = "./sstable";
  private static readonly LOG_FILE_NAME = "log.txt";
  private static readonly SEGMENT_SIZE = 10 * 1024; // 10 KB
  private static readonly MAX_TREE_SIZE = 16 * 1024; // 16 KB
  private static avlTree: AVL;
  private static writeAheadLog: WriteAheadLog;
  private static compaction: Compaction;
  private static currentTreeSize = 0;

  private sparseIndex: SparseIndex;
  private bloomFilter: BloomFilter;
  private byteOffset: number;
  private fileName: string;

  static {
    SSTable.avlTree = new AVL();
    try {
      SSTable.writeAheadLog = new WriteAheadLog(SSTable.LOG_FILE_NAME);
    } catch (error) {
      console.error("Error initializing WriteAheadLog:", error);
    }
    SSTable.compaction = new Compaction();
  }

  constructor(fileName?: string) {
    this.sparseIndex = new SparseIndex();
    this.bloomFilter = new BloomFilter();
    this.byteOffset = 0;
    if (fileName) {
      this.fileName = fileName;
    } else {
      this.fileName = `sstable${SSTable.getSSTableCountPlusOne()}.txt`;
    }
  }

  public getFileName(): string {
    return this.fileName;
  }

  public setFileName(name: string): void {
    this.fileName = name;
  }

  public getSparseIndex(): SparseIndex {
    return this.sparseIndex;
  }

  public setSparseIndex(sparseIndex: SparseIndex): void {
    this.sparseIndex = sparseIndex;
  }

  public getBloomFilter(): BloomFilter {
    return this.bloomFilter;
  }

  public setBloomFilter(bloomFilter: BloomFilter): void {
    this.bloomFilter = bloomFilter;
  }

  public async recover(): Promise<void> {
    const filePath = path.resolve(SSTable.SSTABLE_DIRECTORY, this.fileName);
    try {
      const stats = await fs.stat(filePath);
      this.byteOffset = stats.size;
      this.sparseIndex = await this.rebuildSparseIndex();
      this.bloomFilter = await this.rebuildBloomFilter();
    } catch (error) {
      console.error(`Error recovering SSTable ${this.fileName}:`, error);
    }
  }

  async rebuildSparseIndex(): Promise<SparseIndex> {
    const sIndex = new SparseIndex();
    let chunkSize = 0;
    let firstKeyInChunk: string | null = null;
    let currentOffset = 0;

    const filePath = path.resolve(SSTable.SSTABLE_DIRECTORY, this.fileName);
    const data = await fs.readFile(filePath, "utf-8");
    const lines = data.split("\n");

    for (const line of lines) {
      if (!line.trim()) continue;

      const [key] = line.split(":").map((part) => part.trim());
      const lineSize = Buffer.byteLength(line) + 1; // Include newline character

      if (chunkSize + lineSize <= SSTable.SEGMENT_SIZE) {
        if (!firstKeyInChunk) firstKeyInChunk = key;
        chunkSize += lineSize;
      } else {
        if (firstKeyInChunk) {
          sIndex.add(firstKeyInChunk, currentOffset);
          currentOffset += chunkSize;
        }
        chunkSize = lineSize;
        firstKeyInChunk = key;
      }
    }

    if (firstKeyInChunk) {
      sIndex.add(firstKeyInChunk, currentOffset);
    }

    return sIndex;
  }

  private async rebuildBloomFilter(): Promise<BloomFilter> {
    const bloomFilter = new BloomFilter();
    const filePath = path.resolve(SSTable.SSTABLE_DIRECTORY, this.fileName);
    const data = await fs.readFile(filePath, "utf-8");
    const lines = data.split("\n");

    for (const line of lines) {
      if (!line.trim()) continue;

      const [key] = line.split(":").map((part) => part.trim());
      bloomFilter.add(key);
    }

    return bloomFilter;
  }

  public static getAvlTree(): AVL {
    return SSTable.avlTree;
  }

  public static async insert(key: string, value: string): Promise<void> {
    if (!SSTable.avlTree.findKey(key)) {
      await SSTable.writeAheadLog.writeInsertLog(key, value);
      SSTable.avlTree.insert(key, value);
      SSTable.currentTreeSize +=
        Buffer.byteLength(key) + Buffer.byteLength(value);

      if (SSTable.currentTreeSize >= SSTable.MAX_TREE_SIZE) {
        await SSTable.flushToSSTable();
        SSTable.avlTree.empty();
        SSTable.currentTreeSize = 0;
      }
    } else {
      await SSTable.update(key, value);
    }
  }

  public static async update(key: string, value: string): Promise<void> {
    if (SSTable.avlTree.findKey(key)) {
      const oldValue = SSTable.avlTree.findValue(key)!;
      await SSTable.writeAheadLog.writeUpdateLog(key, value);
      SSTable.avlTree.update(key, value);
      SSTable.currentTreeSize -= Buffer.byteLength(oldValue);
      SSTable.currentTreeSize += Buffer.byteLength(value);
    } else {
      await SSTable.insert(key, value);
    }
  }

  public static async delete(key: string): Promise<void> {
    if (SSTable.avlTree.findKey(key)) {
      const oldValue = SSTable.avlTree.findValue(key)!;
      await SSTable.writeAheadLog.writeDeleteLog(key);
      SSTable.avlTree.update(key, "TOMBSTONE");
      SSTable.currentTreeSize -= Buffer.byteLength(oldValue);
      SSTable.currentTreeSize += Buffer.byteLength("TOMBSTONE");
    } else {
      await SSTable.insert(key, "TOMBSTONE");
    }
  }

  public static async flushToSSTable(): Promise<void> {
    const sstable = new SSTable();
    const dirPath = path.resolve(SSTable.SSTABLE_DIRECTORY);

    // Ensure the directory exists
    try {
      await fs.mkdir(dirPath, { recursive: true });
    } catch (error) {
      console.error("Error creating SSTable directory:", error);
    }

    const filePath = path.resolve(dirPath, sstable.getFileName());
    let chunkSize = 0;
    let firstKeyInChunk: string | null = null;

    const keyValuePairs = SSTable.avlTree.getInOrderTraversal();
    await fs.writeFile(filePath, "");

    for (const [key, value] of keyValuePairs) {
      const line = `${key}: ${value}\n`;
      const lineSize = Buffer.byteLength(line);

      sstable.bloomFilter.add(key);

      if (chunkSize + lineSize <= SSTable.SEGMENT_SIZE) {
        if (!firstKeyInChunk) firstKeyInChunk = key;
        chunkSize += lineSize;
      } else {
        if (firstKeyInChunk) {
          sstable.sparseIndex.add(firstKeyInChunk, sstable.byteOffset);
          sstable.byteOffset += chunkSize;
        }
        chunkSize = lineSize;
        firstKeyInChunk = key;
      }

      await fs.appendFile(filePath, line);
    }

    if (firstKeyInChunk) {
      sstable.sparseIndex.add(firstKeyInChunk, sstable.byteOffset);
    }

    await SSTable.writeAheadLog.clearLog();
    SSTable.fileNameAndSSTableMap.set(sstable.getFileName(), sstable);
    SSTable.compaction.addSSTable(sstable);
    await SSTable.compaction.compactIfNeeded();
  }

  public static async readKey(key: string): Promise<string | null> {
    const dirPath = path.resolve(SSTable.SSTABLE_DIRECTORY);
    const files = await fs.readdir(dirPath);

    let value = SSTable.avlTree.findValue(key);
    if (value) return value;

    const sortedFiles = files
      .filter((file) => file.startsWith("ss-table") && file.endsWith(".txt"))
      .sort((file1, file2) => {
        const num1 = parseInt(file1.substring(7, file1.length - 4));
        const num2 = parseInt(file2.substring(7, file2.length - 4));
        return num2 - num1;
      });

    for (const file of sortedFiles) {
      const sstable = SSTable.fileNameAndSSTableMap.get(file);
      if (!sstable?.bloomFilter.mightContain(key)) continue;

      const segmentStartKey = sstable.getSparseIndex().findSegment(key);
      if (!segmentStartKey) continue;

      const filePath = path.resolve(SSTable.SSTABLE_DIRECTORY, file);
      const startOffset = sstable.getSparseIndex().get(segmentStartKey)!;
      const highEntry = await sstable
        .getSparseIndex()
        .highEntry(segmentStartKey, filePath);
      const endOffset = highEntry[1];

      const result = await SSTable.binarySearchSSTable(
        file,
        startOffset,
        endOffset,
        key
      );
      if (result) return result;
    }

    return null;
  }

  private static async binarySearchSSTable(
    sstableFileName: string,
    startOffset: number,
    endOffset: number,
    key: string
  ): Promise<string | null> {
    const filePath = path.resolve(SSTable.SSTABLE_DIRECTORY, sstableFileName);
    let low = startOffset;
    let high = endOffset;

    const fileHandle = await fs.open(filePath, "r");
    const buffer = Buffer.alloc(1024); // Adjust buffer size as needed

    while (low <= high) {
      const mid = Math.floor(low + (high - low) / 2);
      await fileHandle.read(buffer, 0, buffer.length, mid);

      const line = buffer.toString("utf-8").split("\n")[0];
      const [currKey, currValue] = line.split(":").map((part) => part.trim());

      if (currKey === key) {
        await fileHandle.close();
        return currValue;
      } else if (currKey < key) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }

    await fileHandle.close();
    return null;
  }

  public static updateFileNameAndSSTableMap(
    oldFileName1: string,
    oldFileName2: string,
    newFileName: string,
    newSSTable: SSTable
  ): void {
    SSTable.fileNameAndSSTableMap.delete(oldFileName1);
    SSTable.fileNameAndSSTableMap.delete(oldFileName2);
    SSTable.fileNameAndSSTableMap.set(newFileName, newSSTable);
  }

  public static async recoverFileNameAndSSTableMap(): Promise<void> {
    const dirPath = path.resolve(SSTable.SSTABLE_DIRECTORY);
    const files = await fs.readdir(dirPath);

    for (const file of files) {
      const sstable = new SSTable(file);
      await sstable.recover();
      SSTable.fileNameAndSSTableMap.set(file, sstable);
    }
  }

  public static async getNumberOfSSTableCount(): Promise<number> {
    const dirPath = path.resolve(SSTable.SSTABLE_DIRECTORY);
    const files = await fs.readdir(dirPath);
    return files.filter(
      (file) => file.startsWith("sstable") && file.endsWith(".txt")
    ).length;
  }

  public static async getSSTableCount(): Promise<number> {
    const dirPath = path.resolve(SSTable.SSTABLE_DIRECTORY);
    const files = await fs.readdir(dirPath);
    let highestNumber = 0;

    for (const file of files) {
      if (file.startsWith("sstable")) {
        const number = parseInt(file.substring(7, file.length - 4));
        if (number > highestNumber) highestNumber = number;
      }
    }

    return highestNumber;
  }

  public static async close(): Promise<void> {
    await SSTable.writeAheadLog.close();
  }

  private static async getSSTableCountPlusOne(): Promise<number> {
    return (await SSTable.getSSTableCount()) + 1;
  }
}

export default SSTable;
