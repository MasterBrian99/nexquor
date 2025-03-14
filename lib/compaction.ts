import fs from "fs"; // Import the full fs module
import { promises as fsPromises } from "fs"; // Use fsPromises for async operations
import path from "path";
import SSTable from "./ss-table";

class Compaction {
  private static readonly COMPACTION_THRESHOLD = 5;
  private static readonly SSTABLE_DIRECTORY = "./sstable";
  private sstableBuckets: Map<number, SSTable[]> = new Map();

  constructor() {
    for (let i = 0; i < 4; i++) {
      this.sstableBuckets.set(i, []);
    }
  }

  /**
   * Adds an SSTable to the appropriate bucket based on its size.
   * @param sstable - The SSTable to add.
   */
  public async addSSTable(sstable: SSTable): Promise<void> {
    const sstableName = sstable.getFileName();
    const filePath = path.resolve(Compaction.SSTABLE_DIRECTORY, sstableName);
    const stats = await fsPromises.stat(filePath);
    const sizeInBytes = stats.size;

    const bucket = this.getBucket(sizeInBytes);
    const sstables = this.sstableBuckets.get(bucket) || [];
    sstables.push(sstable);
    this.sstableBuckets.set(bucket, sstables);
  }

  /**
   * Checks if compaction is needed for any bucket and performs compaction if necessary.
   */
  public async compactIfNeeded(): Promise<void> {
    for (const [bucket, sstables] of this.sstableBuckets.entries()) {
      if (sstables.length > Compaction.COMPACTION_THRESHOLD) {
        await this.compactFilesInBucket(bucket);
      }
    }
  }

  /**
   * Rebuilds the buckets by scanning the SSTable directory.
   */
  public async rebuildBuckets(): Promise<void> {
    const dirPath = path.resolve(Compaction.SSTABLE_DIRECTORY);
    const files = await fsPromises.readdir(dirPath);

    for (const file of files) {
      const sstable = new SSTable(file);
      await this.addSSTable(sstable);
    }
  }

  /**
   * Moves an SSTable to a different bucket based on its updated size.
   * @param sstable - The SSTable to move.
   */
  public moveSSTableToDifferentBucket(sstable: SSTable): void {
    for (const sstables of this.sstableBuckets.values()) {
      const index = sstables.findIndex(
        (s) => s.getFileName() === sstable.getFileName()
      );
      if (index !== -1) {
        sstables.splice(index, 1);
        break;
      }
    }
    this.addSSTable(sstable);
  }

  /**
   * Prints all buckets along with their SSTable names.
   */
  public printAllBucketWithSSTablesName(): void {
    for (const [bucket, sstables] of this.sstableBuckets.entries()) {
      console.log(`Bucket: ${bucket}`);
      for (const sstable of sstables) {
        console.log(sstable.getFileName());
      }
    }
  }

  /**
   * Compacts files in a specific bucket.
   * @param bucket - The bucket index to compact.
   */
  private async compactFilesInBucket(bucket: number): Promise<void> {
    const randomNum = Math.floor(Math.random() * 100);
    const sstables = this.sstableBuckets.get(bucket);

    if (sstables && sstables.length > 1) {
      // Sort SSTables by their numeric part in the filename
      sstables.sort((a, b) => {
        const numA = parseInt(
          a.getFileName().substring(7, a.getFileName().length - 4)
        );
        const numB = parseInt(
          b.getFileName().substring(7, b.getFileName().length - 4)
        );
        return numA - numB;
      });

      const firstFileName = sstables[0].getFileName();
      const secondFileName = sstables[1].getFileName();
      const tempFileName = `temp${randomNum}.txt`;

      const file1Path = path.resolve(
        Compaction.SSTABLE_DIRECTORY,
        firstFileName
      );
      const file2Path = path.resolve(
        Compaction.SSTABLE_DIRECTORY,
        secondFileName
      );
      const tempFilePath = path.resolve(
        Compaction.SSTABLE_DIRECTORY,
        tempFileName
      );

      const tempSSTable = new SSTable(tempFileName);

      await this.compact(
        firstFileName,
        secondFileName,
        tempFileName,
        tempSSTable
      );

      // Delete the original files
      await fsPromises.unlink(file1Path);
      await fsPromises.unlink(file2Path);

      // Rename the temporary file to the second file's name
      tempSSTable.setFileName(secondFileName);
      await fsPromises.rename(tempFilePath, file2Path);

      // Rebuild the sparse index for the compacted SSTable
      const sIndex = await tempSSTable.rebuildSparseIndex();
      tempSSTable.setSparseIndex(sIndex);

      // Update the SSTable map
      SSTable.updateFileNameAndSSTableMap(
        firstFileName,
        secondFileName,
        secondFileName,
        tempSSTable
      );

      // Remove old SSTables from the bucket
      this.sstableBuckets.set(
        bucket,
        sstables.filter(
          (s) =>
            s.getFileName() !== firstFileName &&
            s.getFileName() !== secondFileName
        )
      );

      // Add the new SSTable to the bucket
      await this.addSSTable(tempSSTable);
    }
  }

  /**
   * Compacts two SSTable files into a single output file.
   * @param file1 - The first input file name.
   * @param file2 - The second input file name.
   * @param outputFile - The name of the output file.
   * @param outputSSTable - The SSTable object for the output file.
   */
  private async compact(
    file1: string,
    file2: string,
    outputFile: string,
    outputSSTable: SSTable
  ): Promise<void> {
    const file1Path = path.resolve(Compaction.SSTABLE_DIRECTORY, file1);
    const file2Path = path.resolve(Compaction.SSTABLE_DIRECTORY, file2);
    const outputFilePath = path.resolve(
      Compaction.SSTABLE_DIRECTORY,
      outputFile
    );

    const data1 = (await fsPromises.readFile(file1Path, "utf-8"))
      .split("\n")
      .filter((line) => line.trim());
    const data2 = (await fsPromises.readFile(file2Path, "utf-8"))
      .split("\n")
      .filter((line) => line.trim());

    // Use fs.createWriteStream for writing files
    const writer = fs.createWriteStream(outputFilePath, { flags: "w" });

    let i = 0,
      j = 0;

    while (i < data1.length && j < data2.length) {
      const [key1, value1] = data1[i].split(": ").map((part) => part.trim());
      const [key2, value2] = data2[j].split(": ").map((part) => part.trim());

      if (key1 < key2) {
        if (value1 !== "TOMBSTONE") {
          writer.write(`${data1[i]}\n`);
          outputSSTable.getBloomFilter().add(key1);
        }
        i++;
      } else if (key1 > key2) {
        if (value2 !== "TOMBSTONE") {
          writer.write(`${data2[j]}\n`);
          outputSSTable.getBloomFilter().add(key2);
        }
        j++;
      } else {
        if (value2 !== "TOMBSTONE") {
          writer.write(`${data2[j]}\n`);
          outputSSTable.getBloomFilter().add(key2);
        }
        i++;
        j++;
      }
    }

    while (i < data1.length) {
      const [key1, value1] = data1[i].split(": ").map((part) => part.trim());
      if (value1 !== "TOMBSTONE") {
        writer.write(`${data1[i]}\n`);
        outputSSTable.getBloomFilter().add(key1);
      }
      i++;
    }

    while (j < data2.length) {
      const [key2, value2] = data2[j].split(": ").map((part) => part.trim());
      if (value2 !== "TOMBSTONE") {
        writer.write(`${data2[j]}\n`);
        outputSSTable.getBloomFilter().add(key2);
      }
      j++;
    }

    // Close the write stream
    return new Promise((resolve, reject) => {
      writer.on("finish", resolve);
      writer.on("error", reject);
      writer.end();
    });
  }

  /**
   * Determines the bucket for an SSTable based on its size.
   * @param sizeInBytes - The size of the SSTable in bytes.
   * @returns The bucket index.
   */
  private getBucket(sizeInBytes: number): number {
    const sizeInKb = sizeInBytes / 1024;

    if (sizeInKb < 30) {
      return 0;
    } else if (sizeInKb < 60) {
      return 1;
    } else if (sizeInKb < 120) {
      return 2;
    } else {
      return 3;
    }
  }
}

export default Compaction;
