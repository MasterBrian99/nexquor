class BloomFilter {
  private bitSet: boolean[]; // Simulating BitSet using an array of booleans
  private bitSetSize: number;
  private numberOfHashFunctions: number;

  constructor() {
    this.bitSetSize = 224668; // Size of the bitset
    this.bitSet = new Array(this.bitSetSize).fill(false); // Initialize all bits to false
    this.numberOfHashFunctions = 10; // Number of hash functions
  }

  /**
   * Adds a key to the Bloom filter.
   * @param key - The key to add.
   */
  public add(key: string): void {
    for (let i = 0; i < this.numberOfHashFunctions; i++) {
      const hashCode = this.getHash(key, i);
      const index = Math.abs(hashCode % this.bitSetSize);
      this.bitSet[index] = true; // Set the corresponding bit to true
    }
  }

  /**
   * Checks if a key might exist in the Bloom filter.
   * @param key - The key to check.
   * @returns True if the key might exist, false if it definitely does not exist.
   */
  public mightContain(key: string): boolean {
    for (let i = 0; i < this.numberOfHashFunctions; i++) {
      const hashCode = this.getHash(key, i);
      const index = Math.abs(hashCode % this.bitSetSize);
      if (!this.bitSet[index]) {
        return false; // If any bit is false, the key definitely does not exist
      }
    }
    return true; // All bits are true, so the key might exist
  }

  /**
   * Computes a hash code for the given key and hash function index.
   * @param key - The key to hash.
   * @param i - The index of the hash function.
   * @returns The computed hash code.
   */
  private getHash(key: string, i: number): number {
    // Use a combination of the key's hash code and its length
    return key.hashCode() + i * key.length;
  }
}

// Extend String.prototype to include a hashCode method (similar to Java's String.hashCode)
declare global {
  interface String {
    hashCode(): number;
  }
}

String.prototype.hashCode = function (): number {
  let hash = 0;
  for (let i = 0; i < this.length; i++) {
    const char = this.charCodeAt(i);
    hash = (hash << 5) - hash + char; // Same as Java's hashCode implementation
    hash |= 0; // Convert to 32-bit integer
  }
  return hash;
};

export default BloomFilter;
