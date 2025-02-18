//https://medium.com/@vinciabhinav7/cassandra-internals-sstables-the-secret-sauce-that-makes-cassandra-super-fast-3d5badac8eaf

use std::collections::HashMap;



pub struct SSTable {
    file_name_and_sstable_map: HashMap<String, SSTable>,
    sstable_directory: String,
    log_file_name: String,
    segment_size: usize,
    max_tree_size: usize,

}
impl SSTable {
    pub  fn new(){
        
    }
}