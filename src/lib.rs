#![deny(missing_docs)]
//! An append-only, on-disk key-value index with lockless reads

use std::cell::UnsafeCell;
use std::fs::{remove_file, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io;
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};

use arrayvec::ArrayVec;
use lazy_static::lazy_static;
use memmap::MmapMut;
use parking_lot::{Mutex, MutexGuard};
use seahash::SeaHasher;

const NUM_LANES: usize = 64;
const NUM_SHARDS: usize = 1024;
const PAGE_SIZE: usize = 4096;
const FIRST_LANE_PAGES: usize = 64;

// marker struct for shard-mutexes
struct Shard;

lazy_static! {
    static ref SHARDS: ArrayVec<[Mutex<Shard>; NUM_SHARDS]> = {
        let mut locks = ArrayVec::new();
        for _ in 0..NUM_SHARDS {
            locks.push(Mutex::new(Shard))
        }
        locks
    };
}

#[inline(always)]
fn hash_val<T: Hash>(t: &T) -> u64 {
    let mut hasher = SeaHasher::new();
    t.hash(&mut hasher);
    hasher.finish()
}

enum Found<'a, K, V> {
    Some(&'a Entry<K, V>),
    None(usize, usize, usize),
    Invalid(usize, usize, usize),
}

/// Marker type telling you your update was a no-op
pub type AlreadyThere = bool;

/// On-disk index structure mapping keys to values
pub struct Index<K, V> {
    lanes: UnsafeCell<ArrayVec<[MmapMut; NUM_LANES]>>,
    path: PathBuf,
    pages: Mutex<u64>,
    _marker: PhantomData<(K, V)>,
}

unsafe impl<K, V> Send for Index<K, V> {}
unsafe impl<K, V> Sync for Index<K, V> {}

#[derive(Debug)]
#[repr(C)]
struct Entry<K, V> {
    key: K,
    val: V,
    next: u64,
    kv_checksum: u64,
    next_checksum: u64,
}

// Wrapper reference for mutating entries, carrying a mutex guard
struct EntryMut<'a, K, V> {
    entry: &'a mut Entry<K, V>,
    _lock: MutexGuard<'a, Shard>,
}

impl<'a, K, V> Deref for EntryMut<'a, K, V> {
    type Target = Entry<K, V>;
    fn deref(&self) -> &Self::Target {
        &self.entry
    }
}

impl<'a, K, V> DerefMut for EntryMut<'a, K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.entry
    }
}

impl<K: Hash, V: Hash> Entry<K, V> {
    fn new(key: K, val: V) -> Self {
        let kv_checksum = hash_val(&key).wrapping_add(hash_val(&val));
        let entry = Entry {
            key,
            val,
            kv_checksum,
            next: 0,
            next_checksum: 0 + 1,
        };
        debug_assert!(entry.valid());
        entry
    }

    fn valid(&self) -> bool {
        if hash_val(&self.key).wrapping_add(hash_val(&self.val))
            == self.kv_checksum
            && self.next + 1 == self.next_checksum
        {
            true
        } else {
            false
        }
    }

    fn set_next<I: Into<u64>>(&mut self, next: I) {
        let next = next.into();
        self.next = next;
        self.next_checksum = next + 1;
    }
}

impl<K: Hash + Copy + PartialEq, V: Hash + Copy> Index<K, V> {
    /// Create or load an index at `path`
    pub fn new<P: AsRef<Path>>(path: &P) -> io::Result<Self> {
        let mut lanes = ArrayVec::new();

        // check for lane files already on disk
        for n in 0..NUM_LANES {
            let mut pathbuf = PathBuf::from(path.as_ref());
            pathbuf.push(&format!("{:02x}", n));

            if pathbuf.exists() {
                let file =
                    OpenOptions::new().read(true).write(true).open(&pathbuf)?;

                let lane_pages = Self::lane_pages(n);
                let file_len = PAGE_SIZE as u64 * lane_pages as u64;
                file.set_len(file_len)?;
                unsafe { lanes.push(MmapMut::map_mut(&file)?) };
            }
        }

        // find the number of already occupied pages
        let mut num_pages = 0;
        if let Some(last) = lanes.last() {
            // help the type inferance along a bit.
            let last: &MmapMut = last;

            // add up pages of all but the last lane, since they must all be full
            let mut full_pages = 0;
            for n in 0..lanes.len().saturating_sub(1) {
                println!("lane {}, pages {}", n, Self::lane_pages(n));
                full_pages += Self::lane_pages(n)
            }

            // do a binary search to find the last populated page in the last lane
            let mut low_bound = 0;
            let mut high_bound = Self::lane_pages(lanes.len() - 1) - 1;

            while low_bound + 1 != high_bound {
                let check = low_bound + (high_bound - low_bound) / 2;
                println!(
                    "low bound: {}, high bound: {}, check {}",
                    low_bound, high_bound, check,
                );

                let page_ofs = PAGE_SIZE * check;

                // is there a valid entry in this page?
                for slot in 0..Self::entries_per_page() {
                    let slot_ofs =
                        page_ofs + slot * mem::size_of::<Entry<K, V>>();

                    let ptr = last.as_ptr();

                    let entry: &Entry<K, V> = unsafe {
                        mem::transmute(ptr.offset(slot_ofs as isize))
                    };

                    if entry.valid() {
                        low_bound = check;
                        break;
                    }
                }
                if low_bound != check {
                    high_bound = check
                }
            }

            num_pages = full_pages + high_bound;
        }

        // create the index
        let index = Index {
            lanes: UnsafeCell::new(lanes),
            path: PathBuf::from(path.as_ref()),
            pages: Mutex::new(num_pages as u64),
            _marker: PhantomData,
        };

        // initialize index with at least one page
        if num_pages == 0 {
            assert_eq!(index.new_page()?, 0);
        }
        Ok(index)
    }

    /// Returns how many pages have been allocated so far
    pub fn pages(&self) -> usize {
        *self.pages.lock() as usize
    }

    /// Returns how many pages fit into one lane
    #[inline(always)]
    fn lane_pages(n: usize) -> usize {
        2_usize.pow(n as u32) * FIRST_LANE_PAGES
    }

    #[inline(always)]
    fn entries_per_page() -> usize {
        PAGE_SIZE / mem::size_of::<Entry<K, V>>()
    }

    // calculates the slot in the page this hashed key would
    // occupy at a certain depth
    #[inline(always)]
    fn slot(key_hash: u64, depth: usize) -> usize {
        (hash_val(&(key_hash + depth as u64)) % Self::entries_per_page() as u64)
            as usize
    }

    // produces following output over page with FIRST_LANE_PAGES = 2
    // (0, 0), (0, 1),
    // (1, 0), (1, 1), (1, 2), (1, 3),
    // (2, 0), (2, 1), (2, 2), (2, 3), (2, 4), (2, 5), (2, 6), (2, 7),
    // ... and so on and so forth ...
    #[inline(always)]
    fn lane_page(page: usize) -> (usize, usize) {
        let usize_bits = mem::size_of::<usize>() * 8;
        let i = page / FIRST_LANE_PAGES + 1;
        let lane = usize_bits - i.leading_zeros() as usize - 1;
        let page = page - (2usize.pow(lane as u32) - 1) * FIRST_LANE_PAGES;
        (lane, page)
    }

    fn new_lane(&self) -> io::Result<()> {
        let lanes_ptr = self.lanes.get();
        let lane_nr = unsafe { (*lanes_ptr).len() };

        let num_pages = Self::lane_pages(lane_nr);

        let mut path = self.path.clone();
        path.push(format!("{:02x}", lane_nr));

        let file_len = PAGE_SIZE as u64 * num_pages as u64;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        file.set_len(file_len)?;

        unsafe { (*lanes_ptr).push(MmapMut::map_mut(&file)?) }
        Ok(())
    }

    fn new_page(&self) -> io::Result<u64> {
        let mut page_nr = self.pages.lock();

        let (_, offset) = Self::lane_page(*page_nr as usize);

        if offset == 0 {
            // create new lane
            self.new_lane()?
        }

        let new_page_nr = *page_nr;
        *page_nr += 1;

        Ok(new_page_nr)
    }

    // Get a mutable reference to the `Entry`,
    fn entry(&self, lane: usize, page: usize, slot: usize) -> &Entry<K, V> {
        // Get a reference to the `Entry`
        let page_ofs = PAGE_SIZE * page;
        let slot_ofs = page_ofs + slot * mem::size_of::<Entry<K, V>>();
        unsafe {
            mem::transmute(
                (*self.lanes.get())[lane].as_ptr().offset(slot_ofs as isize),
            )
        }
    }

    // Get a mutable reference to the `Entry`,
    // locking the corresponding shard.
    fn entry_mut(
        &self,
        lane: usize,
        page: usize,
        slot: usize,
    ) -> EntryMut<K, V> {
        let shard = (page ^ slot) % NUM_SHARDS;
        // Lock the entry for writing
        let lock = SHARDS[shard].lock();

        let page_ofs = PAGE_SIZE * page;
        let slot_ofs = page_ofs + slot * mem::size_of::<Entry<K, V>>();
        EntryMut {
            entry: unsafe {
                mem::transmute(
                    (*self.lanes.get())[lane]
                        .as_ptr()
                        .offset(slot_ofs as isize),
                )
            },
            _lock: lock,
        }
    }

    // Traverse the tree to find the entry for this key
    fn find_key(&self, k: &K) -> io::Result<Found<K, V>> {
        let mut depth = 0;
        let mut abs_page = 0;
        loop {
            let hash = hash_val(&k);
            let slot = Self::slot(hash, depth);

            let (lane, page) = Self::lane_page(abs_page);
            let entry = self.entry(lane, page, slot);

            if !entry.valid() {
                return Ok(Found::Invalid(lane, page, slot));
            }

            if &entry.key == k {
                return Ok(Found::Some(entry));
            } else if entry.next == 0 {
                return Ok(Found::None(lane, page, slot));
            } else {
                abs_page = entry.next as usize;
            }

            depth += 1;
        }
    }

    /// Inserts a key-value pair into the index, if the key is already
    /// present, this is a no-op
    pub fn insert(&self, key: K, val: V) -> io::Result<AlreadyThere> {
        match self.find_key(&key)? {
            Found::Some(_) => {
                // no-op
                Ok(true)
            }
            Found::Invalid(lane, page, slot) => {
                let mut entry = self.entry_mut(lane, page, slot);

                if entry.valid() && entry.next != 0 {
                    // Someone already wrote here, recurse!
                    // We accept the performance hit of re-traversing
                    // the whole tree, since this case is uncommon,
                    // and makes the implementation simpler.
                    mem::drop(entry);
                    self.insert(key, val)
                } else {
                    *entry = Entry::new(key, val);
                    return Ok(false);
                }
            }
            Found::None(lane, page, slot) => {
                let mut entry = self.entry_mut(lane, page, slot);
                if entry.next != 0 {
                    // again, another thread was here before us
                } else {
                    entry.set_next(self.new_page()?);
                }
                // recurse
                mem::drop(entry);
                self.insert(key, val)
            }
        }
    }

    /// Looks up a value with `key` in the index
    pub fn get(&self, key: &K) -> io::Result<Option<&V>> {
        match self.find_key(key)? {
            Found::Some(entry) => Ok(Some(&entry.val)),
            _ => Ok(None),
        }
    }

    /// Removes all data from disk
    pub fn purge(&mut self) -> std::io::Result<()> {
        self.lanes = UnsafeCell::new(ArrayVec::new());
        for n in 0..NUM_LANES {
            let mut pathbuf = PathBuf::from(&self.path);
            pathbuf.push(&format!("{:02x}", n));
            if pathbuf.exists() {
                match remove_file(&pathbuf) {
                    Ok(_x) => {}
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(())
    }

    /// Get the approximate size on disk for the index
    pub fn on_disk_size(&self) -> usize {
        *self.pages.lock() as usize * PAGE_SIZE
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use rand::{seq::SliceRandom, thread_rng};
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn simple() {
        let dir = tempdir().unwrap();
        let index = Index::new(&dir).unwrap();
        index.insert(0, 0).unwrap();
        assert_eq!(index.get(&0).unwrap(), Some(&0));
        assert_eq!(index.on_disk_size(), PAGE_SIZE);
    }

    const N: u64 = 1024 * 256;

    #[test]
    fn multiple() {
        let dir = tempdir().unwrap();
        let index = Index::new(&dir).unwrap();
        for i in 0..N {
            index.insert(i, i).unwrap();
        }
        for i in 0..N {
            assert_eq!(index.get(&i).unwrap(), Some(&i));
        }
        println!("{}", index.on_disk_size());
    }

    #[test]
    fn reload() {
        let dir = tempdir().unwrap();
        let mut pages;
        {
            {
                let index_a = Index::new(&dir).unwrap();
                for i in 0..N {
                    index_a.insert(i, i).unwrap();
                }
                pages = index_a.pages();
                mem::drop(index_a);
            }

            let index_b = Index::new(&dir).unwrap();

            // make sure the page count matches
            assert_eq!(pages, index_b.pages());

            for i in 0..N {
                assert_eq!(index_b.get(&i).unwrap(), Some(&i));
            }

            for i in N..N * 2 {
                index_b.insert(i, i).unwrap();
            }
            pages = index_b.pages();
            mem::drop(index_b);
        }

        let index_c = Index::new(&dir).unwrap();

        // make sure the page count matches
        assert_eq!(pages, index_c.pages());

        for i in 0..N * 2 {
            assert_eq!(index_c.get(&i).unwrap(), Some(&i));
        }
    }

    const N_THREADS: usize = 8;

    // The stress test creates an index, and simultaneously writes
    // entries in random order from `N_THREADS` threads,
    // while at the same time reading from an equal amount of threads.
    //
    // When all threads are finished, a final read-through is made to see
    // that all key value pairs are present.
    #[test]
    fn stress() {
        let dir = tempdir().unwrap();
        let index = Arc::new(Index::new(&dir).unwrap());

        let mut all_indicies = vec![];
        for i in 0..N {
            all_indicies.push(i);
        }

        let mut rng = thread_rng();

        // shuffle the order of the writes
        let mut shuffles_write = vec![];
        for _ in 0..N_THREADS {
            let mut new = all_indicies.clone();
            SliceRandom::shuffle(&mut new[..], &mut rng);
            shuffles_write.push(new);
        }

        // shuffle the order of the reads
        let mut shuffles_read = vec![];
        for _ in 0..N_THREADS {
            let mut new = all_indicies.clone();
            SliceRandom::shuffle(&mut new[..], &mut rng);
            shuffles_read.push(new);
        }

        let mut threads_running = vec![];

        for i in 0..N_THREADS {
            // shuffled write
            let shuffle_write = mem::replace(&mut shuffles_write[i], vec![]);
            let index_write = index.clone();

            // shuffled reads
            let shuffle_read = mem::replace(&mut shuffles_read[i], vec![]);
            let index_read = index.clone();

            // write threads
            threads_running.push(thread::spawn(move || {
                for write in shuffle_write {
                    index_write.insert(write, write).unwrap();
                }
            }));

            // read threads
            threads_running.push(thread::spawn(move || {
                for read in shuffle_read {
                    match index_read.get(&read).unwrap() {
                        Some(val) => assert_eq!(val, &read),
                        None => (),
                    }
                }
            }));
        }

        // make sure all threads finish successfully
        for thread in threads_running {
            thread.join().unwrap()
        }

        for i in 0..N {
            assert_eq!(index.get(&i).unwrap(), Some(&i));
        }
    }
}
