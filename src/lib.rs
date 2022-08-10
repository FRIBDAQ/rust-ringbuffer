// *** IMPORTANT - see note prior to tests about how tests must be run.

use memmap::MmapMut;
use std::fs::OpenOptions;
use std::mem;
use std::str;
use std::string::ToString;

static MAGIC_STRING: &str = "NSCLRing";
static UNUSED_ENTRY: u32 = 0xffffffff;
///
/// RingHeader
///    is the header of a ring buffer:
///
#[repr(C)]
pub struct RingHeader {
    magic_string: [u8; 32],
    max_consumer: usize,
    data_bytes: usize,
    producer_offset: usize,
    consumer_offset: usize,
    data_offset: usize,
    top_offset: usize,
}

/// Each client (producr or consumer)
/// is represented by a client information structure:
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct ClientInformation {
    offset: usize, // Put/Get offset.
    pid: u32,      // Process ID owning or UNUSED_ENTRY if free.
}

/// The actual ring buffer is quite a bit simpler in practice:
///
#[repr(C)]
pub struct RingBuffer {
    header: RingHeader,
    producer: ClientInformation,
    first_consumer: ClientInformation,
}
pub struct RingBufferMap {
    map: memmap::MmapMut,
}
///
/// Presently the only way to construct a ring buffer
/// is by mapping a ring buffer file.
///  On success, the user receives a raw pointer to
impl RingBufferMap {
    fn as_ref(&self) -> &RingBuffer {
        let p = self.map.as_ptr() as *const RingBuffer;
        unsafe { &*p }
    }
    fn as_mut_ref(&mut self) -> &mut RingBuffer {
        let p = self.map.as_mut_ptr() as *mut RingBuffer;
        unsafe { &mut *p }
    }

    fn check_magic(map: &memmap::MmapMut) -> bool {
        // Make a raw pointer to a ringbuffer and turn it into a ref:

        let p = map.as_ptr() as *const RingBuffer;
        let r = unsafe { &*p };
        let magic_value = String::from(str::from_utf8(&r.header.magic_string).unwrap());
        let magic_value = magic_value.trim();
        let magic_value = magic_value.trim_matches('\0');
        let magic_expected = String::from(MAGIC_STRING);
        let magic_expected = magic_expected.trim();

        magic_value == magic_expected
    }
    // Take a file which ought to be a ring buffer and map it:

    pub fn new(ring_file: &str) -> Result<RingBufferMap, String> {
        match OpenOptions::new()
            .write(true)
            .read(true)
            .create(false)
            .open(ring_file)
        {
            Ok(fp) => {
                match unsafe { MmapMut::map_mut(&fp) } {
                    Ok(map) => {
                        // Ensure this could be a ring buffer:

                        if map.len() < mem::size_of::<RingBuffer>() {
                            return Err(format!("{} is not a valid ring buffer", ring_file));
                        } else {
                            if Self::check_magic(&map) {
                                Ok(RingBufferMap { map: map })
                            } else {
                                Err(format!(
                                    "{} does not have the correct magic string for a ringbuffer",
                                    ring_file
                                ))
                            }
                        }
                    }
                    Err(e) => Err(e.to_string()),
                }
            }
            Err(e) => Err(e.to_string()),
        }
    }
    // getters

    pub fn max_consumers(&self) -> usize {
        self.as_ref().header.max_consumer
    }
    pub fn data_bytes(&self) -> usize {
        self.as_ref().header.data_bytes
    }
    pub fn data_offset(&self) -> usize {
        // Needed for testing.
        self.as_ref().header.data_offset
    }
    pub fn producer(&mut self) -> &mut ClientInformation {
        &mut self.as_mut_ref().producer
    }
    pub fn consumer(&mut self, n: usize) -> Result<&mut ClientInformation, String> {
        let me = self.as_mut_ref();
        if n < me.header.max_consumer {
            let pconsumer = &mut (me.first_consumer) as *mut ClientInformation;
            Ok(unsafe { &mut *pconsumer.offset(n as isize) })
        } else {
            Err(format!(
                "Consumer {} does  not exist, max: {}",
                n,
                me.header.max_consumer - 1
            ))
        }
    }
    // Controlled mutators:

    pub fn set_producer(&mut self, pid: u32) -> Result<u32, String> {
        let mut producer = self.producer();
        if (producer.pid == UNUSED_ENTRY) || (producer.pid == pid) {
            producer.pid = pid;
            Ok(pid)
        } else {
            Err(format!(
                "Producer is already allocated to PID {}",
                producer.pid
            ))
        }
    }
    pub fn free_producer(&mut self, pid: u32) -> Result<u32, String> {
        let mut producer = self.producer();

        // We can only free entries which are already free or
        // that the pid owns:

        if (producer.pid == UNUSED_ENTRY) || (producer.pid == pid) {
            producer.pid = UNUSED_ENTRY;
            Ok(pid)
        } else {
            Err(format!(
                "PID {} is unable to free producer owned by {}",
                pid, producer.pid
            ))
        }
    }
    pub fn set_consumer(&mut self, n: usize, pid: u32) -> Result<u32, String> {
        let producer_offset = self.producer().offset; // snapshot producer offset.
        match self.consumer(n) {
            Ok(cons) => {
                // Note that self ownership fails since we modify the
                // offset.
                if cons.pid == UNUSED_ENTRY {
                    cons.pid = pid;
                    cons.offset = producer_offset; //Skip to producer pos.
                    Ok(pid)
                } else {
                    Err(format!(
                        "Consumer {} is already allocated to {}",
                        n, cons.pid
                    ))
                }
            }
            Err(reason) => Err(reason),
        }
    }

    pub fn free_consumer(&mut self, n: usize, pid: u32) -> Result<u32, String> {
        match self.consumer(n) {
            Ok(cons) => {
                if (cons.pid == UNUSED_ENTRY) || (cons.pid == pid) {
                    cons.pid = UNUSED_ENTRY;
                    Ok(pid)
                } else {
                    Err(format!(
                        "Consumer slot {} is not owned by {}, {} owns it",
                        n, pid, cons.pid
                    ))
                }
            }
            Err(reason) => Err(reason),
        }
    }
}

// Note the tests below must be run:
//  cargo test --lib -- --test-threads=1
// This is because all tests use the same ringbuffer and
// there are therefore race conditions that can occur between threads
// as ring buffer access is _not_ threadsafe.   If threaded use
// of a ring buffer is desired it should be wrapped in a mutex  e.g.

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn map_fail() {
        let result = RingBufferMap::new("Cargo.toml");
        assert!(result.is_err());
    }
    #[test]
    fn map_ok() {
        // Requires our 'poop' file - valid ring buffer.
        let result = RingBufferMap::new("poop");
        assert!(result.is_ok());
    }
    #[test]
    fn max_consumers() {
        let ring = RingBufferMap::new("poop").unwrap();
        assert_eq!(100, ring.max_consumers());
    }
    #[test]
    fn get_producer() {
        let mut ring = RingBufferMap::new("poop").unwrap();
        let p = ring.producer();
        assert_eq!(UNUSED_ENTRY, p.pid);
    }
    #[test]
    fn get_consumer_fail() {
        let mut ring = RingBufferMap::new("poop").unwrap();

        assert!(ring.consumer(100).is_err());
    }
    #[test]
    fn get_consumer_ok1() {
        let mut ring = RingBufferMap::new("poop").unwrap();
        assert!(ring.consumer(0).is_ok());
    }
    #[test]
    fn get_consumer_ok2() {
        let mut ring = RingBufferMap::new("poop").unwrap();
        let offset = ring.data_offset();
        let consumer = ring.consumer(10).unwrap();
        assert_eq!(
            ClientInformation {
                offset: offset,
                pid: UNUSED_ENTRY
            },
            *consumer
        );
    }
    #[test]
    fn alloc_producer_ok1() {
        // Client entry is unsed:

        let mut ring = RingBufferMap::new("poop").unwrap();
        ring.producer().pid = UNUSED_ENTRY;
        let result = ring.set_producer(1234);
        assert!(result.is_ok());
        assert_eq!(1234, result.unwrap());

        ring.producer().pid = UNUSED_ENTRY; // wish I had teardown.
    }
    #[test]
    fn alloc_producer_ok2() {
        // client entry is used but by us already

        let mut ring = RingBufferMap::new("poop").unwrap();

        ring.producer().pid = 1234;
        let result = ring.set_producer(1234);
        assert!(result.is_ok());
        assert_eq!(1234, result.unwrap());
        ring.producer().pid = UNUSED_ENTRY; // wish I had teardown.
    }
    #[test]
    fn alloc_producer_fail() {
        // already owned by someone else:

        let mut ring = RingBufferMap::new("poop").unwrap();
        ring.producer().pid = 1235;
        let result = ring.set_producer(1234);
        assert!(result.is_err());
        ring.producer().pid = UNUSED_ENTRY;
    }
    #[test]
    fn free_producer_fail() {
        // Owned all right but by someone else:

        let mut ring = RingBufferMap::new("poop").unwrap();
        ring.producer().pid = 12345;
        assert!(ring.free_producer(1234).is_err());
        ring.producer().pid = UNUSED_ENTRY;
    }
    #[test]
    fn free_producer_ok1() {
        // if not owned it can be freed by anyone:

        let mut ring = RingBufferMap::new("poop").unwrap();
        ring.producer().pid = UNUSED_ENTRY;
        assert!(ring.free_producer(12345).is_ok());
        assert_eq!(UNUSED_ENTRY, ring.producer().pid);
    }
    #[test]
    fn free_producer_ok2() {
        // We own the producer so it's ok to release it:

        let mut ring = RingBufferMap::new("poop").unwrap();
        ring.producer().pid = 1234;
        assert!(ring.free_producer(1234).is_ok());
        assert_eq!(UNUSED_ENTRY, ring.producer().pid);
    }
    #[test]
    fn alloc_consumer_fail1() {
        // Invalid consumer index:

        let mut ring = RingBufferMap::new("poop").unwrap();
        assert!(ring.set_consumer(ring.max_consumers(), 1).is_err());
    }
    #[test]
    fn alloc_consumer_fail2() {
        // Already allocated to someone else.

        let mut ring = RingBufferMap::new("poop").unwrap();
        ring.consumer(1).unwrap().pid = 123;
        assert!(ring.set_consumer(1, 12345).is_err());
        ring.consumer(1).unwrap().pid = UNUSED_ENTRY;
    }
    #[test]
    fn alloc_consumer_ok() {
        let mut ring = RingBufferMap::new("poop").unwrap();
        ring.consumer(1).unwrap().pid = UNUSED_ENTRY;

        assert!(ring.set_consumer(1, 12345).is_ok());

        assert_eq!(
            ClientInformation {
                pid: 12345,
                offset: ring.producer().offset,
            },
            *ring.consumer(1).unwrap()
        );
        ring.consumer(1).unwrap().pid = UNUSED_ENTRY;
    }
    #[test]
    fn free_consumer_fail1() {
        let mut ring = RingBufferMap::new("poop").unwrap();
        // invalid slot number:

        assert!(ring.free_consumer(ring.max_consumers(), 1234).is_err());
    }
    #[test]
    fn free_consumer_fail2() {
        let mut ring = RingBufferMap::new("poop").unwrap();
        ring.consumer(1).unwrap().pid = 666;
        assert!(ring.free_consumer(1, 12345).is_err());
        ring.consumer(1).unwrap().pid = UNUSED_ENTRY;
    }
    #[test]
    fn free_consumer_ok1() {
        let mut ring = RingBufferMap::new("poop").unwrap();
        ring.consumer(1).unwrap().pid = 12345;
        let result = ring.free_consumer(1, 12345);
        assert!(result.is_ok());
        assert_eq!(12345, result.unwrap());
    }
    #[test]
    fn free_consumer_ok2() {
        // allowed to free a free slot
        let mut ring = RingBufferMap::new("poop").unwrap();
        ring.consumer(1).unwrap().pid = UNUSED_ENTRY;

        assert!(ring.free_consumer(1, 12345).is_ok());
    }
}
