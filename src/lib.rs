use memmap::MmapMut;
use std::fs::File;
use std::fs::OpenOptions;
use std::mem;
use std::str;
use std::string::ToString;

static MAGIC_STRING: &str = "NSCLRing";
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
    pid: u32,      // Process ID owning or 0xffffffff if free.
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
        Err(String::from("unimplemented"))
    }
    pub fn set_consumer(&mut self, pid: u32, n: isize) -> Result<u32, String> {
        Err(String::from("Unimplemented"))
    }
    pub fn free_producer(&mut self, pid: u32) -> Result<u32, String> {
        Err(String::from("Unimplemented"))
    }
    pub fn free_consumer(&mut self, n: isize, pid: u32) -> Result<u32, String> {
        Err(String::from("Unimplemented"))
    }
}

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
        assert_eq!(
            ClientInformation {
                offset: 1355626,
                pid: 0xffffffff,
            },
            *p
        );
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
                pid: 0xffffffff
            },
            *consumer
        );
    }
}
