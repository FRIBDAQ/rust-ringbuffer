use memmap::Mmap;
use std::fs::File;
use std::io;
use std::io::Error;
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
    map: memmap::Mmap,
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

    fn check_magic(map: &memmap::Mmap) -> bool {
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
        match File::open(ring_file) {
            Ok(fp) => {
                match unsafe { Mmap::map(&fp) } {
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
}
