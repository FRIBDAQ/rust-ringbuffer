//!
//! The rinbgbufer crate provides access to NSCLDAQ ring buffers.
//! the primary intent of the package is to provide support for a RUST
//! port of the RingMaster server program.   However reasonably complete
//! support for the capabilities of the CRingBuffer class is also
//! provided which can allow one to build on top of this crate to build Rust
//! clients of NSCLDAQ - as long as those additions contact the RingMaster
//! to register clients.
//!
pub mod ringbuffer {

    // *** IMPORTANT - see note prior to tests about how tests must be run.
    use memmap::MmapMut;
    use std::cmp;
    use std::fs::OpenOptions;
    use std::mem;
    use std::process;
    use std::ptr;
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
    /// is represented by a ClientInformation structure:
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
    ///
    /// Contains the mapping object that provides access to a ringbuffer.
    /// This provides a safe interface to the inherently unsafe ringbuffer.
    ///  See the implementation for more.
    ///
    pub struct RingBufferMap {
        map: memmap::MmapMut,
    }
    ///
    /// for a given consumer (used) this provides
    /// information about the data that's available to it
    /// and the free space the producer has to put data before
    /// running into that consumers get offset.
    ///
    pub struct ConsumerUsage {
        pub pid: u32,
        pub free: usize,
        pub available: usize,
    }

    ///
    /// provides information about the ring status.
    ///
    /// -   producer_pid is the pid of the producer.  This is 0xffffffff
    ///     if there is no producer.
    /// -   free_space is the amount of free space available before the
    ///     producer bumps into the most behind consumer
    /// -   max_queued is the maximum number of bytes available to any
    ///     consumer.
    /// -   consumer_usage is a vector of ConsumerUsage objects, one for
    ///     allocated consumer object.
    ///
    /// Note the computation for free and available are done independent of
    /// the existence of a producer.  This is because even after a producer exits,
    /// the consumers want to get any data from it that they've not yet gotten
    ///
    pub struct RingStatus {
        pub producer_pid: u32,
        pub free_space: usize,
        pub max_queued: usize,
        pub consumer_usage: Vec<ConsumerUsage>,
    }

    /// Presently the only way to construct a ring buffer
    /// is by mapping a ring buffer file.
    ///  On success, the user receives a raw pointer to
    impl RingBufferMap {
        ///
        /// convert the object's map to a reference to a ringbuffer
        ///  
        fn as_ref(&self) -> &RingBuffer {
            let p = self.map.as_ptr() as *const RingBuffer;
            unsafe { &*p }
        }
        ///
        /// Convert the object's map to a mutable reference to a ringbuffer.
        ///
        fn as_mut_ref(&mut self) -> &mut RingBuffer {
            let p = self.map.as_mut_ptr() as *mut RingBuffer;
            unsafe { &mut *p }
        }
        ///
        /// Check that a mapped file has the correct 'magic' string
        /// at the beginning of it.  Note that we must trim the nulls from the
        /// back end  of the magic string in the file.
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
        ///
        /// Determine the modulo distance between two offsets.
        /// The first offset is considered to be before the second offset.
        /// This can be used to determine the number of bytes of data available
        /// to a consumer or the number of free bytes in the ring buffer.
        ///
        fn distance(&self, off1: usize, off2: usize) -> usize {
            // Two cases, off1 < off2, it's a straight difference.
            // if off 1 > off2, it's a modulo distance.

            if off1 <= off2 {
                off2 - off1
            } else {
                (off2 - self.data_offset()) + (self.top_offset() - off1) + 1
            }
        }
        // Take a file which ought to be a ring buffer and map it:

        ///
        ///  Map to an existing ring buffer (the rust interface does not
        /// have a create method to create a new ring buffer file...yet).
        /// The parameter is the path to an existing file.
        /// The RingBufferMap object returned on the successful return
        /// can then be used to call object methods defined below to get things
        /// done.
        ///
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

        ///
        /// returns the maximum number of consumers that can connect
        /// to this object.
        ///
        pub fn max_consumers(&self) -> usize {
            self.as_ref().header.max_consumer
        }
        ///
        /// Returns the size of the data section of the ring buffer file.
        /// Units are units of u8.
        ///
        pub fn data_bytes(&self) -> usize {
            self.as_ref().header.data_bytes
        }
        ///
        /// Return the offset in bytes to the data section of the ring buffer
        /// file.   This represents a single producer, multi-consumer circular
        /// buffer of bytes that is data_bytes() lage
        ///
        pub fn data_offset(&self) -> usize {
            // Needed for testing.
            self.as_ref().header.data_offset
        }
        ///
        /// Return the offset in bytes to the top of the ring buffer.
        /// Note that normally data_offset and data_bytes are sufficient
        /// to perform appropriate computations (in fact, data_offset +  data+bytes
        /// should be top_offset+1)
        ///
        pub fn top_offset(&self) -> usize {
            self.as_ref().header.top_offset
        }
        ///
        /// Returns a mutable refrence to the ClientInformation
        /// struct that defines the producer.  Each ring buffer has
        /// at most one producer.
        ///
        pub fn producer(&mut self) -> &mut ClientInformation {
            &mut self.as_mut_ref().producer
        }
        ///
        /// Returns a mutable reference to the selected (by n) consumer's
        /// ClientInformation struct
        /// on success or an error message on failure.  There are a limited
        /// number of consumer  ClientInformation structs.  Errors include
        /// asking for one with n out of range.
        ///
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

        /// Claims the producer ClientInformation on behalf of the
        /// process identified by pid.  On success, the pid is returned
        /// otherwise an error message is returned.  The main error is
        /// that there's alreadya producer that's claimed the slot and it
        /// is not the specified pid.  We allow claims by the pid that
        /// already is producing just to be nice.  This can also allow different
        /// sections of a producing program to (appropriately locking) contribute
        /// data to same ring buffer.
        ///
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
        ///
        /// Marks the producer ClientInformation struct unused.  
        /// This is legal if the pid parameter matches the pid
        /// of the process that currently owns the ClientInformation
        /// or the ClientInformation struct is already unused.  Producers
        /// must call this _only_ after all puts to the ring buffer are
        /// done.  
        ///
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
        ///
        /// sets the consumer slot n to be owned used by the process pid. On success
        /// the pid is returned, otherwise an error message is returned.  Since
        /// allocating a slot also resets its get pointer to the producer's put pointer,
        /// multiple allocations by the same pid are _not_ allowed in contrast to
        /// the producer slot -- where they are discouraged but allowed.
        ///
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
        ///
        /// free consumer slot n the slot must either already be free or
        /// owned by pid.  On success, the pid is returned.
        ///
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
        // More interesting methods:

        /// Return the RingUsage struct that reflects the current
        /// state of the ringbuffer.
        ///
        pub fn get_usage(&mut self) -> RingStatus {
            // Init the result

            let mut used: usize = 0;
            let mut free = self.data_bytes();
            let mut consumer_info = vec![];

            // Fill it in based on the producer/consumer info:

            let producer_offset = self.producer().offset;
            let size = self.data_bytes();
            for cons in 0..self.max_consumers() {
                let consumer = self.consumer(cons).unwrap();
                let consumer_pid = consumer.pid; // these prevent mutable
                let consumer_offset = consumer.offset; // immutable borrow conflicts

                if consumer.pid != UNUSED_ENTRY {
                    let avail = self.distance(consumer_offset, producer_offset);

                    let usage = ConsumerUsage {
                        pid: consumer_pid,
                        free: size - avail,
                        available: avail,
                    };
                    free = cmp::min(free, usage.free);
                    used = cmp::max(used, usage.available);
                    consumer_info.push(usage);
                }
            }
            RingStatus {
                producer_pid: self.producer().pid,
                free_space: free,
                max_queued: used,
                consumer_usage: consumer_info,
            }
        }
        ///
        /// Produce data into the ring:
        /// - The process doing this must own the producer.
        /// - There must be sufficent free space in the ring.
        ///
        pub fn produce(&mut self, data: &[u8]) -> Result<usize, String> {
            if self.producer().pid != process::id() {
                return Err(String::from("Caller is not the producer"));
            }
            let usage = self.get_usage();
            if usage.free_space < data.len() {
                Err(String::from("Insufficient free space in ring"))
            } else {
                // Two cases - either a single copy or
                // the copy must be broken up into two copies because the
                // data will wrap:

                let mut put_offset = self.producer().offset;
                let top_offset = self.top_offset();
                let bottom_offset = self.data_offset();

                let dist_to_top = self.distance(put_offset, top_offset);
                // Regardless we need a u8 pointer to the
                // put area:

                let dest = self.map.as_mut_ptr() as *mut u8;
                let src = &data[0] as *const u8;
                let dest = unsafe { dest.offset(put_offset as isize) }; // put pointer now.`
                if dist_to_top <= data.len() {
                    unsafe {
                        ptr::copy_nonoverlapping(src, dest, data.len());
                    };
                    put_offset = put_offset + data.len();
                    if put_offset > top_offset {
                        put_offset = bottom_offset;
                    }
                    self.producer().offset = put_offset;
                    return Ok(data.len());
                } else {
                    // Must do two copies.
                    // The first one goes to the end of the ring buffer,
                    // the second one puts the remainder to the beginning:
                    // This can be done recursively as two puts that don't wrap:
                    // Note that this point is not atomic as the put_offset
                    // is updated twice.
                    //
                    self.produce(&data[0..self.distance(put_offset, top_offset)])?;
                    self.produce(&data[self.distance(put_offset, top_offset)..])?;
                    return Ok(data.len());

                    // The two recursive put calls already updated the producer
                    // offset pointer.
                }
            }
        }
    }

    ///
    /// Producer client
    /// Note that the implementation of this client does not provide the
    /// ability to talk to the ring master process.  That must be layered on top
    /// of this code.
    ///
    /// #### Important Note:
    ///     In order to be thread-safe, the objects in this module
    /// don't encpasulate RingBufferMaps. but rather encapsulate
    /// ThreadSafeRingBuffer objects which are just RingBufferMap
    /// objects encapsulated in Arc/Mutex containers.  The code
    /// inside the implementations is written in (as best I can) a
    /// threadsafe manner.
    ///
    pub mod producer {
        use super::*;
        use std::process;
        use std::sync::Arc;
        use std::sync::Mutex;
        use std::thread;
        use std::time::Duration;

        /// Possible errors in enum form.
        ///
        pub enum Error {
            ProducerExists,
            Unimplemented,
        }

        ///
        /// Converts Error values into a string fit for human consumption
        ///
        pub fn error_string(e: &Error) -> String {
            match e {
                Error::ProducerExists => {
                    String::from("A producer already exists for this ringbuffer")
                }
                Error::Unimplemented => String::from("This feature is not yet implemented"),
            }
        }

        type ThreadSafeRingBuffer = Arc<Mutex<RingBufferMap>>;
        ///
        /// Creating this object will result in an attempt to allocate the
        /// producer slot of the ring buffer.  Thus creation _can_ fail  if
        /// there's alread a producer for the ring.
        ///
        pub struct Producer {
            ring_buffer: ThreadSafeRingBuffer,
        }

        impl Producer {
            /// attach - this is how you get a new Producer object
            ///
            pub fn attach(ring: &ThreadSafeRingBuffer) -> Result<Producer, Error> {
                let mut guard = ring.lock().unwrap();
                match guard.set_producer(process::id()) {
                    Err(_) => Err(Error::ProducerExists),
                    Ok(_) => Ok(Producer {
                        ring_buffer: Arc::clone(ring),
                    }),
                }
            }
            ///
            /// Put data into the ring If necessary, we block
            /// until sufficient room is available.
            ///
            pub fn blocking_put(&mut self, data: &[u8]) {
                let nbytes = data.len();

                // Block if needed -- This is done by polling in 100usec
                // intervals.

                let mut capacity = self.ring_buffer.lock().unwrap().get_usage();
                while capacity.free_space < nbytes {
                    thread::sleep(Duration::from_micros(100));
                    capacity = self.ring_buffer.lock().unwrap().get_usage();
                }
                // Now we can do the copy.

                self.ring_buffer.lock().unwrap().produce(data).unwrap();
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
        use std::process;
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
        #[test]
        fn offset_sanity() {
            let ring = RingBufferMap::new("poop").unwrap();
            assert_eq!(
                ring.data_offset() + ring.data_bytes(),
                ring.top_offset() + 1
            );
        }
        #[test]
        fn distance_1() {
            // Offsets are - 0 distance from themselves:
            let ring = RingBufferMap::new("poop").unwrap();
            assert_eq!(0, ring.distance(ring.data_offset(), ring.data_offset()));
            assert_eq!(0, ring.distance(ring.top_offset(), ring.top_offset()));
        }
        #[test]
        fn distance_2() {
            // bottom is data_bytes -1 distance from top.

            let ring = RingBufferMap::new("poop").unwrap();
            assert_eq!(
                ring.data_bytes() - 1,
                ring.distance(ring.data_offset(), ring.top_offset())
            );
            assert_eq!(1, ring.distance(ring.top_offset(), ring.data_offset()),);
        }
        #[test]
        fn distance_3() {
            // just some distances between off1 < off2 cases
            let ring = RingBufferMap::new("poop").unwrap();
            let mut expected = ring.data_bytes() - 1;
            for i in ring.data_offset()..ring.data_offset() + 1000 {
                assert_eq!(expected, ring.distance(i, ring.top_offset()));
                expected = expected - 1;
            }
        }
        #[test]
        fn distance_4() {
            // some distances between off1 > off2:

            let ring = RingBufferMap::new("poop").unwrap();
            let mut expected = 1;

            for i in 0..1000 {
                assert_eq!(
                    expected,
                    ring.distance(ring.top_offset() - i, ring.data_offset())
                );

                expected = expected + 1;
            }
        }
        #[test]
        fn status_1() {
            let mut ring = RingBufferMap::new("poop").unwrap();
            // Ensure there are no used descriptors:

            ring.producer().pid = UNUSED_ENTRY;
            for i in 0..ring.max_consumers() {
                ring.consumer(i).unwrap().pid = UNUSED_ENTRY;
            }

            let usage = ring.get_usage();
            assert_eq!(UNUSED_ENTRY, usage.producer_pid);
            assert_eq!(ring.data_bytes(), usage.free_space);
            assert_eq!(0, usage.max_queued);
            assert_eq!(0, usage.consumer_usage.len());
        }
        #[test]
        fn status_2() {
            let mut ring = RingBufferMap::new("poop").unwrap();
            let producer = 12345;
            ring.producer().pid = producer;
            for i in 0..ring.max_consumers() {
                ring.consumer(i).unwrap().pid = UNUSED_ENTRY;
            }

            let usage = ring.get_usage();
            assert_eq!(producer, usage.producer_pid);
            assert_eq!(ring.data_bytes(), usage.free_space);
            assert_eq!(0, usage.max_queued);
            assert_eq!(0, usage.consumer_usage.len());
        }
        #[test]
        fn status_3() {
            let mut ring = RingBufferMap::new("poop").unwrap();
            let producer = 12345;
            let consumer = 54321;
            ring.producer().pid = producer;

            // one caught up consumer:

            for i in 0..ring.max_consumers() {
                ring.consumer(i).unwrap().pid = UNUSED_ENTRY;
            }
            ring.set_consumer(3, consumer).unwrap();

            let status = ring.get_usage();
            assert_eq!(producer, status.producer_pid);
            assert_eq!(ring.data_bytes(), status.free_space);
            assert_eq!(0, status.max_queued);
            assert_eq!(1, status.consumer_usage.len());
            assert_eq!(consumer, status.consumer_usage[0].pid);
            assert_eq!(ring.data_bytes(), status.consumer_usage[0].free);
            assert_eq!(0, status.consumer_usage[0].available);

            ring.free_consumer(3, consumer).unwrap();
        }
        #[test]
        fn status_4() {
            // 1 consumer that's 100 bytes behind the producer.

            let mut ring = RingBufferMap::new("poop").unwrap();
            let producer = 12345;
            let consumer = 54321;
            let data_size = 100;
            ring.set_producer(producer).unwrap();
            for i in 0..ring.max_consumers() {
                ring.consumer(i).unwrap().pid = UNUSED_ENTRY;
            }
            ring.set_consumer(3, consumer).unwrap();

            // add data_size bytes to the ring the consuerm does not have:
            // Not done this way, the optimizer seems happy enough to order
            // set_consumer following the offset setting done here.

            ring.producer().offset = ring.consumer(3).unwrap().offset + data_size;
            let status = ring.get_usage();

            assert_eq!(producer, status.producer_pid);
            assert_eq!(ring.data_bytes() - data_size, status.free_space);
            assert_eq!(data_size, status.max_queued);
            assert_eq!(1, status.consumer_usage.len());
            assert_eq!(consumer, status.consumer_usage[0].pid);
            assert_eq!(ring.data_bytes() - data_size, status.consumer_usage[0].free);
            assert_eq!(data_size, status.consumer_usage[0].available);

            for i in 0..ring.max_consumers() {
                ring.consumer(i).unwrap().pid = UNUSED_ENTRY;
            }
            ring.free_producer(producer).unwrap();
        }
        #[test]
        fn status_5() {
            // one consumer behind by 100 but over a wrap:__rust_force_expr!

            let mut ring = RingBufferMap::new("poop").unwrap();
            let producer = 12345;
            let consumer = 54321;
            let data_size = 100;
            ring.set_producer(producer).unwrap();
            for i in 0..ring.max_consumers() {
                ring.consumer(i).unwrap().pid = UNUSED_ENTRY;
            }
            ring.set_consumer(4, consumer).unwrap();
            ring.producer().offset = ring.data_offset() + data_size - 1;
            ring.consumer(4).unwrap().offset = ring.top_offset();

            let status = ring.get_usage();

            assert_eq!(producer, status.producer_pid);
            assert_eq!(ring.data_bytes() - data_size, status.free_space);
            assert_eq!(data_size, status.max_queued);
            assert_eq!(1, status.consumer_usage.len());
            assert_eq!(consumer, status.consumer_usage[0].pid);
            assert_eq!(ring.data_bytes() - data_size, status.consumer_usage[0].free);
            assert_eq!(data_size, status.consumer_usage[0].available);

            for i in 0..ring.max_consumers() {
                ring.consumer(i).unwrap().pid = UNUSED_ENTRY;
            }
            ring.free_producer(producer).unwrap();
        }
        #[test]
        fn status_6() {
            // two consumers one 100, the other 200 back.. the point
            // is to see the overall free and avail are correctly computed.

            let mut ring = RingBufferMap::new("poop").unwrap();
            let producer = 12345;
            let consumer = 54321;
            let data_size = 100;
            ring.set_producer(producer).unwrap();
            for i in 0..ring.max_consumers() {
                ring.consumer(i).unwrap().pid = UNUSED_ENTRY;
            }
            ring.set_consumer(2, consumer).unwrap();
            ring.set_consumer(3, consumer + 1).unwrap();
            ring.consumer(3).unwrap().offset = ring.consumer(2).unwrap().offset + data_size;

            ring.set_producer(producer).unwrap();
            ring.producer().offset = ring.consumer(3).unwrap().offset + data_size;

            let status = ring.get_usage();
            assert_eq!(data_size * 2, status.max_queued);
            assert_eq!(ring.data_bytes() - data_size * 2, status.free_space);

            for i in 0..ring.max_consumers() {
                ring.consumer(i).unwrap().pid = UNUSED_ENTRY;
            }
            ring.free_producer(producer).unwrap();
        }
        // test produce method:

        #[test]
        fn produce_err1() {
            let mypid = process::id();
            let mut ring = RingBufferMap::new("poop").unwrap();

            // Set the put pointers etc to the start of the ring and
            // producer pid cleanly to mypid+1 so we don't own:

            ring.producer().pid = UNUSED_ENTRY; // So we can allocated.
            ring.producer().offset = ring.data_offset();

            ring.set_producer(mypid + 1).unwrap(); // anything but us.

            let data: [u8; 10] = [0; 10];
            assert!(ring.produce(&data).is_err());
        }
    }
}
