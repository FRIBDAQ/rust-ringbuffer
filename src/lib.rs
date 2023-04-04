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

    use std::io::{Read, Write};
    use std::mem;
    use std::process;
    use std::ptr;
    use std::str;
    use std::string::ToString;
    use std::sync::Arc;
    use std::sync::Mutex;

    static MAGIC_STRING: &str = "NSCLRing";
    pub static UNUSED_ENTRY: u32 = 0xffffffff;

    pub type ThreadSafeRingBuffer = Arc<Mutex<RingBufferMap>>;
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
    impl ClientInformation {
        pub fn get_pid(&self) -> u32 {
            self.pid
        }
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
    #[derive(Debug, PartialEq)]
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
    #[derive(Debug)]
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
        /// Get the index of the first free consumer:
        ///
        fn first_free_consumer(&mut self) -> Option<u32> {
            let ncons = self.max_consumers();
            for i in 0..ncons {
                if self.consumer(i).unwrap().pid == UNUSED_ENTRY {
                    return Some(i as u32);
                }
            }
            None
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

                let mut dest = self.map.as_mut_ptr() as *mut u8;
                let src = &data[0] as *const u8;
                dest = unsafe { dest.offset(put_offset as isize) }; // put pointer now.`
                if data.len() <= dist_to_top + 1 {
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
                    self.produce(&data[0..self.distance(put_offset, top_offset) + 1])?;
                    self.produce(&data[self.distance(put_offset, top_offset) + 1..])?;
                    return Ok(data.len());

                    // The two recursive put calls already updated the producer
                    // offset pointer.
                }
                // Methods for consumers:
            }
        }
        //
        // consumable bytes in a consumer:
        //
        fn available_bytes(&self, consumer: &ClientInformation, poffset: usize) -> usize {
            self.distance(consumer.offset, poffset)
        }
        //
        // returns the number of bytes a consumer can consume from
        // the ringbuffer _now_.
        //
        fn consumable_bytes(&mut self, cidx: u32) -> Result<usize, String> {
            let poffset = self.producer().offset;
            let c;

            c = self.consumer(cidx as usize);
            if let Ok(consumer) = c {
                let consumer = consumer.clone();
                Ok(self.available_bytes(&consumer, poffset))
            } else {
                Err(String::from("Invalid consumer"))
            }
        }
        // Note that the caller must ensure that there's at least
        // data.len() bytes available to the consumer.
        //
        fn consume_from(
            &self,
            consumer: &mut ClientInformation,
            data: &mut [u8],
            t: usize,
        ) -> usize {
            let toffset = t;
            let coffset = consumer.offset;
            let bytes_to_top = self.distance(coffset, toffset);

            let bytes_to_read = data.len(); // Max we can read.

            // Two cases:  The read is contiguous or it needs to be
            // done in two contiguious gulps.

            if bytes_to_read <= bytes_to_top + 1 {
                let mut src = self.map.as_ptr() as *const u8;
                src = unsafe { src.offset(consumer.offset as isize) }; // get pointer now.`
                let dest = &mut data[0] as *mut u8;
                unsafe {
                    ptr::copy_nonoverlapping(src, dest, bytes_to_read);
                };
                // atomically dust the consumer offset:

                let mut new_offset = consumer.offset + bytes_to_read;
                if new_offset > self.top_offset() {
                    new_offset = self.data_offset();
                }
                consumer.offset = new_offset;
            } else {
                self.consume_from(consumer, &mut data[0..bytes_to_top + 1], t);
                self.consume_from(consumer, &mut data[bytes_to_top + 1..], t);
            }
            bytes_to_read
        }
        ///
        /// consumes at most the number of bytesfrom the consumer index
        /// passed to us.  If fewer bytes are availbale (including 0)
        /// that's returned.  The only errors possible are the
        /// consumer index invalid or the pid passed does not own the
        /// ring.
        pub fn consume(&mut self, index: u32, pid: u32, data: &mut [u8]) -> Result<usize, String> {
            // The tricky manuving with the consumer clone
            // is needed in order to placate the borrow checker.
            // just getting a consumer maintains a mutable reference
            // to self which does not allow the immutable references
            // that follow in method calls, so we clone the consumer
            // do our dirty work and then put the offset back into
            // the consumer in shared memory...this has the added
            // advantage of being demonstrably atomic.

            let t = self.top_offset();
            let poffset = self.producer().offset;
            let mut c;

            if let Ok(consumer) = self.consumer(index as usize) {
                c = consumer.clone();
            } else {
                return Err(String::from("Invalid consumer"));
            }

            if pid != c.pid {
                return Err(format!(
                    "{} does not own consumer {}, {} does",
                    pid, index, c.pid
                ));
            }
            let coffset = c.offset;
            let consumable = self.distance(coffset, poffset);
            if consumable == 0 {
                return Ok(0);
            }
            let n;
            if consumable < data.len() {
                n = self.consume_from(&mut c, &mut data[0..consumable], t);
            } else {
                n = self.consume_from(&mut c, data, t);
            }
            // Now we need to copy the offset back into the consumer:

            self.consumer(index as usize).unwrap().offset = c.offset;
            Ok(n)
        }
    }

    ///
    /// Producer client
    /// Note that the implementation of this client does not provide the
    /// ability to talk to the ring master process.  That must be layered on top
    /// of this code.
    ///
    /// ### Important Note:
    ///
    /// In order to be thread-safe, the objects in this module
    /// don't encpasulate RingBufferMaps. but rather encapsulate
    /// ThreadSafeRingBuffer objects which are just RingBufferMap
    /// objects encapsulated in Arc/Mutex containers.  The code
    /// inside the implementations is written in (as best I can) a
    /// threadsafe manner.
    ///
    pub mod producer {
        use super::*;
        use std::process;
        use std::thread;
        use std::time::Duration;

        /// Possible errors in enum form.
        ///
        #[derive(PartialEq, Debug)]
        pub enum Error {
            ProducerExists,
            TooMuchData,
            Timeout,
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
                Error::TooMuchData => String::from("Attempting to produce too much data"),
                Error::Timeout => String::from("Wait timed out"),
                Error::Unimplemented => String::from("This feature is not yet implemented"),
            }
        }

        ///
        /// Creating this object will result in an attempt to allocate the
        /// producer slot of the ring buffer.  Thus creation _can_ fail  if
        /// there's alread a producer for the ring.
        ///
        pub struct Producer {
            ring_buffer: ThreadSafeRingBuffer,
        }

        impl Producer {
            fn too_big(&mut self, nbytes: usize) -> bool {
                nbytes > self.capacity()
            }
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
            pub fn blocking_put(&mut self, data: &[u8]) -> Result<usize, Error> {
                let nbytes = data.len();
                if self.too_big(nbytes) {
                    return Err(Error::TooMuchData);
                }

                // Block if needed -- This is done by polling in 100usec
                // intervals.

                let mut capacity = self.ring_buffer.lock().unwrap().get_usage();
                while capacity.free_space < nbytes {
                    thread::sleep(Duration::from_micros(100));
                    capacity = self.ring_buffer.lock().unwrap().get_usage();
                }
                // Now we can do the copy.

                self.ring_buffer.lock().unwrap().produce(data).unwrap();
                Ok(nbytes)
            }
            /// put data into the ring  but only block for at most
            /// the specified time.  If we can't get space for the data,
            /// we return a timeout error.
            ///
            pub fn timed_put(&mut self, data: &[u8], max_wait: Duration) -> Result<usize, Error> {
                if self.too_big(data.len()) {
                    Err(Error::TooMuchData)
                } else {
                    let n = data.len();
                    let mut wait_time = Duration::from_secs(0);
                    let sleep_time = Duration::from_micros(100);
                    while self.free() < n {
                        thread::sleep(sleep_time);
                        wait_time = wait_time + sleep_time;
                        if wait_time > max_wait {
                            return Err(Error::Timeout);
                        }
                    }
                    // if we fall here, we have space and did not timeout:

                    self.blocking_put(data) // It won't block.
                }
            }
            ///
            /// Returns the capacity of the ring buffer.
            ///
            pub fn capacity(&mut self) -> usize {
                self.ring_buffer.lock().unwrap().data_bytes()
            }
            ///]
            /// Returns the number of free bytes for put:
            ///
            pub fn free(&mut self) -> usize {
                self.ring_buffer.lock().unwrap().get_usage().free_space
            }
        }
        /// We want to be sure that the producer is freed if we
        /// drop so:
        ///
        impl std::ops::Drop for Producer {
            fn drop(&mut self) {
                self.ring_buffer
                    .lock()
                    .unwrap()
                    .free_producer(process::id())
                    .unwrap();
            }
        }
    }
    /// This allows Producer objects to be written to like stream files:

    impl Write for producer::Producer {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if self.capacity() < buf.len() {
                Err(std::io::Error::new(
                    std::io::ErrorKind::OutOfMemory,
                    "Block too big to write",
                ))
            } else {
                if let Ok(s) = self.blocking_put(&buf) {
                    Ok(s)
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Blocking put to ring buffer failed",
                    ))
                }
            }
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    ///
    /// Implementation of consumer.
    ///   Consumer, of course consume data from
    /// a ring buffer.  We therefore will have a ring buffer map
    /// and a consumer index.   The consumer index will be
    /// allocated by the map object.  Normally all of this
    /// we'll use a drop trait to ensure that our allocaton is
    /// released on drop.
    ///
    pub mod consumer {
        use super::ThreadSafeRingBuffer;
        use std::process;
        use std::sync::Arc;
        use std::thread;
        use std::time::Duration;

        #[derive(PartialEq, Debug)]
        pub enum Error {
            NoFreeConsumers,
            TooMuchData,
            Timeout,
            Unimplemented,
        }
        pub fn error_string(e: &Error) -> String {
            match e {
                Error::NoFreeConsumers => String::from("There are no free consumer slots"),
                Error::TooMuchData => {
                    String::from("Attempting to consume more data than the ring sizse")
                }
                Error::Timeout => String::from("Wait for data timed out"),
                Error::Unimplemented => String::from("This feature is not yet implemented"),
            }
        }

        pub struct Consumer {
            map: ThreadSafeRingBuffer,
            index: u32,
            mypid: u32,
        }

        impl Consumer {
            fn too_big(&mut self, n: usize) -> bool {
                n > self.map.lock().unwrap().data_bytes()
            }
            /// Create a new consumer.  Note that this can fail
            /// if there are no free consumer slots available.
            ///
            pub fn attach(ring: &ThreadSafeRingBuffer) -> Result<Consumer, Error> {
                let mut locked_ring = ring.lock().unwrap();
                match locked_ring.first_free_consumer() {
                    Some(n) => {
                        let pid = process::id();
                        locked_ring.set_consumer(n as usize, pid).unwrap();
                        Ok(Consumer {
                            map: Arc::clone(ring),
                            index: n,
                            mypid: pid,
                        })
                    }
                    None => Err(Error::NoFreeConsumers),
                }
            }
            ///
            ///  for testing need access to the index:
            ///
            pub fn get_index(&self) -> u32 {
                self.index
            }
            /// for testing need access to the pid:
            ///
            pub fn get_pid(&self) -> u32 {
                self.mypid
            }

            ///
            /// Get data from the ring - blocking, if necessary,
            /// until the full get can be satisfied.
            ///
            pub fn blocking_get(&mut self, data: &mut [u8]) -> Result<usize, Error> {
                if self.too_big(data.len()) {
                    return Err(Error::TooMuchData);
                }
                let poll_period = Duration::from_micros(100);
                while self
                    .map
                    .lock()
                    .unwrap()
                    .consumable_bytes(self.index)
                    .unwrap()
                    < data.len()
                {
                    thread::sleep(poll_period);
                }
                Ok(self
                    .map
                    .lock()
                    .unwrap()
                    .consume(self.index, self.mypid, data)
                    .unwrap())
            }
            ///
            ///  Wait for data (if needed) only when the wait time exceeds
            /// the timeout.  If there's no data in the ring, return
            /// a timeout, else return what's there.
            ///
            pub fn timed_get(
                &mut self,
                data: &mut [u8],
                timeout: Duration,
            ) -> Result<usize, Error> {
                if self.too_big(data.len()) {
                    return Err(Error::TooMuchData);
                } else {
                    let poll_period = Duration::from_micros(100);
                    let mut waited = Duration::from_micros(0);
                    while waited < timeout {
                        if self
                            .map
                            .lock()
                            .unwrap()
                            .consumable_bytes(self.index)
                            .unwrap()
                            >= data.len()
                        {
                            return self.blocking_get(data);
                        } else {
                            thread::sleep(poll_period);
                            waited = waited + poll_period;
                        }
                    }
                    // Wait timed out and not all the data:

                    let mut data_available = self
                        .map
                        .lock()
                        .unwrap()
                        .consumable_bytes(self.index)
                        .unwrap();
                    if data_available > 0 {
                        // More data was coming in asynchronously so no larger than the actual len:

                        if data_available > data.len() {
                            data_available = data.len();
                        }
                        return self.blocking_get(&mut data[0..data_available]);
                    } else {
                        return Err(Error::Timeout);
                    }
                }
            }
        }
        impl std::ops::Drop for Consumer {
            fn drop(&mut self) {
                self.map
                    .lock()
                    .unwrap()
                    .free_consumer(self.index as usize, process::id())
                    .unwrap();
            }
        }
    }
    impl Read for consumer::Consumer {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if let Ok(s) = self.blocking_get(buf) {
                Ok(s)
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Blocking read from ringbuffer failed",
                ))
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
    mod map_tests {
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
            ring.free_consumer(4, consumer).unwrap();
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
            ring.free_consumer(2, consumer).unwrap();
            ring.free_consumer(3, consumer + 1).unwrap();
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
            ring.free_producer(mypid + 1).unwrap();
        }
        #[test]
        fn produce_err2() {
            // produce more data than can fit.

            let mut ring = RingBufferMap::new("poop").unwrap();

            ring.producer().pid = UNUSED_ENTRY; // So we can allocated.
            ring.producer().offset = ring.data_offset();
            ring.set_producer(process::id()).unwrap();

            let mut v = Vec::<u8>::new();
            v.resize(ring.data_bytes() + 1, 0); //Too big by one byte.

            assert!(ring.produce(v.as_ref()).is_err());

            ring.free_producer(process::id()).unwrap();
        }
        #[test]
        fn produce_err3() {
            // produce more data than the free space allows:

            let mut ring = RingBufferMap::new("poop").unwrap();

            ring.producer().pid = UNUSED_ENTRY; // So we can allocate.
            ring.producer().offset = ring.data_offset();
            ring.consumer(0).unwrap().pid = UNUSED_ENTRY;

            ring.set_producer(process::id()).unwrap();
            ring.set_consumer(0, process::id()).unwrap();
            ring.consumer(0).unwrap().offset = ring.data_offset() + 1; // only one byte free

            let data: [u8; 2] = [0; 2]; // but we'll try to put 2 bytes.
            assert!(ring.produce(&data).is_err());

            ring.free_producer(process::id()).unwrap();
            ring.free_consumer(0, process::id()).unwrap();
        }
        #[test]
        fn produce_ok1() {
            // Produce data that does not require a wrap.

            let mut ring = RingBufferMap::new("poop").unwrap();

            ring.producer().pid = UNUSED_ENTRY; // So we can allocate
            ring.producer().offset = ring.data_offset();
            ring.set_producer(process::id()).unwrap();

            let data: [u8; 10] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
            assert!(ring.produce(&data).is_ok());
            assert_eq!(ring.data_offset() + 10, ring.producer().offset);

            // The data should be in the ring (that's a bit harder)
            // as it requires pointers and unsafeness:

            let mut data = ring.map.as_mut_ptr() as *const u8;
            data = unsafe { data.offset(ring.data_offset() as isize) };
            for i in 0..10 {
                unsafe {
                    assert_eq!(i as u8, *data);
                    data = data.offset(1);
                }
            }

            ring.free_producer(process::id()).unwrap();
        }
        #[test]
        fn produce_ok2() {
            // if the put pointer is at the top of the
            // circular buffer we can output a single byte then
            // have to wrap.

            let mut ring = RingBufferMap::new("poop").unwrap();

            ring.producer().pid = UNUSED_ENTRY; // So we can allocate
            ring.producer().offset = ring.top_offset();
            ring.set_producer(process::id()).unwrap();

            let data: [u8; 10] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
            assert!(ring.produce(&data).is_ok());

            // To test the data all went in the right place,
            // we need to do some unsafe stuff:
            // we should see a byte of 0 at top_offset and
            // 9 bytes of 1..9 in the at data_offset:

            let data = ring.map.as_mut_ptr() as *const u8;
            let mut second_seg = unsafe { data.offset(ring.data_offset() as isize) };
            let first_seg = unsafe { data.offset(ring.top_offset() as isize) };

            assert_eq!(0, unsafe { *first_seg });
            for i in 1..10 {
                unsafe {
                    assert_eq!(i, *second_seg);
                    second_seg = second_seg.offset(1);
                }
            }
            ring.free_producer(process::id()).unwrap();
            ring.producer().offset = ring.data_offset();
        }

        #[test]
        fn first_free1() {
            // All free.
            let mut ring = RingBufferMap::new("poop").unwrap();
            for i in 0..ring.max_consumers() {
                ring.consumer(i).unwrap().pid = UNUSED_ENTRY;
            }
            let result = ring.first_free_consumer();
            assert!(result.is_some());
            assert_eq!(Some(0), result);
        }
        #[test]
        fn first_free2() {
            // first one not free:
            // All free.
            let mut ring = RingBufferMap::new("poop").unwrap();
            for i in 0..ring.max_consumers() {
                ring.consumer(i).unwrap().pid = UNUSED_ENTRY;
            }
            ring.consumer(0).unwrap().pid = 123;
            let result = ring.first_free_consumer();
            assert!(result.is_some());
            assert_eq!(Some(1), result);

            ring.free_consumer(0, 123).unwrap();
        }
        #[test]
        fn first_free3() {
            // none free:
            let mut ring = RingBufferMap::new("poop").unwrap();
            for i in 0..ring.max_consumers() {
                ring.consumer(i).unwrap().pid = 123;
            }

            let result = ring.first_free_consumer();
            assert_eq!(None, result);

            for i in 0..ring.max_consumers() {
                ring.consumer(i).unwrap().pid = UNUSED_ENTRY;
            }
        }
        #[test]
        fn avail_1() {
            let ring = RingBufferMap::new("poop").unwrap();

            let poffset = ring.data_offset();
            let c = ClientInformation {
                offset: ring.data_offset(),
                pid: 0,
            };
            assert_eq!(0, ring.available_bytes(&c, poffset));
        }
        #[test]
        fn avail_2() {
            let ring = RingBufferMap::new("poop").unwrap();
            let poffset = ring.data_offset() + 1;
            let c = ClientInformation {
                offset: ring.data_offset(),
                pid: 0,
            };

            assert_eq!(1, ring.available_bytes(&c, poffset));
        }
        #[test]
        fn avail_3() {
            let ring = RingBufferMap::new("poop").unwrap();
            let poffset = ring.data_offset();
            let c = ClientInformation {
                offset: ring.data_offset() + 1,
                pid: 0,
            };
            assert_eq!(ring.data_bytes() - 1, ring.available_bytes(&c, poffset));
        }
        #[test]
        fn consumable_err() {
            let mut ring = RingBufferMap::new("poop").unwrap();
            let max_consumers = ring.max_consumers();
            assert!(ring.consumable_bytes(max_consumers as u32 + 1).is_err());
        }
        #[test]
        fn consumable_1() {
            let mut ring = RingBufferMap::new("poop").unwrap();
            ring.producer().offset = ring.data_offset();
            ring.consumer(0).unwrap().offset = ring.data_offset();

            let result = ring.consumable_bytes(0);
            assert!(result.is_ok());
            if let Ok(n) = result {
                assert_eq!(0, n);
            }
        }
        #[test]
        fn consumable_2() {
            let mut ring = RingBufferMap::new("poop").unwrap();
            ring.producer().offset = ring.data_offset() + 1;
            ring.consumer(0).unwrap().offset = ring.data_offset();

            let result = ring.consumable_bytes(0);
            assert!(result.is_ok());
            if let Ok(n) = result {
                assert_eq!(1, n);
            }
            ring.producer().offset = ring.data_offset();
        }
        #[test]
        fn consumable_3() {
            let mut ring = RingBufferMap::new("poop").unwrap();
            ring.producer().offset = ring.data_offset();
            ring.consumer(0).unwrap().offset = ring.data_offset() + 1;

            let result = ring.consumable_bytes(0);
            assert!(result.is_ok());
            if let Ok(n) = result {
                assert_eq!(ring.data_bytes() - 1, n);
            }
            ring.consumer(0).unwrap().offset = ring.data_offset();
        }
        // Consume from tests
        // note that the caller must have already determined there
        // is at least one byte available byt this time.
        //
        #[test]
        fn consume_from_1() {
            // one byte:

            let mut ring = RingBufferMap::new("poop").unwrap();

            ring.producer().offset = ring.data_offset();
            ring.producer().pid = process::id();

            let data: [u8; 1] = [0];
            ring.produce(&data).unwrap(); // put one byte in.

            let mut c = ClientInformation {
                offset: ring.data_offset(),
                pid: 1234,
            };
            let t = ring.top_offset();
            let mut data: [u8; 1] = [0xff];
            assert_eq!(1, ring.consume_from(&mut c, &mut data, t));
            assert_eq!(0, data[0]);

            ring.producer().offset = ring.data_offset();
            ring.producer().pid = UNUSED_ENTRY;
        }
        #[test]
        fn consume_from_2() {
            // a few bytes but no wrap.__rust_force_expr!

            let mut ring = RingBufferMap::new("poop").unwrap();
            ring.producer().offset = ring.data_offset();
            ring.producer().pid = process::id();

            let data: [u8; 10] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
            ring.produce(&data).unwrap();

            let mut c = ClientInformation {
                offset: ring.data_offset(),
                pid: 1234,
            };
            let t = ring.top_offset();
            let mut data: [u8; 10] = [0; 10];
            assert_eq!(data.len(), ring.consume_from(&mut c, &mut data, t));
            for i in 0..data.len() {
                assert_eq!(i as u8, data[i]);
            }

            ring.producer().offset = ring.data_offset();
            ring.producer().pid = UNUSED_ENTRY;
        }
        #[test]
        fn consume_from_3() {
            // several wrapping bytes

            let mut ring = RingBufferMap::new("poop").unwrap();
            ring.producer().offset = ring.top_offset() - 5;
            ring.producer().pid = process::id();

            let data: [u8; 10] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
            ring.produce(&data).unwrap();

            let mut c = ClientInformation {
                offset: ring.top_offset() - 5,
                pid: 1234,
            };
            let t = ring.top_offset();
            let mut data: [u8; 10] = [0; 10];
            assert_eq!(data.len(), ring.consume_from(&mut c, &mut data, t));
            for i in 0..data.len() {
                assert_eq!(i as u8, data[i]);
            }

            ring.producer().offset = ring.data_offset();
            ring.producer().pid = UNUSED_ENTRY;
        }
        #[test]
        fn consume_err1() {
            // invalid consumer.
            let mut ring = RingBufferMap::new("poop").unwrap();

            // Bad consumer index

            let mut data: [u8; 1] = [0];
            let index = ring.max_consumers() as u32;
            let result = ring.consume(index, process::id(), &mut data);
            assert!(result.is_err());
        }
        #[test]
        fn consume_err2() {
            // not owner.
            let mut ring = RingBufferMap::new("poop").unwrap();
            ring.consumer(0).unwrap().pid = UNUSED_ENTRY;
            let mut data: [u8; 1] = [0];

            let result = ring.consume(0, process::id(), &mut data);
            assert!(result.is_err());
        }
        #[test]
        fn consume_1() {
            let mut ring = RingBufferMap::new("poop").unwrap();
            let pid = process::id();
            ring.set_producer(pid).unwrap();
            ring.set_consumer(0, pid).unwrap();

            // Nothing to consume:

            let mut data: [u8; 1] = [0];
            let result = ring.consume(0, pid, &mut data);
            assert!(result.is_ok());
            if let Ok(n) = result {
                assert_eq!(0, n);
            }

            ring.free_producer(pid).unwrap();
            ring.free_consumer(0, pid).unwrap();
        }
        #[test]
        fn consume_2() {
            // There's the exact requested qty of data.

            let mut ring = RingBufferMap::new("poop").unwrap();
            let pid = process::id();
            ring.set_producer(pid).unwrap();
            ring.set_consumer(0, pid).unwrap();

            // produce 10 byte counting pattern:

            let produced: [u8; 10] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
            ring.produce(&produced).unwrap();

            // Should be able to get 10 byte counting pattern:

            let mut data: [u8; 10] = [0xff; 10];
            if let Ok(n) = ring.consume(0, pid, &mut data) {
                assert_eq!(produced.len(), n);
                for i in 0..produced.len() {
                    assert_eq!(produced[i], data[i]);
                }
            } else {
                assert!(false, "Should have gotten Ok from consume");
            }

            ring.free_producer(pid).unwrap();
            ring.free_consumer(0, pid).unwrap();
        }
        #[test]
        fn consume_3() {
            // Too little data to satisfy the request..we get what's
            // there.

            let mut ring = RingBufferMap::new("poop").unwrap();
            let pid = process::id();
            ring.set_producer(pid).unwrap();
            ring.set_consumer(0, pid).unwrap();

            // produce 10 byte counting pattern:

            let produced: [u8; 5] = [0, 1, 2, 3, 4];
            ring.produce(&produced).unwrap();

            let mut data: [u8; 10] = [0xff; 10];
            if let Ok(n) = ring.consume(0, pid, &mut data) {
                assert_eq!(produced.len(), n);
                for i in 0..produced.len() {
                    assert_eq!(produced[i], data[i]);
                }
            } else {
                assert!(false, "Should have gotten Ok from consume");
            }

            ring.free_producer(pid).unwrap();
            ring.free_consumer(0, pid).unwrap();
        }
        #[test]
        fn consume_4() {
            // Multiple consumes:

            let mut ring = RingBufferMap::new("poop").unwrap();
            let pid = process::id();
            ring.set_producer(pid).unwrap();
            ring.set_consumer(0, pid).unwrap();

            // produce 10 byte counting pattern:

            let produced: [u8; 10] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
            ring.produce(&produced).unwrap();

            // first 1/2

            let mut data: [u8; 5] = [0xff; 5];
            if let Ok(n) = ring.consume(0, pid, &mut data) {
                assert_eq!(n, data.len());
                for i in 0..n {
                    assert_eq!(i, data[i] as usize);
                }
            } else {
                assert!(false, "Should have been ok");
            }
            // second 1/2

            if let Ok(n) = ring.consume(0, pid, &mut data) {
                assert_eq!(n, data.len());
                for i in 0..n {
                    assert_eq!(i + 5, data[i] as usize);
                }
            } else {
                assert!(false, "Should have been ok2");
            }

            ring.free_producer(pid).unwrap();
            ring.free_consumer(0, pid).unwrap();
        }
        #[test]
        fn getpid_1() {
            // Test get_pid of Client information-- yes it's trivial but ...

            let mut ring = RingBufferMap::new("poop").unwrap();
            let p = ring.producer();
            assert_eq!(p.pid, p.get_pid());
        }
    }
    #[cfg(test)]
    mod producer_test {
        use super::producer;
        use super::*;
        use std::io::Write;
        use std::sync::Mutex;
        use std::time::Duration;
        #[test]
        fn create_ok() {
            let mut ring = RingBufferMap::new("poop").unwrap();
            ring.producer().pid = UNUSED_ENTRY;

            let safe_ring = ThreadSafeRingBuffer::new(Mutex::new(ring));

            // I should be able to create a producer:

            let result = producer::Producer::attach(&safe_ring);
            assert!(result.is_ok());
            assert_eq!(process::id(), safe_ring.lock().unwrap().producer().pid);

            safe_ring
                .lock()
                .unwrap()
                .free_producer(process::id())
                .unwrap();
        }
        #[test]
        fn create_fail() {
            // pre-init the producer to be someone else:

            let mut ring = RingBufferMap::new("poop").unwrap();
            ring.producer().pid = process::id() + 1;

            let safe_ring = ThreadSafeRingBuffer::new(Mutex::new(ring));
            let result = producer::Producer::attach(&safe_ring);
            assert!(result.is_err());
            if let Err(reason) = result {
                assert_eq!(producer::Error::ProducerExists, reason);
            } else {
                panic!("Result should have been err");
            }

            safe_ring
                .lock()
                .unwrap()
                .free_producer(process::id() + 1)
                .unwrap();
        }
        #[test]
        fn produce_fail() {
            // Will fail if I try to put too many bytes:

            let mut ring = RingBufferMap::new("poop").unwrap();
            ring.producer().pid = UNUSED_ENTRY;
            let max_bytes = ring.data_bytes();
            let safe_ring = ThreadSafeRingBuffer::new(Mutex::new(ring));

            let mut producer = producer::Producer::attach(&safe_ring).unwrap();
            let mut data = Vec::<u8>::new();
            data.resize(max_bytes + 1, 0); // too many bytes.

            let result = producer.blocking_put(&data);
            assert!(result.is_err());
            if let Err(status) = result {
                assert_eq!(producer::Error::TooMuchData, status);
            } else {
                panic!("Expected error from blocking_put");
            }

            safe_ring.lock().unwrap().producer().pid = UNUSED_ENTRY;
        }
        #[test]
        fn produce_ok() {
            // Put a bit of data non-wrapping.  Need to get ok
            // and size back. Note that we've tested the
            // RingBufferMap::produce method so we don't need much checking.,

            let mut ring = RingBufferMap::new("poop").unwrap();
            ring.producer().pid = UNUSED_ENTRY;
            ring.producer().offset = ring.data_offset();

            let safe_ring = ThreadSafeRingBuffer::new(Mutex::new(ring));

            let mut producer = producer::Producer::attach(&safe_ring).unwrap();
            let data: Vec<u8> = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
            let result = producer.blocking_put(&data);
            assert!(result.is_ok());
            if let Ok(n) = result {
                assert_eq!(data.len(), n);
            }
            safe_ring
                .lock()
                .unwrap()
                .free_producer(process::id())
                .unwrap();
        }
        #[test]
        fn produce_write_ok() {
            let mut ring = RingBufferMap::new("poop").unwrap();
            ring.producer().pid = UNUSED_ENTRY;
            ring.producer().offset = ring.data_offset();

            let safe_ring = ThreadSafeRingBuffer::new(Mutex::new(ring));

            let mut producer = producer::Producer::attach(&safe_ring).unwrap();
            let data: Vec<u8> = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

            assert!(producer.write(&data).is_ok());
        }
        #[test]
        fn tmo_put_fail1() {
            // too much data:

            // Will fail if I try to put too many bytes:

            let mut ring = RingBufferMap::new("poop").unwrap();
            ring.producer().pid = UNUSED_ENTRY;
            let max_bytes = ring.data_bytes();
            let safe_ring = ThreadSafeRingBuffer::new(Mutex::new(ring));

            let mut producer = producer::Producer::attach(&safe_ring).unwrap();
            let mut data = Vec::<u8>::new();
            data.resize(max_bytes + 1, 0); // too many bytes.

            let result = producer.timed_put(&data, Duration::from_secs(1));
            assert!(result.is_err());
            if let Err(status) = result {
                assert_eq!(producer::Error::TooMuchData, status);
            } else {
                panic!("Expected error from blocking_put");
            }

            safe_ring
                .lock()
                .unwrap()
                .free_producer(process::id())
                .unwrap();
        }
        #[test]
        fn tmo_put_fail2() {
            // Never get enough free space from the consumers.
            // we'll rig it so the producer is 1 behind the consumer
            // and, since we won't consume it'll timeout.

            let mut ring = RingBufferMap::new("poop").unwrap();
            ring.producer().pid = UNUSED_ENTRY;
            ring.producer().offset = ring.data_offset();

            ring.consumer(1).unwrap().pid = process::id(); // cheat.
            ring.consumer(1).unwrap().offset = ring.data_offset() + 1;

            let data: [u8; 2] = [0, 1]; // two bytes but only one free.

            let safe_ring = ThreadSafeRingBuffer::new(Mutex::new(ring));
            let mut p = producer::Producer::attach(&safe_ring).unwrap();
            let result = p.timed_put(&data, Duration::from_millis(1));
            assert!(result.is_err());
            if let Err(status) = result {
                assert_eq!(producer::Error::Timeout, status);
            }

            safe_ring
                .lock()
                .unwrap()
                .free_producer(process::id())
                .unwrap();
            safe_ring
                .lock()
                .unwrap()
                .free_consumer(1, process::id())
                .unwrap();
        }
        #[test]
        fn tmo_put_ok() {
            let mut ring = RingBufferMap::new("poop").unwrap();
            ring.producer().pid = UNUSED_ENTRY;
            ring.producer().offset = ring.data_offset();

            let data: [u8; 2] = [0, 1];

            let safe_ring = ThreadSafeRingBuffer::new(Mutex::new(ring));
            let mut p = producer::Producer::attach(&safe_ring).unwrap();
            let result = p.timed_put(&data, Duration::from_millis(1));

            assert!(result.is_ok());
            if let Ok(n) = result {
                assert_eq!(data.len(), n);
            }

            safe_ring
                .lock()
                .unwrap()
                .free_producer(process::id())
                .unwrap();
        }
        #[test]
        fn drop_test() {
            // If we drop a producer object the producer is freed:

            let ring = RingBufferMap::new("poop").unwrap();
            let safe_ring = ThreadSafeRingBuffer::new(Mutex::new(ring));
            {
                let _p = producer::Producer::attach(&safe_ring).unwrap();
                assert_eq!(process::id(), safe_ring.lock().unwrap().producer().pid);
            }
            // dropped so there's no producer:
            assert_eq!(UNUSED_ENTRY, safe_ring.lock().unwrap().producer().pid);
        }
    }
    #[cfg(test)]
    mod consumer_tests {
        use super::consumer;
        use super::producer;
        use super::*;
        use std::io::{Read, Write};
        use std::process;
        use std::time::Duration;
        #[test]
        fn attach_1() {
            // can attach:

            let ring = RingBufferMap::new("poop").unwrap();
            let safe_ring = ThreadSafeRingBuffer::new(Mutex::new(ring));
            let n: usize; // Slot number.
            {
                let c = consumer::Consumer::attach(&safe_ring);
                assert!(c.is_ok());
                if let Ok(consumer) = c {
                    n = consumer.get_index() as usize;
                    assert_eq!(process::id(), consumer.get_pid());
                } else {
                    n = safe_ring.lock().unwrap().max_consumers();
                }
            }
            // The slot should have dropped and hence been released:

            assert_eq!(
                UNUSED_ENTRY,
                safe_ring.lock().unwrap().consumer(n).unwrap().pid
            );
        }
        #[test]
        fn attach_2() {
            // No free consumers:

            let mut ring = RingBufferMap::new("poop").unwrap();
            let max = ring.max_consumers();
            for i in 0..max {
                ring.consumer(i).unwrap().pid = 0; // In use.
            }

            let safe_ring = ThreadSafeRingBuffer::new(Mutex::new(ring));
            if let Err(e) = consumer::Consumer::attach(&safe_ring) {
                assert_eq!(consumer::Error::NoFreeConsumers, e);
            } else {
                assert!(false, "Should have failed!!");
            }
            for i in 0..max {
                safe_ring.lock().unwrap().consumer(i).unwrap().pid = UNUSED_ENTRY;
            }
        }
        #[test]
        fn consume_1() {
            // Fail because we ask too much.  This does not depend on
            // data:

            let ring = RingBufferMap::new("poop").unwrap();
            let mut data: Vec<u8> = vec![];
            data.resize(ring.data_bytes() + 1, 0xff);

            let safe_ring = ThreadSafeRingBuffer::new(Mutex::new(ring));
            let mut c = consumer::Consumer::attach(&safe_ring).unwrap();

            let result = c.blocking_get(&mut data);
            if let Err(e) = result {
                assert_eq!(consumer::Error::TooMuchData, e);
            } else {
                assert!(false, "Should have failed");
            }
        }
        #[test]
        fn consume_2() {
            // blocking consume for data that's available.

            let ring = RingBufferMap::new("poop").unwrap();
            let safe_ring = ThreadSafeRingBuffer::new(Mutex::new(ring));
            let mut producer = producer::Producer::attach(&safe_ring).unwrap();
            let mut consumer = consumer::Consumer::attach(&safe_ring).unwrap();

            let produced: [u8; 10] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
            producer.blocking_put(&produced).unwrap();

            let mut consumed: [u8; 10] = [0xff; 10];
            let result = consumer.blocking_get(&mut consumed);
            if let Ok(n) = result {
                assert_eq!(produced.len(), n);
                for i in 0..produced.len() {
                    assert_eq!(produced[i], consumed[i]);
                }
            } else {
                assert!(false, " Should have worked");
            }
        }
        #[test]
        fn read_write_ok() {
            let ring = RingBufferMap::new("poop").unwrap();
            let safe_ring = ThreadSafeRingBuffer::new(Mutex::new(ring));
            let mut producer = producer::Producer::attach(&safe_ring).unwrap();
            let mut consumer = consumer::Consumer::attach(&safe_ring).unwrap();

            let produced: [u8; 10] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
            assert!(producer.write(&produced).is_ok());
            let mut consumed: [u8; 10] = [0xff; 10];
            assert!(consumer.read(&mut consumed).is_ok());

            for i in 0..produced.len() {
                assert_eq!(produced[i], consumed[i]);
            }
        }
        // consume tests that gave timed waits.:

        #[test]
        fn tmo_consume1() {
            // No data evern - timeout error.

            let ring = RingBufferMap::new("poop").unwrap();
            let safe_ring = ThreadSafeRingBuffer::new(Mutex::new(ring));
            let mut consumer = consumer::Consumer::attach(&safe_ring).unwrap();

            // This should timeout:
            let mut data: [u8; 1] = [0xff];
            if let Err(e) = consumer.timed_get(&mut data, Duration::from_millis(10)) {
                assert_eq!(consumer::Error::Timeout, e);
            } else {
                assert!(false, "Was supposed to fail");
            }
        }
        #[test]
        fn tmo_consume2() {
            // Read is fully satisfied:

            let ring = RingBufferMap::new("poop").unwrap();
            let safe_ring = ThreadSafeRingBuffer::new(Mutex::new(ring));
            let mut consumer = consumer::Consumer::attach(&safe_ring).unwrap();
            let mut producer = producer::Producer::attach(&safe_ring).unwrap();

            let produced: [u8; 10] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
            let mut consumed: [u8; 10] = [0xff; 10];

            producer.blocking_put(&produced).unwrap();

            if let Ok(n) = consumer.timed_get(&mut consumed, Duration::from_millis(10)) {
                assert_eq!(produced.len(), n);
                for i in 0..produced.len() {
                    assert_eq!(produced[i], consumed[i]);
                }
            } else {
                assert!(false, "Should have succeeded");
            }
        }
        #[test]
        fn tmo_consume_3() {
            // get partially satisfied after timeout:

            let ring = RingBufferMap::new("poop").unwrap();
            let safe_ring = ThreadSafeRingBuffer::new(Mutex::new(ring));
            let mut consumer = consumer::Consumer::attach(&safe_ring).unwrap();
            let mut producer = producer::Producer::attach(&safe_ring).unwrap();

            let produced: [u8; 5] = [0, 1, 2, 3, 4];
            let mut consumed: [u8; 10] = [0xff; 10];

            producer.blocking_put(&produced).unwrap();

            if let Ok(n) = consumer.timed_get(&mut consumed, Duration::from_millis(10)) {
                assert_eq!(produced.len(), n);
                for i in 0..produced.len() {
                    assert_eq!(produced[i], consumed[i]);
                }
            } else {
                assert!(false, "Should have succeeded");
            }
        }
    }
}
