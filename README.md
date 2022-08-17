# nscldaq_ringbuffer

This crate contains low level access to NSCLDAQ ring buffers.  
This crate roughly duplicates the capabilities of the C++ CRingBuffer class but
in  a rust-like method.

What is not provided in this class are interactions with the ring master server.
It is anticipated that at a later time, additional crates will be provided
to provide both that and the NSCLDAQ remote ring buffer support.