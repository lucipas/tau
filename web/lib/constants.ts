// Mirrors tau/protocol.py::MAX_MESSAGE_CHARS.
export const MAX_MESSAGE_CHARS = 2000;

// Mirrors tau/protocol.py::MAX_FILE_BYTES: the largest file a transfer
// may carry, so the composer can refuse an oversized file before sending.
export const MAX_FILE_BYTES = 10 * 1024 * 1024;
