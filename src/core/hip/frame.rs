use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::errors::HipError;

/// Default maximum frame size (16 MiB).
pub const MAX_DEFAULT_FRAME_BYTES: u64 = 16 * 1024 * 1024;

/// HIP/1 frame type identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameType {
    Hello = 0x01,
    HelloAck = 0x02,
    CheckBatch = 0x03,
    CheckAck = 0x04,
    UploadBegin = 0x05,
    UploadChunk = 0x06,
    UploadEnd = 0x07,
    UploadAck = 0x08,
    Commit = 0x09,
    CommitAck = 0x0A,
    Bye = 0x0B,
    Window = 0x0C,
    Ping = 0x0E,
    Pong = 0x0F,
    Error = 0x1F,
}

impl FrameType {
    pub fn from_u8(value: u8) -> Result<Self, HipError> {
        Ok(match value {
            0x01 => Self::Hello,
            0x02 => Self::HelloAck,
            0x03 => Self::CheckBatch,
            0x04 => Self::CheckAck,
            0x05 => Self::UploadBegin,
            0x06 => Self::UploadChunk,
            0x07 => Self::UploadEnd,
            0x08 => Self::UploadAck,
            0x09 => Self::Commit,
            0x0A => Self::CommitAck,
            0x0B => Self::Bye,
            0x0C => Self::Window,
            0x0E => Self::Ping,
            0x0F => Self::Pong,
            0x1F => Self::Error,
            other => {
                return Err(HipError::Protocol(format!(
                    "unknown frame type byte 0x{other:02X}"
                )))
            }
        })
    }
}

/// A decoded frame with its raw payload.
#[derive(Debug, Clone)]
pub struct Frame {
    pub frame_type: FrameType,
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn new(frame_type: FrameType, payload: Vec<u8>) -> Self {
        Self {
            frame_type,
            payload,
        }
    }

    /// Encode a frame to a byte buffer suitable for wire transmission.
    ///
    /// Wire format: `[u32 be length][u8 type][payload]` where `length` counts
    /// the type byte plus payload size.
    pub fn encode(&self, max_frame_bytes: u64) -> Result<Vec<u8>, HipError> {
        let total_len = self.payload.len() as u64 + 1;
        if total_len > max_frame_bytes {
            return Err(HipError::FrameTooLarge {
                size: total_len,
                max: max_frame_bytes,
            });
        }
        let mut buf = Vec::with_capacity(4 + total_len as usize);
        buf.extend_from_slice(&(total_len as u32).to_be_bytes());
        buf.push(self.frame_type as u8);
        buf.extend_from_slice(&self.payload);
        Ok(buf)
    }
}

/// Read a single frame from `reader`, enforcing `max_frame_bytes` for the
/// declared length prefix. Payload may be empty.
pub async fn read_frame<R>(reader: &mut R, max_frame_bytes: u64) -> Result<Frame, HipError>
where
    R: AsyncReadExt + Unpin,
{
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let declared = u32::from_be_bytes(len_buf) as u64;
    if declared == 0 {
        return Err(HipError::Protocol("frame length is zero".into()));
    }
    if declared > max_frame_bytes {
        return Err(HipError::FrameTooLarge {
            size: declared,
            max: max_frame_bytes,
        });
    }
    let mut type_buf = [0u8; 1];
    reader.read_exact(&mut type_buf).await?;
    let frame_type = FrameType::from_u8(type_buf[0])?;
    let payload_len = (declared - 1) as usize;
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        reader.read_exact(&mut payload).await?;
    }
    Ok(Frame {
        frame_type,
        payload,
    })
}

/// Write an already-encoded frame to `writer`.
pub async fn write_frame<W>(
    writer: &mut W,
    frame: &Frame,
    max_frame_bytes: u64,
) -> Result<(), HipError>
where
    W: AsyncWriteExt + Unpin,
{
    let encoded = frame.encode(max_frame_bytes)?;
    writer.write_all(&encoded).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn round_trip_encodes_and_decodes_frame() {
        let frame = Frame::new(FrameType::Hello, b"hello".to_vec());
        let mut writer = Vec::new();
        write_frame(&mut writer, &frame, MAX_DEFAULT_FRAME_BYTES)
            .await
            .unwrap();

        let mut reader = writer.as_slice();
        let decoded = read_frame(&mut reader, MAX_DEFAULT_FRAME_BYTES)
            .await
            .unwrap();
        assert_eq!(decoded.frame_type, FrameType::Hello);
        assert_eq!(decoded.payload, b"hello");
    }

    #[tokio::test]
    async fn rejects_oversized_frame() {
        let frame = Frame::new(FrameType::Hello, vec![0u8; 10]);
        let err = frame.encode(4).unwrap_err();
        assert!(matches!(err, HipError::FrameTooLarge { .. }));
    }

    #[tokio::test]
    async fn read_frame_rejects_oversized_declared_length() {
        let mut wire = Vec::new();
        wire.extend_from_slice(&u32::MAX.to_be_bytes());
        wire.push(0x01);
        let mut reader = wire.as_slice();
        let err = read_frame(&mut reader, MAX_DEFAULT_FRAME_BYTES)
            .await
            .unwrap_err();
        assert!(matches!(err, HipError::FrameTooLarge { .. }));
    }
}
