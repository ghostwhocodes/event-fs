use crate::StreamSubject;

const ENCODED_SUBJECT_PREFIX: &str = "__eventfs_subject_hex_";
const JSONL_SUFFIX: &str = ".jsonl";

pub fn stream_subject_file_name(subject: &StreamSubject) -> String {
    stream_subject_file_name_from_str(subject.as_str())
}

pub fn stream_subject_file_name_from_str(subject: &str) -> String {
    if is_plain_subject_file_stem(subject) {
        format!("{subject}{JSONL_SUFFIX}")
    } else {
        format!(
            "{ENCODED_SUBJECT_PREFIX}{}{JSONL_SUFFIX}",
            hex_encode(subject.as_bytes())
        )
    }
}

pub(crate) fn parse_stream_subject_file_name(name: &str) -> Result<String, crate::EventFsError> {
    let Some(stem) = name.strip_suffix(JSONL_SUFFIX) else {
        return Err(crate::EventFsError::invalid_path(
            "stream subject files must end in .jsonl",
        ));
    };
    if let Some(hex) = stem.strip_prefix(ENCODED_SUBJECT_PREFIX) {
        let bytes = hex_decode(hex)?;
        return String::from_utf8(bytes)
            .map_err(|_| crate::EventFsError::invalid_path("invalid encoded subject"));
    }
    if !is_plain_subject_file_stem(stem) {
        return Err(crate::EventFsError::invalid_path(
            "invalid stream subject file name",
        ));
    }
    Ok(stem.to_string())
}

fn is_plain_subject_file_stem(subject: &str) -> bool {
    !subject.starts_with(ENCODED_SUBJECT_PREFIX)
        && !subject.is_empty()
        && !subject.starts_with('.')
        && !subject.ends_with('.')
        && !subject.contains("..")
        && subject
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn hex_decode(value: &str) -> Result<Vec<u8>, crate::EventFsError> {
    if value.is_empty() || !value.len().is_multiple_of(2) {
        return Err(crate::EventFsError::invalid_path("invalid encoded subject"));
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_value(pair[0])?;
            let low = hex_value(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_value(byte: u8) -> Result<u8, crate::EventFsError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(crate::EventFsError::invalid_path("invalid encoded subject")),
    }
}
