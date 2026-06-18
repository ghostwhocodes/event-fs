use crate::EventFsError;

pub fn validate_json_document(path: &str, bytes: &[u8]) -> Result<(), EventFsError> {
    serde_json::from_slice::<serde_json::Value>(bytes)
        .map(|_| ())
        .map_err(|err| EventFsError::invalid_json(path, err.to_string()))
}

pub fn validate_json_lines(path: &str, bytes: &[u8]) -> Result<(), EventFsError> {
    for line in json_lines(path, bytes)? {
        serde_json::from_str::<serde_json::Value>(&line)
            .map_err(|err| EventFsError::invalid_json(path, err.to_string()))?;
    }
    Ok(())
}

pub fn json_lines(path: &str, bytes: &[u8]) -> Result<Vec<String>, EventFsError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|err| EventFsError::invalid_json(path, err.to_string()))?;
    Ok(text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_lines_reject_invalid_utf8() {
        assert!(validate_json_lines("/events/system.jsonl", b"{\"bad\":\"\xff\"}\n").is_err());
    }
}
