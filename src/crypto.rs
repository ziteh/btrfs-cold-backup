use age::secrecy::SecretString;
use age::x25519;
use anyhow::{Context, Result, anyhow};
use std::io::{Read, Write};
use std::path::Path;

use crate::util::IO_BUF_SIZE;

pub struct AgeKeyPair {
    pub public_key: String,
    pub private_key: SecretString,
}

pub fn generate_key_pair() -> AgeKeyPair {
    let identity = x25519::Identity::generate();
    AgeKeyPair {
        public_key: identity.to_public().to_string(),
        private_key: identity.to_string(),
    }
}

/// Writes the private key to a new file readable only by its owner; never overwrites.
pub fn write_private_key(path: &Path, key: &str) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("failed to create {path:?}"))?;
    file.write_all(key.as_bytes())
        .with_context(|| format!("failed to write private key to {path:?}"))
}

pub fn parse_recipient(public_key: &str) -> Result<x25519::Recipient> {
    public_key
        .parse()
        .map_err(|e| anyhow!("invalid age public key: {e}"))
}

pub fn parse_identity(private_key: &str) -> Result<x25519::Identity> {
    private_key
        .parse()
        .map_err(|e| anyhow!("invalid age private key: {e}"))
}

/// Fails unless `identity` is the private half of `public_key`, i.e. can decrypt what was
/// encrypted to it.
pub fn ensure_key_pair(public_key: &str, identity: &x25519::Identity) -> Result<()> {
    let actual = identity.to_public().to_string();
    if actual != public_key {
        return Err(anyhow!(
            "the private key belongs to {actual}, not to {public_key}"
        ));
    }
    Ok(())
}

/// Wraps `out` so that everything written to it is encrypted to `recipient`. The returned
/// writer must be `finish`ed, or the ciphertext is truncated.
pub fn encrypting_writer<W: Write>(
    recipient: &x25519::Recipient,
    out: W,
) -> Result<age::stream::StreamWriter<W>> {
    let encryptor =
        age::Encryptor::with_recipients(std::iter::once(recipient as &dyn age::Recipient))
            .map_err(|e| anyhow!("failed to create encryptor: {e}"))?;
    Ok(encryptor.wrap_output(out)?)
}

/// Decrypts the age file `input` into `out`. Blocking.
pub fn decrypt_to_writer(
    input: &Path,
    identity: &x25519::Identity,
    out: &mut impl Write,
) -> Result<()> {
    let file = std::fs::File::open(input).with_context(|| format!("failed to open {input:?}"))?;
    let decryptor = age::Decryptor::new_buffered(std::io::BufReader::new(file))
        .map_err(|e| anyhow!("failed to read age header of {input:?}: {e}"))?;
    let mut reader = decryptor
        .decrypt(std::iter::once(identity as &dyn age::Identity))
        .map_err(|e| anyhow!("failed to decrypt {input:?}: {e}"))?;
    std::io::copy(&mut reader, out)?;
    Ok(())
}

pub async fn blake3_file(path: &Path) -> Result<String> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<String> {
        let mut file =
            std::fs::File::open(&path).with_context(|| format!("failed to open {path:?}"))?;
        let mut hasher = blake3::Hasher::new();
        let mut buf = vec![0u8; IO_BUF_SIZE];
        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        Ok(hasher.finalize().to_hex().to_string())
    })
    .await?
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encrypt(recipient: &x25519::Recipient, plaintext: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        let mut writer = encrypting_writer(recipient, &mut out).unwrap();
        writer.write_all(plaintext).unwrap();
        writer.finish().unwrap();
        out
    }

    fn decrypt(identity: &x25519::Identity, ciphertext: &[u8]) -> Result<Vec<u8>> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("enc.age");
        std::fs::write(&path, ciphertext).unwrap();
        let mut out = Vec::new();
        decrypt_to_writer(&path, identity, &mut out)?;
        Ok(out)
    }

    #[test]
    fn test_encrypt_decrypt_round_trip() {
        let identity = x25519::Identity::generate();
        let ciphertext = encrypt(&identity.to_public(), b"round trip payload");
        assert_ne!(ciphertext, b"round trip payload");
        assert_eq!(
            decrypt(&identity, &ciphertext).unwrap(),
            b"round trip payload"
        );
    }

    #[test]
    fn test_decrypt_with_wrong_key_fails() {
        let ciphertext = encrypt(&x25519::Identity::generate().to_public(), b"secret");
        assert!(decrypt(&x25519::Identity::generate(), &ciphertext).is_err());
    }

    #[test]
    fn test_decrypt_truncated_file_fails() {
        let identity = x25519::Identity::generate();
        let ciphertext = encrypt(&identity.to_public(), &[7u8; 4096]);
        assert!(decrypt(&identity, &ciphertext[..ciphertext.len() / 2]).is_err());
    }

    #[tokio::test]
    async fn test_blake3_file_detects_content_change() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.bin");
        let b = dir.path().join("b.bin");
        let c = dir.path().join("c.bin");
        std::fs::write(&a, b"same content").unwrap();
        std::fs::write(&b, b"same content").unwrap();
        std::fs::write(&c, b"other content").unwrap();

        let ha = blake3_file(&a).await.unwrap();
        assert_eq!(ha, blake3_file(&b).await.unwrap());
        assert_ne!(ha, blake3_file(&c).await.unwrap());
    }

    #[test]
    fn test_ensure_key_pair() {
        let identity = x25519::Identity::generate();
        let public_key = identity.to_public().to_string();
        assert!(ensure_key_pair(&public_key, &identity).is_ok());
        let err = ensure_key_pair(&public_key, &x25519::Identity::generate()).unwrap_err();
        assert!(err.to_string().contains(&public_key));
    }
}
