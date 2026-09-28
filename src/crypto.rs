use age::secrecy::SecretString;
use age::x25519;
use anyhow::{Result, anyhow};
use std::io::Read;
use std::path::Path;
use tokio::task;

use crate::util::{IO_BUF_SIZE, now_unix};

pub struct AgeKeyPair {
    pub public_key: String,
    pub private_key: SecretString,
}

pub fn write_private_key(path: &std::path::Path, key: &str) -> Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| anyhow!("failed to create {:?}: {e}", path))?;
    file.write_all(key.as_bytes())
        .map_err(|e| anyhow!("failed to write private key to {:?}: {e}", path))
}

pub fn generate_key_pair() -> AgeKeyPair {
    let identity = age::x25519::Identity::generate();
    AgeKeyPair {
        public_key: identity.to_public().to_string(),
        private_key: identity.to_string(),
    }
}

pub async fn encrypt_file(input: &Path, output: &Path, public_key_str: &str) -> Result<()> {
    let public_key = public_key_str.to_string();
    let input_buf = input.to_path_buf();
    let output_buf = output.to_path_buf();

    task::spawn_blocking(move || -> Result<()> {
        let recipient: x25519::Recipient = public_key
            .parse()
            .map_err(|e| anyhow!("invalid age public key: {e}"))?;

        let encryptor =
            age::Encryptor::with_recipients(std::iter::once(&recipient as &dyn age::Recipient))
                .map_err(|e| anyhow!("failed to create encryptor: {e}"))?;

        let mut input_file = std::io::BufReader::new(std::fs::File::open(&input_buf)?);
        let mut output_file = std::fs::File::create(&output_buf)?;

        let mut writer = encryptor
            .wrap_output(&mut output_file)
            .map_err(|e| anyhow!("failed to initialize encryption: {e}"))?;
        std::io::copy(&mut input_file, &mut writer)?;
        writer
            .finish()
            .map_err(|e| anyhow!("failed to finalize encryption: {e}"))?;
        output_file.sync_all()?;

        Ok(())
    })
    .await?
}

pub async fn decrypt_file(input: &Path, output: &Path, private_key_str: &str) -> Result<()> {
    let private_key = private_key_str.to_string();
    let input_buf = input.to_path_buf();
    let output_buf = output.to_path_buf();

    task::spawn_blocking(move || -> Result<()> {
        let identity: x25519::Identity = private_key
            .parse()
            .map_err(|e| anyhow!("invalid age private key: {e}"))?;

        let input_file = std::io::BufReader::new(std::fs::File::open(&input_buf)?);

        let decryptor = age::Decryptor::new_buffered(input_file)
            .map_err(|e| anyhow!("failed to create decryptor: {e}"))?;

        let mut reader = decryptor
            .decrypt(std::iter::once(&identity as &dyn age::Identity))
            .map_err(|e| anyhow!("decryption failed: {e}"))?;
        let mut output_file = std::fs::File::create(&output_buf)?;
        std::io::copy(&mut reader, &mut output_file)?;

        Ok(())
    })
    .await?
}

/// Verifies a public/private age key pair can encrypt and decrypt together,
/// by round-tripping a throwaway test file through both.
pub async fn test_key_pair(public_key_str: &str, private_key_str: &str) -> Result<()> {
    let tmp_dir = std::env::temp_dir().join(format!("bcb_key_test_{}", std::process::id()));
    tokio::fs::create_dir_all(&tmp_dir).await?;

    let result: Result<()> = async {
        let test_content = format!("bcb key pair test - {}", now_unix());
        let test_file = tmp_dir.join("test.txt");
        tokio::fs::write(&test_file, test_content.as_bytes()).await?;

        let enc_file = tmp_dir.join("test.txt.age");
        encrypt_file(&test_file, &enc_file, public_key_str).await?;

        let dec_file = tmp_dir.join("test.txt.decrypted");
        decrypt_file(&enc_file, &dec_file, private_key_str).await?;

        let decrypted = tokio::fs::read_to_string(&dec_file).await?;
        if decrypted != test_content {
            return Err(anyhow!(
                "content mismatch: decrypted content does not match original"
            ));
        }

        Ok(())
    }
    .await;

    let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
    result
}

pub async fn blake3_file(path: &Path) -> Result<String> {
    let path = path.to_path_buf();

    task::spawn_blocking(move || -> Result<String> {
        let mut file = std::fs::File::open(&path)?;
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
    use age::secrecy::ExposeSecret;

    #[tokio::test]
    async fn test_encrypt_decrypt_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let pair = generate_key_pair();

        let plain = dir.path().join("plain.bin");
        std::fs::write(&plain, b"round trip payload").unwrap();

        let enc = dir.path().join("enc.age");
        encrypt_file(&plain, &enc, &pair.public_key).await.unwrap();
        assert_ne!(std::fs::read(&enc).unwrap(), b"round trip payload");

        let dec = dir.path().join("dec.bin");
        decrypt_file(&enc, &dec, pair.private_key.expose_secret())
            .await
            .unwrap();
        assert_eq!(std::fs::read(&dec).unwrap(), b"round trip payload");
    }

    #[tokio::test]
    async fn test_decrypt_with_wrong_key_fails() {
        let dir = tempfile::tempdir().unwrap();
        let pair = generate_key_pair();
        let other = generate_key_pair();

        let plain = dir.path().join("plain.bin");
        std::fs::write(&plain, b"secret").unwrap();
        let enc = dir.path().join("enc.age");
        encrypt_file(&plain, &enc, &pair.public_key).await.unwrap();

        let dec = dir.path().join("dec.bin");
        assert!(
            decrypt_file(&enc, &dec, other.private_key.expose_secret())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_decrypt_truncated_file_fails() {
        let dir = tempfile::tempdir().unwrap();
        let pair = generate_key_pair();

        let plain = dir.path().join("plain.bin");
        std::fs::write(&plain, vec![7u8; 4096]).unwrap();
        let enc = dir.path().join("enc.age");
        encrypt_file(&plain, &enc, &pair.public_key).await.unwrap();

        let bytes = std::fs::read(&enc).unwrap();
        let truncated = dir.path().join("truncated.age");
        std::fs::write(&truncated, &bytes[..bytes.len() / 2]).unwrap();

        let dec = dir.path().join("dec.bin");
        assert!(
            decrypt_file(&truncated, &dec, pair.private_key.expose_secret())
                .await
                .is_err()
        );
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
        let hb = blake3_file(&b).await.unwrap();
        let hc = blake3_file(&c).await.unwrap();
        assert_eq!(ha, hb);
        assert_ne!(ha, hc);
    }

    #[tokio::test]
    async fn test_key_pair_mismatch_fails() {
        let pair = generate_key_pair();
        let other = generate_key_pair();

        assert!(
            test_key_pair(&pair.public_key, other.private_key.expose_secret())
                .await
                .is_err()
        );
        assert!(
            test_key_pair(&pair.public_key, pair.private_key.expose_secret())
                .await
                .is_ok()
        );
    }
}
